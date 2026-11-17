"""Silver task functions called by silver_processing_dag.

Flow of one run: list new Bronze files -> process each topic -> finalize (rejected file, manifest,
run summary, consumed ready markers). A topic writes its Parquet only when it has new rows.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.config import bronze_root, load_stations, silver_root
from src.normalization import silver as rules
from src.storage import silver as store
from src.storage.bronze import list_ready_markers, read_headers, utc_stamp

log = logging.getLogger(__name__)

# Columns read back from earlier Silver files for duplicate, revision and outlier checks.
HISTORY_COLUMNS = {
    "observations": ["station", "timestamp", "temperature_c", "record_hash", "processed_utc"],
    "alerts": ["alert_id", "record_hash", "processed_utc"],
    "daily_summaries": ["station", "obs_date", "element", "value", "is_trace", "record_hash", "processed_utc"],
    "storm_events": ["event_id", "record_hash", "processed_utc"],
}


def list_new_files() -> dict:
    """Bronze files not in the manifest, plus the ready markers seen now (consumed at the end)."""
    files = store.list_unprocessed()
    batch = {"stamp": utc_stamp(), "files": files,
             "markers": [m.name for m in list_ready_markers()],
             "total": sum(len(v) for v in files.values())}
    log.info("new Bronze files: %s, ready markers: %d",
             {k: len(v) for k, v in files.items()}, len(batch["markers"]))
    return batch


def _parse_file(topic: str, rel: str, stations: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Parse one Bronze file from disk. Returns (rows, rejected, rows outside Texas for F1)."""
    path = bronze_root() / rel
    source = rules.TOPIC_SOURCE[topic]
    meta = read_headers(path)
    if meta["status"] != 200:
        return pd.DataFrame(), rules.reject_file(source, rel, f"http_status_{meta['status']}",
                                                 path.read_bytes().decode("utf-8", "replace")), 0
    try:
        if topic == "observations":
            rows, rejected = rules.parse_observations(json.loads(path.read_bytes()), rel)
        elif topic == "alerts":
            rows, rejected = rules.parse_alerts(json.loads(path.read_bytes()), rel)
        elif topic == "daily_summaries":
            rows, rejected = rules.parse_daily_summaries(json.loads(path.read_bytes()), rel,
                                                         rules.declared_units(meta["url"]), stations)
        else:
            raw = pd.read_csv(path, dtype=str, keep_default_na=False, compression="gzip")
            rows, rejected, outside = rules.parse_storm_events(raw, rel)
            return rows, rejected, outside
    except (ValueError, KeyError, TypeError, OSError) as exc:
        log.exception("cannot parse %s", rel)
        return pd.DataFrame(), rules.reject_file(source, rel, "unreadable_bronze_file", repr(exc)), 0
    return rows, rejected, 0


def _add_outlier_flags(topic: str, rows: pd.DataFrame, previous: pd.DataFrame) -> pd.DataFrame:
    if topic == "observations":
        rows["is_statistical_outlier"] = rules.flag_observation_outliers(rows, previous).values
    elif topic == "daily_summaries":
        rows["is_statistical_outlier"] = rules.flag_daily_outliers(rows, previous).values
    return rows


def process_topic(topic: str, batch: dict) -> dict:
    """Parse, validate, deduplicate and write one topic. Returns the counts of this topic."""
    files = batch["files"].get(topic, [])
    summary = {"topic": topic, "bronze_files": len(files), "rows_in": 0, "rows_out": 0,
               "rejected": {}, "duplicates_in_run": 0, "duplicates_across_runs": 0,
               "revisions": 0, "rows_outside_texas": 0, "silver_file": None}
    if not files:
        log.info("topic=%s no new Bronze files", topic)
        return summary
    stamp = batch["stamp"]
    processed_utc = pd.Timestamp(datetime.strptime(stamp, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)).as_unit("ns")
    stations = load_stations()

    frames, rejected = [], []
    for rel in files:
        rows, bad, outside = _parse_file(topic, rel, stations)
        summary["rows_outside_texas"] += outside
        summary["rows_in"] += len(rows) + len(bad)
        if len(rows):
            frames.append(rows)
        if len(bad):
            rejected.append(bad)
        log.info("topic=%s file=%s rows=%d rejected=%d", topic, rel, len(rows), len(bad))

    if frames:
        rows = pd.concat(frames, ignore_index=True)
        # concat of categories from different files gives plain text, so we set them again
        for field in rules.SCHEMAS[topic]:
            if field.type == rules.CATEGORY:
                rows[field.name] = rows[field.name].astype("category")
        rows, summary["duplicates_in_run"] = rules.dedupe_keep_last(rows, rules.KEYS[topic])
        rows["processed_utc"] = processed_utc
        rows["record_hash"] = rules.record_hash(rows)
        previous = store.read_previous(topic, HISTORY_COLUMNS[topic])
        rows, summary["duplicates_across_runs"], revised = rules.compare_with_previous(
            rows, previous, rules.KEYS[topic])
        summary["revisions"] = len(revised)
        if len(revised):
            log.info("topic=%s revisions (same key, new content): %s", topic,
                     revised.head(10).to_dict(orient="records"))
        if len(rows):
            rows = _add_outlier_flags(topic, rows, previous)
            table = rules.to_table(rows, topic)
            path = store.write_table(table, topic, stamp)
            summary["silver_file"] = path.name
            summary["rows_out"] = table.num_rows
            log.info("topic=%s wrote %s rows=%d", topic, path.name, table.num_rows)

    if rejected:
        bad = pd.concat(rejected, ignore_index=True)
        summary["rejected"] = {k: int(v) for k, v in bad["reason"].value_counts().items()}
        bad["processed_utc"] = processed_utc
        path = store.staging_rejected_path(stamp, topic)
        path.parent.mkdir(parents=True, exist_ok=True)
        bad["source"] = bad["source"].astype("category")
        bad["reason"] = bad["reason"].astype("category")
        pq.write_table(rules.to_table(bad, "rejected"), path)
    log.info("topic=%s summary %s", topic, summary)
    return summary


def finalize(batch: dict, summaries: list[dict]) -> dict:
    """Runs after every topic succeeded: one rejected file, manifest, run summary, markers."""
    stamp = batch["stamp"]
    staged = sorted(store.staging_rejected_path(stamp, "x").parent.glob(f"rejected_{stamp}_*.parquet"))
    rejected_file = None
    if staged:
        frames = [pd.read_parquet(p) for p in staged]
        bad = pd.concat(frames, ignore_index=True)
        for column in ("source", "reason"):
            bad[column] = bad[column].astype("str").astype("category")
        rejected_file = store.write_table(rules.to_table(bad, "rejected"), "rejected", stamp).name
        for p in staged:
            p.unlink()

    by_file = {}
    for topic, files in batch["files"].items():
        for rel in files:
            by_file[rel] = {"topic": topic, "silver_run": stamp,
                            "processed_utc": datetime.strptime(stamp, "%Y%m%d_%H%M%S")
                            .replace(tzinfo=timezone.utc).isoformat()}
    manifest_size = store.add_to_manifest(by_file) if by_file else len(store.read_manifest()["files"])

    ready = bronze_root() / "_ready"
    removed = 0
    for name in batch["markers"]:
        marker = ready / name
        if marker.exists():
            marker.unlink()
            removed += 1

    run = {"stamp": stamp, "bronze_files": batch["total"], "topics": summaries,
           "rejected_file": rejected_file, "markers_consumed": removed, "manifest_size": manifest_size}
    store.write_json_atomic(store.run_summary_path(stamp), run)
    log.info("silver run %s finished: %s", stamp, json.dumps(run, default=str))
    return run
