"""Silver layer: turn pending Bronze files into one Parquet file per topic and run."""
import json
import logging
import os
import re
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src import settings
from src.normalization import alerts, common, daily_summaries, observations, storm_events

log = logging.getLogger(__name__)

NORMALIZERS = {
    "nws_alerts": alerts,
    "nws_observations": observations,
    "ghcn_daily": daily_summaries,
    "storm_events": storm_events,
}
BRONZE_NAME = re.compile(r"^(?P<source>[a-z_]+)_\d{8}_\d{6}\.json$")


def pending_files():
    """Bronze files not processed yet, grouped by source: {source: [paths, oldest first]}."""
    pending = {}
    for path in sorted(settings.BRONZE_DIR.glob("*.json")):
        match = BRONZE_NAME.match(path.name)
        if match and match.group("source") in NORMALIZERS:
            pending.setdefault(match.group("source"), []).append(str(path))
        else:
            log.warning("Ignored %s: the name does not follow source_YYYYMMDD_HHMMSS.json", path.name)
    return pending


def _key_text(df, columns):
    """One text per row made of the given columns, used to compare records between files."""
    return df[columns].astype(str).agg("|".join, axis=1)


def _known_records(module, skip_name):
    """Key plus content hash of every record already stored in Silver for this topic.

    The hash is computed again from the stored values with the current rule, instead of reading the stored
    record_hash. Workshop 3: when the rule changed (HASH_EXCLUDE), the stored hashes no longer matched the
    new ones and the next runs wrote again every record of their overlap window.
    """
    known = set()
    for path in settings.SILVER_DIR.glob(f"{module.TOPIC}_*.parquet"):
        if path.name != skip_name:
            stored = pd.read_parquet(path).drop(columns=common.METADATA_COLUMNS)
            stored = common.add_record_hash(stored, exclude=module.HASH_EXCLUDE)
            known.update(_key_text(stored, module.KEY + ["record_hash"]))
    return known


def process_source(source, paths, run_stamp):
    """Apply the preprocessing pipeline to the Bronze files of one source.

    Steps: normalize each file, add traceability columns, enforce the schema, send rows without a required
    field to quarantine, remove duplicates inside the batch and against earlier Silver files, write Parquet.
    Returns a summary dict. No Parquet file is written when every record was already in Silver.
    """
    module = NORMALIZERS[source]
    out_name = f"{module.TOPIC}_{run_stamp}.parquet"
    frames = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        frame = common.add_record_hash(module.normalize(payload), exclude=module.HASH_EXCLUDE)
        frame["bronze_file"] = os.path.basename(path)
        frame["ingested_at"] = payload["extracted_utc"]
        if len(frame):
            frames.append(frame)
    summary = {"source": source, "topic": module.TOPIC, "bronze_files": [os.path.basename(p) for p in paths],
               "rows_in": sum(len(f) for f in frames), "rows_rejected": 0, "rows_duplicated": 0, "rows_out": 0,
               "silver_file": None}
    if not frames:
        log.info("%s: the Bronze files have no records", source)
        return summary

    df = pd.concat(frames, ignore_index=True)
    df["processed_at"] = datetime.now(timezone.utc)
    df, rejected = common.enforce_schema(df, module.SCHEMA, module.REQUIRED)
    if len(rejected):
        settings.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        rejected_path = settings.QUARANTINE_DIR / f"{module.TOPIC}_{run_stamp}.json"
        rejected.to_json(rejected_path, orient="records", date_format="iso")
        summary["rows_rejected"] = len(rejected)
        log.warning("%s: %s rows without a required field %s sent to %s", source, len(rejected), module.REQUIRED,
                    rejected_path.name)

    # Duplicates inside the batch: the overlap window repeats records, the newest file wins.
    before = len(df)
    df = df.sort_values("ingested_at").drop_duplicates(subset=module.KEY, keep="last")
    # Duplicates against earlier runs: same key and same content. A changed value has a new hash and is kept.
    known = _known_records(module, out_name)
    df = df[~_key_text(df, module.KEY + ["record_hash"]).isin(known)].reset_index(drop=True)
    summary["rows_duplicated"] = before - len(df)
    if df.empty:
        log.info("%s: all %s records are already in Silver, no file written", source, before)
        return summary

    table = pa.Table.from_pandas(df, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    metadata.update({b"topic": module.TOPIC.encode(), b"run_stamp": run_stamp.encode(),
                     b"bronze_files": json.dumps(summary["bronze_files"]).encode()})
    settings.SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out_path = settings.SILVER_DIR / out_name
    temporary = settings.SILVER_DIR / f".{out_name}.tmp"
    pq.write_table(table.replace_schema_metadata(metadata), temporary)
    os.replace(temporary, out_path)
    summary.update(rows_out=len(df), silver_file=str(out_path))
    log.info("%s: %s rows in, %s rejected, %s duplicated, %s written to %s", source, summary["rows_in"],
             summary["rows_rejected"], summary["rows_duplicated"], len(df), out_name)
    return summary


def check_silver_file(path, source):
    """Read a Silver file back and confirm it follows the target schema. Returns the number of rows."""
    module = NORMALIZERS[source]
    df = pd.read_parquet(path)
    if list(df.columns) != list(module.SCHEMA):
        raise ValueError(f"{path}: columns differ from the schema of {module.TOPIC}")
    if df[module.REQUIRED].isna().any().any():
        raise ValueError(f"{path}: null value in a required field {module.REQUIRED}")
    if df.duplicated(subset=module.KEY).any():
        raise ValueError(f"{path}: duplicated key {module.KEY}")
    log.info("Checked %s: %s rows, %s columns", os.path.basename(path), len(df), len(df.columns))
    return len(df)


def archive(paths):
    """Move processed Bronze files to datalake_bronze/processed/, so the sensor only sees new files."""
    settings.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    for path in paths:
        if os.path.exists(path):
            os.replace(path, settings.PROCESSED_DIR / os.path.basename(path))
    log.info("Archived %s Bronze files", len(paths))
    return len(paths)
