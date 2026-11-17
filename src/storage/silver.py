"""Silver layer files: Parquet per topic and run, rejected rows, run summaries and the manifest.

Layout:
  datalake_silver/<topic>/<topic>_YYYYMMDD_HHMMSS.parquet
  datalake_silver/rejected/rejected_YYYYMMDD_HHMMSS.parquet
  datalake_silver/_state/processed_files.json   Bronze files already processed (idempotency)
  datalake_silver/_state/runs/silver_run_YYYYMMDD_HHMMSS.json   counts of one run
  datalake_silver/_state/staging/   per-topic rejected rows until the run finishes
"""
import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import bronze_root, silver_root
from src.storage.bronze import HEADERS_SUFFIX, is_complete, utc_now

# Topic -> Bronze folders and file patterns that feed it.
TOPIC_INPUTS = {
    "observations": [("nws_observations", "nws_observations_*.geojson")],
    "alerts": [("nws_alerts", "nws_alerts_*.geojson"), ("nws_alerts_active", "nws_alerts_active_*.geojson")],
    "daily_summaries": [("ghcn_daily", "ghcn_daily_*.json")],
    "storm_events": [("storm_events", "storm_events_d*.csv.gz")],
}


def manifest_path(root: Path | None = None) -> Path:
    return (root or silver_root()) / "_state" / "processed_files.json"


def read_manifest(root: Path | None = None) -> dict:
    path = manifest_path(root)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"files": {}}


def write_json_atomic(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=1, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def add_to_manifest(entries: dict[str, dict], root: Path | None = None) -> int:
    manifest = read_manifest(root)
    manifest["files"].update(entries)
    manifest["updated_utc"] = utc_now().isoformat(timespec="seconds")
    write_json_atomic(manifest_path(root), manifest)
    return len(manifest["files"])


def list_unprocessed(bronze: Path | None = None, silver: Path | None = None) -> dict[str, list[str]]:
    """Complete Bronze data files (their .headers.json exists) that are not in the manifest yet.

    Paths are relative to datalake_bronze/ and sorted by name, which is time order inside a source.
    """
    bronze = bronze or bronze_root()
    done = read_manifest(silver)["files"]
    result = {}
    for topic, inputs in TOPIC_INPUTS.items():
        files = []
        for folder, pattern in inputs:
            for path in (bronze / folder).glob(pattern) if (bronze / folder).exists() else []:
                if path.name.endswith(HEADERS_SUFFIX) or not is_complete(path):
                    continue
                rel = path.relative_to(bronze).as_posix()
                if rel not in done:
                    files.append(rel)
        result[topic] = sorted(files, key=lambda r: Path(r).name)
    return result


def topic_files(topic: str, root: Path | None = None) -> list[Path]:
    folder = (root or silver_root()) / topic
    return sorted(folder.glob(f"{topic}_*.parquet")) if folder.exists() else []


def read_previous(topic: str, columns: list[str], root: Path | None = None) -> pd.DataFrame:
    """Earlier Silver rows of a topic (only the columns we need), for cross-run checks."""
    files = topic_files(topic, root)
    if not files:
        return pd.DataFrame(columns=columns)
    return pd.concat([pd.read_parquet(f, columns=columns) for f in files], ignore_index=True)


def write_table(table: pa.Table, folder: str, stamp: str, root: Path | None = None) -> Path:
    path = (root or silver_root()) / folder / f"{folder}_{stamp}.parquet"
    if path.exists():
        raise FileExistsError(f"{path} already exists, Silver files are never overwritten")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, path)
    return path


def staging_rejected_path(stamp: str, topic: str, root: Path | None = None) -> Path:
    return (root or silver_root()) / "_state" / "staging" / f"rejected_{stamp}_{topic}.parquet"


def run_summary_path(stamp: str, root: Path | None = None) -> Path:
    return (root or silver_root()) / "_state" / "runs" / f"silver_run_{stamp}.json"
