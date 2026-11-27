"""Loading of the Gold Parquet files with pandas, the only data source of the dashboards.

The Gold DAG writes datalake_gold/governance_YYYYMMDD_HHMMSS.parquet and storytelling_YYYYMMDD_HHMMSS.parquet
in one long layout (section, topic, period, dim_1, dim_2, metric, value, label, computed_at). The dashboards
always take the newest file of each kind, so a new Gold run shows up on the next refresh without a restart.
Spark writes into a hidden folder first and then moves the finished file, so the glob below never sees a file
that is still being written.
"""
import os
from functools import lru_cache
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parents[2]
GOLD_DIR = Path(os.environ.get("GOLD_DIR", PROJECT_DIR / "datalake_gold"))
COLUMNS = ["section", "topic", "period", "dim_1", "dim_2", "metric", "value", "label", "computed_at"]


class GoldFileMissing(FileNotFoundError):
    """No Gold file of the requested kind exists yet (run the gold_processing DAG)."""


def latest_file(kind, gold_dir=None):
    """Newest Gold file of a kind ("governance" or "storytelling"). The timestamp in the name sorts in time."""
    files = sorted(Path(gold_dir or GOLD_DIR).glob(f"{kind}_*.parquet"))
    if not files:
        raise GoldFileMissing(f"No {kind}_*.parquet in {gold_dir or GOLD_DIR}. Trigger the gold_processing DAG.")
    return files[-1]


@lru_cache(maxsize=4)
def _read(path, mtime):
    """Read one file. The modification time is part of the cache key, so a rewritten file is read again."""
    frame = pd.read_parquet(path)
    missing = set(COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"{path}: Gold columns missing {sorted(missing)}")
    frame["period"] = pd.to_datetime(frame["period"])
    frame["computed_at"] = pd.to_datetime(frame["computed_at"], utc=True)
    return frame


def load(kind, gold_dir=None):
    """Return (DataFrame, file name) of the newest Gold file of a kind."""
    path = latest_file(kind, gold_dir)
    return _read(str(path), path.stat().st_mtime), path.name


def section(gold, name, topic=None):
    """Rows of one section (and topic). Returns a copy, so callers can add columns freely."""
    rows = gold[gold["section"] == name]
    if topic is not None:
        rows = rows[rows["topic"] == topic]
    return rows.copy()


def computed_at(gold):
    """Moment of the Gold run that produced the file (UTC)."""
    return gold["computed_at"].max()
