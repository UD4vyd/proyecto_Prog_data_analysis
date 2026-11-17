"""Paths and settings shared by the DAGs and by ingest.py."""
import csv
import os
from pathlib import Path

import yaml

PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parents[1]))
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", PROJECT_DIR / "datalake_bronze"))
SILVER_DIR = Path(os.environ.get("SILVER_DIR", PROJECT_DIR / "datalake_silver"))

STAGING_DIR = BRONZE_DIR / "_staging"      # files being written, not yet visible to the Silver DAG
PROCESSED_DIR = BRONZE_DIR / "processed"   # Bronze files already converted to Silver
STATE_FILE = BRONZE_DIR / "_state" / "watermarks.json"
QUARANTINE_DIR = SILVER_DIR / "quarantine"

STAMP_FORMAT = "%Y%m%d_%H%M%S"


def load_settings():
    with open(PROJECT_DIR / "config" / "pipeline.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_stations():
    """Return the selected rows of config/stations.csv as a list of dicts."""
    with open(PROJECT_DIR / "config" / "stations.csv", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    wanted = load_settings()["stations"]
    if wanted == "all":
        return rows
    selected = [r for r in rows if r["icao"] in wanted]
    missing = set(wanted) - {r["icao"] for r in selected}
    if missing:
        raise ValueError(f"Stations not found in config/stations.csv: {sorted(missing)}")
    return selected


def user_agent():
    """User-Agent for api.weather.gov. Read from the environment, or from .env when run outside Docker."""
    value = os.environ.get("NWS_USER_AGENT")
    env_file = PROJECT_DIR / ".env"
    if not value and env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("NWS_USER_AGENT="):
                value = line.split("=", 1)[1].strip().strip('"')
    return value or "(extreme-weather-monitoring, team5-ud)"
