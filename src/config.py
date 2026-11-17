"""Paths and project settings shared by the Bronze and Silver code.

Inside the Airflow containers the paths come from environment variables set in docker-compose.yml.
On a laptop the defaults point to the folders of this repository, so the same code runs in both places.
"""
import os
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]


def _path_from_env(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default)))


def bronze_root() -> Path:
    return _path_from_env("DATALAKE_BRONZE", REPO_ROOT / "datalake_bronze")


def silver_root() -> Path:
    return _path_from_env("DATALAKE_SILVER", REPO_ROOT / "datalake_silver")


def stations_file() -> Path:
    return _path_from_env("STATIONS_FILE", REPO_ROOT / "config" / "stations.csv")


def load_stations(path: Path | None = None) -> pd.DataFrame:
    """Read config/stations.csv with every id as text, so leading zeros in WBAN are kept."""
    return pd.read_csv(path or stations_file(), dtype=str)


# Texas extent is 25 deg 50 min N to 36 deg 30 min N and 93 deg 31 min W to 106 deg 39 min W.
# We pad it by 0.5 degrees so stations and polygons on the border are not rejected.
TEXAS_BBOX = {
    "lat_min": 25.83 - 0.5,
    "lat_max": 36.50 + 0.5,
    "lon_min": -106.65 - 0.5,
    "lon_max": -93.51 + 0.5,
}
