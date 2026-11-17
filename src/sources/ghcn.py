"""F2 client: GHCN-Daily through the NCEI Access Data Service (dataset=daily-summaries)."""
import json
import logging
from datetime import date
from pathlib import Path

import requests

from src.config import bronze_root
from src.sources.http import get_with_retry
from src.sources.nws import raise_for_saved_status
from src.storage.bronze import HEADERS_SUFFIX, is_complete, read_headers, save_raw

log = logging.getLogger(__name__)

URL = "https://www.ncei.noaa.gov/access/services/data/v1"
FOLDER = "ghcn_daily"


def ghcn_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "extreme-weather-monitoring (course project)"})
    return session


def daily_params(station: str, start: date, end: date) -> dict:
    """Same request as Workshop 1: three elements, flags included, metric units, JSON."""
    return {
        "dataset": "daily-summaries",
        "stations": station,
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "dataTypes": "TMAX,TMIN,PRCP",
        "includeAttributes": "true",
        "units": "metric",
        "format": "json",
    }


def fetch_daily(session: requests.Session, station: str, start: date, end: date, label: str,
                root: Path | None = None) -> Path:
    """One request for one station. `label` is the year (historical) or `recent` (incremental)."""
    response = get_with_retry(session, URL, daily_params(station, start, end))
    path = save_raw(response, FOLDER, f"ghcn_daily_{station}_{label}", "json", root)
    raise_for_saved_status(response, path)
    rows = json.loads(path.read_bytes())
    log.info("source=F2 station=%s label=%s window=%s..%s status=%s rows=%d bytes=%d", station, label,
             start, end, response.status_code, len(rows), len(response.content))
    return path


def data_files(pattern: str, root: Path | None = None) -> list[Path]:
    folder = (root or bronze_root()) / FOLDER
    if not folder.exists():
        return []
    return sorted(p for p in folder.glob(pattern) if not p.name.endswith(HEADERS_SUFFIX))


def year_partition_done(station: str, year: int, root: Path | None = None) -> bool:
    """A station-year is done when a complete Bronze file with status 200 exists for it."""
    for path in data_files(f"ghcn_daily_{station}_{year}_*.json", root):
        if is_complete(path) and read_headers(path)["status"] == 200:
            return True
    return False
