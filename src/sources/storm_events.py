"""F1 client: NOAA Storm Events yearly bulk files (details CSV.gz).

F1 is not JSON. We keep the CSV.gz exactly as received, because Bronze means raw as received.
"""
import gzip
import logging
import re
from pathlib import Path

import requests

from src.config import bronze_root
from src.sources.http import get_with_retry
from src.sources.nws import raise_for_saved_status
from src.storage.bronze import HEADERS_SUFFIX, is_complete, read_headers, save_raw

log = logging.getLogger(__name__)

LISTING_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
DETAILS_RE = re.compile(r"StormEvents_details-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz")
FOLDER = "storm_events"
FIRST_YEAR = 1996  # before 1996 F1 only recorded a few event types (Workshop 1 scope)


def storm_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "extreme-weather-monitoring (course project)"})
    return session


def fetch_listing(session: requests.Session, root: Path | None = None) -> Path:
    """Save the bulk listing too: it is the evidence of which compilation we downloaded."""
    response = get_with_retry(session, LISTING_URL)
    path = save_raw(response, FOLDER, "storm_events_listing", "html", root)
    raise_for_saved_status(response, path)
    return path


def details_files(listing_html: str) -> dict[int, tuple[str, str]]:
    """{year: (file name, compilation date)}; if a year appears twice we keep the newest compilation."""
    found: dict[int, tuple[str, str]] = {}
    for match in DETAILS_RE.finditer(listing_html):
        year, cdate = int(match.group(1)), match.group(2)
        if year not in found or cdate > found[year][1]:
            found[year] = (match.group(0), cdate)
    return found


def compilation_done(year: int, cdate: str, root: Path | None = None) -> bool:
    """A year is done when this exact compilation is already in Bronze. A new `_c` suffix means a
    recompiled file, which we download again (ingestion contract)."""
    folder = (root or bronze_root()) / FOLDER
    for path in folder.glob(f"storm_events_d{year}_c{cdate}_*.csv.gz"):
        if not path.name.endswith(HEADERS_SUFFIX) and is_complete(path) and read_headers(path)["status"] == 200:
            return True
    return False


def download_year(session: requests.Session, file_name: str, year: int, cdate: str,
                  root: Path | None = None) -> Path:
    response = get_with_retry(session, LISTING_URL + file_name)
    path = save_raw(response, FOLDER, f"storm_events_d{year}_c{cdate}", "csv.gz", root)
    raise_for_saved_status(response, path)
    log.info("source=F1 year=%s compilation=%s status=%s bytes=%d", year, cdate,
             response.status_code, len(response.content))
    return path


def read_header_line(path: Path) -> str:
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
        return handle.readline()
