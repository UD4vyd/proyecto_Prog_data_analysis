"""F1: NOAA Storm Events Database. The yearly files are found by scraping the bulk listing page."""
import csv
import gzip
import io
import logging
import re

from bs4 import BeautifulSoup

from src.sources import http

log = logging.getLogger(__name__)

LISTING_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
DETAILS_FILE = re.compile(r"^StormEvents_details-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz$")


def parse_listing(html):
    """Return {year: (file name, compilation date)} with the latest details file of each year."""
    soup = BeautifulSoup(html, "html.parser")
    latest = {}
    for link in soup.find_all("a", href=True):
        match = DETAILS_FILE.match(link["href"])
        if match:
            year, compiled = int(match.group(1)), match.group(2)
            if year not in latest or compiled > latest[year][1]:
                latest[year] = (link["href"], compiled)
    return latest


def fetch_events(years, loaded, state_fips="48"):
    """Download the year files whose compilation date is new and keep the rows of one state.

    `loaded` maps year (text) to the compilation date already ingested. Rows keep every field as text.
    """
    response = http.get(LISTING_URL)
    listing = parse_listing(response.text)
    if not listing:
        raise RuntimeError("No details files found in the Storm Events listing. The page layout may have changed.")
    requests_meta, rows, new_state = [http.request_meta(response, len(listing))], [], {}
    for year in years:
        if year not in listing:
            log.warning("F1: no details file for %s in the listing", year)
            continue
        name, compiled = listing[year]
        if loaded.get(str(year)) == compiled:
            log.info("F1 %s: compilation %s already ingested, nothing to do", year, compiled)
            continue
        response = http.get(LISTING_URL + name, timeout=300)
        text = gzip.decompress(response.content).decode("utf-8", errors="replace")
        state_rows = [r for r in csv.DictReader(io.StringIO(text)) if r.get("STATE_FIPS") == state_fips]
        for row in state_rows:
            row["_source_file"] = name
        requests_meta.append(http.request_meta(response, len(state_rows)))
        rows.extend(state_rows)
        new_state[str(year)] = compiled
        log.info("F1 %s: %s rows for state %s in %s", year, len(state_rows), state_fips, name)
    return {"requests": requests_meta, "records": rows, "state": new_state}
