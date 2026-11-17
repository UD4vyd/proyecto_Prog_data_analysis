"""F2: GHCN-Daily through the NCEI Access Data Service (dataset daily-summaries)."""
import logging
from datetime import date, timedelta

from src.sources import http

log = logging.getLogger(__name__)

URL = "https://www.ncei.noaa.gov/access/services/data/v1"


def fetch_daily(ghcn_ids, start_dates, end_dates, elements=("TMAX", "TMIN", "PRCP")):
    """One request per station and date range. Values arrive as strings and are kept as received.

    `start_dates` and `end_dates` map each GHCN id to a datetime.date.
    """
    requests_meta, rows, new_state = [], [], {}
    for ghcn_id in ghcn_ids:
        params = {
            "dataset": "daily-summaries",
            "stations": ghcn_id,
            "dataTypes": ",".join(elements),
            "startDate": start_dates[ghcn_id].isoformat(),
            "endDate": end_dates[ghcn_id].isoformat(),
            "includeAttributes": "true",
            "units": "metric",
            "format": "json",
        }
        response = http.get(URL, params=params)
        page = response.json()
        requests_meta.append(http.request_meta(response, len(page)))
        rows.extend(page)
        if page:
            new_state[ghcn_id] = max(r["DATE"] for r in page)
        log.info("F2 %s: %s station-days from %s to %s", ghcn_id, len(page), params["startDate"], params["endDate"])
    return {"requests": requests_meta, "records": rows, "state": new_state}


def incremental_start_dates(ghcn_ids, watermarks, today, overlap_days=30):
    """Start date per station: watermark minus the overlap, or today minus the overlap on the first run."""
    starts = {}
    for ghcn_id in ghcn_ids:
        last = date.fromisoformat(watermarks[ghcn_id]) if ghcn_id in watermarks else today
        starts[ghcn_id] = last - timedelta(days=overlap_days)
    return starts
