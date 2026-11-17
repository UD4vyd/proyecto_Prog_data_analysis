"""F3 and F4: National Weather Service API (api.weather.gov)."""
import logging
from datetime import datetime, timedelta, timezone

from src.settings import user_agent
from src.sources import http

log = logging.getLogger(__name__)

BASE = "https://api.weather.gov"


def _headers():
    return {"User-Agent": user_agent(), "Accept": "application/geo+json"}


def fetch_alerts(area="TX", page_limit=500):
    """Alerts of the last 7 days for one state, following the pagination cursor.

    Also asks for the alerts active now, to keep their ids and the ETag of that list.
    """
    requests_meta, features = [], []
    url, params = f"{BASE}/alerts", {"area": area, "limit": page_limit}
    while url:
        response = http.get(url, params=params, headers=_headers())
        body = response.json()
        page = body.get("features", [])
        requests_meta.append(http.request_meta(response, len(page)))
        features.extend(page)
        # The last "next" link returns an empty page, so we stop on an empty list.
        url = body.get("pagination", {}).get("next") if page else None
        params = None

    response = http.get(f"{BASE}/alerts/active", params={"area": area}, headers=_headers())
    active = response.json().get("features", [])
    requests_meta.append(http.request_meta(response, len(active)))
    active_ids = [f["properties"]["id"] for f in active]
    log.info("F3: %s alerts in the 7-day list, %s active now", len(features), len(active_ids))
    return {"requests": requests_meta, "records": features, "active_ids": active_ids, "state": {}}


def fetch_observations(icaos, watermarks, now, overlap_hours=3, first_run_hours=6, max_lookback_days=7):
    """Observations per station since its watermark minus the overlap window.

    `watermarks` maps ICAO code to the latest timestamp already ingested (ISO text).
    The request always sends `start`, because the default answer is capped at 500 records.
    """
    requests_meta, features, new_state = [], [], {}
    oldest = now - timedelta(days=max_lookback_days)
    for icao in icaos:
        if icao in watermarks:
            start = datetime.fromisoformat(watermarks[icao]) - timedelta(hours=overlap_hours)
        else:
            start = now - timedelta(hours=first_run_hours)
        start = max(start, oldest).astimezone(timezone.utc)
        params = {"start": start.strftime("%Y-%m-%dT%H:%M:%SZ")}
        response = http.get(f"{BASE}/stations/{icao}/observations", params=params, headers=_headers())
        page = response.json().get("features", [])
        requests_meta.append(http.request_meta(response, len(page)))
        features.extend(page)
        stamps = [f["properties"]["timestamp"] for f in page if f["properties"].get("timestamp")]
        latest = max(stamps, default=watermarks.get(icao))
        if latest:
            new_state[icao] = latest
        log.info("F4 %s: %s observations since %s", icao, len(page), params["start"])
    return {"requests": requests_meta, "records": features, "state": new_state}
