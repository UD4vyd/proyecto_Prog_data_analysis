"""F3 (alerts) and F4 (station observations) clients for the NWS API, api.weather.gov.

Every response is saved to Bronze first and then read back from disk, never parsed from memory.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.sources.http import get_with_retry
from src.storage.bronze import save_raw

log = logging.getLogger(__name__)

BASE_URL = "https://api.weather.gov"
PAGE_LIMIT = 500  # maximum of the `limit` parameter in the NWS OpenAPI document
PAUSE_SECONDS = 1.0  # NWS does not publish a numeric rate limit, so we stay polite


def nws_session() -> requests.Session:
    """NWS asks every client for a User-Agent with a contact. It comes from .env, never from code."""
    user_agent = os.environ.get("NWS_USER_AGENT", "").strip().strip('"')
    if not user_agent:
        raise RuntimeError("NWS_USER_AGENT is not set. Copy .env.example to .env and fill it in.")
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent, "Accept": "application/geo+json"})
    return session


def iso_z(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def raise_for_saved_status(response: requests.Response, path: Path) -> None:
    """Called after saving, so even an error body stays in Bronze as evidence."""
    if response.status_code != 200:
        raise requests.HTTPError(
            f"status {response.status_code} for {response.url} (raw body saved in {path.name})",
            response=response)


def read_geojson(path: Path) -> dict:
    return json.loads(path.read_bytes())


def fetch_alerts(session: requests.Session, start: datetime, root: Path | None = None) -> list[Path]:
    """F3 `/alerts?area=TX&start=...`. The API filters on `sent >= start`.

    The response always has a `pagination.next` link, even when the page is not full, so we follow
    it only when a page has PAGE_LIMIT features.
    """
    url = f"{BASE_URL}/alerts"
    params: dict | None = {"area": "TX", "start": iso_z(start), "limit": PAGE_LIMIT}
    paths = []
    while True:
        response = get_with_retry(session, url, params)
        path = save_raw(response, "nws_alerts", "nws_alerts", "geojson", root)
        raise_for_saved_status(response, path)
        data = read_geojson(path)
        features = data.get("features", [])
        log.info("source=F3 endpoint=alerts start=%s status=%s features=%d bytes=%d",
                 params["start"] if params else "cursor", response.status_code, len(features),
                 len(response.content))
        paths.append(path)
        next_url = (data.get("pagination") or {}).get("next")
        if len(features) < PAGE_LIMIT or not next_url:
            return paths
        url, params = next_url, None
        time.sleep(PAUSE_SECONDS)


def fetch_active_alerts(session: requests.Session, root: Path | None = None) -> Path:
    """F3 `/alerts/active?area=TX`: every alert in force now, whatever its `sent` time."""
    response = get_with_retry(session, f"{BASE_URL}/alerts/active", {"area": "TX"})
    path = save_raw(response, "nws_alerts_active", "nws_alerts_active", "geojson", root)
    raise_for_saved_status(response, path)
    features = read_geojson(path).get("features", [])
    log.info("source=F3 endpoint=alerts_active status=%s features=%d bytes=%d",
             response.status_code, len(features), len(response.content))
    return path


def fetch_observations(session: requests.Session, icao: str, start: datetime,
                       end: datetime | None = None, root: Path | None = None) -> list[Path]:
    """F4 `/stations/{icao}/observations?start=...` with the window split at PAGE_LIMIT.

    The API returns the newest records first and stops at PAGE_LIMIT. Its `next` cursor drops our
    `start` filter (checked on 2026-11-17), so we do not follow it. When a page is full we ask again
    with `end` = the oldest timestamp received, until a page is not full.
    """
    url = f"{BASE_URL}/stations/{icao}/observations"
    window_end = end
    paths = []
    while True:
        params = {"start": iso_z(start), "limit": PAGE_LIMIT}
        if window_end is not None:
            params["end"] = iso_z(window_end)
        response = get_with_retry(session, url, params)
        path = save_raw(response, "nws_observations", f"nws_observations_{icao}", "geojson", root)
        raise_for_saved_status(response, path)
        features = read_geojson(path).get("features", [])
        log.info("source=F4 station=%s start=%s end=%s status=%s features=%d bytes=%d", icao,
                 params["start"], params.get("end", "now"), response.status_code, len(features),
                 len(response.content))
        paths.append(path)
        if len(features) < PAGE_LIMIT:
            return paths
        oldest = min(datetime.fromisoformat(f["properties"]["timestamp"]) for f in features)
        if window_end is not None and oldest >= window_end:
            log.warning("source=F4 station=%s window did not shrink, stopping at %s", icao, oldest)
            return paths
        window_end = oldest
        time.sleep(PAUSE_SECONDS)
