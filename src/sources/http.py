"""HTTP helper used by all source clients: retries and request metadata."""
import logging
import time
from datetime import datetime, timezone

import requests

log = logging.getLogger(__name__)

RETRY_STATUS = {429, 500, 502, 503, 504}


MAX_WAIT = 120   # seconds, so a long Retry-After never blocks an hourly task


def wait_seconds(response, attempt):
    """Seconds to wait before the next attempt.

    Workshop 3: when the server sends Retry-After (NWS does it with 429 and 503) we follow it, as the NWS
    API documentation asks. Otherwise the wait grows with each attempt (5, 10, 15 s).
    """
    retry_after = response.headers.get("Retry-After") if response is not None else None
    if retry_after and retry_after.strip().isdigit():
        return min(int(retry_after), MAX_WAIT)
    return 5 * attempt


def get(url, params=None, headers=None, tries=4, timeout=60):
    """GET with back-off. Raises if no attempt returns a valid answer."""
    for attempt in range(1, tries + 1):
        response = None
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            if response.status_code not in RETRY_STATUS:
                response.raise_for_status()
                return response
            log.warning("HTTP %s from %s (attempt %s of %s)", response.status_code, url, attempt, tries)
        except (requests.ConnectionError, requests.Timeout) as error:
            log.warning("%s for %s (attempt %s of %s)", type(error).__name__, url, attempt, tries)
        if attempt < tries:
            time.sleep(wait_seconds(response, attempt))
    raise RuntimeError(f"No valid answer from {url} after {tries} attempts")


def request_meta(response, records):
    """What we keep about each request, so a Bronze file can be traced to its origin."""
    return {
        "url": response.url,
        "status": response.status_code,
        "retrieved_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "records": records,
    }
