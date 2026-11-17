"""HTTP helper used by all source clients: retries and request metadata."""
import logging
import time
from datetime import datetime, timezone

import requests

log = logging.getLogger(__name__)

RETRY_STATUS = {429, 500, 502, 503, 504}


def get(url, params=None, headers=None, tries=4, timeout=60):
    """GET with a simple back-off. Raises if no attempt returns a valid answer."""
    for attempt in range(1, tries + 1):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            if response.status_code not in RETRY_STATUS:
                response.raise_for_status()
                return response
            log.warning("HTTP %s from %s (attempt %s of %s)", response.status_code, url, attempt, tries)
        except (requests.ConnectionError, requests.Timeout) as error:
            log.warning("%s for %s (attempt %s of %s)", type(error).__name__, url, attempt, tries)
        if attempt < tries:
            time.sleep(5 * attempt)
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
