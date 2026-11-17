"""One GET helper with timeout, a single in-code retry and clear log lines.

Airflow retries the whole task three times with exponential backoff. This helper adds one quick
retry inside the task for short problems (429, 5xx, timeouts), so one slow response does not
repeat the requests of all other stations.
"""
import logging
import time

import requests

log = logging.getLogger(__name__)

TIMEOUT_SECONDS = 60
RETRY_STATUS = {429, 500, 502, 503, 504}


def get_with_retry(session: requests.Session, url: str, params: dict | None = None,
                   retries: int = 1, backoff_seconds: float = 5.0) -> requests.Response:
    """Return the response of the last attempt. The caller saves it before checking the status."""
    for attempt in range(retries + 1):
        try:
            response = session.get(url, params=params, timeout=TIMEOUT_SECONDS)
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == retries:
                raise
            wait = backoff_seconds * 2 ** attempt
            log.warning("request failed (%s), retry in %.0f s: %s", type(exc).__name__, wait, url)
            time.sleep(wait)
            continue
        if response.status_code in RETRY_STATUS and attempt < retries:
            retry_after = response.headers.get("Retry-After", "")
            wait = float(retry_after) if retry_after.isdigit() else backoff_seconds * 2 ** attempt
            log.warning("status %s, retry in %.0f s: %s", response.status_code, wait, response.url)
            time.sleep(wait)
            continue
        return response
    raise RuntimeError("unreachable")
