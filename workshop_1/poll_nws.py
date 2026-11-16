"""Take one evidence poll of F3 (NWS alerts) and F4 (NWS observations).

Usage: poetry run python workshop_1/poll_nws.py workshop_1/samples/poll2

Each response is saved byte-for-byte (r.content) with a .headers.json file next to it.
All files of one poll share the UTC stamp of the start of the poll.
"""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

USER_AGENT = os.environ.get("NWS_USER_AGENT", "(extreme-weather-monitoring, team5-ud)")
STATIONS = ["KDFW", "KIAH", "KAUS"]
REQUESTS = [("nws_alerts_active", "https://api.weather.gov/alerts/active?area=TX"),
            ("nws_alerts_recent", "https://api.weather.gov/alerts?area=TX&limit=500")]
REQUESTS += [(f"nws_observations_{s}", f"https://api.weather.gov/stations/{s}/observations") for s in STATIONS]


def utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def main(out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = utc_stamp()
    for name, url in REQUESTS:
        r = requests.get(url, headers={"User-Agent": USER_AGENT, "Accept": "application/geo+json"}, timeout=60)
        path = out_dir / f"{name}_{stamp}.geojson"
        path.write_bytes(r.content)
        meta = {"url": url, "status": r.status_code, "retrieved_utc": utc_stamp(), "headers": dict(r.headers)}
        Path(str(path) + ".headers.json").write_text(json.dumps(meta, indent=1))
        print(r.status_code, len(r.content), path)


if __name__ == "__main__":
    main(sys.argv[1])
