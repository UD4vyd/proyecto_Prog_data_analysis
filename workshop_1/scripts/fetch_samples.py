"""Workshop 1 - download real, unmodified samples from the four Team 5 sources.

Every response is saved exactly as received under ``workshop_1/samples`` (small files)
or ``data/raw`` (large files, git-ignored), named ``<source>_YYYYMMDD_HHMMSS.<ext>``.
HTTP response headers are stored next to each file (``.headers.json``) as evidence
of ETag / Last-Modified behaviour.

Usage:
    python workshop_1/scripts/fetch_samples.py historical   # Storm Events + GHCN-Daily
    python workshop_1/scripts/fetch_samples.py incremental  # NWS alerts + NWS observations (one poll)
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml

ROOT = Path(__file__).resolve().parents[2]
SAMPLES = ROOT / "workshop_1" / "samples"
RAW = ROOT / "data" / "raw"
CONFIG = yaml.safe_load((ROOT / "config" / "sources.yaml").read_text(encoding="utf-8"))

USER_AGENT = os.getenv(
    "NWS_USER_AGENT", "Team5-ExtremeWeatherMonitor/0.1 (Universidad Distrital course project)"
)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"})


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def get(url: str, params: dict | None = None, accept: str | None = None, retries: int = 4):
    headers = {"Accept": accept} if accept else {}
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, headers=headers, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            wait = 2 ** attempt
            print(f"  ! {exc} -> retry in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"giving up on {url}")


def save(resp: requests.Response, folder: Path, source: str, ext: str, ts: str) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{source}_{ts}.{ext}"
    path.write_bytes(resp.content)
    meta = {
        "url": resp.url,
        "status": resp.status_code,
        "retrieved_utc": ts,
        "headers": {k: v for k, v in resp.headers.items()
                    if k.lower() in {"etag", "last-modified", "content-type", "content-length",
                                     "expires", "cache-control", "date", "x-correlation-id"}},
    }
    path.with_suffix(path.suffix + ".headers.json").write_text(json.dumps(meta, indent=2))
    print(f"  saved {path.relative_to(ROOT)} ({len(resp.content):,} bytes)")
    return path


# --------------------------------------------------------------------------- historical
def fetch_storm_events(ts: str) -> None:
    cfg = CONFIG["sources"]["noaa_storm_events"]
    print("[noaa_storm_events] listing archive")
    listing = get(cfg["url"]).text
    (SAMPLES / f"noaa_storm_events_listing_{ts}.html").write_text(listing, encoding="utf-8")
    year = cfg["sample_year"]
    names = sorted(set(re.findall(rf"StormEvents_details-ftp_v1\.0_d{year}_c\d{{8}}\.csv\.gz", listing)))
    name = names[-1]
    print(f"  latest compilation for {year}: {name}")
    resp = get(cfg["url"] + name)
    full = save(resp, RAW, "noaa_storm_events", "csv.gz", ts)
    extract_texas(full, year, ts)


def extract_texas(full: Path, year: int, ts: str) -> None:
    """Evidence sample: header + every row with STATE_FIPS == 48, as byte-for-byte original lines.

    Filtering on the text "TEXAS" is wrong: it also matches Texas County (Missouri, Oklahoma) in the
    TOR_OTHER_* columns and places named Texas in other states. A physical line is accumulated until it
    parses into a complete record, so quoted multi-line narratives are kept intact.
    """
    with gzip.open(full, "rt", encoding="utf-8", errors="replace", newline="") as fh:
        header = fh.readline()
        cols = next(csv.reader([header]))
        i_fips, n = cols.index("STATE_FIPS"), len(cols)
        kept, buf = [], ""
        for line in fh:
            buf += line
            rec = next(csv.reader(io.StringIO(buf)), [])
            if len(rec) < n:
                continue  # record continues on the next physical line
            if rec[i_fips].strip() == "48":
                kept.append(buf)
            buf = ""
    out = SAMPLES / f"noaa_storm_events_TX_{year}_{ts}.csv"
    out.write_text(header + "".join(kept), encoding="utf-8", newline="")
    print(f"  wrote {out.relative_to(ROOT)} ({len(kept)} rows with STATE_FIPS=48, original lines)")


def fetch_ghcn_daily(ts: str) -> None:
    cfg = CONFIG["sources"]["ghcn_daily"]
    stations = ",".join(s["ghcn_id"] for s in CONFIG["region"]["stations"])
    end = datetime.now(timezone.utc).date()
    params = {
        "dataset": "daily-summaries",
        "stations": stations,
        "startDate": cfg["sample_start"],
        "endDate": end.isoformat(),
        "dataTypes": ",".join(cfg["data_types"]),
        "units": "metric",
        "includeAttributes": "true",
        "includeStationName": "true",
        "includeStationLocation": "1",
        "format": "json",
    }
    print("[ghcn_daily] querying NCEI Access Data Service")
    save(get(cfg["url"], params), SAMPLES, "ghcn_daily", "json", ts)
    # Same query in CSV for one station to document the alternative format.
    params_csv = dict(params, stations=CONFIG["region"]["stations"][0]["ghcn_id"], format="csv",
                      startDate=(end - timedelta(days=60)).isoformat())
    save(get(cfg["url"], params_csv), SAMPLES, "ghcn_daily_csv", "csv", ts)


# --------------------------------------------------------------------------- incremental
def fetch_nws_alerts(ts: str, folder: Path) -> None:
    cfg = CONFIG["sources"]["nws_alerts"]
    print("[nws_alerts] active alerts for TX")
    save(get(cfg["url"] + "/active", {"area": "TX"}, accept="application/geo+json"),
         folder, "nws_alerts_active", "geojson", ts)
    print("[nws_alerts] recent alerts (all statuses, last ~7 days) for TX")
    save(get(cfg["url"], {"area": "TX", "limit": 500}, accept="application/geo+json"),
         folder, "nws_alerts_recent", "geojson", ts)


def fetch_nws_observations(ts: str, folder: Path) -> None:
    cfg = CONFIG["sources"]["nws_observations"]
    start = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    print("[nws_observations] last 7 days per station")
    for st in CONFIG["region"]["stations"]:
        url = cfg["url"].format(station=st["nws_id"])
        save(get(url, {"start": start}, accept="application/geo+json"),
             folder, f"nws_observations_{st['nws_id']}", "geojson", ts)
        time.sleep(0.5)  # be polite


def compress_observations(folder: Path) -> None:
    """Replace large observation responses by byte-identical .gz copies (keeps the repo small)."""
    for path in sorted(folder.glob("nws_observations_*.geojson")):
        gz = path.with_name(path.name + ".gz")
        with open(path, "rb") as src, gzip.open(gz, "wb") as dst:
            dst.write(src.read())
        with gzip.open(gz, "rb") as fh:
            assert fh.read() == path.read_bytes()
        path.unlink()
    print(f"  compressed observation files in {folder.relative_to(ROOT)}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    tag = sys.argv[2] if len(sys.argv) > 2 else None  # e.g. poll1 / poll2
    ts = stamp()
    if mode in ("historical", "all"):
        fetch_storm_events(ts)
        fetch_ghcn_daily(ts)
    if mode in ("incremental", "all"):
        folder = SAMPLES / (tag or "poll")
        fetch_nws_alerts(ts, folder)
        fetch_nws_observations(ts, folder)
        compress_observations(folder)
