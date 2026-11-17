"""Bronze task functions. The DAG files only call these, so they run and test without Airflow.

Windows and keys follow docs/ingestion_contract.md:
- F3: start = max `sent` - 2 h, plus the active alerts snapshot;
- F4: start = max `timestamp` per station - 3 h;
- F2 recent: from max DATE per station - 30 days to today, at most once every 23 hours;
- F1 and F2 historical: one file per year, one request per station and year, skipped when done.
"""
import gzip
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

from src.config import bronze_root, load_stations
from src.sources import ghcn, nws, storm_events
from src.storage.bronze import (read_headers, read_watermarks, relative_to_bronze, utc_now,
                                write_ready_marker, write_watermarks)

log = logging.getLogger(__name__)

F3_OVERLAP = timedelta(hours=2)
F4_OVERLAP = timedelta(hours=3)
F2_OVERLAP = timedelta(days=30)
F2_MIN_INTERVAL = timedelta(hours=23)
# First run only: NWS keeps about 7 days, so we take everything that is still available.
FIRST_RUN_LOOKBACK = timedelta(days=7)
F2_FIRST_YEAR = 1991


def _parse_utc(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _station_list(column: str, stations: list[str] | None = None) -> list[str]:
    values = load_stations()[column].tolist()
    if stations:
        unknown = sorted(set(stations) - set(values))
        if unknown:
            raise ValueError(f"stations not in config/stations.csv: {unknown}")
        values = [v for v in values if v in set(stations)]
    return values


# Incremental extraction -----------------------------------------------------------------------

def extract_alerts(watermarks: dict) -> list[str]:
    last_sent = watermarks.get("nws_alerts", {}).get("sent_utc")
    start = _parse_utc(last_sent) - F3_OVERLAP if last_sent else utc_now() - FIRST_RUN_LOOKBACK
    session = nws.nws_session()
    paths = nws.fetch_alerts(session, start)
    time.sleep(nws.PAUSE_SECONDS)
    paths.append(nws.fetch_active_alerts(session))
    return [relative_to_bronze(p) for p in paths]


def extract_observations(watermarks: dict, stations: list[str] | None = None) -> list[str]:
    last_by_station = watermarks.get("nws_observations", {})
    session = nws.nws_session()
    saved = []
    for icao in _station_list("icao", stations):
        last = last_by_station.get(icao)
        start = _parse_utc(last) - F4_OVERLAP if last else utc_now() - FIRST_RUN_LOOKBACK
        saved += [relative_to_bronze(p) for p in nws.fetch_observations(session, icao, start)]
        time.sleep(nws.PAUSE_SECONDS)
    return saved


def f2_recent_due(watermarks: dict, now: datetime | None = None) -> bool:
    last_fetch = watermarks.get("ghcn_daily", {}).get("last_fetch_utc")
    if not last_fetch:
        return True
    return (now or utc_now()) - _parse_utc(last_fetch) >= F2_MIN_INTERVAL


def extract_ghcn_recent(watermarks: dict, stations: list[str] | None = None) -> list[str]:
    """F2 recent window. Returns [] when the last F2 fetch is less than 23 hours old."""
    if not f2_recent_due(watermarks):
        log.info("source=F2 recent window not due, last fetch %s",
                 watermarks["ghcn_daily"]["last_fetch_utc"])
        return []
    max_dates = watermarks.get("ghcn_daily", {}).get("stations", {})
    today = utc_now().date()
    session = ghcn.ghcn_session()
    saved = []
    for station in _station_list("ghcn_id", stations):
        last = max_dates.get(station)
        start = date.fromisoformat(last) - F2_OVERLAP if last else today - F2_OVERLAP
        path = ghcn.fetch_daily(session, station, start, today, "recent")
        saved.append(relative_to_bronze(path))
        time.sleep(nws.PAUSE_SECONDS)
    return saved


# Schema check -----------------------------------------------------------------------------------

def check_schema(files: list[str]) -> dict:
    """Basic contract check on the saved files: status 200 and the expected top-level structure.

    A failure stops the run before the watermark moves, so the next run repeats the window.
    """
    root = bronze_root()
    errors, counts = [], {"files": 0, "records": 0}
    for rel in files:
        path = root / rel
        status = read_headers(path)["status"]
        if status != 200:
            errors.append(f"{rel}: status {status}")
            continue
        try:
            if path.name.endswith(".geojson"):
                data = json.loads(path.read_bytes())
                missing = {"type", "features"} - set(data)
                if missing or not isinstance(data["features"], list):
                    errors.append(f"{rel}: missing top-level keys {sorted(missing)}")
                    continue
                counts["records"] += len(data["features"])
            elif path.name.endswith(".json"):
                rows = json.loads(path.read_bytes())
                if not isinstance(rows, list) or any({"STATION", "DATE"} - set(r) for r in rows):
                    errors.append(f"{rel}: expected a list of objects with STATION and DATE")
                    continue
                counts["records"] += len(rows)
            elif path.name.endswith(".csv.gz"):
                header = storm_events.read_header_line(path)
                if "EVENT_ID" not in header or "STATE_FIPS" not in header:
                    errors.append(f"{rel}: CSV header without EVENT_ID or STATE_FIPS")
                    continue
            # .html listings only need status 200
        except (ValueError, OSError, gzip.BadGzipFile) as exc:
            errors.append(f"{rel}: cannot be parsed ({exc})")
            continue
        counts["files"] += 1
    if errors:
        raise ValueError("schema check failed:\n" + "\n".join(errors))
    log.info("schema check passed: %s", counts)
    return counts


# Watermarks -------------------------------------------------------------------------------------

def _later(old: str | None, new: datetime | None) -> str | None:
    """Watermarks never move backwards."""
    if new is None:
        return old
    if old and _parse_utc(old) >= new:
        return old
    return new.isoformat()


def update_watermarks(f3_files: list[str], f4_files: list[str], f2_files: list[str]) -> dict:
    """Read the saved files from disk, move the watermarks, then signal Silver with a ready marker.

    This task runs only when every extract task and the schema check succeeded.
    """
    root = bronze_root()
    watermarks = read_watermarks()

    sent_times = []
    for rel in f3_files:
        for feature in json.loads((root / rel).read_bytes())["features"]:
            sent_times.append(_parse_utc(feature["properties"]["sent"]))
    alerts = watermarks.setdefault("nws_alerts", {})
    alerts["sent_utc"] = _later(alerts.get("sent_utc"), max(sent_times, default=None))

    observations = watermarks.setdefault("nws_observations", {})
    for rel in f4_files:
        for feature in json.loads((root / rel).read_bytes())["features"]:
            props = feature["properties"]
            station = props["stationId"]
            observations[station] = _later(observations.get(station), _parse_utc(props["timestamp"]))

    daily = watermarks.setdefault("ghcn_daily", {"stations": {}})
    daily.setdefault("stations", {})
    for rel in f2_files:
        for row in json.loads((root / rel).read_bytes()):
            station, day = row["STATION"], row["DATE"]
            if day > daily["stations"].get(station, ""):
                daily["stations"][station] = day
    if f2_files:
        daily["last_fetch_utc"] = utc_now().isoformat(timespec="seconds")

    watermarks["updated_utc"] = utc_now().isoformat(timespec="seconds")
    write_watermarks(watermarks)
    files = f3_files + f4_files + f2_files
    marker = write_ready_marker(files, producer="bronze_ingestion_dag") if files else None
    summary = {"files": len(files), "f3_files": len(f3_files), "f4_files": len(f4_files),
               "f2_files": len(f2_files), "alerts_sent_utc": alerts["sent_utc"],
               "stations_with_observations": len(observations),
               "marker": marker.name if marker else None}
    log.info("watermarks updated: %s", summary)
    return summary


# Historical load ---------------------------------------------------------------------------------

def load_f1_years(years: list[int]) -> list[str]:
    """One details file per year. A year whose compilation is already in Bronze is skipped."""
    bad = [y for y in years if y < storm_events.FIRST_YEAR]
    if bad:
        raise ValueError(f"F1 years before {storm_events.FIRST_YEAR} are out of scope: {bad}")
    session = storm_events.storm_session()
    listing = storm_events.fetch_listing(session)
    available = storm_events.details_files(listing.read_text(encoding="utf-8", errors="replace"))
    saved = [relative_to_bronze(listing)]
    for year in years:
        if year not in available:
            log.warning("source=F1 year=%s not in the bulk listing, skipped", year)
            continue
        file_name, cdate = available[year]
        if storm_events.compilation_done(year, cdate):
            log.info("source=F1 year=%s compilation=%s already in Bronze, skipped", year, cdate)
            continue
        saved.append(relative_to_bronze(storm_events.download_year(session, file_name, year, cdate)))
        time.sleep(nws.PAUSE_SECONDS)
    return saved


def load_f2_station_years(start_year: int, end_year: int, stations: list[str] | None = None) -> list[str]:
    """One request per station and year. Station-years already in Bronze are skipped."""
    if start_year < F2_FIRST_YEAR or end_year < start_year:
        raise ValueError(f"F2 years must satisfy {F2_FIRST_YEAR} <= start <= end, got {start_year}..{end_year}")
    session = ghcn.ghcn_session()
    saved, skipped = [], 0
    for station in _station_list("ghcn_id", stations):
        for year in range(start_year, end_year + 1):
            if ghcn.year_partition_done(station, year):
                skipped += 1
                continue
            path = ghcn.fetch_daily(session, station, date(year, 1, 1), date(year, 12, 31), str(year))
            saved.append(relative_to_bronze(path))
            time.sleep(nws.PAUSE_SECONDS)
    log.info("source=F2 historical saved=%d skipped=%d", len(saved), skipped)
    return saved


def signal_historical(files: list[str]) -> str | None:
    """Ready marker for Silver, only when the historical run saved new data files."""
    data_files = [f for f in files if not f.endswith(".html")]
    if not data_files:
        log.info("historical run saved no new data files, no marker written")
        return None
    return write_ready_marker(files, producer="bronze_historical_dag").name
