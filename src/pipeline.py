"""Extraction steps shared by the Bronze DAG and by ingest.py.

Each function asks one source, writes the answer to the staging folder and returns the file path,
or None when there is nothing new to ingest.
"""
import logging
from datetime import date

from src import settings
from src.sources import ghcn, nws, storm_events
from src.storage import bronze

log = logging.getLogger(__name__)

SOURCES = ["nws_alerts", "nws_observations", "ghcn_daily", "storm_events"]


def run_stamp(moment):
    """UTC moment of a run as text, used in every file name of that run."""
    return moment.strftime(settings.STAMP_FORMAT)


def extract_alerts(stamp):
    config = settings.load_settings()["nws_alerts"]
    result = nws.fetch_alerts(area=config["area"], page_limit=config["page_limit"])
    return bronze.write_staging("nws_alerts", stamp, result)


def extract_observations(stamp, now):
    config = settings.load_settings()["nws_observations"]
    watermarks = {k: v for k, v in bronze.read_state().get("nws_observations", {}).items() if not k.startswith("_")}
    icaos = [s["icao"] for s in settings.load_stations()]
    result = nws.fetch_observations(icaos, watermarks, now, overlap_hours=config["overlap_hours"],
                                    first_run_hours=config["first_run_hours"],
                                    max_lookback_days=config["max_lookback_days"])
    return bronze.write_staging("nws_observations", stamp, result)


def extract_daily(stamp, now):
    """GHCN-Daily is published once a day, so only the first run of each UTC day asks for it."""
    config = settings.load_settings()["ghcn_daily"]
    state = bronze.read_state().get("ghcn_daily", {})
    last = state.get("_last_run_stamp", "")
    if last[:8] == stamp[:8] and last != stamp:
        log.info("F2 already ingested today (run %s), nothing to do", last)
        return None
    ghcn_ids = [s["ghcn_id"] for s in settings.load_stations()]
    today = now.date()
    starts = ghcn.incremental_start_dates(ghcn_ids, state, today, overlap_days=config["overlap_days"])
    result = ghcn.fetch_daily(ghcn_ids, starts, {g: today for g in ghcn_ids}, elements=config["elements"])
    return bronze.write_staging("ghcn_daily", stamp, result)


def extract_storm_events(stamp, years=None):
    """Scrape the listing and download a year file only when its compilation date changed."""
    config = settings.load_settings()["storm_events"]
    loaded = bronze.read_state().get("storm_events", {})
    result = storm_events.fetch_events(years or config["years"], loaded, state_fips=config["state_fips"])
    if not result["records"]:
        return None
    return bronze.write_staging("storm_events", stamp, result)


def extract_daily_history(stamp, start_year, end_year):
    """Historical load of F2: one request per station and year."""
    ghcn_ids = [s["ghcn_id"] for s in settings.load_stations()]
    merged = {"requests": [], "records": [], "state": {}}
    for year in range(start_year, end_year + 1):
        starts = {g: date(year, 1, 1) for g in ghcn_ids}
        ends = {g: min(date(year, 12, 31), date.today()) for g in ghcn_ids}
        result = ghcn.fetch_daily(ghcn_ids, starts, ends)
        merged["requests"] += result["requests"]
        merged["records"] += result["records"]
        merged["state"].update(result["state"])
    return bronze.write_staging("ghcn_daily", stamp, merged)
