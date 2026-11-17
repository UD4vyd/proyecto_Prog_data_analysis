"""Bronze layer: write raw answers as JSON files, check their basic structure, keep the watermarks."""
import json
import logging
import os
from datetime import datetime, timezone

from src import settings

log = logging.getLogger(__name__)

# Fields every raw record must have. For GeoJSON sources they are inside "properties".
REQUIRED = {
    "nws_alerts": ["id", "event", "sent", "severity", "messageType"],
    "nws_observations": ["timestamp", "temperature"],
    "ghcn_daily": ["STATION", "DATE"],
    "storm_events": ["EVENT_ID", "STATE_FIPS", "EVENT_TYPE", "BEGIN_YEARMONTH", "BEGIN_DAY", "BEGIN_TIME",
                     "CZ_TIMEZONE"],
}
GEOJSON_SOURCES = {"nws_alerts", "nws_observations"}


def read_state():
    """Watermarks per source, as written by persist_file."""
    if settings.STATE_FILE.exists():
        return json.loads(settings.STATE_FILE.read_text(encoding="utf-8"))
    return {}


def write_staging(source, run_stamp, result):
    """Write one extraction as a JSON file in the staging folder and return its path."""
    payload = {
        "source": source,
        "run_stamp": run_stamp,
        "extracted_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "record_count": len(result["records"]),
        "requests": result["requests"],
        "state": result.get("state", {}),
    }
    if "active_ids" in result:
        payload["active_ids"] = result["active_ids"]
    payload["records"] = result["records"]
    settings.STAGING_DIR.mkdir(parents=True, exist_ok=True)
    path = settings.STAGING_DIR / f"{source}_{run_stamp}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    log.info("Staged %s records of %s in %s", payload["record_count"], source, path.name)
    return str(path)


def validate_file(path):
    """Basic schema check of a staged file. Raises ValueError when the file must not reach Bronze."""
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    for key in ("source", "run_stamp", "extracted_utc", "record_count", "requests", "records"):
        if key not in payload:
            raise ValueError(f"{path}: missing key '{key}'")
    source, records = payload["source"], payload["records"]
    if source not in REQUIRED:
        raise ValueError(f"{path}: unknown source '{source}'")
    if payload["record_count"] != len(records):
        raise ValueError(f"{path}: record_count does not match the number of records")
    bad_status = [r["url"] for r in payload["requests"] if r["status"] != 200]
    if bad_status:
        raise ValueError(f"{path}: requests without status 200: {bad_status}")
    incomplete = 0
    for record in records:
        fields = record.get("properties", {}) if source in GEOJSON_SOURCES else record
        if any(name not in fields for name in REQUIRED[source]):
            incomplete += 1
    if incomplete:
        raise ValueError(f"{path}: {incomplete} of {len(records)} records lack a required field {REQUIRED[source]}")
    if not records:
        log.warning("%s has no records. This is valid for a quiet period, but check the source.", path)
    log.info("Validated %s: %s records, %s requests", path, len(records), len(payload["requests"]))
    return path


def persist_file(path):
    """Move a validated file from staging to Bronze and only then advance the watermarks."""
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    final = settings.BRONZE_DIR / os.path.basename(path)
    os.replace(path, final)  # a rename, so the Silver DAG never sees a half-written file
    state = read_state()
    state.setdefault(payload["source"], {}).update(payload.get("state", {}))
    state[payload["source"]]["_last_run_stamp"] = payload["run_stamp"]
    settings.STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    settings.STATE_FILE.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
    log.info("Persisted %s (%s records)", final.name, payload["record_count"])
    return str(final)
