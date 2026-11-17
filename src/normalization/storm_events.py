"""Silver normalizer for F1, NOAA Storm Events (topic storm_events)."""
import math
import re

import pandas as pd

from src.normalization import common, text

TOPIC = "storm_events"
KEY = ["event_id"]
REQUIRED = ["event_id", "event_type", "begin_utc"]
SCHEMA = {
    "event_id": "int64",
    "episode_id": "Int64",
    "event_type": "category",
    "cz_type": "category",              # C county, Z forecast zone
    "cz_fips": "Int64",
    "cz_name": "string",
    "begin_local": "string",            # local standard time as received
    "cz_timezone": "category",          # for example CST-6
    "begin_utc": "datetime_utc",
    "end_utc": "datetime_utc",
    "begin_lat": "float64",             # null for zone rows
    "begin_lon": "float64",
    "magnitude": "float64",             # converted to the unit in magnitude_unit
    "magnitude_unit": "category",       # mm for hail, m/s for wind, null for other types
    "magnitude_type": "category",       # EG estimated gust, MG measured gust, ...
    "damage_property_usd": "float64",   # null means unknown, not zero
    "damage_crops_usd": "float64",
    "deaths_direct": "Int64",
    "injuries_direct": "Int64",
    "narrative_clean": "string",
    "damage_outlier": "bool",           # IQR flag on log10 of positive damage, by event type
    "source_file": "string",
    "record_hash": "string",
    "bronze_file": "string",
    "ingested_at": "datetime_utc",
    "processed_at": "datetime_utc",
}
DAMAGE = re.compile(r"^([0-9]*\.?[0-9]+)\s*([HKMB]?)$", re.IGNORECASE)
MULTIPLIER = {"": 1.0, "H": 1e2, "K": 1e3, "M": 1e6, "B": 1e9}
OFFSET = re.compile(r"-(\d{1,2})$")
INCH_TO_MM, KNOT_TO_MS = 25.4, 0.514444
WIND_TYPES = {"Thunderstorm Wind", "High Wind", "Strong Wind", "Marine Thunderstorm Wind", "Marine High Wind",
              "Marine Strong Wind"}


def parse_damage(value):
    """'10.00K' to 10000.0. A blank means the damage is unknown, so it stays null."""
    match = DAMAGE.match((value or "").strip())
    if not match:
        return None
    return float(match.group(1)) * MULTIPLIER[match.group(2).upper()]


def to_utc(yearmonth, day, hhmm, timezone_text):
    """Build UTC from the local standard time fields. 'CST-6' means UTC = local time + 6 hours.

    The 2-digit year of BEGIN_DATE_TIME is not used.
    """
    offset = OFFSET.search((timezone_text or "").strip())
    try:
        clock = str(hhmm).strip().zfill(4)
        local = pd.Timestamp(year=int(yearmonth[:4]), month=int(yearmonth[4:6]), day=int(day),
                             hour=int(clock[:2]), minute=int(clock[2:]))
    except (TypeError, ValueError):
        return None
    if not offset:
        return None
    return (local + pd.Timedelta(hours=int(offset.group(1)))).tz_localize("UTC")


def _magnitude(event_type, value):
    """Convert MAGNITUDE to SI. Its unit depends on the event type (inches for hail, knots for wind)."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None, None
    if event_type in ("Hail", "Marine Hail"):
        return number * INCH_TO_MM, "mm"
    if event_type in WIND_TYPES:
        return number * KNOT_TO_MS, "m/s"
    return number, None


def normalize(payload):
    rows = []
    for r in payload["records"]:
        magnitude, unit = _magnitude(r.get("EVENT_TYPE"), r.get("MAGNITUDE"))
        rows.append({
            "event_id": r.get("EVENT_ID"),
            "episode_id": r.get("EPISODE_ID"),
            "event_type": r.get("EVENT_TYPE") or None,
            "cz_type": r.get("CZ_TYPE") or None,
            "cz_fips": r.get("CZ_FIPS"),
            "cz_name": r.get("CZ_NAME"),
            "begin_local": r.get("BEGIN_DATE_TIME"),
            "cz_timezone": r.get("CZ_TIMEZONE") or None,
            "begin_utc": to_utc(r.get("BEGIN_YEARMONTH"), r.get("BEGIN_DAY"), r.get("BEGIN_TIME"), r.get("CZ_TIMEZONE")),
            "end_utc": to_utc(r.get("END_YEARMONTH"), r.get("END_DAY"), r.get("END_TIME"), r.get("CZ_TIMEZONE")),
            "begin_lat": r.get("BEGIN_LAT"),
            "begin_lon": r.get("BEGIN_LON"),
            "magnitude": magnitude,
            "magnitude_unit": unit,
            "magnitude_type": r.get("MAGNITUDE_TYPE") or None,
            "damage_property_usd": parse_damage(r.get("DAMAGE_PROPERTY")),
            "damage_crops_usd": parse_damage(r.get("DAMAGE_CROPS")),
            "deaths_direct": r.get("DEATHS_DIRECT"),
            "injuries_direct": r.get("INJURIES_DIRECT"),
            "narrative_clean": text.clean_text(r.get("EVENT_NARRATIVE")),
            "source_file": r.get("_source_file"),
        })
    df = pd.DataFrame(rows, columns=[c for c in SCHEMA if c not in common.METADATA_COLUMNS])
    for column in ["event_id", "episode_id", "cz_fips", "deaths_direct", "injuries_direct"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    for column in ["begin_lat", "begin_lon", "magnitude", "damage_property_usd", "damage_crops_usd"]:
        df[column] = common.to_number(df[column])
    lat_bad = common.null_out_of_range(df, "begin_lat", -90, 90)
    lon_bad = common.null_out_of_range(df, "begin_lon", -180, 180)
    df.loc[lat_bad | lon_bad, ["begin_lat", "begin_lon"]] = float("nan")

    # Damage spans several orders of magnitude, so the quartiles are computed on log10 of positive values.
    log_damage = df["damage_property_usd"].where(df["damage_property_usd"] > 0).map(
        lambda v: math.log10(v) if pd.notna(v) else float("nan"))
    df["damage_outlier"] = common.iqr_outliers(log_damage, df["event_type"])
    return df
