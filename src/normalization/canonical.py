"""First draft of the canonical mapping (Workshop 1).

Canonical ``event`` row (one row per source record / measurement):
    event_id, source, event_type, timestamp (UTC), latitude, longitude,
    severity, severity_unit, raw_reference, content_hash
The functions below implement the rules documented in ``workshop_1/mapping.md``.
"""
from __future__ import annotations

import hashlib
import json
import re

import numpy as np
import pandas as pd

KNOT_TO_MS = 0.514444
INCH_TO_MM = 25.4
MPH_TO_MS = 0.44704
KMH_TO_MS = 1 / 3.6

# Storm Events time-zone codes -> UTC offset in hours (standard and daylight variants appear
# across the 1950-2026 archive, sometimes in lower/mixed case).
TZ_OFFSETS = {
    "AST": -4, "EST": -5, "EDT": -4, "CST": -6, "CDT": -5, "MST": -7, "MDT": -6,
    "PST": -8, "PDT": -7, "AKST": -9, "HST": -10, "SST": -11, "GST": 10,
}

CAP_SEVERITY = {"Unknown": 0, "Minor": 1, "Moderate": 2, "Severe": 3, "Extreme": 4}

# Station local standard time offset (GHCN-Daily "day" at first-order stations is local standard time).
STATION_LST = {"USW00023044": -7}  # El Paso is in Mountain Time; all others default to -6


def snake(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")


def content_hash(record: dict) -> str:
    """SHA-256 of the raw record serialised with sorted keys: used for change detection."""
    payload = json.dumps(record, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_tz_offset(code: str | float) -> float:
    """'CST-6' -> -6, 'CDT' -> -5, 'cst' -> -6, unknown -> NaN."""
    if not isinstance(code, str) or not code.strip():
        return np.nan
    m = re.match(r"^\s*([A-Za-z]{3,4})\s*(-?\d+)?\s*$", code)
    if not m:
        return np.nan
    if m.group(2) is not None:
        return float(m.group(2))
    return float(TZ_OFFSETS.get(m.group(1).upper(), np.nan))


def parse_damage(text: str | float) -> float:
    """'10.00K' -> 10000.0, '1.5M' -> 1.5e6, '0.00K' -> 0.0, '' -> NaN."""
    if not isinstance(text, str) or not text.strip():
        return np.nan
    m = re.match(r"^\s*([0-9.]*)\s*([HKMB]?)\s*$", text.upper())
    if not m:
        return np.nan
    num = float(m.group(1)) if m.group(1) else 1.0
    return num * {"": 1, "H": 1e2, "K": 1e3, "M": 1e6, "B": 1e9}[m.group(2)]


def storm_events_local_to_utc(df: pd.DataFrame, prefix: str = "BEGIN") -> pd.Series:
    """Use YEARMONTH + DAY + TIME (4-digit year) instead of the ambiguous dd-MON-yy text field."""
    ym = df[f"{prefix}_YEARMONTH"].astype(str)
    day = df[f"{prefix}_DAY"].astype(str).str.zfill(2)
    hhmm = df[f"{prefix}_TIME"].astype(str).str.zfill(4)
    local = pd.to_datetime(ym + day + hhmm, format="%Y%m%d%H%M", errors="coerce")
    offset = df["CZ_TIMEZONE"].map(parse_tz_offset)
    return (local - pd.to_timedelta(offset, unit="h")).dt.tz_localize("UTC")


def storm_event_severity(row: pd.Series) -> tuple[float, str]:
    """Physical magnitude when the event type has one; otherwise an impact level 0-3."""
    et, mag, mtype = row["EVENT_TYPE"], row.get("MAGNITUDE"), row.get("MAGNITUDE_TYPE")
    mag = pd.to_numeric(mag, errors="coerce")
    if et == "Hail" and pd.notna(mag):
        return mag * INCH_TO_MM, "mm (hail diameter)"
    if et in {"Thunderstorm Wind", "High Wind", "Strong Wind", "Marine Thunderstorm Wind"} and pd.notna(mag):
        return mag * KNOT_TO_MS, f"m/s (wind, {mtype or 'unknown type'})"
    if et == "Tornado" and isinstance(row.get("TOR_F_SCALE"), str):
        digits = re.sub(r"\D", "", row["TOR_F_SCALE"])
        if digits:
            return float(digits), "EF scale"
    casualties = sum(pd.to_numeric(row.get(c), errors="coerce") or 0 for c in
                     ["DEATHS_DIRECT", "DEATHS_INDIRECT", "INJURIES_DIRECT", "INJURIES_INDIRECT"])
    damage = (np.nan_to_num(parse_damage(row.get("DAMAGE_PROPERTY")))
              + np.nan_to_num(parse_damage(row.get("DAMAGE_CROPS"))))
    level = 3 if casualties >= 10 or damage >= 1e7 else 2 if casualties > 0 or damage >= 1e5 else \
        1 if damage > 0 else 0
    return float(level), "impact level 0-3"


def map_storm_events(df: pd.DataFrame) -> pd.DataFrame:
    sev = df.apply(storm_event_severity, axis=1, result_type="expand")
    return pd.DataFrame({
        "event_id": "noaa_storm_events:" + df["EVENT_ID"].astype(str),
        "source": "noaa_storm_events",
        "event_type": df["EVENT_TYPE"].map(snake),
        "timestamp": storm_events_local_to_utc(df),
        "latitude": pd.to_numeric(df["BEGIN_LAT"], errors="coerce"),
        "longitude": pd.to_numeric(df["BEGIN_LON"], errors="coerce"),
        "severity": sev[0],
        "severity_unit": sev[1],
        "raw_reference": "EVENT_ID=" + df["EVENT_ID"].astype(str),
        "content_hash": [content_hash(r) for r in df.to_dict("records")],
    })


GHCN_ELEMENTS = {  # element -> (event_type, unit) after units=metric
    "TMAX": ("daily_max_temperature", "degC"),
    "TMIN": ("daily_min_temperature", "degC"),
    "PRCP": ("daily_precipitation", "mm"),
    "SNOW": ("daily_snowfall", "mm"),
    "SNWD": ("snow_depth", "mm"),
    "AWND": ("daily_mean_wind_speed", "m/s"),
    "WSF2": ("daily_fastest_2min_wind", "m/s"),
    "WSF5": ("daily_fastest_5s_wind", "m/s"),
}


def map_ghcn_daily(df: pd.DataFrame) -> pd.DataFrame:
    """One canonical event per (station, date, element) measurement."""
    frames = []
    for element, (etype, unit) in GHCN_ELEMENTS.items():
        if element not in df:
            continue
        sub = df[df[element].notna()]
        offset = sub["STATION"].map(STATION_LST).fillna(-6)
        ts = (pd.to_datetime(sub["DATE"]) - pd.to_timedelta(offset, unit="h")).dt.tz_localize("UTC")
        frames.append(pd.DataFrame({
            "event_id": "ghcn_daily:" + sub["STATION"] + ":" + sub["DATE"] + ":" + element,
            "source": "ghcn_daily",
            "event_type": etype,
            "timestamp": ts,
            "latitude": pd.to_numeric(sub["LATITUDE"]),
            "longitude": pd.to_numeric(sub["LONGITUDE"]),
            "severity": pd.to_numeric(sub[element], errors="coerce"),
            "severity_unit": unit,
            "raw_reference": sub["STATION"] + "/" + sub["DATE"] + "/" + element,
            "content_hash": [content_hash({"v": v, "a": a}) for v, a in
                             zip(sub[element], sub.get(f"{element}_ATTRIBUTES", ""))],
        }))
    return pd.concat(frames, ignore_index=True)


def map_nws_alerts(df: pd.DataFrame) -> pd.DataFrame:
    def centroid(geom):
        if not geom or geom.get("type") != "Polygon":
            return (np.nan, np.nan)
        ring = np.array(geom["coordinates"][0])
        return (ring[:, 1].mean(), ring[:, 0].mean())

    cen = df["geometry"].map(centroid)
    return pd.DataFrame({
        "event_id": "nws_alerts:" + df["id"],
        "source": "nws_alerts",
        "event_type": df["event"].map(snake),
        "timestamp": pd.to_datetime(df["effective"], utc=True),
        "latitude": cen.map(lambda c: c[0]),
        "longitude": cen.map(lambda c: c[1]),
        "severity": df["severity"].map(CAP_SEVERITY).astype(float),
        "severity_unit": "CAP severity 0-4",
        "raw_reference": df["@id"],
        "content_hash": [content_hash(r) for r in df.drop(columns=["raw_file"], errors="ignore")
                         .to_dict("records")],
    })


NWS_VARIABLES = {  # property -> (event_type, canonical unit)
    "temperature": ("air_temperature", "degC"),
    "windSpeed": ("wind_speed", "m/s"),
    "windGust": ("wind_gust", "m/s"),
    "precipitationLastHour": ("precipitation_1h", "mm"),
    "heatIndex": ("heat_index", "degC"),
    "windChill": ("wind_chill", "degC"),
}

# Conversion is driven by each value's own unitCode, never assumed per field.
WMO_UNIT_FACTOR = {
    "wmoUnit:degC": ("degC", 1.0),
    "wmoUnit:km_h-1": ("m/s", KMH_TO_MS),
    "wmoUnit:m_s-1": ("m/s", 1.0),
    "wmoUnit:mm": ("mm", 1.0),
    "wmoUnit:m": ("mm", 1000.0),
}


def _convert(values: pd.Series, units: pd.Series, target: str) -> pd.Series:
    out = []
    for v, u in zip(values, units):
        unit, factor = WMO_UNIT_FACTOR.get(u, (None, np.nan))
        out.append(float(v) * factor if unit == target else np.nan)
    return pd.Series(out, index=values.index)


def map_nws_observations(df: pd.DataFrame) -> pd.DataFrame:
    frames = []
    lon = df["geometry"].map(lambda g: g["coordinates"][0] if g else np.nan)
    lat = df["geometry"].map(lambda g: g["coordinates"][1] if g else np.nan)
    for prop, (etype, unit) in NWS_VARIABLES.items():
        col = f"{prop}.value"
        if col not in df:
            continue
        mask = df[col].notna()
        sub = df[mask]
        frames.append(pd.DataFrame({
            "event_id": "nws_observations:" + sub["stationId"] + ":" + sub["timestamp"] + ":" + prop,
            "source": "nws_observations",
            "event_type": etype,
            "timestamp": pd.to_datetime(sub["timestamp"], utc=True),
            "latitude": lat[mask],
            "longitude": lon[mask],
            "severity": _convert(sub[col], sub[f"{prop}.unitCode"], unit),
            "severity_unit": unit,
            "raw_reference": sub["@id"],
            "content_hash": [content_hash({"v": v, "qc": q}) for v, q in
                             zip(sub[col], sub[f"{prop}.qualityControl"])],
        }))
    return pd.concat(frames, ignore_index=True)
