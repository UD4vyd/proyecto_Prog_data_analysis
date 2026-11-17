"""Silver preprocessing rules. Pure functions: data in, data out, no files and no network.

Every rule names the Workshop 1 issue it solves ("W1 issue", from workshop_1/data_quality_issues.csv).
Each parse function returns two DataFrames: the clean rows and the rejected rows
(columns source, bronze_file, reason, payload), which are the quarantine of the ingestion contract.
"""
import json
import re
from datetime import datetime
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa

from src.config import TEXAS_BBOX

# Physical bounds. A value outside them is impossible for Texas and goes to rejected.
TEMP_BOUNDS_C = (-35.0, 55.0)
PRCP_BOUNDS_MM = (0.0, 700.0)
WIND_BOUNDS_MS = (0.0, 120.0)

KNOTS_TO_MS = 0.514444
INCH_TO_MM = 25.4

# MADIS QC descriptors (the NWS OpenAPI document points to them for `qualityControl`):
# X = rejected/erroneous, B = subjective bad. Z, C, S, V, Q, G and T are kept.
NWS_REJECTED_QC = {"X", "B"}

REJECT_COLUMNS = ["source", "bronze_file", "reason", "payload"]
LINEAGE_COLUMNS = ["bronze_file", "processed_utc", "record_hash"]
NOT_HASHED = set(LINEAGE_COLUMNS) | {"is_statistical_outlier"}

KEYS = {
    "observations": ["station", "timestamp"],
    "alerts": ["alert_id"],
    "daily_summaries": ["station", "obs_date", "element"],
    "storm_events": ["event_id"],
}
TOPIC_SOURCE = {"storm_events": "F1", "daily_summaries": "F2", "alerts": "F3", "observations": "F4"}


class UnknownUnit(ValueError):
    pass


class SchemaError(ValueError):
    pass


# Shared helpers ----------------------------------------------------------------------------------

# (declared unit, canonical unit) -> factor. A pair that is not here is an unknown unit.
UNIT_FACTORS = {
    ("wmoUnit:degC", "degC"): 1.0,
    ("wmoUnit:percent", "percent"): 1.0,
    ("wmoUnit:degree_(angle)", "degree"): 1.0,
    ("wmoUnit:km_h-1", "m/s"): 1 / 3.6,
    ("wmoUnit:m_s-1", "m/s"): 1.0,
    ("wmoUnit:Pa", "hPa"): 0.01,
    ("wmoUnit:mm", "mm"): 1.0,
    ("wmoUnit:m", "m"): 1.0,
    ("knots", "m/s"): KNOTS_TO_MS,
    ("inches", "mm"): INCH_TO_MM,
}


def convert(value: float, declared_unit: str | None, canonical_unit: str) -> float:
    """Unit conversion driven by the unit the source declares (W1 issues: F4 value objects, F1 MAGNITUDE)."""
    factor = UNIT_FACTORS.get((declared_unit, canonical_unit))
    if factor is None:
        raise UnknownUnit(f"cannot convert {declared_unit!r} to {canonical_unit!r}")
    return float(value) * factor


def snake_case(text: str) -> str:
    """'Thunderstorm Wind' -> 'thunderstorm_wind', 'Cold/Wind Chill' -> 'cold_wind_chill'."""
    return re.sub(r"[^0-9a-z]+", "_", str(text).strip().lower()).strip("_")


def outside_texas(lat: pd.Series, lon: pd.Series) -> pd.Series:
    """True only for rows that have coordinates outside the padded Texas box. NULL stays allowed."""
    lat = pd.to_numeric(lat, errors="coerce")
    lon = pd.to_numeric(lon, errors="coerce")
    has = lat.notna() & lon.notna()
    inside = (lat.between(TEXAS_BBOX["lat_min"], TEXAS_BBOX["lat_max"])
              & lon.between(TEXAS_BBOX["lon_min"], TEXAS_BBOX["lon_max"]))
    return has & ~inside


def to_utc_ns(values) -> pd.Series:
    """ISO strings with any offset to datetime64[ns, UTC]. pandas 3 would infer microseconds."""
    return pd.to_datetime(values, utc=True, format="ISO8601").astype("datetime64[ns, UTC]")


def _split_off(df: pd.DataFrame, mask: pd.Series, reason: str, source: str,
               rejected: list[pd.DataFrame]) -> pd.DataFrame:
    """Move the rows in `mask` to the rejected list with a reason; return the rest."""
    mask = mask.reindex(df.index).fillna(False).astype(bool)
    if mask.any():
        bad = df[mask]
        rejected.append(pd.DataFrame({"source": source, "bronze_file": bad["bronze_file"].astype(str),
                                      "reason": reason, "payload": bad["_payload"].astype(str)}))
    return df[~mask]


def _rejected_frame(parts: list[pd.DataFrame]) -> pd.DataFrame:
    parts = [p for p in parts if len(p)]
    if not parts:
        return pd.DataFrame({c: pd.Series(dtype="str") for c in REJECT_COLUMNS})
    return pd.concat(parts, ignore_index=True)[REJECT_COLUMNS]


def reject_file(source: str, bronze_file: str, reason: str, payload: str) -> pd.DataFrame:
    """A whole Bronze file that cannot be used (bad status or unreadable) becomes one rejected row."""
    return pd.DataFrame([{"source": source, "bronze_file": bronze_file, "reason": reason,
                          "payload": payload[:2000]}])


# F4 NWS observations -------------------------------------------------------------------------------

# NWS property -> (Silver column, canonical unit). QC column is <column without unit>_qc.
F4_VARIABLES = {
    "temperature": ("temperature_c", "degC"),
    "dewpoint": ("dewpoint_c", "degC"),
    "relativeHumidity": ("relative_humidity_pct", "percent"),
    "windDirection": ("wind_direction_deg", "degree"),
    "windSpeed": ("wind_speed_ms", "m/s"),
    "windGust": ("wind_gust_ms", "m/s"),
    "barometricPressure": ("pressure_hpa", "hPa"),
    "precipitationLastHour": ("precip_1h_mm", "mm"),
    "heatIndex": ("heat_index_c", "degC"),
    "windChill": ("wind_chill_c", "degC"),
}


def qc_column(value_column: str) -> str:
    return value_column.rsplit("_", 1)[0] + "_qc"


def parse_observations(data: dict, bronze_file: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """F4 GeoJSON to one row per (station, timestamp).

    W1 issues: values are {value, unitCode, qualityControl} objects (flatten, convert by unitCode,
    keep the QC code); coordinates are [lon, lat]; nulls are `value: null` and stay NULL.
    """
    rows, rejected = [], []
    for feature in data.get("features", []):
        props = feature.get("properties") or {}
        payload = json.dumps(feature)
        row = {"station": props.get("stationId"), "timestamp": props.get("timestamp"),
               "bronze_file": bronze_file, "_payload": payload, "_reason": None}
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None]
        row["longitude"], row["latitude"] = coords[0], coords[1]  # GeoJSON order is [lon, lat]
        elevation = props.get("elevation") or {}
        row["elevation_m"] = elevation.get("value")
        for prop, (column, unit) in F4_VARIABLES.items():
            obj = props.get(prop) or {}  # precipitationLastHour is absent when it does not apply
            value, qc = obj.get("value"), obj.get("qualityControl")
            row[qc_column(column)] = qc
            row[column] = None
            if value is None:
                continue
            try:
                row[column] = convert(value, obj.get("unitCode"), unit)
            except UnknownUnit:
                row["_reason"] = row["_reason"] or "unknown_unit"
            if qc in NWS_REJECTED_QC:
                row["_reason"] = row["_reason"] or "failed_nws_qc"
        rows.append(row)
    if not rows:
        return pd.DataFrame(), _rejected_frame([])
    df = pd.DataFrame(rows)
    df = _split_off(df, df["station"].isna() | df["timestamp"].isna(), "missing_key", "F4", rejected)
    for reason in ("unknown_unit", "failed_nws_qc"):
        df = _split_off(df, df["_reason"] == reason, reason, "F4", rejected)
    df = _split_off(df, outside_texas(df["latitude"], df["longitude"]), "out_of_scope", "F4", rejected)
    numeric = ["latitude", "longitude", "elevation_m"] + [c for c, _ in F4_VARIABLES.values()]
    df[numeric] = df[numeric].apply(pd.to_numeric, errors="coerce").astype("float64")
    out = ~df["temperature_c"].between(*TEMP_BOUNDS_C) & df["temperature_c"].notna()
    for column in ("wind_speed_ms", "wind_gust_ms"):
        out |= ~df[column].between(*WIND_BOUNDS_MS) & df[column].notna()
    out |= ~df["precip_1h_mm"].between(*PRCP_BOUNDS_MM) & df["precip_1h_mm"].notna()
    df = _split_off(df, out, "out_of_physical_bounds", "F4", rejected)
    if df.empty:
        return pd.DataFrame(), _rejected_frame(rejected)
    df = df.drop(columns=["_payload", "_reason"]).copy()
    df["timestamp"] = to_utc_ns(df["timestamp"])  # already UTC (+00:00) in F4
    df["station"] = df["station"].astype("category")
    return df.reset_index(drop=True), _rejected_frame(rejected)


# F3 NWS alerts ------------------------------------------------------------------------------------

def polygon_centroid(geometry: dict | None) -> tuple[float | None, float | None]:
    """Area centroid of a Polygon or MultiPolygon (outer rings). Returns (lat, lon).

    GeoJSON positions are [lon, lat], so x is longitude and y is latitude.
    """
    if not geometry or geometry.get("type") not in ("Polygon", "MultiPolygon"):
        return None, None
    polygons = [geometry["coordinates"]] if geometry["type"] == "Polygon" else geometry["coordinates"]
    total_area = cx = cy = 0.0
    points = []
    for polygon in polygons:
        ring = np.asarray(polygon[0], dtype=float)
        points.append(ring)
        x, y = ring[:, 0], ring[:, 1]
        cross = x[:-1] * y[1:] - x[1:] * y[:-1]
        area = cross.sum() / 2
        if area:
            cx += ((x[:-1] + x[1:]) * cross).sum() / 6
            cy += ((y[:-1] + y[1:]) * cross).sum() / 6
            total_area += area
    if total_area:
        return cy / total_area, cx / total_area
    allpoints = np.vstack(points)
    return float(allpoints[:, 1].mean()), float(allpoints[:, 0].mean())


ALERT_TIMES = ["sent", "effective", "onset", "expires", "ends"]


def parse_alerts(data: dict, bronze_file: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """F3 GeoJSON (CAP fields) to one row per alert id.

    W1 issues: timestamps carry local offsets (parse with the offset, store UTC, keep the offset);
    44.8% of alerts have no geometry (keep NULL coordinates and the UGC zone list);
    coordinates are [lon, lat].
    """
    rows, rejected = [], []
    for feature in data.get("features", []):
        props = feature.get("properties") or {}
        lat, lon = polygon_centroid(feature.get("geometry"))
        sent = props.get("sent")
        rows.append({
            "alert_id": props.get("id"),
            "message_type": props.get("messageType"),
            "status": props.get("status"),
            "event": props.get("event"),
            "severity": props.get("severity"),
            "certainty": props.get("certainty"),
            "urgency": props.get("urgency"),
            **{name: props.get(name) for name in ALERT_TIMES},
            "sent_utc_offset": sent[-6:] if isinstance(sent, str) and len(sent) > 6 and sent[-6] in "+-" else None,
            "sender_name": props.get("senderName"),
            "area_desc": (props.get("areaDesc") or "").strip().upper() or None,
            "ugc_codes": list((props.get("geocode") or {}).get("UGC") or []),
            "references": [r.get("identifier") for r in props.get("references") or []],
            "has_geometry": feature.get("geometry") is not None,
            "latitude": lat,
            "longitude": lon,
            "bronze_file": bronze_file,
            "_payload": json.dumps(feature),
        })
    if not rows:
        return pd.DataFrame(), _rejected_frame([])
    df = pd.DataFrame(rows)
    df = _split_off(df, df["alert_id"].isna() | df["sent"].isna() | df["message_type"].isna(),
                    "missing_key", "F3", rejected)
    df = _split_off(df, outside_texas(df["latitude"], df["longitude"]), "out_of_scope", "F3", rejected)
    if df.empty:
        return pd.DataFrame(), _rejected_frame(rejected)
    df = df.drop(columns=["_payload"]).copy()
    for name in ALERT_TIMES:
        df[name] = to_utc_ns(df[name])
    df[["latitude", "longitude"]] = df[["latitude", "longitude"]].astype("float64")
    for name in ("message_type", "status", "event", "severity", "certainty", "urgency"):
        df[name] = df[name].astype("category")
    return df.reset_index(drop=True), _rejected_frame(rejected)


# F2 GHCN-Daily ------------------------------------------------------------------------------------

F2_ELEMENTS = {"TMAX": "degC", "TMIN": "degC", "PRCP": "mm"}  # with units=metric


def declared_units(url: str) -> str | None:
    """F2 units come from the request itself (`units=metric` in the URL saved in .headers.json)."""
    return (parse_qs(urlparse(url).query).get("units") or [None])[0]


def standard_offset_hours(tz_name: str) -> float:
    """UTC offset of the zone without daylight saving (America/Chicago -6, America/Denver -7)."""
    moment = datetime(2001, 1, 15, 12, tzinfo=ZoneInfo(tz_name))
    return (moment.utcoffset() - moment.dst()).total_seconds() / 3600


def split_attributes(attrs: pd.Series) -> pd.DataFrame:
    """'T,,W' -> mflag 'T', qflag '', sflag 'W' (W1 issue: _ATTRIBUTES holds three flags)."""
    parts = attrs.fillna("").astype(str).str.split(",", expand=True)
    for i in range(3):
        if i not in parts.columns:
            parts[i] = ""
    parts = parts[[0, 1, 2]].fillna("")
    parts.columns = ["mflag", "qflag", "sflag"]
    return parts.apply(lambda s: s.str.strip())


def parse_daily_summaries(records: list[dict], bronze_file: str, units: str | None,
                          stations: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """F2 JSON to long format, one row per (station, obs_date, element).

    W1 issues: values are strings (explicit to_numeric, failures rejected as not_numeric);
    trace days are PRCP 0.0 with mflag T (keep 0.0, add is_trace); missing days are absent rows
    (Silver does not invent them); non-blank qflag means the value failed NCEI QC.
    """
    rejected = []
    if not records:
        return pd.DataFrame(), _rejected_frame([])
    if units != "metric":
        return pd.DataFrame(), _rejected_frame([pd.DataFrame(
            {"source": "F2", "bronze_file": bronze_file, "reason": "unknown_unit",
             "payload": [json.dumps(r) for r in records]})])
    raw = pd.DataFrame(records)
    raw["_payload"] = [json.dumps(r) for r in records]
    long_parts = []
    for element, unit in F2_ELEMENTS.items():
        if element not in raw.columns:
            continue
        part = raw[raw[element].notna()]
        flags = split_attributes(part.get(f"{element}_ATTRIBUTES", pd.Series("", index=part.index)))
        long_parts.append(pd.DataFrame({
            "station": part["STATION"], "obs_date": part["DATE"], "element": element,
            "raw_value": part[element].astype(str), "unit": unit,
            "mflag": flags["mflag"], "qflag": flags["qflag"], "sflag": flags["sflag"],
            "_payload": [json.dumps({"element": element, "record": json.loads(p)}) for p in part["_payload"]],
        }))
    df = pd.concat(long_parts, ignore_index=True)
    df["bronze_file"] = bronze_file
    df = _split_off(df, df["station"].isna() | df["obs_date"].isna(), "missing_key", "F2", rejected)
    df = _split_off(df, ~df["station"].isin(stations["ghcn_id"]), "out_of_scope", "F2", rejected)
    df["value"] = pd.to_numeric(df["raw_value"], errors="coerce")
    df = _split_off(df, df["value"].isna(), "not_numeric", "F2", rejected)
    df = _split_off(df, df["qflag"] != "", "failed_ncei_qc", "F2", rejected)
    is_temp = df["element"].isin(["TMAX", "TMIN"])
    out = (is_temp & ~df["value"].between(*TEMP_BOUNDS_C)) | (~is_temp & ~df["value"].between(*PRCP_BOUNDS_MM))
    df = _split_off(df, out, "out_of_physical_bounds", "F2", rejected)

    meta = stations.set_index("ghcn_id")
    df = df.copy()
    df["latitude"] = pd.to_numeric(df["station"].map(meta["lat"]), errors="coerce").astype("float64")
    df["longitude"] = pd.to_numeric(df["station"].map(meta["lon"]), errors="coerce").astype("float64")
    df = _split_off(df, outside_texas(df["latitude"], df["longitude"]), "out_of_scope", "F2", rejected)
    if df.empty:
        return pd.DataFrame(), _rejected_frame(rejected)
    df = df.drop(columns=["_payload", "raw_value"]).copy()
    offsets = {s: standard_offset_hours(tz) for s, tz in meta["timezone"].items()}
    day = pd.to_datetime(df["obs_date"], format="%Y-%m-%d")
    df["obs_date"] = day.dt.date
    # local standard midnight in UTC: Chicago (-6) gives 06:00 UTC, Denver (-7) gives 07:00 UTC
    shift = pd.to_timedelta(-df["station"].map(offsets), unit="h")
    df["timestamp"] = (day + shift).dt.tz_localize("UTC").astype("datetime64[ns, UTC]")
    df["is_trace"] = (df["element"] == "PRCP") & (df["mflag"] == "T")
    for name in ("station", "element", "unit"):
        df[name] = df[name].astype("category")
    df["value"] = df["value"].astype("float64")
    return df.reset_index(drop=True), _rejected_frame(rejected)


# F1 Storm Events ----------------------------------------------------------------------------------

HAIL_TYPES = {"Hail", "Marine Hail"}
WIND_TYPES = {"Thunderstorm Wind", "High Wind", "Strong Wind",
              "Marine Thunderstorm Wind", "Marine High Wind", "Marine Strong Wind"}
CZ_TIMEZONE_RE = r"^[A-Z]{3}-(\d{1,2})$"  # CST-6, MST-7: fixed offset, never a DST-aware zone
DAMAGE_RE = r"^(\d+(?:\.\d+)?)([KMB]?)$"
DAMAGE_FACTOR = {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9}


def f1_local_time(yearmonth: pd.Series, day: pd.Series, hhmm: pd.Series) -> pd.Series:
    """Local standard time from BEGIN_YEARMONTH, BEGIN_DAY, BEGIN_TIME (4-digit year).

    W1 issue: BEGIN_DATE_TIME has a 2-digit year ('09-JAN-25 20:00:00'), so we do not use it.
    """
    text = yearmonth.str.strip() + day.str.strip().str.zfill(2) + hhmm.str.strip().str.zfill(4)
    return pd.to_datetime(text, format="%Y%m%d%H%M", errors="coerce").astype("datetime64[ns]")


def f1_offset_hours(cz_timezone: pd.Series) -> pd.Series:
    """'CST-6' -> 6.0. UTC = local + 6 h. Unknown formats give NaN."""
    return pd.to_numeric(cz_timezone.str.extract(CZ_TIMEZONE_RE)[0], errors="coerce")


def f1_to_utc(local: pd.Series, offset_hours: pd.Series) -> pd.Series:
    return (local + pd.to_timedelta(offset_hours, unit="h")).dt.tz_localize("UTC").astype("datetime64[ns, UTC]")


def parse_damage(text: pd.Series) -> tuple[pd.Series, pd.Series]:
    """'10.00K' -> 10000.0, '1.50M' -> 1500000.0. Blank stays NULL (unknown, not 0).

    Returns (values, bad) where bad marks non-blank text that does not parse.
    """
    clean = text.fillna("").str.strip().str.upper()
    parts = clean.str.extract(DAMAGE_RE)
    values = pd.to_numeric(parts[0], errors="coerce") * parts[1].map(DAMAGE_FACTOR)
    bad = (clean != "") & values.isna()
    return values.astype("float64"), bad


def f1_magnitude_unit(event_type: pd.Series) -> pd.Series:
    """W1 issue: MAGNITUDE units depend on EVENT_TYPE (hail inches, wind knots, others none)."""
    unit = pd.Series(None, index=event_type.index, dtype="object")
    unit[event_type.isin(HAIL_TYPES)] = "inches"
    unit[event_type.isin(WIND_TYPES)] = "knots"
    return unit


def parse_storm_events(raw: pd.DataFrame, bronze_file: str) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """F1 details CSV (all columns as text) to one row per EVENT_ID, Texas only.

    Returns (rows, rejected, rows_outside_texas). Rows of other states are out of the project scope,
    not bad data, so they are counted but not sent to quarantine.
    """
    rejected = []
    texas = raw[raw["STATE_FIPS"].str.strip() == "48"].copy()  # W1: filter by FIPS, not by text
    outside = len(raw) - len(texas)
    if texas.empty:
        return pd.DataFrame(), _rejected_frame([]), outside
    texas["bronze_file"] = bronze_file
    texas["_payload"] = texas.drop(columns=["bronze_file"]).to_dict(orient="records")
    texas["_payload"] = texas["_payload"].map(json.dumps)

    texas = _split_off(texas, texas["EVENT_ID"].str.strip() == "", "missing_key", "F1", rejected)
    offset = f1_offset_hours(texas["CZ_TIMEZONE"])
    texas = _split_off(texas, offset.isna(), "unknown_timezone", "F1", rejected)
    begin_local = f1_local_time(texas["BEGIN_YEARMONTH"], texas["BEGIN_DAY"], texas["BEGIN_TIME"])
    texas = _split_off(texas, begin_local.isna(), "invalid_time", "F1", rejected)

    damage_property, bad_property = parse_damage(texas["DAMAGE_PROPERTY"])
    damage_crops, bad_crops = parse_damage(texas["DAMAGE_CROPS"])
    texas = _split_off(texas, bad_property | bad_crops, "not_numeric", "F1", rejected)

    unit = f1_magnitude_unit(texas["EVENT_TYPE"])
    magnitude = pd.to_numeric(texas["MAGNITUDE"].replace("", np.nan), errors="coerce")
    has_text = texas["MAGNITUDE"].str.strip() != ""
    texas = _split_off(texas, has_text & magnitude.isna(), "not_numeric", "F1", rejected)
    texas = _split_off(texas, has_text & unit.reindex(texas.index).isna(), "unknown_unit", "F1", rejected)

    unit = unit.reindex(texas.index)
    magnitude = magnitude.reindex(texas.index)
    canonical = unit.map({"inches": "mm", "knots": "m/s"})
    factor = unit.map({"inches": INCH_TO_MM, "knots": KNOTS_TO_MS})
    magnitude_si = magnitude * factor
    is_wind = canonical == "m/s"
    texas = _split_off(texas, is_wind & magnitude_si.notna() & ~magnitude_si.between(*WIND_BOUNDS_MS),
                       "out_of_physical_bounds", "F1", rejected)
    texas = _split_off(texas, outside_texas(texas["BEGIN_LAT"].replace("", np.nan),
                                            texas["BEGIN_LON"].replace("", np.nan)),
                       "out_of_scope", "F1", rejected)

    if texas.empty:
        return pd.DataFrame(), _rejected_frame(rejected), outside
    idx = texas.index
    offset, begin_local = offset.reindex(idx), begin_local.reindex(idx)
    end_local = f1_local_time(texas["END_YEARMONTH"], texas["END_DAY"], texas["END_TIME"])
    cz_fips = texas["CZ_FIPS"].str.strip().str.zfill(3)
    out = pd.DataFrame({
        "event_id": pd.to_numeric(texas["EVENT_ID"]).astype("int64"),
        "episode_id": pd.to_numeric(texas["EPISODE_ID"], errors="coerce").astype("Int64"),
        "event_type": texas["EVENT_TYPE"].map(snake_case).astype("category"),
        "cz_type": texas["CZ_TYPE"].str.strip().astype("category"),
        "cz_fips": cz_fips,
        # zone_code matches station.county_zone (TXC027) or station.forecast_zone (TXZ158)
        "zone_code": "TX" + texas["CZ_TYPE"].str.strip() + cz_fips,
        "cz_name": texas["CZ_NAME"].str.strip().str.upper(),
        "wfo": texas["WFO"].str.strip(),
        "cz_timezone": texas["CZ_TIMEZONE"].astype("category"),
        "begin_local": begin_local,
        "begin_utc": f1_to_utc(begin_local, offset),
        "end_utc": f1_to_utc(end_local, offset),
        # zone rows (CZ_TYPE=Z) have no coordinates: they stay NULL, no invented points
        "latitude": pd.to_numeric(texas["BEGIN_LAT"].replace("", np.nan), errors="coerce").astype("float64"),
        "longitude": pd.to_numeric(texas["BEGIN_LON"].replace("", np.nan), errors="coerce").astype("float64"),
        "magnitude_si": magnitude_si.reindex(idx).astype("float64"),
        "magnitude_unit": canonical.reindex(idx).astype("category"),
        "magnitude_type": texas["MAGNITUDE_TYPE"].str.strip().pipe(lambda s: s.where(s != "")),
        "damage_property_usd": damage_property.reindex(idx),
        "damage_crops_usd": damage_crops.reindex(idx),
        "injuries_direct": pd.to_numeric(texas["INJURIES_DIRECT"], errors="coerce").astype("Int64"),
        "deaths_direct": pd.to_numeric(texas["DEATHS_DIRECT"], errors="coerce").astype("Int64"),
        "bronze_file": bronze_file,
    })
    return out.reset_index(drop=True), _rejected_frame(rejected), outside


# Deduplication and revisions -----------------------------------------------------------------------

def record_hash(df: pd.DataFrame) -> pd.Series:
    """Hash of every content column (not lineage), used to tell duplicates from revisions."""
    columns = sorted(c for c in df.columns if c not in NOT_HASHED)
    text = df[columns].astype("string").fillna("<NA>")
    return pd.util.hash_pandas_object(text, index=False).map(lambda h: f"{h:016x}").astype("str")


def dedupe_keep_last(df: pd.DataFrame, keys: list[str]) -> tuple[pd.DataFrame, int]:
    """Within one run the last copy wins (files are processed in time order)."""
    out = df.drop_duplicates(subset=keys, keep="last")
    return out.reset_index(drop=True), len(df) - len(out)


def _key_frame(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    return df[keys].astype("string")


def compare_with_previous(new: pd.DataFrame, previous: pd.DataFrame, keys: list[str]
                          ) -> tuple[pd.DataFrame, int, pd.DataFrame]:
    """Drop rows already in Silver with the same content; keep changed rows as revisions.

    Returns (rows to write, unchanged duplicates dropped, revised keys).
    """
    if previous is None or previous.empty or new.empty:
        return new, 0, pd.DataFrame(columns=keys)
    latest = previous.sort_values("processed_utc").drop_duplicates(subset=keys, keep="last")
    known = _key_frame(latest, keys).assign(_old_hash=latest["record_hash"].astype(str).values)
    probe = _key_frame(new, keys).assign(_new_hash=new["record_hash"].astype(str).values, _pos=range(len(new)))
    merged = probe.merge(known, on=keys, how="left")
    same = merged["_old_hash"] == merged["_new_hash"]
    revised = merged["_old_hash"].notna() & ~same
    keep_pos = merged.loc[~same, "_pos"].to_numpy()
    return new.iloc[np.sort(keep_pos)].reset_index(drop=True), int(same.sum()), merged.loc[revised, keys]


# Statistical outliers (flag only, never removed) -----------------------------------------------------

def iqr_flags(values: pd.Series, groups: pd.DataFrame, history_values: pd.Series | None = None,
              history_groups: pd.DataFrame | None = None, k: float = 1.5, min_n: int = 10) -> pd.Series:
    """Tukey fences per group: outside [Q1 - k*IQR, Q3 + k*IQR] is True.

    The fences use the new values plus the history already in Silver. A group with fewer than
    `min_n` values gives NA, because a few values cannot say what is unusual.
    """
    group_cols = list(groups.columns)
    pool = pd.concat([groups.assign(_v=values.values),
                      history_groups.assign(_v=history_values.values) if history_values is not None
                      else groups.iloc[0:0].assign(_v=pd.Series(dtype="float64"))], ignore_index=True)
    pool = pool.dropna(subset=["_v"])
    for c in group_cols:
        pool[c] = pool[c].astype("string")
    stats = pool.groupby(group_cols)["_v"].agg(q1=lambda s: s.quantile(0.25), q3=lambda s: s.quantile(0.75),
                                               n="count").reset_index()
    probe = groups.astype("string").reset_index(drop=True).merge(stats, on=group_cols, how="left")
    iqr = probe["q3"] - probe["q1"]
    low, high = probe["q1"] - k * iqr, probe["q3"] + k * iqr
    v = values.reset_index(drop=True)
    flag = pd.Series(((v < low) | (v > high)).values, dtype="boolean")
    flag[(probe["n"].fillna(0) < min_n).values | v.isna().values] = pd.NA
    return flag


def flag_daily_outliers(new: pd.DataFrame, history: pd.DataFrame | None) -> pd.Series:
    """F2: per station, calendar month and element. PRCP uses wet days only (value > 0, not trace),
    because most days are dry and the IQR of all days would be 0. Dry and trace days are False."""
    def prep(df):
        g = pd.DataFrame({"station": df["station"].astype(str).values,
                          "month": pd.to_datetime(df["obs_date"]).dt.month.astype(str).values,
                          "element": df["element"].astype(str).values})
        v = df["value"].astype("float64").reset_index(drop=True)
        wet = (g["element"] != "PRCP") | ((v > 0) & ~df["is_trace"].astype(bool).reset_index(drop=True))
        return g, v.where(wet)
    g_new, v_new = prep(new)
    g_hist, v_hist = prep(history) if history is not None and len(history) else (None, None)
    flag = iqr_flags(v_new, g_new, v_hist, g_hist)
    dry = (g_new["element"] == "PRCP") & v_new.isna() & new["value"].reset_index(drop=True).notna()
    flag[dry.values] = False
    return flag


def flag_observation_outliers(new: pd.DataFrame, history: pd.DataFrame | None) -> pd.Series:
    """F4: temperature per station and calendar month (UTC), with the observations already in Silver."""
    def prep(df):
        g = pd.DataFrame({"station": df["station"].astype(str).values,
                          "month": df["timestamp"].dt.month.astype(str).values})
        return g, df["temperature_c"].astype("float64").reset_index(drop=True)
    g_new, v_new = prep(new)
    g_hist, v_hist = prep(history) if history is not None and len(history) else (None, None)
    return iqr_flags(v_new, g_new, v_hist, g_hist)


# Schemas ------------------------------------------------------------------------------------------

CATEGORY = pa.dictionary(pa.int32(), pa.string())
UTC_NS = pa.timestamp("ns", tz="UTC")
LINEAGE_FIELDS = [pa.field("bronze_file", pa.string(), nullable=False),
                  pa.field("processed_utc", UTC_NS, nullable=False),
                  pa.field("record_hash", pa.string(), nullable=False)]

SCHEMAS = {
    "observations": pa.schema(
        [pa.field("station", CATEGORY, nullable=False), pa.field("timestamp", UTC_NS, nullable=False),
         pa.field("latitude", pa.float64()), pa.field("longitude", pa.float64()),
         pa.field("elevation_m", pa.float64())]
        + [pa.field(c, pa.float64()) for c, _ in F4_VARIABLES.values()]
        + [pa.field(qc_column(c), pa.string()) for c, _ in F4_VARIABLES.values()]
        + [pa.field("is_statistical_outlier", pa.bool_())] + LINEAGE_FIELDS),
    "alerts": pa.schema(
        [pa.field("alert_id", pa.string(), nullable=False),
         pa.field("message_type", CATEGORY, nullable=False), pa.field("status", CATEGORY),
         pa.field("event", CATEGORY), pa.field("severity", CATEGORY),
         pa.field("certainty", CATEGORY), pa.field("urgency", CATEGORY),
         pa.field("sent", UTC_NS, nullable=False), pa.field("effective", UTC_NS), pa.field("onset", UTC_NS),
         pa.field("expires", UTC_NS), pa.field("ends", UTC_NS), pa.field("sent_utc_offset", pa.string()),
         pa.field("sender_name", pa.string()), pa.field("area_desc", pa.string()),
         pa.field("ugc_codes", pa.list_(pa.string())), pa.field("references", pa.list_(pa.string())),
         pa.field("has_geometry", pa.bool_(), nullable=False),
         pa.field("latitude", pa.float64()), pa.field("longitude", pa.float64())] + LINEAGE_FIELDS),
    "daily_summaries": pa.schema(
        [pa.field("station", CATEGORY, nullable=False), pa.field("obs_date", pa.date32(), nullable=False),
         pa.field("timestamp", UTC_NS, nullable=False), pa.field("element", CATEGORY, nullable=False),
         pa.field("value", pa.float64(), nullable=False), pa.field("unit", CATEGORY, nullable=False),
         pa.field("mflag", pa.string()), pa.field("qflag", pa.string()), pa.field("sflag", pa.string()),
         pa.field("is_trace", pa.bool_(), nullable=False),
         pa.field("latitude", pa.float64()), pa.field("longitude", pa.float64()),
         pa.field("is_statistical_outlier", pa.bool_())] + LINEAGE_FIELDS),
    "storm_events": pa.schema(
        [pa.field("event_id", pa.int64(), nullable=False), pa.field("episode_id", pa.int64()),
         pa.field("event_type", CATEGORY, nullable=False), pa.field("cz_type", CATEGORY, nullable=False),
         pa.field("cz_fips", pa.string(), nullable=False), pa.field("zone_code", pa.string(), nullable=False),
         pa.field("cz_name", pa.string()), pa.field("wfo", pa.string()),
         pa.field("cz_timezone", CATEGORY, nullable=False),
         pa.field("begin_local", pa.timestamp("ns"), nullable=False),
         pa.field("begin_utc", UTC_NS, nullable=False), pa.field("end_utc", UTC_NS),
         pa.field("latitude", pa.float64()), pa.field("longitude", pa.float64()),
         pa.field("magnitude_si", pa.float64()), pa.field("magnitude_unit", CATEGORY),
         pa.field("magnitude_type", pa.string()),
         pa.field("damage_property_usd", pa.float64()), pa.field("damage_crops_usd", pa.float64()),
         pa.field("injuries_direct", pa.int64()), pa.field("deaths_direct", pa.int64())] + LINEAGE_FIELDS),
    "rejected": pa.schema(
        [pa.field("source", CATEGORY, nullable=False), pa.field("bronze_file", pa.string(), nullable=False),
         pa.field("reason", CATEGORY, nullable=False), pa.field("payload", pa.string()),
         pa.field("processed_utc", UTC_NS, nullable=False)]),
}


def to_table(df: pd.DataFrame, topic: str) -> pa.Table:
    """Enforce the explicit schema of the topic. Missing, extra or NULL required fields fail loudly."""
    schema = SCHEMAS[topic]
    missing = [f.name for f in schema if f.name not in df.columns]
    extra = [c for c in df.columns if c not in schema.names]
    if missing or extra:
        raise SchemaError(f"{topic}: missing fields {missing}, unexpected fields {extra}")
    for field in schema:
        if not field.nullable and df[field.name].isna().any():
            raise SchemaError(f"{topic}: required field {field.name} has NULL values")
    return pa.Table.from_pandas(df[schema.names], schema=schema, preserve_index=False)
