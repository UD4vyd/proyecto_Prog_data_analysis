"""Silver normalizer for F4, NWS station observations (topic station_observations)."""
import pandas as pd

from src.normalization import common, text

TOPIC = "station_observations"
KEY = ["icao", "timestamp_utc"]
REQUIRED = ["icao", "timestamp_utc"]
SCHEMA = {
    "icao": "category",
    "timestamp_utc": "datetime_utc",
    "latitude": "float64",
    "longitude": "float64",
    "temperature_c": "float64",
    "dewpoint_c": "float64",
    "relative_humidity_pct": "float64",
    "wind_speed_ms": "float64",
    "wind_gust_ms": "float64",
    "wind_direction_deg": "float64",
    "pressure_hpa": "float64",
    "sea_level_pressure_hpa": "float64",
    "precip_1h_mm": "float64",
    "precip_3h_mm": "float64",
    "temperature_qc": "category",       # qualityControl code of the temperature (V verified, Z no data, ...)
    "text_description": "string",
    "out_of_range": "string",           # columns set to null by the physical range check
    "temperature_outlier": "bool",      # Z-score flag inside the station batch
    "record_hash": "string",
    "bronze_file": "string",
    "ingested_at": "datetime_utc",
    "processed_at": "datetime_utc",
}

# Silver column, field of the API, unit we store. The factor depends on the unitCode of each value.
VARIABLES = [
    ("temperature_c", "temperature"),
    ("dewpoint_c", "dewpoint"),
    ("relative_humidity_pct", "relativeHumidity"),
    ("wind_speed_ms", "windSpeed"),
    ("wind_gust_ms", "windGust"),
    ("wind_direction_deg", "windDirection"),
    ("pressure_hpa", "barometricPressure"),
    ("sea_level_pressure_hpa", "seaLevelPressure"),
    ("precip_1h_mm", "precipitationLastHour"),      # not present in every answer
    ("precip_3h_mm", "precipitationLast3Hours"),
]
# Conversions to the canonical units of workshop_1/mapping.md.
UNIT_FACTORS = {
    "wmoUnit:degC": 1.0,
    "wmoUnit:percent": 1.0,
    "wmoUnit:degree_(angle)": 1.0,
    "wmoUnit:km_h-1": 1 / 3.6,      # km/h to m/s
    "wmoUnit:m_s-1": 1.0,
    "wmoUnit:Pa": 0.01,             # Pa to hPa
    "wmoUnit:mm": 1.0,
    "wmoUnit:m": 1000.0,            # m to mm (precipitation)
}
# Physical limits. A value outside them is a sensor or coding error, not weather.
RANGES = {
    "temperature_c": (-60.0, 60.0),
    "dewpoint_c": (-80.0, 45.0),
    "relative_humidity_pct": (0.0, 100.0),
    "wind_speed_ms": (0.0, 115.0),
    "wind_gust_ms": (0.0, 115.0),
    "wind_direction_deg": (0.0, 360.0),
    "pressure_hpa": (500.0, 1100.0),
    "sea_level_pressure_hpa": (850.0, 1100.0),
    "precip_1h_mm": (0.0, 400.0),
    "precip_3h_mm": (0.0, 1000.0),
}


def _value(measure):
    """Read one {value, unitCode, qualityControl} object and convert it to the canonical unit."""
    if not isinstance(measure, dict) or measure.get("value") is None:
        return None
    factor = UNIT_FACTORS.get(measure.get("unitCode"))
    if factor is None:
        raise ValueError(f"Unit not expected in F4: {measure.get('unitCode')}")
    return measure["value"] * factor


def normalize(payload):
    rows = []
    for feature in payload["records"]:
        p = feature["properties"]
        coordinates = (feature.get("geometry") or {}).get("coordinates") or [None, None]
        row = {
            # stationId is not present in every answer, so the code is read from the station URL as a fallback.
            "icao": p.get("stationId") or (p.get("station") or "").rsplit("/", 1)[-1] or None,
            "timestamp_utc": p.get("timestamp"),
            "latitude": coordinates[1],     # GeoJSON order is [lon, lat]
            "longitude": coordinates[0],
            "temperature_qc": (p.get("temperature") or {}).get("qualityControl"),
            "text_description": text.clean_text(p.get("textDescription")),
        }
        for column, field in VARIABLES:
            row[column] = _value(p.get(field))
        rows.append(row)
    df = pd.DataFrame(rows, columns=[c for c in SCHEMA if c not in common.METADATA_COLUMNS])
    df["timestamp_utc"] = common.to_utc(df["timestamp_utc"])
    for column, _ in VARIABLES:
        df[column] = common.to_number(df[column])

    flags = pd.DataFrame({c: common.null_out_of_range(df, c, low, high) for c, (low, high) in RANGES.items()})
    df["out_of_range"] = flags.apply(lambda row: ",".join(c for c in flags.columns if row[c]), axis=1)
    df["temperature_outlier"] = common.zscore_outliers(df["temperature_c"], df["icao"])
    return df
