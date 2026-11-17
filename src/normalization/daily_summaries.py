"""Silver normalizer for F2, GHCN-Daily (topic daily_summaries)."""
import pandas as pd

from src import settings
from src.normalization import common

TOPIC = "daily_summaries"
KEY = ["ghcn_id", "date"]
REQUIRED = ["ghcn_id", "date"]
SCHEMA = {
    "ghcn_id": "category",
    "icao": "category",
    "date": "date",                 # local station day
    "tmax_c": "float64",
    "tmin_c": "float64",
    "prcp_mm": "float64",
    "is_trace": "bool",             # PRCP is 0.0 with measurement flag T
    "tmax_s_flag": "string",        # source flag of each element
    "tmin_s_flag": "string",
    "prcp_s_flag": "string",
    "qc_failed": "string",          # elements set to null because NCEI marked them with a quality flag
    "tmin_above_tmax": "bool",
    "tmax_outlier": "bool",         # IQR flags inside the station batch
    "tmin_outlier": "bool",
    "prcp_outlier": "bool",         # computed on wet days only
    "record_hash": "string",
    "bronze_file": "string",
    "ingested_at": "datetime_utc",
    "processed_at": "datetime_utc",
}
ELEMENTS = {"TMAX": "tmax_c", "TMIN": "tmin_c", "PRCP": "prcp_mm"}


def _flags(attributes):
    """Split "measurement,quality,source[,time]" into its first three parts."""
    parts = (attributes or "").split(",") + ["", "", ""]
    return parts[0].strip(), parts[1].strip(), parts[2].strip()


def normalize(payload):
    icao_by_ghcn = {s["ghcn_id"]: s["icao"] for s in settings.load_stations()}
    rows = []
    for record in payload["records"]:
        row = {"ghcn_id": record.get("STATION"), "icao": icao_by_ghcn.get(record.get("STATION")),
               "date": record.get("DATE"), "is_trace": False}
        failed = []
        for element, column in ELEMENTS.items():
            m_flag, q_flag, s_flag = _flags(record.get(f"{element}_ATTRIBUTES"))
            value = record.get(element)
            if q_flag:
                # NCEI already decided this value failed a quality check, so we do not use it.
                value = None
                failed.append(element)
            row[column] = value
            row[f"{element.lower()}_s_flag"] = s_flag
            if element == "PRCP" and m_flag == "T":
                row["is_trace"] = True
        row["qc_failed"] = ",".join(failed)
        rows.append(row)
    df = pd.DataFrame(rows, columns=[c for c in SCHEMA if c not in common.METADATA_COLUMNS])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for column in ELEMENTS.values():
        df[column] = common.to_number(df[column])      # values arrive as text, for example "10.0"

    df["tmin_above_tmax"] = (df["tmin_c"] > df["tmax_c"]).fillna(False)
    df["tmax_outlier"] = common.iqr_outliers(df["tmax_c"], df["ghcn_id"])
    df["tmin_outlier"] = common.iqr_outliers(df["tmin_c"], df["ghcn_id"])
    # Most days have no rain, so quartiles over all days would be 0 and any rain would look like an outlier.
    wet = df["prcp_mm"].where((df["prcp_mm"] > 0) & ~df["is_trace"])
    df["prcp_outlier"] = common.iqr_outliers(wet, df["ghcn_id"])
    return df
