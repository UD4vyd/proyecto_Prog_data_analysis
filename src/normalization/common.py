"""Helpers shared by the four Silver normalizers."""
import hashlib
import json

import pandas as pd

# Columns added by the pipeline. They are not part of the content hash.
METADATA_COLUMNS = ["record_hash", "bronze_file", "ingested_at", "processed_at"]


def to_number(series):
    """Text to float. Text that is not a number becomes NaN instead of raising."""
    return pd.to_numeric(series, errors="coerce").astype("float64")


def to_utc(series):
    """ISO timestamps with any offset to UTC datetimes. Unreadable values become NaT."""
    return pd.to_datetime(series, errors="coerce", utc=True, format="ISO8601")


def null_out_of_range(df, column, low, high):
    """Set physically impossible values to null and return a boolean column that marks them."""
    bad = df[column].notna() & ~df[column].between(low, high)
    df.loc[bad, column] = float("nan")
    return bad


def iqr_outliers(values, groups, k=1.5, min_count=8):
    """Flag values outside [Q1 - k*IQR, Q3 + k*IQR] inside each group.

    Groups with fewer than `min_count` values are not evaluated, because their quartiles are not stable.
    The flag does not remove anything: in this project an extreme value is the signal we look for.
    """
    grouped = values.groupby(groups)
    q1, q3 = grouped.transform(lambda s: s.quantile(0.25)), grouped.transform(lambda s: s.quantile(0.75))
    count = grouped.transform("count")
    spread = q3 - q1
    flag = (values < q1 - k * spread) | (values > q3 + k * spread)
    return (flag & (count >= min_count)).fillna(False).astype(bool)


def zscore_outliers(values, groups, limit=3.0, min_count=8):
    """Flag values more than `limit` standard deviations from the mean of their group."""
    grouped = values.groupby(groups)
    mean, std, count = grouped.transform("mean"), grouped.transform("std"), grouped.transform("count")
    z = (values - mean) / std
    return ((z.abs() > limit) & (count >= min_count)).fillna(False).astype(bool)


def _plain(value):
    """Turn a cell into something json.dumps can write in a stable way."""
    if hasattr(value, "tolist"):       # numpy scalars and arrays to plain Python
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if value is None or (isinstance(value, float) and value != value) or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value if isinstance(value, (str, int, float, bool)) else str(value)


def add_record_hash(df, exclude=()):
    """SHA-1 of the content columns of each row. Two rows with the same key and hash are the same record.

    `exclude` lists the columns derived by the pipeline (outlier flags, cleaned text, source file name).
    They are left out so the hash only changes when the source changes a value. Workshop 3 finding: the flags
    are computed on the whole batch, and the Storm Events file name changes with each NOAA compilation, so
    with those columns inside the hash every re-download looked like a new version of every record.
    """
    content = [c for c in df.columns if c not in METADATA_COLUMNS and c not in exclude]

    def row_hash(row):
        text = json.dumps([_plain(v) for v in row], ensure_ascii=False)
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    df["record_hash"] = [row_hash(row) for row in df[content].itertuples(index=False, name=None)]
    return df


def enforce_schema(df, schema, required):
    """Apply the target schema: column order, data types and required fields.

    Returns (valid rows, rejected rows). A row is rejected when a required field is null.
    A column missing from the input is created as null, and extra columns are dropped.
    """
    for column in schema:
        if column not in df.columns:
            df[column] = None
    df = df[list(schema)].copy()
    for column, dtype in schema.items():
        if dtype == "list":
            df[column] = df[column].apply(lambda v: list(v) if isinstance(v, (list, tuple)) else [])
        elif dtype == "datetime_utc":
            df[column] = pd.to_datetime(df[column], utc=True).astype("datetime64[us, UTC]")
        elif dtype == "date":
            df[column] = pd.to_datetime(df[column]).astype("datetime64[us]")
        else:
            df[column] = df[column].astype(dtype)
    missing = df[required].isna().any(axis=1)
    return df[~missing].reset_index(drop=True), df[missing].reset_index(drop=True)
