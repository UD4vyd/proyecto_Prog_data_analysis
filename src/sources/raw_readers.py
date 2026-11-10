"""Readers that turn raw landing-zone files into pandas DataFrames, without changing values.

Each reader keeps the source's own field names; renaming and unit conversion happen in
``src.normalization.canonical``. Keeping both steps apart lets us re-process raw files later.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pandas as pd


def read_storm_events(path: str | Path) -> pd.DataFrame:
    """NOAA Storm Events 'details' CSV (plain or .gz). All columns read as text first."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""], low_memory=False)


def read_ghcn_daily_json(path: str | Path) -> pd.DataFrame:
    """NCEI Access Data Service JSON: a list of station-day dicts; absent element = missing key."""
    with open(path, encoding="utf-8") as fh:
        return pd.DataFrame(json.load(fh))


def _open_text(path: str | Path):
    path = Path(path)
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def read_nws_geojson(path: str | Path) -> pd.DataFrame:
    """NWS GeoJSON FeatureCollection (plain or .gz) -> one row per feature, properties flattened.

    Quantitative values ``{"unitCode", "value", "qualityControl"}`` become three columns:
    ``<name>.value``, ``<name>.unitCode``, ``<name>.qualityControl``.
    """
    with _open_text(path) as fh:
        fc = json.load(fh)
    rows = []
    for feat in fc.get("features", []):
        props = dict(feat.get("properties") or {})
        row: dict = {"feature_id": feat.get("id"), "geometry": feat.get("geometry")}
        for key, val in props.items():
            if isinstance(val, dict) and "value" in val:
                for sub, subval in val.items():
                    row[f"{key}.{sub}"] = subval
            else:
                row[key] = val
        rows.append(row)
    return pd.DataFrame(rows)


def read_nws_folder(folder: str | Path, pattern: str) -> pd.DataFrame:
    """Concatenate every NWS GeoJSON file in ``folder`` matching ``pattern``."""
    frames = []
    paths = {}
    folder = Path(folder)
    for path in sorted([*folder.glob(pattern), *folder.glob(pattern + ".gz")]):
        if path.name.endswith(".headers.json"):
            continue
        # prefer the plain file when both plain and .gz copies exist
        paths.setdefault(path.name.removesuffix(".gz"), path)
    for path in paths.values():
        df = read_nws_geojson(path)
        df["raw_file"] = path.name
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
