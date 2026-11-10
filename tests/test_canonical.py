"""Tests for the Workshop 1 canonical-mapping rules, using recorded fixtures (no live calls)."""
from pathlib import Path

import pandas as pd
import pytest

from src.normalization import canonical as C
from src.sources.raw_readers import read_ghcn_daily_json, read_nws_geojson, read_storm_events

FIX = Path(__file__).parent / "fixtures"
CANONICAL = ["event_id", "source", "event_type", "timestamp", "latitude", "longitude",
             "severity", "severity_unit", "raw_reference", "content_hash"]


@pytest.mark.parametrize("code,expected", [("CST-6", -6), ("MST-7", -7), ("CDT", -5), ("cst", -6), ("", None)])
def test_parse_tz_offset(code, expected):
    got = C.parse_tz_offset(code)
    assert (pd.isna(got) and expected is None) or got == expected


@pytest.mark.parametrize("text,expected", [("10.00K", 1e4), ("1.5M", 1.5e6), ("0.00K", 0.0), ("2B", 2e9), ("K", 1e3)])
def test_parse_damage(text, expected):
    assert C.parse_damage(text) == pytest.approx(expected)


def test_parse_damage_blank_is_unknown_not_zero():
    assert pd.isna(C.parse_damage(""))
    assert pd.isna(C.parse_damage(None))


def test_storm_events_mapping():
    df = read_storm_events(FIX / "storm_events_tx_2025_subset.csv")
    ev = C.map_storm_events(df)
    assert list(ev.columns) == CANONICAL
    assert ev.event_id.is_unique
    assert str(ev.timestamp.dt.tz) == "UTC"
    # CST-6 local 20:00 on 2025-01-09 -> 02:00 UTC next day
    row = ev[ev.event_id == "noaa_storm_events:" + df.iloc[0].EVENT_ID].iloc[0]
    assert row.timestamp == pd.Timestamp("2025-01-10 02:00", tz="UTC")
    hail = ev[ev.event_type == "hail"]
    assert (hail.severity_unit == "mm (hail diameter)").all() and (hail.severity >= 19).all()


def test_storm_events_mapping_is_deterministic():
    df = read_storm_events(FIX / "storm_events_tx_2025_subset.csv")
    a, b = C.map_storm_events(df), C.map_storm_events(df)
    pd.testing.assert_frame_equal(a, b)


def test_ghcn_mapping_units_and_local_day():
    ev = C.map_ghcn_daily(read_ghcn_daily_json(FIX / "ghcn_daily_lubbock_2025-06-1x.json"))
    tmax = ev[ev.event_id == "ghcn_daily:USW00023042:2025-06-17:TMAX"].iloc[0]
    assert tmax.severity_unit == "degC"
    assert tmax.timestamp == pd.Timestamp("2025-06-17 06:00", tz="UTC")  # Lubbock: UTC-6 standard
    assert ev.event_id.is_unique


def test_nws_alerts_mapping_handles_missing_geometry():
    df = read_nws_geojson(FIX / "nws_alerts_tx_subset.geojson")
    ev = C.map_nws_alerts(df)
    assert ev.latitude.isna().sum() == df.geometry.isna().sum()
    assert ev.severity.between(0, 4).all()
    assert str(ev.timestamp.dt.tz) == "UTC"


def test_nws_observations_unit_conversion():
    df = read_nws_geojson(FIX / "nws_observations_kdfw_subset.geojson")
    ev = C.map_nws_observations(df)
    ws = ev[ev.event_type == "wind_speed"]
    raw = df.set_index("timestamp")["windSpeed.value"]
    for _, r in ws.iterrows():
        ts = r.event_id.split(":", 2)[2].rsplit(":", 1)[0]
        assert r.severity == pytest.approx(raw[ts] / 3.6)


def test_content_hash_changes_with_content():
    assert C.content_hash({"a": 1, "b": 2}) == C.content_hash({"b": 2, "a": 1})
    assert C.content_hash({"a": 1}) != C.content_hash({"a": 2})
