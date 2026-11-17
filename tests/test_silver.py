"""Tests of the Silver rules. They only read files in tests/fixtures/ and never use the network."""
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.config import load_stations
from src.normalization import silver as s

FIXTURES = Path(__file__).parent / "fixtures"
STATIONS = load_stations(FIXTURES / "stations_excerpt.csv")  # KDFW (Chicago) and KELP (Denver)
EDGE = json.loads((FIXTURES / "silver_edge_cases.json").read_text())


def load_json(name):
    return json.loads((FIXTURES / name).read_text())


def f2_rows(records=None, units="metric"):
    records = records if records is not None else load_json("ghcn_daily_excerpt.json")
    return s.parse_daily_summaries(records, "ghcn_daily/test.json", units, STATIONS)


def f1_rows(name="storm_events_tx_mixed_excerpt.csv"):
    raw = pd.read_csv(FIXTURES / name, dtype=str, keep_default_na=False)
    return s.parse_storm_events(raw, "storm_events/test.csv.gz")


# Units ------------------------------------------------------------------------------------------

def test_unit_conversions():
    assert s.convert(36, "wmoUnit:km_h-1", "m/s") == pytest.approx(10.0)
    assert s.convert(64, "knots", "m/s") == pytest.approx(64 * 0.514444)
    assert s.convert(1, "inches", "mm") == pytest.approx(25.4)
    assert s.convert(102776.9, "wmoUnit:Pa", "hPa") == pytest.approx(1027.769)
    with pytest.raises(s.UnknownUnit):
        s.convert(50, "wmoUnit:degF", "degC")


def test_f4_units_lon_lat_swap_and_nulls():
    rows, rejected = s.parse_observations(load_json("nws_observations_KDFW_excerpt.geojson"), "f4.geojson")
    assert len(rows) == 3 and rejected.empty
    first = rows.iloc[0]
    assert (first.latitude, first.longitude) == (32.9, -97.03)  # GeoJSON gives [-97.03, 32.9]
    assert first.wind_speed_ms == pytest.approx(5.544 / 3.6)
    assert first.pressure_hpa == pytest.approx(1027.769)
    assert pd.isna(first.wind_gust_ms) and first.wind_gust_qc == "Z"  # value: null stays NULL
    assert str(rows["timestamp"].dtype) == "datetime64[ns, UTC]"
    assert isinstance(rows["station"].dtype, pd.CategoricalDtype)


def test_f1_magnitude_units_damage_and_zone_rows():
    rows, rejected, outside = f1_rows()
    assert rejected.empty and outside == 0
    by_id = rows.set_index("event_id")
    assert by_id.loc[1230425, "magnitude_si"] == pytest.approx(25.4)  # hail 1.00 in
    assert by_id.loc[1230425, "magnitude_unit"] == "mm"
    assert by_id.loc[1244626, "magnitude_si"] == pytest.approx(64 * 0.514444)  # wind 64 kt
    assert by_id.loc[1244626, "event_type"] == "thunderstorm_wind"
    assert by_id.loc[1247378, "damage_property_usd"] == 6_000_000  # '6.00M'
    assert pd.isna(by_id.loc[1243404, "damage_property_usd"])  # blank means unknown, not 0
    assert by_id.loc[1230425, "damage_property_usd"] == 0.0  # '0.00K'
    zone = by_id.loc[1305042]
    assert zone.cz_type == "Z" and zone.zone_code == "TXZ422" and pd.isna(zone.latitude)


# Time -------------------------------------------------------------------------------------------

def test_f1_time_to_utc_with_fixed_offsets():
    by_id = f1_rows()[0].set_index("event_id")
    # MST-7 (El Paso zone): local 2025-12-01 00:00 is 07:00 UTC
    assert by_id.loc[1305042, "begin_local"] == pd.Timestamp("2025-12-01 00:00")
    assert by_id.loc[1305042, "begin_utc"] == pd.Timestamp("2025-12-01 07:00", tz="UTC")
    # CST-6: local 2025-03-02 14:15 is 20:15 UTC
    assert by_id.loc[1230425, "begin_utc"] == pd.Timestamp("2025-03-02 20:15", tz="UTC")


def test_f1_uses_four_digit_year_not_begin_date_time():
    by_id = f1_rows("storm_events_tx_excerpt.csv")[0].set_index("event_id")
    # BEGIN_DATE_TIME is '09-JAN-25 20:00:00' CST-6; the UTC time falls on the next day
    assert by_id.loc[1230373, "begin_utc"] == pd.Timestamp("2025-01-10 02:00", tz="UTC")


def test_f3_offsets_converted_to_utc():
    rows, rejected = s.parse_alerts(load_json("nws_alerts_active_20261116_231234.geojson"), "f3.geojson")
    assert len(rows) == 6 and rejected.empty
    first = rows.iloc[0]
    assert first.sent == pd.Timestamp("2026-11-16 22:49", tz="UTC")  # 16:49 at -06:00
    assert first.sent_utc_offset == "-06:00"
    no_geometry = rows[~rows.has_geometry]
    assert len(no_geometry) == 5 and no_geometry.latitude.isna().all()
    flood = rows[rows.has_geometry].iloc[0]
    assert flood.ugc_codes == ["TXC311"] and 25 < flood.latitude < 37 and -107 < flood.longitude < -93


def test_f2_timestamp_is_local_standard_midnight_in_utc():
    rows, _ = f2_rows()
    assert rows.iloc[0].obs_date == date(2025, 1, 1)
    assert rows.iloc[0].timestamp == pd.Timestamp("2025-01-01 06:00", tz="UTC")  # Chicago, -6
    record = dict(load_json("ghcn_daily_excerpt.json")[0], STATION="USW00023044")  # El Paso, Denver -7
    denver, _ = f2_rows([record])
    assert (denver.timestamp == pd.Timestamp("2025-01-01 07:00", tz="UTC")).all()


# F2 values and flags ----------------------------------------------------------------------------

def test_f2_types_and_trace_flag():
    rows, rejected = f2_rows()
    assert len(rows) == 30 and rejected.empty  # 10 station-days x 3 elements
    assert rows["value"].dtype == "float64"
    trace = rows[(rows.obs_date == date(2025, 1, 5)) & (rows.element == "PRCP")].iloc[0]
    assert trace.value == 0.0 and bool(trace.is_trace) and trace.mflag == "T" and trace.sflag == "W"
    assert rows.is_trace.sum() == 1


@pytest.mark.parametrize("reason", ["failed_ncei_qc", "not_numeric", "out_of_physical_bounds", "out_of_scope"])
def test_f2_rejections(reason):
    rows, rejected = f2_rows([EDGE["f2_records"][reason]])
    if reason == "out_of_scope":  # unknown station: all three elements of the day are rejected
        assert rows.empty and set(rejected.reason) == {reason} and len(rejected) == 3
    else:
        assert list(rejected.reason) == [reason]
    assert set(rejected.columns) == {"source", "bronze_file", "reason", "payload"}
    assert json.loads(rejected.payload.iloc[0])["record"]["DATE"] == "2025-01-01"


def test_f2_unknown_units_are_rejected():
    rows, rejected = f2_rows(units="standard")
    assert rows.empty and set(rejected.reason) == {"unknown_unit"}


# F4 and F3 rejections ---------------------------------------------------------------------------

@pytest.mark.parametrize("reason", ["out_of_physical_bounds", "unknown_unit", "failed_nws_qc",
                                    "out_of_scope", "missing_key"])
def test_f4_rejections(reason):
    data = {"type": "FeatureCollection", "features": [EDGE["f4_features"][reason]]}
    rows, rejected = s.parse_observations(data, "f4.geojson")
    assert rows.empty and list(rejected.reason) == [reason]


def test_f3_polygon_outside_texas_is_rejected():
    data = {"type": "FeatureCollection", "features": [EDGE["f3_out_of_scope"]]}
    rows, rejected = s.parse_alerts(data, "f3.geojson")
    assert rows.empty and list(rejected.reason) == ["out_of_scope"]


# Deduplication ----------------------------------------------------------------------------------

def _with_lineage(rows, stamp="2026-11-17 01:00"):
    rows = rows.copy()
    rows["processed_utc"] = pd.Timestamp(stamp, tz="UTC").as_unit("ns")
    rows["record_hash"] = s.record_hash(rows)
    return rows


def test_dedupe_keep_last_within_run():
    rows, _ = s.parse_observations(load_json("nws_observations_KDFW_excerpt.geojson"), "first.geojson")
    later, _ = s.parse_observations(load_json("nws_observations_KDFW_excerpt.geojson"), "second.geojson")
    deduped, dropped = s.dedupe_keep_last(pd.concat([rows, later], ignore_index=True), s.KEYS["observations"])
    assert dropped == 3 and len(deduped) == 3 and set(deduped.bronze_file) == {"second.geojson"}


def test_duplicates_across_runs_and_revisions():
    old = _with_lineage(s.parse_observations(load_json("nws_observations_KDFW_excerpt.geojson"), "a")[0])
    new, _ = s.parse_observations(load_json("nws_observations_KDFW_excerpt.geojson"), "b")
    new.loc[0, "temperature_qc"] = "S"  # same key, new content: a revision
    new = _with_lineage(new, "2026-11-17 02:00")
    keep, unchanged, revised = s.compare_with_previous(new, old, s.KEYS["observations"])
    assert unchanged == 2 and len(keep) == 1 and len(revised) == 1
    assert keep.iloc[0].temperature_qc == "S"


def test_record_hash_ignores_lineage():
    rows, _ = f2_rows()
    a = _with_lineage(rows.assign(bronze_file="x"), "2026-11-17 01:00")
    b = _with_lineage(rows.assign(bronze_file="y"), "2026-11-18 01:00")
    assert (a.record_hash == b.record_hash).all()


# Outliers ---------------------------------------------------------------------------------------

def test_statistical_outliers_are_flagged_not_removed():
    rows, _ = f2_rows()
    flags = s.flag_daily_outliers(rows, None)
    assert len(flags) == len(rows)  # nothing removed
    assert flags[rows.element.eq("PRCP").values & rows.value.eq(0).values].eq(False).all()  # dry days
    assert flags[rows.element.eq("TMAX").values].notna().all()  # 10 values = min_n, so judged
    few, _ = f2_rows(load_json("ghcn_daily_excerpt.json")[:5])
    assert s.flag_daily_outliers(few, None)[few.element.eq("TMAX").values].isna().all()  # 5 < min_n
    values = pd.Series([20.0] * 12 + [45.0])
    groups = pd.DataFrame({"station": ["X"] * 13, "month": ["7"] * 13})
    assert list(s.iqr_flags(values, groups)) == [False] * 12 + [True]


# Parquet schema ---------------------------------------------------------------------------------

def test_parquet_schema_roundtrip(tmp_path):
    rows, _ = f2_rows()
    rows = _with_lineage(rows)
    rows["is_statistical_outlier"] = s.flag_daily_outliers(rows, None).values
    path = tmp_path / "daily_summaries_test.parquet"
    pq.write_table(s.to_table(rows, "daily_summaries"), path)
    assert pq.read_schema(path).equals(s.SCHEMAS["daily_summaries"], check_metadata=False)
    back = pd.read_parquet(path)
    assert isinstance(back["station"].dtype, pd.CategoricalDtype)
    assert str(back["timestamp"].dtype) == "datetime64[ns, UTC]"


def test_schema_fails_loudly():
    rows = _with_lineage(f2_rows()[0])
    rows["is_statistical_outlier"] = pd.NA
    with pytest.raises(s.SchemaError, match="missing fields"):
        s.to_table(rows.drop(columns=["station"]), "daily_summaries")
    rows.loc[0, "value"] = None
    with pytest.raises(s.SchemaError, match="required field value"):
        s.to_table(rows, "daily_summaries")
