"""Tests of the Bronze validation and the Silver normalizers, using the recorded samples in tests/fixtures."""
import csv
import json
from pathlib import Path

import pandas as pd
import pytest

from src.normalization import alerts, common, daily_summaries, observations, storm_events, text
from src.sources.storm_events import parse_listing
from src.storage import bronze

FIXTURES = Path(__file__).parent / "fixtures"


def test_clean_text_and_tokens():
    clean = text.clean_text("* WHAT...Heavy rain,\nup to 3 inches.")
    assert clean == "what heavy rain up to 3 inches"
    assert text.tokenize(clean) == ["heavy", "rain", "inches"]
    assert text.clean_text(None) == ""


def test_parse_damage():
    assert storm_events.parse_damage("10.00K") == 10000.0
    assert storm_events.parse_damage("1.5M") == 1500000.0
    assert storm_events.parse_damage("0.00K") == 0.0
    assert storm_events.parse_damage("") is None      # blank is unknown, not zero


def test_storm_local_time_to_utc():
    # EVENT_ID 1230373: 09-JAN-25 20:00 in CST-6 is 02:00 UTC of the next day.
    assert storm_events.to_utc("202501", "9", "2000", "CST-6") == pd.Timestamp("2025-01-10 02:00", tz="UTC")
    assert storm_events.to_utc("202507", "4", "30", "MST-7") == pd.Timestamp("2025-07-04 07:30", tz="UTC")
    assert storm_events.to_utc("202501", "9", "2000", "") is None


def test_storm_events_fixture():
    with open(FIXTURES / "storm_events_tx_excerpt.csv", encoding="utf-8", newline="") as f:
        records = list(csv.DictReader(f))
    df = common.add_record_hash(storm_events.normalize({"records": records}))
    assert len(df) == 20 and df["event_id"].is_unique
    assert df["begin_utc"].notna().all()
    zone_rows = df[df["cz_type"] == "Z"]
    assert zone_rows["begin_lat"].isna().all()         # zone rows have no coordinates
    assert set(df["magnitude_unit"].dropna()) <= {"mm", "m/s"}


def test_alerts_fixture():
    payload = json.loads((FIXTURES / "nws_alerts_active_20261116_231234.geojson").read_text(encoding="utf-8"))
    df = alerts.normalize({"records": payload["features"]})
    assert len(df) == 6 and df["alert_id"].is_unique
    assert str(df["sent_utc"].dt.tz) == "UTC"
    assert df["severity"].isin(alerts.CAP_SEVERITY).all()
    assert (df["token_count"] > 0).all()
    with_geometry = df[df["has_geometry"]]
    assert with_geometry["centroid_lat"].between(25, 37).all()      # Texas latitudes, so lon/lat were swapped right


def test_daily_summaries_values_and_flags(monkeypatch):
    monkeypatch.setattr(daily_summaries.settings, "load_stations",
                        lambda: [{"ghcn_id": "USW00003927", "icao": "KDFW"}])
    records = [
        {"STATION": "USW00003927", "DATE": "2025-01-01", "TMAX": "10.0", "TMAX_ATTRIBUTES": ",,W",
         "TMIN": "2.2", "TMIN_ATTRIBUTES": ",,W", "PRCP": "0.0", "PRCP_ATTRIBUTES": "T,,W"},
        {"STATION": "USW00003927", "DATE": "2025-01-02", "TMAX": "55.0", "TMAX_ATTRIBUTES": ",G,W",
         "TMIN": "3.0", "TMIN_ATTRIBUTES": ",,W"},
    ]
    df = daily_summaries.normalize({"records": records})
    assert df.loc[0, "tmax_c"] == 10.0 and df.loc[0, "icao"] == "KDFW"
    assert bool(df.loc[0, "is_trace"]) and df.loc[0, "prcp_mm"] == 0.0
    assert pd.isna(df.loc[1, "tmax_c"]) and df.loc[1, "qc_failed"] == "TMAX"   # value with a quality flag
    assert pd.isna(df.loc[1, "prcp_mm"])                                        # element absent in the answer


def test_observations_units_and_range():
    def feature(stamp, temperature, wind_kmh, pressure_pa):
        return {"geometry": {"type": "Point", "coordinates": [-97.03, 32.9]},
                "properties": {"stationId": "KDFW", "timestamp": stamp,
                               "temperature": {"unitCode": "wmoUnit:degC", "value": temperature, "qualityControl": "V"},
                               "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": wind_kmh},
                               "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None},
                               "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": pressure_pa}}}
    df = observations.normalize({"records": [feature("2026-11-16T22:55:00+00:00", 16, 36, 101325),
                                             feature("2026-11-16T23:55:00+00:00", 999, 18, 101300)]})
    assert df.loc[0, "latitude"] == 32.9 and df.loc[0, "longitude"] == -97.03
    assert df.loc[0, "wind_speed_ms"] == pytest.approx(10.0)
    assert df.loc[0, "pressure_hpa"] == pytest.approx(1013.25)
    assert pd.isna(df.loc[0, "wind_gust_ms"])
    assert pd.isna(df.loc[1, "temperature_c"]) and df.loc[1, "out_of_range"] == "temperature_c"


def test_iqr_outliers_flag_only_the_extreme_value():
    values = pd.Series([20.0, 21, 22, 21, 20, 22, 21, 20, 45])
    flags = common.iqr_outliers(values, pd.Series(["A"] * 9))
    assert flags.tolist() == [False] * 8 + [True]


def test_enforce_schema_rejects_rows_without_required_field():
    df = pd.DataFrame({"alert_id": ["a", None], "extra": [1, 2]})
    valid, rejected = common.enforce_schema(df, {"alert_id": "string", "event": "string"}, ["alert_id"])
    assert list(valid.columns) == ["alert_id", "event"]
    assert len(valid) == 1 and len(rejected) == 1


def test_parse_listing_keeps_latest_compilation():
    html = """<a href="StormEvents_details-ftp_v1.0_d2025_c20260619.csv.gz">a</a>
              <a href="StormEvents_details-ftp_v1.0_d2025_c20260819.csv.gz">b</a>
              <a href="StormEvents_fatalities-ftp_v1.0_d2025_c20260819.csv.gz">c</a>"""
    assert parse_listing(html) == {2025: ("StormEvents_details-ftp_v1.0_d2025_c20260819.csv.gz", "20260819")}


def test_validate_file_rejects_incomplete_records(tmp_path):
    good = {"source": "ghcn_daily", "run_stamp": "20261117_000000", "extracted_utc": "2026-11-17T00:00:00+00:00",
            "record_count": 1, "requests": [{"url": "u", "status": 200}],
            "records": [{"STATION": "USW00003927", "DATE": "2025-01-01"}]}
    path = tmp_path / "ghcn_daily_20261117_000000.json"
    path.write_text(json.dumps(good), encoding="utf-8")
    assert bronze.validate_file(str(path)) == str(path)

    bad = dict(good, records=[{"STATION": "USW00003927"}])
    path.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(ValueError):
        bronze.validate_file(str(path))
