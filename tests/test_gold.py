"""Tests of the Gold transformations on small in-memory DataFrames (no files are read or written)."""
import shutil
from datetime import date, datetime

import pytest

pyspark = pytest.importorskip("pyspark")
if not shutil.which("java"):
    pytest.skip("Spark needs a Java runtime", allow_module_level=True)

from src.gold import spark as gold_spark  # noqa: E402
from src.gold import storytelling  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = gold_spark.get_spark("tests")
    yield session
    session.stop()


def test_latest_version_keeps_last_ingested(spark):
    df = spark.createDataFrame([
        (1, "old", datetime(2026, 11, 17), datetime(2026, 11, 17), "a.parquet"),
        (1, "new", datetime(2026, 11, 19), datetime(2026, 11, 19), "b.parquet"),
        (2, "only", datetime(2026, 11, 17), datetime(2026, 11, 17), "a.parquet"),
    ], "event_id long, value string, ingested_at timestamp, processed_at timestamp, silver_file string")
    result = {r.event_id: r.value for r in gold_spark.latest_version(df, ["event_id"]).collect()}
    assert result == {1: "new", 2: "only"}


def test_classes_against_percentiles(spark):
    """p10 = 10, p90 = 30, p1 = 5, p99 = 35 for every day; PRCP has no low side."""
    percentiles = spark.createDataFrame(
        [("KDFW", element, doy, [5.0, 10.0, 30.0, 35.0], 450, 30)
         for element in ["TMAX", "PRCP"] for doy in range(1, 366)],
        "icao string, element string, doy int, p array<double>, baseline_n long, baseline_years long")
    long = spark.createDataFrame([
        ("KDFW", date(2025, 7, 1), "TMAX", 20.0),
        ("KDFW", date(2025, 7, 2), "TMAX", 32.0),
        ("KDFW", date(2025, 7, 3), "TMAX", 40.0),
        ("KDFW", date(2025, 7, 4), "TMAX", 8.0),
        ("KDFW", date(2025, 7, 5), "TMAX", 1.0),
        ("KDFW", date(2025, 7, 6), "PRCP", 0.0),
        ("KDFW", date(2025, 7, 7), "PRCP", 50.0),
    ], "icao string, date date, element string, value double")
    classes = {(r.element, r.date.day): r["class"] for r in storytelling.classify(long, percentiles).collect()}
    assert classes == {("TMAX", 1): "NORMAL", ("TMAX", 2): "UNUSUAL_HIGH", ("TMAX", 3): "EXTREME_HIGH",
                       ("TMAX", 4): "UNUSUAL_LOW", ("TMAX", 5): "EXTREME_LOW",
                       ("PRCP", 6): "NORMAL", ("PRCP", 7): "EXTREME_HIGH"}


def test_leap_day_shares_index_with_28_february(spark):
    df = spark.createDataFrame([(date(2024, 2, 28),), (date(2024, 2, 29),), (date(2024, 3, 1),),
                                (date(2025, 3, 1),)], "d date")
    index = [r.i for r in df.select(storytelling._day_index(df.d).alias("i")).collect()]
    assert index == [59, 59, 60, 60]


def test_test_messages_are_not_alerts(spark):
    alerts = spark.createDataFrame([("Actual", "Alert"), ("Actual", "Cancel"), ("Test", "Alert")],
                                   "status string, message_type string")
    assert storytelling._issued(alerts).count() == 1


def test_keyword_severity_covers_the_top_alert_words(spark):
    """Workshop 4: every alert word of the keyword chart has a mean severity to colour it."""
    alerts = spark.createDataFrame([
        ("Actual", "Alert", "Severe", "flood warning river"),
        ("Actual", "Alert", "Minor", "rip current risk rip current"),
        ("Actual", "Alert", "Moderate", "dense fog visibility river"),
    ], "status string, message_type string, severity string, description_clean string")
    storms = spark.createDataFrame([("hail reported near the city",)], "narrative_clean string")
    result = storytelling.union_all(storytelling.keyword_rows(alerts, storms))
    words = {r.dim_1 for r in result.filter("section = 'top_keywords' AND topic = 'weather_alerts' "
                                            "AND dim_2 = 'word'").collect()}
    severity = {r.dim_1: r.value for r in result.filter("section = 'keyword_severity' "
                                                         "AND metric = 'mean_severity_score'").collect()}
    assert words and words == set(severity)
    assert severity["river"] == 2.5  # one Severe (3) and one Moderate (2) alert
