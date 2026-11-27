"""Tests of the dashboard data preparation (pandas only) and a smoke test of every figure on the sample Gold files.

The preparation tests build small Gold frames in memory. The figure tests need dash, plotly and pyarrow
(Poetry group "dashboard") and are skipped without them.
"""
import pandas as pd
import pytest

from dashboard.common import data
from dashboard.common import governance_prep as gp
from dashboard.common import storytelling_prep as sp


def gold(rows):
    """Long Gold frame from (section, topic, period, dim_1, dim_2, metric, value, label) tuples."""
    frame = pd.DataFrame(rows, columns=data.COLUMNS[:-1])
    frame["period"] = pd.to_datetime(frame["period"])
    frame["computed_at"] = pd.Timestamp("2026-11-20 16:44:14", tz="UTC")
    return frame


def test_latest_file_takes_the_newest_stamp(tmp_path):
    for name in ["governance_20261119_061500.parquet", "governance_20261120_164414.parquet",
                 "storytelling_20261121_061500.parquet"]:
        (tmp_path / name).touch()
    (tmp_path / ".governance_20261121_061500").mkdir()  # Spark temporary folder, not a Gold file
    assert data.latest_file("governance", tmp_path).name == "governance_20261120_164414.parquet"
    with pytest.raises(data.GoldFileMissing):
        data.latest_file("governance", tmp_path / "empty")



def test_empty_gold_file_is_reported_not_crashed(tmp_path):
    pytest.importorskip("pyarrow")
    gold([]).to_parquet(tmp_path / "governance_20261127_000000.parquet")
    with pytest.raises(ValueError, match="no rows"):
        data.load("governance", tmp_path)

def test_null_cell_rate_weights_topics_by_cells():
    frame = gold([
        ("null_rate", "a", None, "x", None, "null_pct", 100.0, None),
        ("null_rate", "a", None, "y", None, "null_pct", 0.0, None),
        ("null_rate", "b", None, "z", None, "null_pct", 0.0, None),
        ("volume_total", "a", None, None, None, "rows_after_dedup", 10.0, None),
        ("volume_total", "b", None, None, None, "rows_after_dedup", 80.0, None),
    ])
    per_topic, overall = gp.null_cell_pct(frame)
    assert per_topic["a"] == 50.0 and per_topic["b"] == 0.0
    # topic a: 20 cells, 10 null; topic b: 80 cells, none null
    assert overall == pytest.approx(10.0)


def test_status_against_thresholds():
    rules = gp.load_thresholds()
    assert gp.status_of(0.0, rules["duplicate_rate_after_pct"]) == "good"
    assert gp.status_of(0.1, rules["duplicate_rate_after_pct"]) == "critical"
    assert gp.status_of(1.5, rules["freshness_hours"], "weather_alerts") == "good"
    assert gp.status_of(100, rules["freshness_hours"], "storm_events") == "good"
    assert gp.status_of(3, rules["freshness_hours"], "station_observations") == "critical"
    assert gp.status_of(float("nan"), rules["null_cell_pct"]) == "info"


def test_freshness_is_measured_until_now():
    frame = gold([("freshness", "weather_alerts", None, None, None, "hours_since_last_ingestion", 0.5,
                   "2026-11-20T16:00:00Z")])
    fresh = gp.freshness(frame, now=pd.Timestamp("2026-11-20 19:00", tz="UTC"))
    assert fresh.at["weather_alerts", "hours_now"] == pytest.approx(3.0)


def test_high_share_counts_unusual_and_extreme_high_days():
    frame = gold([
        ("class_distribution", "daily_summaries", "2025-07-01", "KDFW", "TMAX", "days", 20.0, "NORMAL"),
        ("class_distribution", "daily_summaries", "2025-07-01", "KDFW", "TMAX", "days", 8.0, "UNUSUAL_HIGH"),
        ("class_distribution", "daily_summaries", "2025-07-01", "KDFW", "TMAX", "days", 2.0, "EXTREME_HIGH"),
        ("class_distribution", "daily_summaries", "2025-07-01", "KDFW", "TMAX", "days", 1.0, "UNUSUAL_LOW"),
        ("class_distribution", "daily_summaries", "2025-07-01", "KDFW", "TMAX", "days", 9.0, "NO_BASELINE"),
    ])
    month = sp.high_share(frame, "TMAX").iloc[0]
    assert month["days"] == 31 and month["high_days"] == 10
    assert month["high_pct"] == pytest.approx(100 * 10 / 31)
    shares = sp.class_shares(frame).set_index("class")["days"]
    assert shares.to_dict() == {"NORMAL": 20.0, "UNUSUAL": 9.0, "EXTREME": 2.0}


def test_alert_words_get_their_mean_severity():
    frame = gold([
        ("top_keywords", "weather_alerts", None, "flood", "word", "count", 50.0, None),
        ("top_keywords", "weather_alerts", None, "rip", "word", "count", 40.0, None),
        ("keyword_severity", "weather_alerts", None, "flood", None, "mean_severity_score", 2.5, None),
        ("keyword_severity", "weather_alerts", None, "flood", None, "alerts", 20.0, None),
    ])
    words = sp.keywords(frame).set_index("term")
    assert words.at["flood", "mean_severity_score"] == 2.5
    assert pd.isna(words.at["rip", "mean_severity_score"])  # older Gold files: shown in grey


def test_narrative_uses_the_numbers_of_the_file():
    frame = gold([
        ("class_distribution", "daily_summaries", "2023-07-01", "KDFW", "TMAX", "days", 70.0, "NORMAL"),
        ("class_distribution", "daily_summaries", "2023-07-01", "KDFW", "TMAX", "days", 30.0, "UNUSUAL_HIGH"),
        ("extreme_vs_storms", "storm_events", None, "ALL", "EXTREME", "days_with_storm_report_pct", 20.0, None),
        ("extreme_vs_storms", "storm_events", None, "ALL", "EXTREME", "station_days", 10.0, None),
        ("extreme_vs_storms", "storm_events", None, "ALL", "NORMAL", "days_with_storm_report_pct", 4.0, None),
        ("extreme_vs_storms", "storm_events", None, "ALL", "NORMAL", "station_days", 90.0, None),
    ])
    text = sp.narrative(frame)
    assert "3.0 times" in text["headline"]
    assert any("30.0 %" in f for f in text["findings"])
    assert any("5.0 times more" in f for f in text["findings"])



def test_narrative_says_one_station_is(monkeypatch):
    now = pd.DataFrame({"icao": ["KAUS", "KDFW", "KIAH"], "active_alerts": [1.0, 0.0, 0.0],
                        "alert_events": ["Wind Advisory", "", ""],
                        "observed_at": pd.Timestamp("2026-11-27 18:00", tz="UTC")})
    monkeypatch.setattr(sp, "current", lambda story: now)
    findings = sp.narrative(gold([]))["findings"]
    assert findings[0] == "Right now 1 of 3 stations is inside an active NWS alert: KAUS (Wind Advisory)."

def test_source_severity_puts_api_sources_first():
    frame = gold([
        ("source_severity", "storm_events", None, "F1 Storm Events", "web scraping", "records", 9.0, "LOW"),
        ("source_severity", "storm_events", None, "F1 Storm Events", "web scraping", "records_pct", 90.0, "LOW"),
        ("source_severity", "storm_events", None, "F1 Storm Events", "web scraping", "records", 1.0, "HIGH"),
        ("source_severity", "storm_events", None, "F1 Storm Events", "web scraping", "records_pct", 10.0, "HIGH"),
        ("source_severity", "weather_alerts", None, "F3 NWS alerts", "API", "records", 3.0, "MEDIUM"),
        ("source_severity", "weather_alerts", None, "F3 NWS alerts", "API", "records_pct", 100.0, "MEDIUM"),
    ])
    table = sp.source_severity(frame).set_index("source")
    assert table.index.tolist() == ["F3 NWS alerts", "F1 Storm Events"]
    assert table.at["F1 Storm Events", "records"] == 10
    assert table.at["F1 Storm Events", "MEDIUM"] == 0  # a level without rows is 0 %, not missing
    assert sp.source_severity(gold([])).empty  # Gold files written before Workshop 4


# ---------------------------------------------------------------- figures on the committed sample Gold files

@pytest.fixture(scope="module")
def apps():
    pytest.importorskip("dash")
    pytest.importorskip("dash_bootstrap_components")
    pytest.importorskip("pyarrow")
    from dashboard import governance_app, storytelling_app
    try:
        governance, gname = data.load("governance")
        story, sname = data.load("storytelling")
    except data.GoldFileMissing:
        pytest.skip("no Gold files in datalake_gold/")
    return governance_app, storytelling_app, governance, gname, story, sname


def test_every_figure_builds_from_gold(apps):
    gov_app, story_app, governance, gname, story, sname = apps
    figures = [
        gov_app.null_rate_figure(governance, gname), gov_app.null_rate_figure(governance, gname, "storm_events", True),
        gov_app.outlier_figure(governance, gname), gov_app.volume_figure(governance, gname, "linear"),
        gov_app.ingestion_figure(governance, gname), gov_app.completeness_figure(governance, gname),
        story_app.map_figure(story, sname), story_app.donut_figure(story, sname),
        story_app.high_share_figure(story, sname, "KDFW", "TMIN"),
        story_app.alert_volume_figure(story, sname), story_app.severity_trend_figure(story, sname),
        story_app.keyword_figure(story, sname), story_app.keyword_figure(story, sname, "storm_words"),
        story_app.storms_match_figure(story, sname), story_app.storm_volume_figure(story, sname),
        story_app.sources_figure(story, sname),
    ]
    station = next(iter(sp.stations(story)))
    figures.append(story_app.band_figure(story, sname, station, "PRCP"))
    for fig in figures:
        fig.to_dict()  # Plotly validates every property here
        assert fig.layout.title.text
    assert gov_app.kpi_table(governance, gp.load_thresholds()) is not None
    assert story_app.narrative_card(story) is not None


def test_source_severity_figure(apps):
    story_app = apps[1]
    frame = gold([("source_severity", "storm_events", None, "F1 Storm Events", "web scraping", metric, value, level)
                  for level, n in [("LOW", 8.0), ("MEDIUM", 1.0), ("HIGH", 1.0)]
                  for metric, value in [("records", n), ("records_pct", 10 * n)]])
    fig = story_app.source_severity_figure(frame, "storytelling_test.parquet")
    fig.to_dict()
    assert [trace.name.split(":")[0] for trace in fig.data] == ["Low", "Medium", "High"]
    assert not story_app.source_severity_figure(gold([]), "old.parquet").layout.title.text  # placeholder
