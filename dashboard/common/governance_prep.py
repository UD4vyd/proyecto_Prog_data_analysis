"""Data preparation of the governance dashboard: pandas only, so it can be tested without Dash.

Every function takes the Gold governance DataFrame (long layout) and returns a small, chart-ready table.
"""
from pathlib import Path

import pandas as pd
import yaml

from dashboard.common.data import PROJECT_DIR, section
from dashboard.common.palette import TOPIC_ORDER

THRESHOLDS_FILE = PROJECT_DIR / "config" / "governance_thresholds.yaml"
HOURLY_TOPICS = ["weather_alerts", "station_observations"]


def load_thresholds(path=None):
    with open(Path(path or THRESHOLDS_FILE), encoding="utf-8") as f:
        return yaml.safe_load(f)["kpis"]


def _metric(rows, metric):
    """Value of one metric per topic, as a Series indexed by topic."""
    return rows[rows["metric"] == metric].set_index("topic")["value"]


def totals(gold):
    """One row per topic: files, raw rows, rows after de-duplication and duplicate rates."""
    volume = section(gold, "volume_total").pivot_table(index="topic", columns="metric", values="value")
    duplicates = section(gold, "duplicate_rate").pivot_table(index="topic", columns="metric", values="value")
    table = volume.join(duplicates, how="outer")
    return table.reindex([t for t in TOPIC_ORDER if t in table.index])


def null_rates(gold, topic=None, include_zero=False):
    """Null percentage per field, highest first. Fields at 0 % are left out unless include_zero."""
    rows = section(gold, "null_rate", topic)[["topic", "dim_1", "value"]]
    rows = rows.rename(columns={"dim_1": "field", "value": "null_pct"})
    if not include_zero:
        rows = rows[rows["null_pct"] > 0]
    return rows.sort_values(["null_pct", "field"], ascending=[False, True]).reset_index(drop=True)


def null_cell_pct(gold):
    """Share of null cells per topic and overall.

    The mean of the per-field null rates of a topic is exactly its share of null cells (every field has the
    same number of rows). Topics are weighted by rows x fields for the overall figure.
    """
    rates = section(gold, "null_rate")
    per_topic = rates.groupby("topic")["value"].agg(["mean", "count"])
    rows = _metric(section(gold, "volume_total"), "rows_after_dedup")
    per_topic["cells"] = per_topic["count"] * rows.reindex(per_topic.index).fillna(0)
    overall = (per_topic["mean"] * per_topic["cells"]).sum() / per_topic["cells"].sum() \
        if per_topic["cells"].sum() else float("nan")
    return per_topic["mean"], overall


def outlier_rates(gold):
    """Flag rate per topic, flag and station (station ALL is the whole topic)."""
    rows = section(gold, "outlier_rate")[["topic", "dim_1", "dim_2", "value"]]
    return rows.rename(columns={"dim_1": "flag", "dim_2": "station", "value": "flagged_pct"}) \
        .sort_values(["topic", "flag", "station"]).reset_index(drop=True)


def volume_daily(gold):
    """Records received per topic and UTC day of ingestion (historical loads appear as peaks)."""
    rows = section(gold, "volume_daily")
    rows = rows[rows["metric"] == "records"][["period", "topic", "value"]]
    return rows.rename(columns={"period": "day", "value": "records"}).sort_values(["day", "topic"]) \
        .reset_index(drop=True)


def ingestion(gold):
    """Hourly runs that reached Silver per day and topic, against the runs expected."""
    rows = section(gold, "ingestion_compliance")
    table = rows.pivot_table(index=["period", "topic"], columns="metric", values="value").reset_index()
    return table.rename(columns={"period": "day"}).sort_values(["day", "topic"]).reset_index(drop=True)


def recent_compliance(gold, days=7):
    """Mean compliance of the hourly topics over the last days with data (runs actual / runs expected)."""
    table = ingestion(gold)
    if table.empty:
        return float("nan")
    last = table[table["day"] > table["day"].max() - pd.Timedelta(days=days)]
    return 100 * last["runs_actual"].sum() / last["runs_expected"].sum()


def completeness(gold, metric="days_present_pct"):
    """Station x year matrix of one completeness metric (days present or nulls per element)."""
    rows = section(gold, "station_completeness")
    rows = rows[rows["metric"] == metric]
    rows = rows.assign(year=rows["period"].dt.year)
    return rows.pivot_table(index="dim_1", columns="year", values="value").sort_index()


def freshness(gold, now=None):
    """Last ingestion per topic, hours since then at the Gold run and now."""
    rows = section(gold, "freshness")[["topic", "value", "label"]]
    rows = rows.rename(columns={"value": "hours_at_gold_run", "label": "last_ingestion"})
    rows["last_ingestion"] = pd.to_datetime(rows["last_ingestion"], utc=True)
    now = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    rows["hours_now"] = (now - rows["last_ingestion"]).dt.total_seconds() / 3600
    return rows.set_index("topic").reindex([t for t in TOPIC_ORDER if t in set(rows["topic"])])


def status_of(value, rule, topic=None):
    """good, or the breach level of the rule, for a value against its threshold."""
    if value is None or pd.isna(value):
        return "info"
    limit = rule["limit"][topic] if isinstance(rule["limit"], dict) else rule["limit"]
    ok = value <= limit + 1e-9 if rule["direction"] == "max" else value >= limit - 1e-9
    return "good" if ok else rule["breach"]


def _limit_text(rule, topic=None):
    limit = rule["limit"][topic] if isinstance(rule["limit"], dict) else rule["limit"]
    sign = "≤" if rule["direction"] == "max" else "≥"
    return f"{sign} {limit:g}"


def kpi_table(gold, thresholds, now=None):
    """All governance KPIs with their value, threshold and status: the data quality summary table."""
    out = []

    def add(topic, key, value, detail=""):
        rule = thresholds[key]
        out.append({"topic": topic, "kpi": rule["text"], "value": value, "threshold": _limit_text(rule, topic),
                    "status": status_of(value, rule, topic), "detail": detail, "why": rule["why"]})

    table = totals(gold)
    null_topic, _ = null_cell_pct(gold)
    schema = section(gold, "schema_compliance").pivot_table(index="topic", columns="metric", values="value")
    outliers = outlier_rates(gold)
    text = section(gold, "text_length")
    fresh = freshness(gold, now)
    for topic in table.index:
        add(topic, "duplicate_rate_after_pct", table.at[topic, "duplicate_rate_after_pct"])
        add(topic, "duplicate_rate_before_pct", table.at[topic, "duplicate_rate_before_pct"],
            f"{table.at[topic, 'duplicated_rows']:,.0f} copies in {table.at[topic, 'rows_raw']:,.0f} Silver rows")
        add(topic, "null_cell_pct", null_topic.get(topic))
        if topic in schema.index:
            add(topic, "files_compliant_pct", schema.at[topic, "files_compliant_pct"],
                f"{schema.at[topic, 'files_checked']:.0f} files checked")
            add(topic, "rows_with_required_fields_pct", schema.at[topic, "rows_with_required_fields_pct"])
        flags = outliers[(outliers["topic"] == topic) & (outliers["station"] == "ALL")]
        if not flags.empty:
            worst = flags.loc[flags["flagged_pct"].idxmax()]
            add(topic, "outlier_flagged_pct", worst["flagged_pct"], f"flag {worst['flag']}")
        empty = text[(text["topic"] == topic) & (text["metric"] == "empty_pct")]
        if not empty.empty:
            add(topic, "text_empty_pct", empty["value"].iloc[0], empty["dim_1"].iloc[0])
        if topic in fresh.index:
            row = fresh.loc[topic]
            add(topic, "freshness_hours", row["hours_now"],
                f"last ingestion {row['last_ingestion']:%Y-%m-%d %H:%M} UTC")
    compliance = ingestion(gold)
    for topic in HOURLY_TOPICS:
        part = compliance[compliance["topic"] == topic]
        if not part.empty:
            last = part[part["day"] > part["day"].max() - pd.Timedelta(days=7)]
            value = 100 * last["runs_actual"].sum() / last["runs_expected"].sum()
            add(topic, "ingestion_compliance_pct", value,
                f"{last['runs_actual'].sum():.0f} of {last['runs_expected'].sum():.0f} hourly runs")
    days = completeness(gold)
    recent = days.loc[:, [c for c in days.columns if c >= 2021]]
    for station, values in recent.iterrows():
        add(f"daily_summaries ({station})", "station_days_present_pct", values.min(),
            f"worst year {values.idxmin()}")
    frame = pd.DataFrame(out)
    order = {"critical": 0, "warning": 1, "info": 2, "good": 3}
    return frame.assign(_o=frame["status"].map(order)).sort_values(["_o", "topic"], kind="stable") \
        .drop(columns="_o").reset_index(drop=True)


def headline(gold, thresholds, now=None):
    """Figures of the KPI cards at the top of the dashboard."""
    table = totals(gold)
    _, null_overall = null_cell_pct(gold)
    schema = section(gold, "schema_compliance").pivot_table(index="topic", columns="metric", values="value")
    files = (schema["files_compliant_pct"] * schema["files_checked"]).sum() / schema["files_checked"].sum()
    outliers = outlier_rates(gold)
    overall = outliers[outliers["station"] == "ALL"]
    worst = overall.loc[overall["flagged_pct"].idxmax()] if not overall.empty else None
    fresh = freshness(gold, now)
    fresh_status = {t: status_of(fresh.at[t, "hours_now"], thresholds["freshness_hours"], t) for t in fresh.index}
    stale = [t for t, s in fresh_status.items() if s != "good"]
    raw = table["rows_raw"].sum()
    return {
        "records": table["rows_after_dedup"].sum(),
        "records_raw": raw,
        "topics": len(table),
        "duplicate_before_pct": 100 * table["duplicated_rows"].sum() / raw if raw else float("nan"),
        "duplicate_after_pct": (table["duplicate_rate_after_pct"] * table["rows_after_dedup"]).sum()
        / table["rows_after_dedup"].sum(),
        "null_cell_pct": null_overall,
        "files_compliant_pct": files,
        "files_checked": schema["files_checked"].sum(),
        "worst_outlier_pct": None if worst is None else worst["flagged_pct"],
        "worst_outlier_flag": None if worst is None else f"{worst['flag']} ({worst['topic']})",
        "stale_topics": stale,
        "freshest_hours": fresh["hours_now"].min() if not fresh.empty else float("nan"),
        "ingestion_7d_pct": recent_compliance(gold),
    }
