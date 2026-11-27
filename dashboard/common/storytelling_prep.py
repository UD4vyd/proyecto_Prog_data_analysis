"""Data preparation of the storytelling dashboard: pandas only, so it can be tested without Dash.

In this project the role of "sentiment" is played by two ordinal scores (Workshop 3 report, section 1):
the class of each station-day against its 1991-2020 history (NORMAL / UNUSUAL / EXTREME) and the CAP
severity of each NWS alert (Minor 1 ... Extreme 4).
"""
import pandas as pd

from dashboard.common.data import section
from dashboard.common.palette import CLASS_ORDER, SEVERITY_ORDER

ELEMENT_TEXT = {"TMAX": "Daily maximum temperature", "TMIN": "Daily minimum temperature",
                "PRCP": "Daily precipitation"}
ELEMENT_UNIT = {"TMAX": "°C", "TMIN": "°C", "PRCP": "mm"}
EXPECTED_HIGH_PCT = 10.0  # by construction 10 % of baseline days are above p90


def broad_class(label):
    """EXTREME_HIGH -> EXTREME, UNUSUAL_LOW -> UNUSUAL, NORMAL -> NORMAL."""
    return label.str.replace(r"_(HIGH|LOW)$", "", regex=True)


def _filter(rows, station="all", element="all"):
    if station != "all":
        rows = rows[rows["dim_1"] == station]
    if element != "all":
        rows = rows[rows["dim_2"] == element]
    return rows


def stations(story):
    """Station codes and names, from the current situation section (fallback: the class section)."""
    current = section(story, "current_situation")
    if not current.empty:
        return current.drop_duplicates("dim_1").set_index("dim_1")["dim_2"].sort_index().to_dict()
    codes = sorted(section(story, "station_day_class")["dim_1"].dropna().unique())
    return {code: code for code in codes}


def class_shares(story, station="all", element="all"):
    """Station-days per broad class (NORMAL / UNUSUAL / EXTREME) and their share, for the donut chart."""
    rows = section(story, "class_distribution")
    rows = _filter(rows[(rows["metric"] == "days") & (rows["label"] != "NO_BASELINE")], station, element)
    counts = rows.groupby(broad_class(rows["label"]))["value"].sum().reindex(CLASS_ORDER, fill_value=0)
    total = counts.sum()
    return pd.DataFrame({"class": counts.index, "days": counts.values,
                         "pct": 100 * counts.values / total if total else 0.0})


def high_share(story, element="TMAX", station="all", freq="MS"):
    """Share of days above p90 (UNUSUAL_HIGH or EXTREME_HIGH) per month or year, against 10 % expected."""
    rows = section(story, "class_distribution")
    rows = _filter(rows[(rows["metric"] == "days") & (rows["label"] != "NO_BASELINE")], station, element)
    if rows.empty:
        return pd.DataFrame(columns=["period", "days", "high_days", "extreme_days", "high_pct", "extreme_pct"])
    rows = rows.assign(high=rows["label"].isin(["UNUSUAL_HIGH", "EXTREME_HIGH"]) * rows["value"],
                       extreme=(rows["label"] == "EXTREME_HIGH") * rows["value"])
    period = rows["period"].dt.to_period("M" if freq == "MS" else "Y").dt.to_timestamp()
    grouped = rows.groupby(period).agg(days=("value", "sum"), high_days=("high", "sum"),
                                       extreme_days=("extreme", "sum"))
    grouped["high_pct"] = 100 * grouped["high_days"] / grouped["days"]
    grouped["extreme_pct"] = 100 * grouped["extreme_days"] / grouped["days"]
    return grouped.rename_axis("period").reset_index()


def station_days(story, station, element):
    """Observed value, class and baseline percentiles of every classified day of one station and element."""
    rows = section(story, "station_day_class")
    rows = rows[(rows["dim_1"] == station) & (rows["dim_2"] == element)]
    if rows.empty:
        return pd.DataFrame(columns=["date", "observed", "class", "p01", "p10", "p90", "p99", "baseline_n"])
    wide = rows.pivot_table(index="period", columns="metric", values="value")
    labels = rows[rows["metric"] == "observed"].set_index("period")["label"]
    wide["class"] = labels.reindex(wide.index)
    return wide.rename_axis("date").reset_index().sort_values("date").reset_index(drop=True)


def latest_classes(story):
    """Class of the last classified day of each station and element (GHCN-Daily arrives a few days late)."""
    rows = section(story, "station_day_class")
    rows = rows[rows["metric"] == "observed"]
    last = rows.sort_values("period").groupby(["dim_1", "dim_2"]).tail(1)
    return last[["dim_1", "dim_2", "period", "value", "label"]] \
        .rename(columns={"dim_1": "icao", "dim_2": "element", "period": "date"}).reset_index(drop=True)


def current(story):
    """Latest observation of each station and the NWS alerts active on its zone or county."""
    rows = section(story, "current_situation")
    if rows.empty:
        return pd.DataFrame()
    wide = rows.pivot_table(index=["dim_1", "dim_2"], columns="metric", values="value").reset_index()
    wide = wide.rename(columns={"dim_1": "icao", "dim_2": "name"})
    obs = rows[rows["topic"] == "station_observations"].drop_duplicates("dim_1").set_index("dim_1")["label"]
    events = rows[rows["metric"] == "active_alerts"].set_index("dim_1")["label"]
    wide["observed_at"] = pd.to_datetime(wide["icao"].map(obs), utc=True)
    wide["alert_events"] = wide["icao"].map(events).fillna("")
    if "active_alerts" not in wide:
        wide["active_alerts"] = 0.0
    wide["active_alerts"] = wide["active_alerts"].fillna(0)
    return wide.sort_values("icao").reset_index(drop=True)


def alerts_daily(story):
    """Alerts per local day and severity (wide), plus the total and the mean severity score per day."""
    rows = section(story, "alert_severity_daily")
    if rows.empty:
        return pd.DataFrame(columns=["day", "alerts", "mean_severity_score", *SEVERITY_ORDER])
    per_severity = rows[(rows["metric"] == "alerts") & (rows["dim_1"] != "ALL")] \
        .pivot_table(index="period", columns="dim_1", values="value", aggfunc="sum")
    per_severity = per_severity.reindex(columns=SEVERITY_ORDER).fillna(0)
    totals = rows[rows["dim_1"] == "ALL"].pivot_table(index="period", columns="metric", values="value")
    table = totals.join(per_severity, how="outer")
    table[SEVERITY_ORDER] = table[SEVERITY_ORDER].fillna(0)
    return table.rename_axis("day").reset_index().sort_values("day").reset_index(drop=True)


def alert_events(story):
    """Alerts and mean severity per event type, with the most frequent severity of each type."""
    rows = section(story, "alert_event_breakdown")
    if rows.empty:
        return pd.DataFrame(columns=["event", "alerts", "mean_severity_score", "main_severity"])
    counts = rows[rows["metric"] == "alerts"]
    per_event = counts.groupby("dim_1")["value"].sum().rename("alerts")
    main = counts.sort_values("value").groupby("dim_1").tail(1).set_index("dim_1")["dim_2"].rename("main_severity")
    score = rows[rows["metric"] == "mean_severity_score"].set_index("dim_1")["value"].rename("mean_severity_score")
    table = pd.concat([per_event, score, main], axis=1).rename_axis("event").reset_index()
    return table.sort_values(["alerts", "event"], ascending=[False, True]).reset_index(drop=True)


def keywords(story, topic="weather_alerts", kind="word"):
    """Top terms of alert descriptions or storm narratives, with the mean severity of the alerts using them.

    The severity exists only for alert words (keyword_severity section); other terms get NaN.
    """
    rows = section(story, "top_keywords", topic)
    rows = rows[rows["dim_2"] == kind][["dim_1", "value"]].rename(columns={"dim_1": "term", "value": "count"})
    severity = section(story, "keyword_severity", "weather_alerts")
    if topic == "weather_alerts" and kind == "word" and not severity.empty:
        wide = severity.pivot_table(index="dim_1", columns="metric", values="value")
        rows = rows.join(wide.rename(columns={"alerts": "alerts_with_term"}), on="term")
    for column in ["mean_severity_score", "alerts_with_term"]:
        if column not in rows:
            rows[column] = float("nan")
    return rows.sort_values(["count", "term"], ascending=[False, True]).reset_index(drop=True)


def sources(story):
    """Records, extraction method and covered period of each source."""
    rows = section(story, "source_comparison")
    if rows.empty:
        return pd.DataFrame(columns=["source", "method", "topic", "records", "first", "last"])
    covered = rows["label"].str.split(" to ", expand=True)
    table = pd.DataFrame({"source": rows["dim_1"].values, "method": rows["dim_2"].values,
                          "topic": rows["topic"].values, "records": rows["value"].values,
                          "first": pd.to_datetime(covered[0].values), "last": pd.to_datetime(covered[1].values)})
    return table.sort_values("first").reset_index(drop=True)


def storm_monthly(story):
    """Storm Events reports per month, with the most reported event type of each month."""
    rows = section(story, "storm_monthly_volume")
    if rows.empty:
        return pd.DataFrame(columns=["month", "reports", "top_type", "top_reports"])
    total = rows.groupby("period")["value"].sum().rename("reports")
    top = rows.sort_values(["value", "dim_1"]).groupby("period").tail(1).set_index("period")
    table = pd.concat([total, top["dim_1"].rename("top_type"), top["value"].rename("top_reports")], axis=1)
    return table.rename_axis("month").reset_index().sort_values("month").reset_index(drop=True)


def storm_impact(story, metric="damage_usd", top=12):
    """Storm Events impact per event type (events, deaths, injuries or damage), largest first."""
    rows = section(story, "storm_impact_by_type")
    rows = rows[rows["metric"] == metric][["dim_1", "value"]].rename(columns={"dim_1": "event_type"})
    rows = rows[rows["value"] > 0]
    return rows.sort_values(["value", "event_type"], ascending=[False, True]).head(top).reset_index(drop=True)


def extreme_vs_storms(story):
    """Station-days and share with a storm report in the station county or zone, per station and class."""
    rows = section(story, "extreme_vs_storms")
    if rows.empty:
        return pd.DataFrame(columns=["station", "class", "station_days", "days_with_storm_report_pct"])
    table = rows.pivot_table(index=["dim_1", "dim_2"], columns="metric", values="value").reset_index()
    table = table.rename(columns={"dim_1": "station", "dim_2": "class"})
    table["class"] = pd.Categorical(table["class"], CLASS_ORDER, ordered=True)
    return table.sort_values(["station", "class"]).reset_index(drop=True)


def narrative(story):
    """Key findings for the narrative card, computed from the Gold file (nothing is hard-coded).

    Returns a dict with a headline and a list of plain-language findings.
    """
    findings = []
    now = current(story)
    if not now.empty:
        with_alert = now[now["active_alerts"] > 0]
        when = now["observed_at"].max()
        if with_alert.empty:
            findings.append(f"Right now no official NWS alert covers any of the {len(now)} stations "
                            f"(latest observations {when:%b %d, %H:%M} UTC).")
        else:
            names = ", ".join(f"{r.icao} ({r.alert_events})" for r in with_alert.itertuples())
            findings.append(f"Right now {len(with_alert)} of {len(now)} stations are inside an active NWS alert: "
                            f"{names}.")
    yearly = high_share(story, "TMAX", freq="YS")
    headline = "How unusual is the weather at our Texas stations?"
    if not yearly.empty:
        overall = 100 * yearly["high_days"].sum() / yearly["days"].sum()
        hottest = yearly.loc[yearly["high_pct"].idxmax()]
        first, last = yearly["period"].min().year, yearly["period"].max().year
        ratio = overall / EXPECTED_HIGH_PCT
        headline = (f"Since {first}, unusually hot days are {ratio:.1f} times as frequent as in 1991-2020"
                    if ratio >= 1.2 else f"Since {first}, unusually hot days are about as frequent as in 1991-2020")
        findings.append(f"From {first} to {last}, {overall:.1f} % of days had an unusually high maximum temperature, "
                        f"against 10 % in the stations' own 1991-2020 history. {hottest['period'].year} was the "
                        f"hottest year ({hottest['high_pct']:.1f} % of days).")
    storms = extreme_vs_storms(story)
    overall_storms = storms[storms["station"] == "ALL"].set_index("class")["days_with_storm_report_pct"]
    if {"EXTREME", "NORMAL"} <= set(overall_storms.index) and overall_storms["NORMAL"] > 0:
        extreme, normal = overall_storms["EXTREME"], overall_storms["NORMAL"]
        findings.append(f"Days we label EXTREME had a NOAA storm report in the station's county or zone "
                        f"{extreme:.1f} % of the time, against {normal:.1f} % for normal days "
                        f"({extreme / normal:.1f} times more): the label points at real impacts.")
    events = alert_events(story)
    daily = alerts_daily(story)
    if not events.empty and not daily.empty:
        top = events.iloc[0]
        frequent = events[events["alerts"] >= 10]
        worst = (frequent if not frequent.empty else events).sort_values("mean_severity_score").iloc[-1]
        days = (daily["day"].max() - daily["day"].min()).days + 1
        findings.append(f"In the last {days} days the NWS issued {daily['alerts'].sum():.0f} alerts in Texas; "
                        f"the most common was {top['event']} ({top['alerts']:.0f}) and the most severe frequent "
                        f"type was {worst['event']} (mean severity {worst['mean_severity_score']:.1f} of 4).")
    return {"headline": headline, "findings": findings}
