"""Storytelling dashboard (Workshop 4) for the functional user: an emergency-management analyst in Texas.

Reads only the newest datalake_gold/storytelling_*.parquet with pandas and runs at http://localhost:8051.

    poetry run python dashboard/storytelling_app.py        # local
    docker compose up -d dashboard-storytelling            # Docker

Story, from overview to detail:
    1. Key findings and right now: is any station under an official alert? (US2, US4)
    2. How unusual has the weather been since 2021? class mix and the share of hot days (US3)
    3. One station, day by day, against its own 1991-2020 range (US3)
    4. What does the National Weather Service warn about? volume, severity and words (US1)
    5. Do our EXTREME days match real storm reports? (US7)
    6. Where the data come from (US5)
"""
import os
import sys
from pathlib import Path

if __package__ in (None, ""):  # run as a script: make the project root importable
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, ctx, dcc, html, no_update

from dashboard.common import components as ui
from dashboard.common import storytelling_prep as prep
from dashboard.common.data import GoldFileMissing, computed_at, load
from dashboard.common.palette import (CLASS_COLORS, CLASS_ORDER, CLASS_TEXT, METHOD_COLORS, NEUTRAL,
                                      SEVERITY_COLORS, SEVERITY_ORDER, TEXT, TEXT_MUTED)
from dashboard.common.theme import day_axis, empty_figure, finish

KIND = "storytelling"
ELEMENT_SHORT = {"TMAX": "Max temperature", "TMIN": "Min temperature", "PRCP": "Rain"}
HIGH_LINE, HIGH_TREND = "#c98f00", "#7a4f00"  # darker steps of the UNUSUAL hue, readable as lines on white
BAND_OUTER, BAND_INNER, OBSERVED = "#ededed", "#d4d4d4", "#4d4d4d"
STORM_BAR = "#257de0"


def source(file_name, detail):
    return f"Gold {file_name} · {detail}"


def pill(label):
    """Coloured class label; the word is always written, so colour is never the only cue."""
    broad = label.replace("_HIGH", "").replace("_LOW", "")
    arrow = " ▲" if label.endswith("_HIGH") else " ▼" if label.endswith("_LOW") else ""
    color = CLASS_COLORS.get(broad, NEUTRAL)
    return html.Span(CLASS_TEXT.get(broad, broad.title()) + arrow, className="class-pill",
                     style={"background": color, "color": TEXT if broad == "UNUSUAL" else "#ffffff"})


# ---------------------------------------------------------------- 1. findings and right now

def narrative_card(story):
    text = prep.narrative(story)
    return dbc.Card(dbc.CardBody([
        html.Div("Key findings", className="kpi-title"),
        html.H3(text["headline"], className="mt-1 mb-3"),
        html.Ul([html.Li(f) for f in text["findings"]], className="ps-3 mb-3"),
        html.Div([
            html.Span("How to read the labels: ", className="fw-semibold"),
            pill("NORMAL"), " inside the range of 8 in 10 days for that date in 1991-2020 · ",
            pill("UNUSUAL_HIGH"), " outside it · ", pill("EXTREME_HIGH"),
            " beyond the rarest 1 in 100 days. ▲ hotter or wetter, ▼ colder.",
        ], className="small"),
    ]), className="h-100 shadow-sm story-card")


def map_figure(story, file_name):
    now = prep.current(story)
    if now.empty:
        return empty_figure("No current observations in this Gold file.", height=380)
    fig = go.Figure()
    for has_alert, name in [(False, "No active alert"), (True, "Inside an active NWS alert")]:
        part = now[(now["active_alerts"] > 0) == has_alert]
        if part.empty:
            continue
        if has_alert:  # colour of the most severe active alert (Moderate if the severity is unknown)
            colors = [SEVERITY_COLORS[SEVERITY_ORDER[int(v) - 1]] if pd.notna(v) else SEVERITY_COLORS["Moderate"]
                      for v in part.get("max_severity", pd.Series([float("nan")] * len(part)))]
        else:
            colors = [CLASS_COLORS["NORMAL"]] * len(part)
        hover = [f"<b>{r.icao}</b> · {r.name.title()}<br>{r.temperature_c:.0f} °C · humidity "
                 f"{r.relative_humidity_pct:.0f} % · wind {r.wind_speed_ms:.1f} m/s<br>"
                 f"{('Alerts: ' + r.alert_events) if r.alert_events else 'No active alert'}<br>"
                 f"Observed {r.observed_at:%b %d %H:%M} UTC" for r in part.itertuples()]
        fig.add_trace(go.Scattergeo(
            lon=part["longitude"], lat=part["latitude"], text=part["icao"], mode="markers+text",
            textposition="top center", name=name, hovertext=hover, hoverinfo="text",
            marker=dict(size=16, color=colors, symbol="diamond" if has_alert else "circle",
                        line=dict(width=2, color="#ffffff"))))
    fig.update_geos(scope="north america", projection_type="mercator", resolution=50,
                    lataxis_range=[25.5, 36.8], lonaxis_range=[-107, -93], showland=True, landcolor="#f2f2f2",
                    showsubunits=True, subunitcolor="#9c9c9c", showcountries=True, countrycolor="#9c9c9c",
                    showocean=True, oceancolor="#e8f1fb", showlakes=False, bgcolor="rgba(0,0,0,0)")
    fig.update_layout(margin=dict(l=0, r=0, t=70, b=85), legend=dict(y=1.0))
    when = now["observed_at"].max()
    return finish(fig, "Right now: latest observation of each station",
                  source(file_name, "section current_situation (F4 observations + F3 active alerts)"), height=380,
                  subtitle=f"Observations of {when:%b %d, %H:%M} UTC. Hover a station for details.")


def station_cards(story):
    now = prep.current(story)
    last = prep.latest_classes(story)
    cards = []
    for r in now.itertuples():
        days = last[last["icao"] == r.icao].set_index("element")
        day = days["date"].max()
        history = []
        for element in ["TMAX", "TMIN", "PRCP"]:
            if element in days.index:
                row = days.loc[element]
                history.append(html.Div([f"{ELEMENT_SHORT[element]} {row['value']:.1f} "
                                         f"{prep.ELEMENT_UNIT[element]} ", pill(row["label"])],
                                        className="station-metric"))
        alert = (html.Div(f"⚠ Active alert: {r.alert_events}", className="fw-semibold", style={"color": "#b3261e"})
                 if r.active_alerts > 0 else html.Div("✔ No active NWS alert", style={"color": "#1a7a5c"}))
        cards.append(dbc.Col(dbc.Card(dbc.CardBody([
            html.Div(f"{r.icao} · {r.name.title()}", className="station-name"),
            html.Div(f"{r.temperature_c:.0f} °C now · humidity {r.relative_humidity_pct:.0f} % · "
                     f"wind {r.wind_speed_ms:.1f} m/s", className="station-metric"),
            alert,
            html.Hr(className="my-2"),
            html.Div(f"Last complete day in the station history ({day:%b %d}):" if pd.notna(day)
                     else "No classified day yet", className="small text-muted"),
            *history,
        ]), className="h-100 shadow-sm station-card"), md=4, className="mb-3"))
    return cards


# ---------------------------------------------------------------- 2. how unusual since 2021

def donut_figure(story, file_name, station="all", element="all"):
    shares = prep.class_shares(story, station, element)
    total = shares["days"].sum()
    if total == 0:
        return empty_figure("No classified days for this selection.")
    fig = go.Figure(go.Pie(
        labels=[CLASS_TEXT[c] for c in shares["class"]], values=shares["days"], hole=0.58, sort=False,
        direction="clockwise", marker=dict(colors=[CLASS_COLORS[c] for c in shares["class"]],
                                           line=dict(color="#ffffff", width=2)),
        texttemplate="%{label}<br>%{percent:.1%}", textposition="outside",
        hovertemplate="%{label}: %{value:,.0f} station-days (%{percent:.1%})<extra></extra>"))
    fig.add_annotation(text=f"<b>{total:,.0f}</b><br>station-days", x=0.5, y=0.5, xref="paper", yref="paper",
                       showarrow=False, font=dict(size=14))
    # Every slice carries its name and share, so a separate legend is not needed (it collided with the labels).
    fig.update_layout(showlegend=False, margin=dict(l=40, r=40, t=110, b=90))
    what = "all stations" if station == "all" else station
    measure = "all measures" if element == "all" else ELEMENT_SHORT[element].lower()
    return finish(fig, "How often is a day unusual?", source(file_name, "section class_distribution"),
                  subtitle=f"Days since 2021 against 1991-2020 · {what} · {measure}")


def high_share_figure(story, file_name, station="all", element="TMAX"):
    element = "TMAX" if element == "all" else element
    monthly = prep.high_share(story, element, station)
    if monthly.empty:
        return empty_figure("No classified days for this selection.")
    rolling = monthly.set_index("period")["high_days"].rolling(12, min_periods=6).sum() / \
        monthly.set_index("period")["days"].rolling(12, min_periods=6).sum() * 100
    word = {"TMAX": "hot (max temperature)", "TMIN": "warm nights (min temperature)", "PRCP": "very wet"}[element]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=monthly["period"], y=monthly["high_pct"], name="Each month", mode="lines",
                             line=dict(color=HIGH_LINE, width=1.5),
                             customdata=monthly[["high_days", "days"]],
                             hovertemplate="%{x|%b %Y}: %{y:.0f} % of days (%{customdata[0]:.0f} of "
                                           "%{customdata[1]:.0f})<extra></extra>"))
    fig.add_trace(go.Scatter(x=rolling.index, y=rolling.values, name="12-month average", mode="lines",
                             line=dict(color=HIGH_TREND, width=3),
                             hovertemplate="12 months to %{x|%b %Y}: %{y:.1f} %<extra></extra>"))
    fig.add_hline(y=prep.EXPECTED_HIGH_PCT, line_dash="dash", line_color=TEXT_MUTED,
                  annotation_text="10 % expected from 1991-2020", annotation_position="top left",
                  annotation_font_color=TEXT_MUTED)
    fig.update_xaxes(title_text="Month", hoverformat="%b %Y")
    fig.update_yaxes(title_text="Days above the usual range (%)", ticksuffix=" %", rangemode="tozero")
    fig.update_layout(hovermode="x unified")
    what = "all stations" if station == "all" else station
    return finish(fig, f"Share of {word} days, month by month", source(file_name, "section class_distribution"),
                  subtitle=f"Days above the 90th percentile of the same dates in 1991-2020 · {what}")


# ---------------------------------------------------------------- 3. one station day by day

def band_figure(story, file_name, station, element):
    days = prep.station_days(story, station, element)
    if days.empty:
        return empty_figure("No classified days for this station and measure.", height=420)
    unit = prep.ELEMENT_UNIT[element]
    fig = go.Figure()
    for low, high, color, name in [("p01", "p99", BAND_OUTER, "Rarest 1 % limits (p1-p99)"),
                                   ("p10", "p90", BAND_INNER, "Usual range, 8 in 10 days (p10-p90)")]:
        fig.add_trace(go.Scatter(x=days["date"], y=days[high], mode="lines", line=dict(width=0), hoverinfo="skip",
                                 showlegend=False))
        fig.add_trace(go.Scatter(x=days["date"], y=days[low], mode="lines", line=dict(width=0), fill="tonexty",
                                 fillcolor=color, name=name, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=days["date"], y=days["observed"], mode="lines", name="Observed", line=dict(color=OBSERVED, width=1.2),
        customdata=days[["p10", "p90", "class"]],
        hovertemplate="%{x|%a %b %d, %Y}<br>Observed %{y:.1f} " + unit
                      + "<br>Usual range %{customdata[0]:.1f} to %{customdata[1]:.1f} " + unit
                      + "<br>%{customdata[2]}<extra></extra>"))
    for label in ["UNUSUAL_HIGH", "EXTREME_HIGH", "UNUSUAL_LOW", "EXTREME_LOW"]:
        part = days[days["class"] == label]
        if part.empty:
            continue
        broad = label.split("_")[0]
        fig.add_trace(go.Scatter(
            x=part["date"], y=part["observed"], mode="markers",
            name=f"{CLASS_TEXT[broad]} {'high' if label.endswith('HIGH') else 'low'}",
            marker=dict(color=CLASS_COLORS[broad], size=8 if broad == "UNUSUAL" else 10,
                        symbol="triangle-up" if label.endswith("HIGH") else "triangle-down",
                        line=dict(width=1, color="#ffffff")),
            hoverinfo="skip"))
    end = days["date"].max()
    fig.update_xaxes(title_text="Day", range=[end - pd.Timedelta(days=365), end + pd.Timedelta(days=3)],
                     rangeselector=dict(buttons=[dict(count=3, label="3 months", step="month", stepmode="backward"),
                                                 dict(count=1, label="1 year", step="year", stepmode="backward"),
                                                 dict(count=3, label="3 years", step="year", stepmode="backward"),
                                                 dict(label="All since 2021", step="all")], y=1.02, x=1,
                                        xanchor="right"))
    fig.update_yaxes(title_text=f"{prep.ELEMENT_TEXT[element]} ({unit})")
    # Six legend entries: on the right, so they never collide with the title or the range buttons.
    fig.update_layout(hovermode="x unified", legend=dict(orientation="v", x=1.01, y=1, xanchor="left",
                                                         yanchor="top"))
    baseline = days["baseline_n"].median()
    return finish(fig, f"{station}: {prep.ELEMENT_TEXT[element].lower()} against its own history",
                  source(file_name, f"section station_day_class (F2 GHCN-Daily, about {baseline:.0f} baseline "
                                    f"values per date)"), height=460,
                  subtitle="Grey bands: what is usual for each date in 1991-2020. Triangles: unusual (yellow) and "
                           "extreme (red) days.")


# ---------------------------------------------------------------- 4. NWS alerts

def alert_volume_figure(story, file_name):
    daily = prep.alerts_daily(story)
    if daily.empty:
        return empty_figure("No alerts in this Gold file.")
    fig = go.Figure()
    for severity in SEVERITY_ORDER:
        if daily[severity].sum() == 0:
            continue
        fig.add_trace(go.Bar(x=daily["day"], y=daily[severity], name=severity,
                             marker=dict(color=SEVERITY_COLORS[severity], line=dict(color="#ffffff", width=1)),
                             hovertemplate="%{x|%a %b %d}: %{y:.0f} " + severity + " alerts<extra></extra>"))
    peak = daily.loc[daily["alerts"].idxmax()]
    fig.add_annotation(x=peak["day"], y=peak["alerts"], text=f"Peak: {peak['alerts']:.0f} alerts",
                       showarrow=True, arrowhead=0, ax=0, ay=-28, font=dict(color=TEXT))
    fig.update_layout(barmode="stack")
    day_axis(fig, daily["day"], "Day (Texas time)")
    fig.update_yaxes(title_text="Alerts issued")
    return finish(fig, "Alerts issued per day in Texas, by severity", source(file_name, "section alert_severity_daily "
                  "(F3 NWS alerts, real alerts only)"), subtitle="Taller bars are days of more official warnings.")


def severity_trend_figure(story, file_name):
    daily = prep.alerts_daily(story)
    if daily.empty:
        return empty_figure("No alerts in this Gold file.")
    fig = go.Figure(go.Scatter(
        x=daily["day"], y=daily["mean_severity_score"], mode="lines+markers", name="Average severity",
        line=dict(color=SEVERITY_COLORS["Severe"], width=2), marker=dict(size=8),
        customdata=daily[["alerts"]],
        hovertemplate="%{x|%a %b %d}: average %{y:.2f} over %{customdata[0]:.0f} alerts<extra></extra>"))
    day_axis(fig, daily["day"], "Day (Texas time)")
    fig.update_yaxes(title_text="Average severity", range=[0.8, 4.2], tickvals=[1, 2, 3, 4],
                     ticktext=["1 Minor", "2 Moderate", "3 Severe", "4 Extreme"])
    return finish(fig, "How serious were the alerts? Average severity per day",
                  source(file_name, "section alert_severity_daily (CAP severity: Minor 1 to Extreme 4)"),
                  subtitle="Higher means the day's warnings were, on average, more serious.")


KEYWORD_SETS = {"alert_words": ("weather_alerts", "word", "Words in NWS alert texts"),
                "alert_bigrams": ("weather_alerts", "bigram", "Two-word phrases in NWS alert texts"),
                "storm_words": ("storm_events", "word", "Words in NOAA storm reports (2021-2025)")}
SEVERITY_BINS = [(1.5, "Mostly minor (below 1.5)", "Minor"), (2.5, "Mostly moderate (1.5 to 2.5)", "Moderate"),
                 (3.5, "Mostly severe (2.5 to 3.5)", "Severe"), (9, "Extreme (3.5 or more)", "Extreme")]


def keyword_figure(story, file_name, choice="alert_words", top=20):
    topic, kind, title = KEYWORD_SETS[choice]
    words = prep.keywords(story, topic, kind).head(top).iloc[::-1]
    if words.empty:
        return empty_figure("No keywords for this selection.", height=480)
    group = pd.Series("No severity for this term", index=words.index)
    lower = 0
    for upper, name, _ in SEVERITY_BINS:
        group[(words["mean_severity_score"] >= lower) & (words["mean_severity_score"] < upper)] = name
        lower = upper
    colors = {name: SEVERITY_COLORS[severity] for _, name, severity in SEVERITY_BINS}
    colors["No severity for this term"] = NEUTRAL
    fig = go.Figure()
    for name in [n for _, n, _ in SEVERITY_BINS] + ["No severity for this term"]:
        part = words[group == name]
        if part.empty:
            continue
        hover = [f"<b>{r.term}</b>: {r.count:,.0f} mentions" + (
            f"<br>average severity {r.mean_severity_score:.2f} in {r.alerts_with_term:.0f} alerts"
            if pd.notna(r.mean_severity_score) else "") for r in part.itertuples()]
        fig.add_trace(go.Bar(
            x=part["count"], y=part["term"], orientation="h", name=name,
            marker=dict(color=colors[name], cornerradius=4), hovertext=hover, hoverinfo="text"))
    fig.update_yaxes(categoryorder="array", categoryarray=words["term"].tolist(), title_text=None)
    fig.update_xaxes(title_text="Mentions")
    subtitle = ("Colour: average severity of the alerts that use the word." if topic == "weather_alerts"
                and kind == "word" else "Severity is only measured for single words of alert texts.")
    return finish(fig, f"Most frequent terms · {title}", source(file_name, "sections top_keywords and "
                  "keyword_severity"), height=520, subtitle=subtitle)


# ---------------------------------------------------------------- 5. indicator against storm reports

def storms_match_figure(story, file_name):
    table = prep.extreme_vs_storms(story)
    if table.empty:
        return empty_figure("No storm match rows in this Gold file.")
    order = ["ALL"] + sorted(s for s in table["station"].unique() if s != "ALL")
    names = {s: ("All stations" if s == "ALL" else s) for s in order}
    fig = go.Figure()
    for cls in CLASS_ORDER:
        part = table[table["class"] == cls].set_index("station").reindex(order).dropna(subset=["station_days"])
        fig.add_trace(go.Bar(
            x=[names[s] for s in part.index], y=part["days_with_storm_report_pct"], name=f"{CLASS_TEXT[cls]} days",
            marker=dict(color=CLASS_COLORS[cls], cornerradius=4),
            text=part["days_with_storm_report_pct"].map(lambda v: f"{v:.0f} %"), textposition="outside",
            cliponaxis=False, customdata=part[["station_days"]],
            hovertemplate="%{x} · " + CLASS_TEXT[cls] + " days: %{y:.1f} % had a storm report "
                          "(%{customdata[0]:,.0f} station-days)<extra></extra>"))
    fig.update_layout(barmode="group", bargroupgap=0.06)
    fig.update_xaxes(title_text=None)
    fig.update_yaxes(title_text="Days with a storm report nearby (%)", ticksuffix=" %", rangemode="tozero")
    return finish(fig, "Do our EXTREME days match real storm reports?",
                  source(file_name, "section extreme_vs_storms (F1 Storm Events, same county or zone, same day, "
                                    "2021-2025)"),
                  subtitle="If the label works, extreme days should have storm reports more often than normal days.")


def storm_volume_figure(story, file_name):
    monthly = prep.storm_monthly(story)
    if monthly.empty:
        return empty_figure("No storm reports in this Gold file.")
    fig = go.Figure(go.Bar(
        x=monthly["month"], y=monthly["reports"], name="Storm reports", marker=dict(color=STORM_BAR),
        customdata=monthly[["top_type", "top_reports"]],
        hovertemplate="%{x|%b %Y}: %{y:,.0f} reports<br>Most reported: %{customdata[0]} "
                      "(%{customdata[1]:,.0f})<extra></extra>"))
    for row in monthly.nlargest(3, "reports").itertuples():
        fig.add_annotation(x=row.month, y=row.reports, text=f"{row.month:%b %Y}<br>{row.top_type}",
                           showarrow=True, arrowhead=0, ax=0, ay=-30, font=dict(size=11, color=TEXT))
    fig.update_xaxes(title_text="Month")
    fig.update_yaxes(title_text="Storm reports in Texas per month", rangemode="tozero")
    return finish(fig, "When is storm activity highest?",
                  source(file_name, "section storm_monthly_volume (F1 Storm Events, Texas)"),
                  subtitle="Spring hail and thunderstorm seasons produce the peaks.")


# ---------------------------------------------------------------- 6. sources

def sources_figure(story, file_name):
    table = prep.sources(story)
    if table.empty:
        return empty_figure("No source rows in this Gold file.", height=300)
    fig = go.Figure()
    for method in table["method"].unique():
        part = table[table["method"] == method]
        span = (part["last"] - part["first"]).dt.total_seconds() * 1000 + 86400000
        fig.add_trace(go.Bar(
            x=span, base=part["first"], y=part["source"], orientation="h", name=f"Collected by {method}",
            marker=dict(color=METHOD_COLORS.get(method, NEUTRAL), cornerradius=4),
            hovertext=[f"<b>{r.source}</b><br>{r.records:,.0f} records<br>{r.first:%Y-%m-%d} to {r.last:%Y-%m-%d}"
                       for r in part.itertuples()], hoverinfo="text"))
        # Recent sources cover a few days only: a marker keeps them visible next to 35 years of history.
        fig.add_trace(go.Scatter(
            x=part["last"], y=part["source"], mode="markers+text", showlegend=False, hoverinfo="skip",
            marker=dict(color=METHOD_COLORS.get(method, NEUTRAL), size=10, symbol="diamond"),
            text=[f"  {r:,.0f} records" for r in part["records"]], textposition="middle right",
            textfont=dict(color=TEXT)))
    start = table["first"].min() - pd.Timedelta(days=365)
    end = table["last"].max() + pd.Timedelta(days=8 * 365)
    fig.update_xaxes(type="date", title_text="Period covered", range=[start, end])
    fig.update_yaxes(title_text=None, categoryorder="array", categoryarray=table["source"].tolist()[::-1])
    return finish(fig, "Where the data come from: period and size of each source",
                  source(file_name, "section source_comparison"), height=300,
                  subtitle="Three APIs and one web-scraped NOAA file listing. History explains, the APIs say what "
                           "is happening now.")


LEVEL_TEXT = {"LOW": "Low: normal day, minor alert, no reported damage",
              "MEDIUM": "Medium: unusual day, moderate alert, damage reported",
              "HIGH": "High: extreme day, severe alert, deaths or injuries"}
LEVEL_CLASS = {"LOW": "NORMAL", "MEDIUM": "UNUSUAL", "HIGH": "EXTREME"}  # same colours as the station-day classes


def source_severity_figure(story, file_name):
    table = prep.source_severity(story)
    if table.empty:
        return empty_figure("This Gold file has no source_severity section (written before Workshop 4).",
                            height=300)
    labels = [f"{r.source} · {r.method}<br>{r.records:,.0f} records" for r in table.itertuples()]
    fig = go.Figure()
    for level in prep.LEVEL_ORDER:
        fig.add_trace(go.Bar(
            x=table[level], y=labels, orientation="h", name=LEVEL_TEXT[level],
            marker=dict(color=CLASS_COLORS[LEVEL_CLASS[level]], line=dict(color="#ffffff", width=1)),
            text=table[level].map(lambda v: f"{v:.0f} %" if v >= 4 else ""), textposition="inside",
            insidetextanchor="middle", textfont=dict(color=TEXT if level == "MEDIUM" else "#ffffff"),
            customdata=table[[f"{level}_records"]],
            hovertemplate="%{y}<br>" + level.title() + ": %{x:.1f} % (%{customdata[0]:,.0f} records)<extra></extra>"))
    # Legend in the same order as the bar segments, left to right (Plotly reverses stacked bars by default).
    fig.update_layout(barmode="stack", legend=dict(traceorder="normal"))
    fig.update_xaxes(title_text="Share of the records of each source (%)", range=[0, 100.5], ticksuffix=" %")
    fig.update_yaxes(title_text=None, categoryorder="array", categoryarray=labels[::-1])
    return finish(fig, "How serious is what each source reports? API sources next to the scraped source",
                  source(file_name, "section source_severity (F4 observations have no severity scale)"),
                  height=420, subtitle="Each source brought to the same three levels. Bar label: share of its "
                                       "records; the record count is next to the source name.")


# ---------------------------------------------------------------- layout

def layout():
    element_options = [{"label": "All measures", "value": "all"}] + \
        [{"label": ELEMENT_SHORT[e], "value": e} for e in ["TMAX", "TMIN", "PRCP"]]
    band_elements = [{"label": prep.ELEMENT_TEXT[e], "value": e} for e in ["TMAX", "TMIN", "PRCP"]]
    return dbc.Container([
        dcc.Store(id="gold-file"),
        ui.header("Storytelling dashboard: how unusual is the weather, and is anyone warned?",
                  "emergency-management analyst of a Texas state or county office", "updated"),
        ui.refresh_controls("refresh", "interval"),
        html.Div(id="load-error"),
        ui.section_title("1. Key findings and right now", "Is today normal, and is there an official alert?"),
        dbc.Row([
            dbc.Col(html.Div(id="narrative", className="h-100"), lg=5, className="mb-3"),
            dbc.Col(ui.chart_card("map-chart"), lg=7, className="mb-3"),
        ]),
        dbc.Row(id="station-cards"),
        ui.section_title("2. How unusual has the weather been since 2021?",
                         "Each station-day is compared with the same dates in 1991-2020"),
        dbc.Row([
            dbc.Col(dcc.Dropdown(id="story-station", options=[{"label": "All stations", "value": "all"}],
                                 value="all", clearable=False), md=3),
            dbc.Col(dcc.Dropdown(id="story-element", options=element_options, value="all", clearable=False), md=3),
            dbc.Col(html.Small("These filters apply to both charts of this section. The trend uses max temperature "
                               "when all measures are selected.", style={"color": TEXT_MUTED}), md=6),
        ], className="mb-2 g-2"),
        dbc.Row([
            dbc.Col(ui.chart_card("donut-chart"), lg=4, className="mb-3"),
            dbc.Col(ui.chart_card("trend-chart", note="If the climate of the stations had not changed, about 1 day "
                                                      "in 10 would be above the usual range."), lg=8, className="mb-3"),
        ]),
        ui.section_title("3. One station, day by day", "Pick a station and a measure; zoom with the buttons or by "
                                                         "dragging over the chart"),
        dbc.Row(dbc.Col(ui.chart_card("band-chart", controls=dbc.Row([
            dbc.Col(dcc.Dropdown(id="band-station", clearable=False), md=4),
            dbc.Col(dcc.Dropdown(id="band-element", options=band_elements, value="TMAX", clearable=False), md=4),
        ], className="g-2")), className="mb-3")),
        ui.section_title("4. What does the National Weather Service warn about?",
                         "Official alerts for Texas over the last days"),
        dbc.Row([
            dbc.Col(ui.chart_card("alert-volume-chart"), lg=6, className="mb-3"),
            dbc.Col(ui.chart_card("severity-chart"), lg=6, className="mb-3"),
        ]),
        dbc.Row(dbc.Col(ui.chart_card("keyword-chart", controls=dbc.RadioItems(
            id="keyword-set", options=[{"label": v[2], "value": k} for k, v in KEYWORD_SETS.items()],
            value="alert_words", inline=True),
            note="Advisory words (coastal, water) come with milder alerts; flood and fog words with more serious "
                 "ones."), className="mb-3")),
        ui.section_title("5. Does our label match real impacts?",
                         "Past EXTREME days next to NOAA storm reports of the same county"),
        dbc.Row([
            dbc.Col(ui.chart_card("storms-chart"), lg=6, className="mb-3"),
            dbc.Col(ui.chart_card("storm-volume-chart"), lg=6, className="mb-3"),
        ]),
        ui.section_title("6. Where the data come from", "How big, how long and how serious is each source?"),
        dbc.Row(dbc.Col(ui.chart_card("source-severity-chart", note="Alerts are only issued when there is something "
                                                                     "to warn about, so they lean to Medium and High; "
                                                                     "most station days and storm reports are Low."),
                        className="mb-3")),
        dbc.Row(dbc.Col(ui.chart_card("sources-chart", note="The label is a monitoring aid: it says how rare a "
                                                             "measured value is, it does not forecast."),
                        className="mb-4")),
    ], fluid=True, className="px-4 py-3", style={"maxWidth": "1800px"})


app = Dash(__name__, title="Storytelling · Extreme Weather TX", external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = layout


# ---------------------------------------------------------------- callbacks

@app.callback(Output("gold-file", "data"), Output("updated", "children"), Output("load-error", "children"),
              Input("refresh", "n_clicks"), Input("interval", "n_intervals"), State("gold-file", "data"))
def refresh(_clicks, _ticks, current):
    """Find the newest Gold file. The automatic refresh only redraws the charts when the file changed."""
    try:
        story, name = load(KIND)
    except GoldFileMissing as error:
        return None, "No Gold data yet", ui.error_panel(str(error))
    run = computed_at(story)
    stamp = [html.Div(f"Data last updated: {run:%Y-%m-%d %H:%M} UTC", className="fw-semibold"),
             html.Div(f"File {name}", className="small")]
    if ctx.triggered_id == "interval" and current and current.get("file") == name:
        return no_update, stamp, None
    return {"file": name, "run": run.isoformat()}, stamp, None


def _story(data):
    if not data:
        return None, None
    return load(KIND)


@app.callback(Output("narrative", "children"), Output("station-cards", "children"), Output("map-chart", "figure"),
              Output("story-station", "options"), Output("band-station", "options"), Output("band-station", "value"),
              Input("gold-file", "data"), State("band-station", "value"))
def update_overview(data, band_station):
    story, name = _story(data)
    if story is None:
        return None, [], empty_figure("No Gold data"), [], [], None
    stations = prep.stations(story)
    options = [{"label": f"{code} · {label.title()}", "value": code} for code, label in stations.items()]
    if band_station not in stations:
        band_station = "KDFW" if "KDFW" in stations else next(iter(stations), None)
    return (narrative_card(story), station_cards(story), map_figure(story, name),
            [{"label": "All stations", "value": "all"}] + options, options, band_station)


@app.callback(Output("donut-chart", "figure"), Output("trend-chart", "figure"), Input("gold-file", "data"),
              Input("story-station", "value"), Input("story-element", "value"))
def update_unusual(data, station, element):
    story, name = _story(data)
    if story is None:
        return empty_figure("No Gold data"), empty_figure("No Gold data")
    return donut_figure(story, name, station, element), high_share_figure(story, name, station, element)


@app.callback(Output("band-chart", "figure"), Input("gold-file", "data"), Input("band-station", "value"),
              Input("band-element", "value"))
def update_band(data, station, element):
    story, name = _story(data)
    if story is None or not station:
        return empty_figure("No Gold data", height=420)
    return band_figure(story, name, station, element)


@app.callback(Output("alert-volume-chart", "figure"), Output("severity-chart", "figure"), Input("gold-file", "data"))
def update_alerts(data):
    story, name = _story(data)
    if story is None:
        return empty_figure("No Gold data"), empty_figure("No Gold data")
    return alert_volume_figure(story, name), severity_trend_figure(story, name)


@app.callback(Output("keyword-chart", "figure"), Input("gold-file", "data"), Input("keyword-set", "value"))
def update_keywords(data, choice):
    story, name = _story(data)
    return empty_figure("No Gold data") if story is None else keyword_figure(story, name, choice)


@app.callback(Output("storms-chart", "figure"), Output("storm-volume-chart", "figure"),
              Output("sources-chart", "figure"), Output("source-severity-chart", "figure"), Input("gold-file", "data"))
def update_storms(data):
    story, name = _story(data)
    if story is None:
        return [empty_figure("No Gold data")] * 4
    return (storms_match_figure(story, name), storm_volume_figure(story, name), sources_figure(story, name),
            source_severity_figure(story, name))


if __name__ == "__main__":
    app.run(host=os.environ.get("DASH_HOST", "127.0.0.1"), port=int(os.environ.get("STORYTELLING_PORT", 8051)),
            debug=os.environ.get("DASH_DEBUG", "0") == "1")
