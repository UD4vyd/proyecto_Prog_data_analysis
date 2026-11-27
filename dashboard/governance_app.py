"""Governance dashboard (Workshop 4): data quality KPIs of the pipeline for a technical audience.

Reads only the newest datalake_gold/governance_*.parquet with pandas and runs at http://localhost:8050.

    poetry run python dashboard/governance_app.py          # local
    docker compose up -d dashboard-governance              # Docker

Layout, top to bottom: KPI cards, null rate per field, outlier rate per check, volume per ingestion day,
hourly ingestion compliance, station completeness, and the data quality summary table with thresholds.
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
from dashboard.common import governance_prep as prep
from dashboard.common.data import GoldFileMissing, computed_at, load
from dashboard.common.palette import (NEUTRAL, STATION_COLORS, STATUS, TEXT_MUTED, TOPIC_COLORS, TOPIC_ORDER,
                                      TOPIC_TEXT)
from dashboard.common.theme import empty_figure, finish

KIND = "governance"
FIELD_UNUSABLE_PCT = 50  # a field above this null rate cannot feed a chart (reference line)


def source(file_name, detail):
    return f"Gold {file_name} · {detail}"


# ---------------------------------------------------------------- figures (pure: DataFrame in, figure out)

def null_rate_figure(gold, file_name, topic="all", include_zero=False):
    rows = prep.null_rates(gold, None if topic == "all" else topic, include_zero)
    if rows.empty:
        return empty_figure("No field has null values for this selection.")
    rows = rows.iloc[::-1]  # Plotly draws the first category at the bottom: highest rate on top
    labels = rows["field"] if topic != "all" else rows["field"] + " · " + rows["topic"].map(TOPIC_TEXT)
    fig = go.Figure(go.Bar(
        x=rows["null_pct"], y=labels, orientation="h",
        marker=dict(color=rows["topic"].map(TOPIC_COLORS), cornerradius=4),
        text=rows["null_pct"].map(lambda v: f"{v:.1f} %"), textposition="outside", cliponaxis=False,
        customdata=rows["topic"].map(TOPIC_TEXT),
        hovertemplate="<b>%{y}</b><br>%{customdata}<br>Null or empty: %{x:.2f} %<extra></extra>"))
    fig.add_vline(x=FIELD_UNUSABLE_PCT, line_dash="dash", line_color=TEXT_MUTED,
                  annotation_text="50 %: too empty to chart", annotation_position="top right",
                  annotation_font_color=TEXT_MUTED)
    fig.update_xaxes(title_text="Null or empty values (% of records)", range=[0, 112], ticksuffix=" %")
    fig.update_yaxes(title_text=None)
    height = max(320, 120 + 24 * len(rows))
    subtitle = "Colour = source topic. Fields at 0 % are hidden unless selected." if topic == "all" else None
    return finish(fig, "Null rate per field, most severe first", source(file_name, "section null_rate"),
                  height=height, subtitle=subtitle)


def outlier_figure(gold, file_name):
    rows = prep.outlier_rates(gold)
    if rows.empty:
        return empty_figure("No outlier checks in this Gold file.")
    rows["check"] = rows["flag"] + " · " + rows["topic"].map(TOPIC_TEXT)
    order = rows[rows["station"] == "ALL"].sort_values("flagged_pct")["check"].tolist()
    fig = go.Figure()
    stations = ["ALL"] + sorted(s for s in rows["station"].unique() if s != "ALL")
    for station in stations:
        part = rows[rows["station"] == station].set_index("check").reindex(order).dropna(subset=["flagged_pct"])
        fig.add_trace(go.Bar(
            x=part["flagged_pct"], y=part.index, orientation="h", name="All stations" if station == "ALL"
            else station, marker=dict(color=STATION_COLORS.get(station, NEUTRAL), cornerradius=4),
            hovertemplate=f"<b>%{{y}}</b><br>{station}: %{{x:.3f}} % of records flagged<extra></extra>"))
    limit = prep.load_thresholds()["outlier_flagged_pct"]["limit"]
    fig.add_vline(x=limit, line_dash="dash", line_color=STATUS["warning"][0],
                  annotation_text=f"{limit:g} % threshold", annotation_position="top left",
                  annotation_font_color=TEXT_MUTED)
    fig.update_layout(barmode="group", bargroupgap=0.08)
    top = max(limit, rows["flagged_pct"].max()) * 1.15
    fig.update_xaxes(title_text="Records flagged by the check (%)", range=[0, top], ticksuffix=" %")
    fig.update_yaxes(title_text=None, categoryorder="array", categoryarray=order)
    return finish(fig, "Outlier rate per numeric check and station",
                  source(file_name, "section outlier_rate (Silver IQR, Z-score, range and NCEI QC flags)"),
                  height=max(360, 140 + 34 * len(order)),
                  subtitle="Extreme values are kept and flagged, never removed: here they are the signal.")


def volume_figure(gold, file_name, scale="log"):
    rows = prep.volume_daily(gold)
    if rows.empty:
        return empty_figure("No volume rows in this Gold file.")
    fig = go.Figure()
    for topic in [t for t in TOPIC_ORDER if t in set(rows["topic"])]:
        part = rows[rows["topic"] == topic]
        fig.add_trace(go.Bar(x=part["day"], y=part["records"], name=TOPIC_TEXT[topic],
                             marker=dict(color=TOPIC_COLORS[topic], cornerradius=4),
                             hovertemplate="%{x|%Y-%m-%d}<br>" + TOPIC_TEXT[topic]
                             + ": %{y:,.0f} records<extra></extra>"))
    fig.update_layout(barmode="group")
    fig.update_xaxes(title_text="UTC day of ingestion into Silver", tickformat="%b %d", dtick=86400000)
    fig.update_yaxes(title_text="Records written (log scale)" if scale == "log" else "Records written",
                     type=scale)
    return finish(fig, "Ingested record volume per day and source", source(file_name, "section volume_daily"),
                  subtitle="Peaks are the historical loads; hourly runs add a few hundred records a day.")


def ingestion_figure(gold, file_name):
    rows = prep.ingestion(gold)
    if rows.empty:
        return empty_figure("No hourly ingestion rows in this Gold file.")
    fig = go.Figure()
    for topic in [t for t in TOPIC_ORDER if t in set(rows["topic"])]:
        part = rows[rows["topic"] == topic]
        fig.add_trace(go.Scatter(
            x=part["day"], y=part["compliance_pct"], name=TOPIC_TEXT[topic], mode="lines+markers",
            line=dict(color=TOPIC_COLORS[topic], width=2), marker=dict(size=8),
            customdata=part[["runs_actual", "runs_expected"]],
            hovertemplate="%{x|%Y-%m-%d}<br>" + TOPIC_TEXT[topic]
            + ": %{customdata[0]:.0f} of %{customdata[1]:.0f} runs (%{y:.0f} %)<extra></extra>"))
    limit = prep.load_thresholds()["ingestion_compliance_pct"]["limit"]
    fig.add_hline(y=limit, line_dash="dash", line_color=STATUS["critical"][0],
                  annotation_text=f"target {limit:g} %", annotation_position="bottom right",
                  annotation_font_color=TEXT_MUTED)
    fig.update_xaxes(title_text="UTC day", tickformat="%b %d", dtick=86400000)
    fig.update_yaxes(title_text="Hourly runs that reached Silver (%)", range=[-3, 105], ticksuffix=" %")
    return finish(fig, "Ingestion compliance of the hourly sources", source(file_name, "section ingestion_compliance"),
                  subtitle="24 Bronze runs expected per day; the stack only runs while the laptop is on.")


COMPLETENESS_METRICS = {"days_present_pct": "Days present (% of the year)", "tmax_null_pct": "TMAX null (%)",
                        "tmin_null_pct": "TMIN null (%)", "prcp_null_pct": "PRCP null (%)"}


def completeness_figure(gold, file_name, metric="days_present_pct"):
    matrix = prep.completeness(gold, metric)
    if matrix.empty:
        return empty_figure("No station completeness rows in this Gold file.")
    # Days present: dark = complete. Null shares: dark = many nulls. One blue hue, light to dark.
    blues = [[0, "#eef5fd"], [0.5, "#6aabf1"], [1, "#0e3d7e"]]
    fig = go.Figure(go.Heatmap(
        z=matrix.values, x=matrix.columns.astype(str), y=matrix.index, colorscale=blues, zmin=0, zmax=100,
        xgap=2, ygap=2, colorbar=dict(title=dict(text="%"), thickness=12),
        hovertemplate="%{y} · %{x}<br>" + COMPLETENESS_METRICS[metric] + ": %{z:.1f} %<extra></extra>"))
    fig.update_xaxes(title_text="Year", type="category", tickangle=-45, showgrid=False)
    fig.update_yaxes(title_text="Station", showgrid=False)
    return finish(fig, f"Station completeness per year · {COMPLETENESS_METRICS[metric]}",
                  source(file_name, "section station_completeness (F2 GHCN-Daily)"), height=300,
                  subtitle="The indicator needs the 1991-2020 baseline: gaps there weaken a station's percentiles.")


def kpi_cards(gold, thresholds, now=None):
    h = prep.headline(gold, thresholds, now)
    stale = h["stale_topics"]
    cards = [
        ui.kpi_card("Total records in Gold", f"{h['records']:,.0f}",
                    f"{h['topics']} topics · {h['records_raw']:,.0f} rows stored in Silver"),
        ui.kpi_card("Duplicate rate", f"{h['duplicate_after_pct']:.1f} %",
                    f"after de-duplication · {h['duplicate_before_pct']:.1f} % of Silver rows were copies",
                    prep.status_of(h["duplicate_after_pct"], thresholds["duplicate_rate_after_pct"])),
        ui.kpi_card("Null cells", f"{h['null_cell_pct']:.1f} %", "all fields of all topics",
                    prep.status_of(h["null_cell_pct"], thresholds["null_cell_pct"])),
        ui.kpi_card("Schema compliance", f"{h['files_compliant_pct']:.0f} %",
                    f"{h['files_checked']:.0f} Silver files with the target schema",
                    prep.status_of(h["files_compliant_pct"], thresholds["files_compliant_pct"])),
        ui.kpi_card("Highest outlier rate", f"{h['worst_outlier_pct']:.2f} %", h["worst_outlier_flag"],
                    prep.status_of(h["worst_outlier_pct"], thresholds["outlier_flagged_pct"])),
        ui.kpi_card("Freshness", f"{h['freshest_hours']:.1f} h",
                    "since the newest ingestion" + (f" · stale: {', '.join(TOPIC_TEXT[t] for t in stale)}"
                                                    if stale else " · every source on time"),
                    "critical" if stale else "good"),
    ]
    return [dbc.Col(card, xs=12, sm=6, lg=4, xl=2, className="mb-3") for card in cards]


def kpi_table(gold, thresholds, now=None):
    table = prep.kpi_table(gold, thresholds, now)
    head = html.Thead(html.Tr([html.Th(c) for c in ["Status", "Topic", "KPI", "Value", "Threshold", "Detail",
                                                     "Why this threshold"]]))
    body = []
    for row in table.itertuples():
        color, icon, word = STATUS[row.status]
        topic = row.topic
        for key, text in TOPIC_TEXT.items():
            topic = topic.replace(key, text)
        value = "n/a" if pd.isna(row.value) else f"{row.value:,.2f}"
        body.append(html.Tr([
            html.Td(f"{icon} {word}", style={"color": color, "fontWeight": 600, "whiteSpace": "nowrap"}),
            html.Td(topic), html.Td(row.kpi), html.Td(value, className="text-end"),
            html.Td(row.threshold, style={"whiteSpace": "nowrap"}), html.Td(row.detail), html.Td(row.why),
        ]))
    return dbc.Table([head, html.Tbody(body)], bordered=False, hover=True, responsive=True, size="sm",
                     className="kpi-table mb-0")


# ---------------------------------------------------------------- layout

def layout():
    topic_options = [{"label": "All topics", "value": "all"}] + \
        [{"label": TOPIC_TEXT[t], "value": t} for t in TOPIC_ORDER]
    return dbc.Container([
        dcc.Store(id="gold-file"),
        ui.header("Data governance dashboard: quality, volume and freshness of the pipeline",
                  "data engineers of the team and the course reviewers", "updated"),
        ui.refresh_controls("refresh", "interval"),
        html.Div(id="load-error"),
        dbc.Row(id="kpi-cards"),
        ui.section_title("Completeness and validity", "Which fields and checks need attention?"),
        dbc.Row([
            dbc.Col(ui.chart_card("null-chart", controls=dbc.Row([
                dbc.Col(dcc.Dropdown(id="null-topic", options=topic_options, value="all", clearable=False),
                        md=7),
                dbc.Col(dbc.Checklist(id="null-zero", options=[{"label": "Show fields at 0 %", "value": "zero"}],
                                      value=[], switch=True), md=5, className="pt-1"),
            ]), note="Wind gust and short-term precipitation are never reported by the NWS at our stations, "
                     "so precipitation comes from GHCN-Daily."), lg=7, className="mb-3"),
            dbc.Col(ui.chart_card("outlier-chart"), lg=5, className="mb-3"),
        ]),
        ui.section_title("Volume and timeliness", "Is data arriving as the schedule expects?"),
        dbc.Row([
            dbc.Col(ui.chart_card("volume-chart", controls=dbc.RadioItems(
                id="volume-scale", options=[{"label": "Log scale", "value": "log"},
                                            {"label": "Linear scale", "value": "linear"}],
                value="log", inline=True)), lg=6, className="mb-3"),
            dbc.Col(ui.chart_card("ingestion-chart"), lg=6, className="mb-3"),
        ]),
        dbc.Row(dbc.Col(ui.chart_card("completeness-chart", controls=dcc.Dropdown(
            id="completeness-metric", options=[{"label": v, "value": k} for k, v in COMPLETENESS_METRICS.items()],
            value="days_present_pct", clearable=False, style={"maxWidth": "360px"})), className="mb-3")),
        ui.section_title("Data quality summary", "Every KPI with its current value and threshold (worst first)"),
        dbc.Card(dbc.CardBody([html.Div(id="kpi-table"),
                               html.Small(id="table-source", className="d-block mt-2",
                                          style={"color": TEXT_MUTED})]), className="shadow-sm mb-4"),
    ], fluid=True, className="px-4 py-3", style={"maxWidth": "1800px"})


app = Dash(__name__, title="Governance · Extreme Weather TX", external_stylesheets=[dbc.themes.BOOTSTRAP],
           suppress_callback_exceptions=False)
app.layout = layout


# ---------------------------------------------------------------- callbacks

@app.callback(Output("gold-file", "data"), Output("updated", "children"), Output("load-error", "children"),
              Input("refresh", "n_clicks"), Input("interval", "n_intervals"), State("gold-file", "data"))
def refresh(_clicks, _ticks, current):
    """Find the newest Gold file. The automatic refresh only redraws the charts when the file changed."""
    try:
        gold, name = load(KIND)
    except GoldFileMissing as error:
        return None, "No Gold data yet", ui.error_panel(str(error))
    run = computed_at(gold)
    stamp = [html.Div(f"Data last updated (Gold run): {run:%Y-%m-%d %H:%M} UTC", className="fw-semibold"),
             html.Div(f"File {name}", className="small")]
    data = {"file": name, "run": run.isoformat(), "loaded": pd.Timestamp.now(tz="UTC").isoformat()}
    if ctx.triggered_id == "interval" and current and current.get("file") == name:
        return no_update, stamp, None
    return data, stamp, None


def _gold(data):
    if not data:
        return None, None
    gold, name = load(KIND)
    return gold, name


@app.callback(Output("kpi-cards", "children"), Output("kpi-table", "children"), Output("table-source", "children"),
              Input("gold-file", "data"))
def update_kpis(data):
    gold, name = _gold(data)
    if gold is None:
        return [], None, None
    thresholds = prep.load_thresholds()
    note = (f"Source: Gold {name}. Thresholds: config/governance_thresholds.yaml. Freshness is measured from "
            f"the last ingestion to the time of this refresh.")
    return kpi_cards(gold, thresholds), kpi_table(gold, thresholds), note


@app.callback(Output("null-chart", "figure"), Input("gold-file", "data"), Input("null-topic", "value"),
              Input("null-zero", "value"))
def update_null(data, topic, zero):
    gold, name = _gold(data)
    return empty_figure("No Gold data") if gold is None else null_rate_figure(gold, name, topic, "zero" in zero)


@app.callback(Output("outlier-chart", "figure"), Input("gold-file", "data"))
def update_outliers(data):
    gold, name = _gold(data)
    return empty_figure("No Gold data") if gold is None else outlier_figure(gold, name)


@app.callback(Output("volume-chart", "figure"), Input("gold-file", "data"), Input("volume-scale", "value"))
def update_volume(data, scale):
    gold, name = _gold(data)
    return empty_figure("No Gold data") if gold is None else volume_figure(gold, name, scale)


@app.callback(Output("ingestion-chart", "figure"), Input("gold-file", "data"))
def update_ingestion(data):
    gold, name = _gold(data)
    return empty_figure("No Gold data") if gold is None else ingestion_figure(gold, name)


@app.callback(Output("completeness-chart", "figure"), Input("gold-file", "data"), Input("completeness-metric", "value"))
def update_completeness(data, metric):
    gold, name = _gold(data)
    return empty_figure("No Gold data") if gold is None else completeness_figure(gold, name, metric)


if __name__ == "__main__":
    app.run(host=os.environ.get("DASH_HOST", "127.0.0.1"), port=int(os.environ.get("GOVERNANCE_PORT", 8050)),
            debug=os.environ.get("DASH_DEBUG", "0") == "1")
