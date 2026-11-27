"""Layout pieces shared by both dashboards (Dash Bootstrap Components)."""
import dash_bootstrap_components as dbc
from dash import dcc, html

from dashboard.common.palette import STATUS, TEXT_MUTED

PROJECT_TITLE = "Extreme Weather Monitoring (Texas)"
TOPIC_ANGLE = ("Daily heat, cold and heavy rain at Texas first-order stations compared with their own "
               "1991-2020 history, next to official NWS alerts and NOAA storm reports")
TEAM = "Team 5: Andrés Mendoza, David Martínez, Juan Escallón"
GRAPH_CONFIG = {"displaylogo": False, "responsive": True,
                "modeBarButtonsToRemove": ["lasso2d", "select2d", "autoScale2d"]}


def header(dashboard_name, audience, updated_id):
    """Header with the project title, topic angle, team and the last-updated time (filled by a callback)."""
    return dbc.Row(dbc.Col(html.Div([
        html.Div([
            html.Div([
                html.H1(PROJECT_TITLE, className="h3 mb-1"),
                html.Div(dashboard_name, className="dash-subtitle"),
            ]),
            html.Div(id=updated_id, className="updated text-end"),
        ], className="d-flex justify-content-between align-items-start flex-wrap gap-2"),
        html.P(TOPIC_ANGLE, className="mb-1 mt-2"),
        html.Small(f"{TEAM} · Data Analysis Programming 2026-I, Universidad Distrital · Audience: {audience}",
                   style={"color": TEXT_MUTED}),
    ], className="app-header")), className="mb-3")


def refresh_controls(button_id, interval_id, minutes=5):
    """Manual refresh button plus an automatic refresh every few minutes."""
    return html.Div([
        dbc.Button("Refresh data", id=button_id, color="secondary", outline=True, size="sm"),
        html.Small(f" Reloads the newest Gold file. Automatic refresh every {minutes} min.",
                   className="ms-2", style={"color": TEXT_MUTED}),
        dcc.Interval(id=interval_id, interval=minutes * 60 * 1000, n_intervals=0),
    ], className="mb-3")


def section_title(title, question=None):
    children = [html.H2(title, className="h5 mb-0")]
    if question:
        children.append(html.P(question, className="section-question mb-0"))
    return html.Div(children, className="section-title mt-4 mb-2")


def chart_card(graph_id, controls=None, note=None):
    """A chart in a card, with optional filter controls above it and a plain-language note below it."""
    body = []
    if controls is not None:
        body.append(html.Div(controls, className="chart-controls mb-2"))
    body.append(dcc.Loading(dcc.Graph(id=graph_id, config=GRAPH_CONFIG), type="dot"))
    if note:
        body.append(html.P(note, className="chart-note mb-0"))
    return dbc.Card(dbc.CardBody(body), className="h-100 shadow-sm")


def kpi_card(title, value, detail=None, status=None):
    """Metric card. The status colour always comes with an icon and a word, never colour alone."""
    children = [html.Div(title, className="kpi-title"), html.Div(value, className="kpi-value")]
    if detail:
        children.append(html.Div(detail, className="kpi-detail"))
    style = {}
    if status:
        color, icon, word = STATUS[status]
        children.append(html.Div(f"{icon} {word}", className="kpi-status", style={"color": color}))
        style = {"borderTop": f"4px solid {color}"}
    return dbc.Card(dbc.CardBody(children), className="h-100 shadow-sm kpi-card", style=style)


def error_panel(message):
    return dbc.Alert(message, color="warning", className="mb-3")
