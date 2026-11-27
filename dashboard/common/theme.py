"""Plotly figure style shared by both dashboards: template, titles, source notes."""
import plotly.graph_objects as go
import plotly.io as pio

from dashboard.common.palette import FONT, GRID, SURFACE, TEXT, TEXT_MUTED, TOPIC_COLORS, TOPIC_ORDER

pio.templates["team5"] = go.layout.Template(layout=dict(
    font=dict(family=FONT, size=13, color=TEXT),
    paper_bgcolor=SURFACE,
    plot_bgcolor=SURFACE,
    colorway=[TOPIC_COLORS[t] for t in TOPIC_ORDER],
    title=dict(font=dict(size=15, color=TEXT), x=0, xanchor="left", y=0.97, yanchor="top"),
    xaxis=dict(gridcolor=GRID, linecolor="#bdbdbd", zeroline=False, ticks="outside", tickcolor="#bdbdbd",
               automargin=True),
    yaxis=dict(gridcolor=GRID, linecolor="#bdbdbd", zeroline=False, automargin=True),
    legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0, title=dict(text="")),
    hoverlabel=dict(bgcolor=SURFACE, font=dict(family=FONT, color=TEXT), bordercolor="#bdbdbd"),
    margin=dict(l=10, r=10, t=70, b=90),
    bargap=0.25,
))
pio.templates.default = "plotly_white+team5"


def finish(fig, title, source, height=360, subtitle=None):
    """Title, optional subtitle and a source annotation at the bottom: required on every chart."""
    text = f"<b>{title}</b>"
    if subtitle:
        text += f"<br><span style='font-size:12px;color:{TEXT_MUTED}'>{subtitle}</span>"
    # The legend sits between the title and the plot, so a two-line title needs a taller top margin.
    fig.update_layout(title_text=text, height=height, margin_t=100 if subtitle else 70)
    fig.add_annotation(text=f"Source: {source}", xref="paper", yref="paper", x=0, y=0, yshift=-72,
                       xanchor="left", yanchor="top", showarrow=False,
                       font=dict(size=11, color=TEXT_MUTED))
    return fig


def empty_figure(message, height=360):
    """Placeholder when a section has no rows, so the layout keeps its shape and says why."""
    fig = go.Figure()
    fig.add_annotation(text=message, x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False,
                       font=dict(size=14, color=TEXT_MUTED))
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(height=height)
    return fig


def day_axis(fig, days, title):
    """Date axis with one label per day, or every few days when there are many: labels never need rotating."""
    span = (max(days) - min(days)).days + 1 if len(days) else 1
    step = max(1, -(-span // 10))  # at most about 10 labels
    fig.update_xaxes(title_text=title, tickformat="%b %d", dtick=step * 86400000, tickangle=0)
    return fig

