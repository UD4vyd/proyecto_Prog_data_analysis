"""Draw the architecture and ER figures of the Workshop 1 report."""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

OUT = Path(__file__).parent / "figures"
OUT.mkdir(exist_ok=True)


def box(ax, x, y, w, h, text, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02", fc=color, ec="#333333", lw=1))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=8.5, wrap=True)


def arrow(ax, x1, y1, x2, y2, label=""):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle="->", color="#333333"))
    if label:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.03, label, ha="center", fontsize=7)


def architecture():
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 4)
    ax.axis("off")
    sources = ["F1 Storm Events\n(yearly CSV.gz)", "F2 GHCN-Daily\n(NCEI API, JSON)",
               "F3 NWS alerts\n(GeoJSON, 15 min)", "F4 NWS observations\n(GeoJSON, hourly)"]
    for i, s in enumerate(sources):
        box(ax, 0.1, 3.1 - i * 0.95, 1.8, 0.7, s, "#dbe8f6")
        arrow(ax, 1.95, 3.45 - i * 0.95, 2.45, 1.95)
    box(ax, 2.5, 1.55, 1.5, 0.8, "Raw landing zone\ndata/raw/\n(bytes + headers)", "#f3e6c8")
    arrow(ax, 4.05, 1.95, 4.45, 1.95)
    box(ax, 4.5, 1.45, 1.6, 1.0, "Normalize\nvalidate\ndeduplicate", "#e2f0d9")
    box(ax, 4.6, 0.2, 1.4, 0.6, "Quarantine", "#f8d7d7")
    arrow(ax, 5.3, 1.4, 5.3, 0.85)
    arrow(ax, 6.15, 1.95, 6.55, 1.95)
    box(ax, 6.6, 1.55, 1.4, 0.8, "PostgreSQL\ncanonical model", "#e4dcf1")
    arrow(ax, 8.05, 1.95, 8.35, 1.95)
    box(ax, 8.4, 2.3, 1.5, 0.7, "Analytics\n(percentile indicator)", "#e2f0d9")
    box(ax, 8.4, 0.9, 1.5, 0.7, "Dash dashboard", "#dbe8f6")
    arrow(ax, 9.15, 2.25, 9.15, 1.65)
    plt.tight_layout()
    fig.savefig(OUT / "architecture.png", dpi=170)


def er():
    fig, ax = plt.subplots(figsize=(10, 4.6))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 5)
    ax.axis("off")
    tables = {
        "station": (3.9, 2.2, "station\nPK ghcn_id\nwban, icao (UK), name\nlat, lon\nforecast_zone, county_zone\ntimezone"),
        "daily": (0.1, 3.3, "daily_observation (F2)\nPK ghcn_id, date, element\nvalue, unit\nm/q/s flags, is_trace"),
        "obs": (0.1, 0.3, "observation (F4)\nPK icao, timestamp_utc\ntemperature_c, dewpoint_c\nwind_speed_ms, pressure_hpa"),
        "storm": (7.5, 3.3, "storm_event (F1)\nPK event_id\nevent_type, cz_type, cz_fips\nbegin_utc, lat, lon\nmagnitude, damage_usd"),
        "zone": (7.5, 1.7, "alert_zone (F3)\nPK alert_id, ugc"),
        "alert": (7.5, 0.2, "alert (F3)\nPK alert_id\nmessage_type, event, severity\nsent_utc, expires_utc"),
        "ref": (4.1, 0.1, "alert_reference (F3)\nPK alert_id, referenced_id"),
    }
    for x, y, text in tables.values():
        box(ax, x, y, 2.3 if x != 3.9 else 2.2, 1.25 if "\n" in text[20:] else 0.6, text, "#f2f2f2")
    arrow(ax, 3.85, 3.2, 2.45, 3.7, "1 : N  ghcn_id")
    arrow(ax, 3.85, 2.5, 2.45, 1.1, "1 : N  icao")
    arrow(ax, 6.15, 3.2, 7.45, 3.8, "county or zone")
    arrow(ax, 6.15, 2.6, 7.45, 2.0, "forecast or county zone = ugc")
    arrow(ax, 8.65, 1.4, 8.65, 1.65, "")
    arrow(ax, 7.45, 0.6, 6.45, 0.45, "Update / Cancel")
    plt.tight_layout()
    fig.savefig(OUT / "er_draft.png", dpi=170)


if __name__ == "__main__":
    architecture()
    er()
    print("figures written to", OUT)
