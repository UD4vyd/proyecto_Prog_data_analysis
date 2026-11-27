"""Colour palettes shared by both dashboards (no Plotly import, so data preparation can use it).

Every palette here was checked for colour-vision deficiency (CVD) separation and contrast with a palette
validator (OKLab Delta E, simulated protan, deutan and tritan vision):

    CLASS_COLORS     NORMAL / UNUSUAL / EXTREME. The course asks for green, yellow or grey, and red in
                     sentiment charts; a plain green and red pair fails for deuteranopia (Delta E 4), so the
                     green is bluish and the red is darker. All three pairs pass (worst Delta E 13.8).
                     High and low days also differ by marker shape (triangle up / down), never by colour alone.
    SEVERITY_COLORS  CAP severity Minor < Moderate < Severe < Extreme: one warm hue from light to dark
                     (ordinal ramp, monotone lightness, light end above 2:1 contrast on white).
    TOPIC_COLORS     the four Silver topics, a fixed colour per topic in both dashboards (categorical
                     order that passes the adjacent-pair CVD check).
    STATION_COLORS   one colour per station (adjacent pairs pass the CVD check); ALL is grey on purpose.
    STATUS           good / warning / critical of the governance thresholds, always shown with an icon and
                     a word as well.
"""

FONT = "Inter, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"
TEXT = "#191919"
TEXT_MUTED = "#666666"
GRID = "#e6e6e6"
SURFACE = "#ffffff"
NEUTRAL = "#9c9c9c"

CLASS_COLORS = {"NORMAL": "#1a9e77", "UNUSUAL": "#e6ab02", "EXTREME": "#a50f15", "NO_BASELINE": NEUTRAL}
CLASS_ORDER = ["NORMAL", "UNUSUAL", "EXTREME"]
CLASS_TEXT = {"NORMAL": "Normal", "UNUSUAL": "Unusual", "EXTREME": "Extreme"}

SEVERITY_ORDER = ["Minor", "Moderate", "Severe", "Extreme"]
SEVERITY_COLORS = {"Minor": "#f4a259", "Moderate": "#e4572e", "Severe": "#b3261e", "Extreme": "#6b0f0f"}
# Continuous version of the same ramp for mean severity scores (1 = Minor ... 4 = Extreme).
SEVERITY_SCALE = [[0.0, "#f4a259"], [1 / 3, "#e4572e"], [2 / 3, "#b3261e"], [1.0, "#6b0f0f"]]

TOPIC_ORDER = ["weather_alerts", "station_observations", "daily_summaries", "storm_events"]
TOPIC_COLORS = {"weather_alerts": "#257de0", "station_observations": "#f47230",
                "daily_summaries": "#00b894", "storm_events": "#f2a600"}
TOPIC_TEXT = {"weather_alerts": "F3 NWS alerts", "station_observations": "F4 NWS observations",
              "daily_summaries": "F2 GHCN-Daily", "storm_events": "F1 Storm Events"}

# Stations: one fixed colour each; ALL (the whole topic) is grey because it is an aggregate. Stations added
# later (config/pipeline.yaml accepts "all") get NEUTRAL until they are given a colour here.
STATION_COLORS = {"ALL": "#5f5f5f", "KAUS": "#ee7fac", "KDFW": "#6a4ce2", "KIAH": "#008a17"}

METHOD_COLORS = {"API": "#257de0", "web scraping": "#f47230"}
STATUS = {"good": ("#0aac23", "✔", "OK"), "warning": ("#d28e05", "▲", "Watch"),
          "critical": ("#dc3c3d", "✖", "Breach"), "info": (NEUTRAL, "●", "Info")}
