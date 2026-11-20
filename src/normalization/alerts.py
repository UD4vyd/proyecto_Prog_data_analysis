"""Silver normalizer for F3, NWS alerts (topic weather_alerts)."""
import pandas as pd

from src.normalization import common, text

TOPIC = "weather_alerts"
KEY = ["alert_id"]
REQUIRED = ["alert_id", "event", "message_type", "sent_utc"]
# Derived columns left out of the record hash (see common.add_record_hash). The raw description stays in it.
# The centroid is derived too: a float sum depends on the order of the vertices, and its last digit changed
# between two answers for the same alert.
HASH_EXCLUDE = ["description_clean", "description_tokens", "token_count", "centroid_lat", "centroid_lon"]
SCHEMA = {
    "alert_id": "string",
    "event": "category",
    "message_type": "category",        # Alert, Update, Cancel
    "status": "category",
    "severity": "category",            # CAP: Minor, Moderate, Severe, Extreme, Unknown
    "certainty": "category",
    "urgency": "category",
    "sent_utc": "datetime_utc",
    "effective_utc": "datetime_utc",
    "onset_utc": "datetime_utc",
    "expires_utc": "datetime_utc",
    "ends_utc": "datetime_utc",
    "sender_name": "string",
    "area_desc": "string",
    "ugc_codes": "list",               # zones or counties of the alert
    "referenced_ids": "list",          # earlier messages of the same warning (Update, Cancel)
    "has_geometry": "bool",
    "centroid_lat": "float64",
    "centroid_lon": "float64",
    "headline": "string",
    "description": "string",
    "description_clean": "string",
    "description_tokens": "list",
    "token_count": "int64",
    "record_hash": "string",
    "bronze_file": "string",
    "ingested_at": "datetime_utc",
    "processed_at": "datetime_utc",
}
CAP_SEVERITY = {"Minor", "Moderate", "Severe", "Extreme", "Unknown"}


def _centroid(geometry):
    """Mean of the polygon vertices as (lat, lon). GeoJSON stores [lon, lat], so the order is swapped."""
    if not geometry:
        return None, None
    rings = geometry["coordinates"] if geometry["type"] == "Polygon" else [r for p in geometry["coordinates"] for r in p]
    points = [point for ring in rings for point in ring]
    if not points:
        return None, None
    # Rounded to 5 decimals (about 1 m): more digits only carry floating point noise.
    return round(sum(p[1] for p in points) / len(points), 5), round(sum(p[0] for p in points) / len(points), 5)


def normalize(payload):
    rows = []
    for feature in payload["records"]:
        p = feature["properties"]
        lat, lon = _centroid(feature.get("geometry"))
        clean = text.clean_text(p.get("description"))
        tokens = text.tokenize(clean)
        severity = p.get("severity")
        rows.append({
            "alert_id": p.get("id"),
            "event": p.get("event"),
            "message_type": p.get("messageType"),
            "status": p.get("status"),
            # A missing or unexpected severity is stored as the CAP value "Unknown", never guessed.
            "severity": severity if severity in CAP_SEVERITY else "Unknown",
            "certainty": p.get("certainty") or "Unknown",
            "urgency": p.get("urgency") or "Unknown",
            "sent_utc": p.get("sent"),
            "effective_utc": p.get("effective"),
            "onset_utc": p.get("onset"),
            "expires_utc": p.get("expires"),
            "ends_utc": p.get("ends"),
            "sender_name": p.get("senderName"),
            "area_desc": p.get("areaDesc"),
            "ugc_codes": (p.get("geocode") or {}).get("UGC") or [],
            "referenced_ids": [r.get("identifier") for r in p.get("references") or [] if r.get("identifier")],
            "has_geometry": feature.get("geometry") is not None,
            "centroid_lat": lat,
            "centroid_lon": lon,
            "headline": p.get("headline") or "",
            "description": p.get("description") or "",
            "description_clean": clean,
            "description_tokens": tokens,
            "token_count": len(tokens),
        })
    df = pd.DataFrame(rows, columns=[c for c in SCHEMA if c not in common.METADATA_COLUMNS])
    for column in ["sent_utc", "effective_utc", "onset_utc", "expires_utc", "ends_utc"]:
        df[column] = common.to_utc(df[column])
    return df
