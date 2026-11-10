# Source Status Log

Short log of availability and changes for each source (required by the Team Project Assignment).
Report any breaking change to the professor as soon as it is detected.

| Date (UTC) | Source | Observation | Action |
|---|---|---|---|
| 2026-11-09 | NOAA Storm Events | Directory listing OK. Latest compilations: 2025 → `c20260819`, 2026 → `c20261026`, 2012 recompiled on `c20261026`. | Confirms that old years are revised; compilation suffix used as watermark. |
| 2026-11-09 | GHCN-Daily (ADS) | HTTP 200, JSON. Last available date 2026-11-06 for the 10 stations (3-day latency). | 45-day overlap window. |
| 2026-11-09 | NWS alerts | HTTP 200, GeoJSON, cursor pagination. 10 active alerts for TX (flood warnings in progress). | — |
| 2026-11-09 | NWS observations | HTTP 200 for the 10 stations; response capped at 500 features (≈ 4 days), not the 7 days requested. | Poll every 30 min; split window if 500 rows returned. |
