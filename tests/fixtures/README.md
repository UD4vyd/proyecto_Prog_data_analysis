- `nws_alerts_active_20261116_231234.geojson`: F3 response of `https://api.weather.gov/alerts/active?area=TX` (6 alerts), copied byte for byte from `workshop_1/samples/poll1/` (poll 1, 2026-11-16 23:12:34 UTC).
- `nws_alerts_active_20261116_231234.geojson.headers.json`: URL, status, UTC time and HTTP headers of that same request, copied byte for byte.
- `storm_events_tx_excerpt.csv`: header plus the first 20 data rows of `workshop_1/samples/storm_events_tx_d2025_c20260819.csv` (F1 Texas rows of `StormEvents_details-ftp_v1.0_d2025_c20260819.csv.gz`), cut at a record boundary without changing any byte.

Added in Workshop 2 for `tests/test_silver.py`:

- `nws_observations_KDFW_excerpt.geojson`: top-level `type` plus the first 3 features of `workshop_1/samples/poll2/nws_observations_KDFW_20261117_001350.geojson`, re-serialized with `json.dumps(indent=1)`. Values are unchanged.
- `ghcn_daily_excerpt.json`: the first 10 records (USW00003927, 2025-01-01 to 2025-01-10) of `data/raw/ghcn_daily_3st_2025_20261116_235238.json`, cut at a record boundary and closed with `]`. It contains the trace day 2025-01-05 (`PRCP_ATTRIBUTES="T,,W"`). `ghcn_daily_excerpt.json.headers.json` is the headers file of that request, re-serialized.
- `storm_events_tx_mixed_excerpt.csv`: header plus 6 real rows of `workshop_1/samples/storm_events_tx_d2025_c20260819.csv` (EVENT_ID 1305042 and 1305041 with `MST-7`, 1230425 and 1243404 Hail, 1244626 and 1247378 Thunderstorm Wind, one blank and one `6.00M` damage), written again with Python `csv.writer`.
- `stations_excerpt.csv`: header plus the KDFW (America/Chicago) and KELP (America/Denver) rows of `config/stations.csv`.
- `silver_edge_cases.json`: hand-made test inputs. Each one is a copy of a real record above with one field changed to trigger one rejection reason: F4 temperature 70 degC, unit `wmoUnit:degF`, QC `X`, coordinates moved to Miami, `stationId` removed; F2 qflag `I`, TMIN `abc`, PRCP `800.0`, unknown station `USW00099999`; F3 Flood Warning polygon shifted 8 degrees west and 10 degrees north. They are test inputs, not data.
