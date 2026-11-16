# Source status log

| Source | Endpoint checked | Validated (UTC) | Result |
|---|---|---|---|
| F1 Storm Events | bulk listing and `StormEvents_details-ftp_v1.0_d2025_c20260819.csv.gz` | 2026-11-16 23:52 | 200, 72,360 US rows, 5,641 Texas rows |
| F2 GHCN-Daily | Access Data Service, 3 stations, 2025 | 2026-11-16 23:52 | 200, 1,095 station-days |
| F3 NWS alerts | `/alerts?area=TX&limit=500`, `/alerts/active?area=TX` | 2026-11-16 23:12 (poll 1) | 200, 172 recent and 6 active alerts |
| F4 NWS observations | `/stations/{KDFW,KIAH,KAUS}/observations` | 2026-11-16 23:12 (poll 1) | 200, 500 records per station |
