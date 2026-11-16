# Ingestion contract (Workshop 1)

This contract says how each source will be loaded in Workshop 2. Workshop 1 only defines it; the
evidence behind each rule is in `notebooks/01_source_profiling.ipynb`.

## Summary

| | F1 Storm Events | F2 GHCN-Daily | F3 NWS alerts | F4 NWS observations |
|---|---|---|---|---|
| Historical strategy | one details file per year, 1996 to present | one request per station and year, 1991 to present | not available (the API keeps recent alerts only) | not available (NWS keeps about 7 days) |
| Incremental strategy | check the bulk listing; re-load a year when its `_cYYYYMMDD` suffix changes | per station, request from max DATE minus 30 days to today | every 15 minutes, `/alerts?area=TX` plus `/alerts/active?area=TX` | every hour per station, `/stations/{icao}/observations` with a start time |
| Watermark | compilation date `_cYYYYMMDD` per year file | max DATE per station | max `sent` | max `timestamp` per station |
| Overlap window | whole year file | 30 days | 2 hours | 3 hours |
| Duplicate key | EVENT_ID | (station, date, element) | alert `id` | (station, timestamp) |
| Revisions | a new compilation replaces the year | re-published values overwrite the old value (upsert) | Update and Cancel are new ids linked by `references` | none observed; upsert on the key |

## F1 NOAA Storm Events

- Historical: download `StormEvents_details-ftp_v1.0_dYYYY_cYYYYMMDD.csv.gz` for 1996 to the current
  year. One file per year is also the partition. Before 1996 only a few event types were recorded,
  so earlier frequencies are not comparable.
- Incremental: read the bulk listing. If the compilation suffix of a year changed (for example 2025
  is `c20260819`, 2012 was recompiled as `c20261026`), download the new file and replace that year.
- Keep Texas rows with `STATE_FIPS == "48"`.
- Key: EVENT_ID. A reload of a year replaces all rows of that year.
- Delay: F1 is published months after the events, so it is used only for history and validation.

## F2 GHCN-Daily

- Historical: one request per station and year to the NCEI Access Data Service
  (`dataset=daily-summaries`, `dataTypes=TMAX,TMIN,PRCP`, `includeAttributes=true`, `units=metric`).
  46 stations x 36 years (1991-2026) = 1,656 requests. Partition by station and year.
- Incremental: per station, from max DATE minus 30 days to today. The 30-day overlap catches late
  days and values re-published from another source stream (in 2025 the source flag changes from `W`
  to `1` at the end of August).
- Key: (station, date, element). Upsert, so a re-published value replaces the old one.
- Missing days are absent rows, so completeness is checked against a calendar after each load.

## F3 NWS alerts

- Schedule: every 15 minutes, `/alerts?area=TX&limit=500` and `/alerts/active?area=TX`.
- Watermark: max `sent` converted to UTC, minus 2 hours of overlap.
- Key: alert `id`. Update and Cancel messages have their own id and point to earlier ids in
  `references`; we store the link in `alert_reference` instead of overwriting the first message.
- A `User-Agent` header with a contact is required by the API.

## F4 NWS station observations

- Schedule: every hour per station in `config/stations.csv`.
- Watermark: max `timestamp` per station minus 3 hours. The request uses `start` so it does not depend
  on the default response, which returned only the latest 500 records (88 to 97 hours) in poll 1.
- Key: (station, timestamp).
- Main risk: NWS keeps observations about 7 days. An outage longer than that loses data for good, so
  the loader must alert when a station watermark is older than 24 hours.

## Failure rule (all sources)

1. Save the raw response to `data/raw/` first, byte for byte, with its `.headers.json`.
2. Parse from the file on disk, not from memory.
3. Rows that fail validation go to quarantine, not to the canonical tables.
4. The watermark only moves after the load is committed. A failed run is repeated from the old
   watermark, and the duplicate key makes the repeat safe.
