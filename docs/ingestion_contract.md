# Ingestion Contract (draft) — Team 5 Extreme Weather Monitoring

Common rules for every source:

* Every response is first written **unchanged** to the raw landing zone `data/raw/<source>_YYYYMMDD_HHMMSS.<ext>`
  (plus a `.headers.json` with URL, status, ETag, Last-Modified). Parsing always reads from that file.
* `event_id` is deterministic (`<source>:<native key>`), so loads use `INSERT … ON CONFLICT (event_id) DO UPDATE
  … WHERE event.content_hash <> EXCLUDED.content_hash`: re-running any mode never duplicates rows and only touches
  rows whose content changed.
* Each run opens an `ingestion_run` row (`status='running'`). For every source the canonical rows, the domain rows,
  the quarantined rows and the new watermark in `ingestion_state` are written **in one transaction**. The watermark
  only moves after the commit.
* HTTP: timeout 60 s, retries with exponential back-off (1, 2, 4, 8 s) on 429/5xx/timeouts, a descriptive
  `User-Agent` (from `NWS_USER_AGENT`), and a small delay between requests.

| | H1 NOAA Storm Events | H2 GHCN-Daily (NCEI ADS) | I1 NWS alerts | I2 NWS observations |
|---|---|---|---|---|
| **Historical strategy** | One `details` CSV.gz per year 2010→current (17 files, ~180 MB gz US-wide). Filter `STATE='TEXAS'`. Partition = year. | One request per station per year (10 × 17 = 170 requests, `format=json`, `units=metric`, 8 elements). Partition = station-year. | Not a historical source: the `/alerts` collection only served the last ~7 days (oldest `sent` 2026-11-02 when queried on 2026-11-09). History of impacts comes from Storm Events. | Not a historical source: only recent days are served and responses are capped at 500 features. History comes from GHCN-Daily at the same stations. |
| **Pagination / limits** | None (static files). | None documented; keep requests to station-year size. | Cursor in `pagination.next`; follow until absent or empty. | Hard cap of 500 features per request (≈ 4 days at 5-min reporting). |
| **Incremental strategy** | Monthly: re-read directory listing; a year is reloaded when its `_cYYYYMMDD` suffix changes. | Daily: request `[watermark − 45 days, today]` per station (values arrive ~3 days late and QC flags change). Monthly: re-read the previous 13 months to catch late re-publication (source-flag change). | Every 15 min: `GET /alerts?area=TX&start=<watermark − 2 h>`; also `GET /alerts/active?area=TX` to refresh the active set. | Every 30 min: `GET /stations/{id}/observations?start=<watermark − 3 h>` per station; if exactly 500 rows come back, split the window in halves. |
| **Watermark** | Map `{year: compilation_date}`. | Per station: max `DATE` loaded. | Max `properties.sent` committed. | Per station: max `properties.timestamp` committed. |
| **Overlap window** | Whole year (file replaced). | 45 days. | 2 hours. | 3 hours. |
| **Change detection** | Compilation suffix + per-row `content_hash`. Rows absent from a new compilation are flagged `withdrawn`, not deleted. | `content_hash` of value + attributes (QC flag changes count as revisions). | New `id` per message; `Update`/`Cancel` reference previous ids (`references`) → stored in `alert_reference`. ETag/Last-Modified not reliable for change detection (see report §5). | `content_hash` of value + `qualityControl` per variable; same `(station, timestamp)` with different values = revision. |
| **Duplicate key** | `EVENT_ID` | `(STATION, DATE, ELEMENT)` | `properties.id` | `(stationId, timestamp, variable)` |
| **Revisions / deletions** | Yes — whole years are recompiled (e.g. the 2012 file was recompiled on 2026-10-26). | Yes — in the sample, days up to Aug 2025 carry source flag `W` and later days carry `1`: station-days are re-published from a different source stream roughly a year later (meaning of flag `1` to be confirmed in the GHCN-Daily readme). The hash includes the attributes, so this is detected as a revision. | Messages are immutable; an event evolves through new messages; expired alerts disappear from `/active`. | Yes — late QC can change values/QC codes within hours. |

## Correctness if a run fails halfway

1. **Raw first:** the response is on disk before parsing, so a parse failure can be replayed without
   calling the source again.
2. **Atomic per source:** event + domain rows + watermark commit together. If the process dies before
   the commit, the transaction is rolled back and the watermark still points to the previous successful
   position, so the next run re-requests the same interval.
3. **Idempotent writes:** deterministic `event_id` + `ON CONFLICT` upsert means the re-requested interval
   (and the overlap window) cannot create duplicates.
4. **Partition log for the historical load:** each finished partition (year file / station-year) is
   recorded in `ingestion_run`; a re-run of `--mode historical` skips completed partitions and resumes
   the rest.
5. **Audit:** the failed `ingestion_run` row keeps `status='failed'` and the error, so `--mode status`
   shows the gap and the watermark age.
