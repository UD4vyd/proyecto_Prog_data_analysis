# Workshop 1 — Deliverables index (Team 5)

| Rubric criterion (points) | Where |
|---|---|
| Hazard understanding and scope (2) | `docs/Workshop1_Report.pdf` §1–2 |
| Four validated sources with evidence (5) | Report §3 (real response snippets + HTTP headers); raw responses in `samples/` and `samples/poll1`, `samples/poll2`; script `scripts/fetch_samples.py` |
| Source profiling and data-quality findings (4) | `notebooks/01_source_profiling.ipynb` (executed); `data_quality_issues.csv`; report §4–7 |
| Canonical mapping and domain tables (4) | `mapping.md` (+ Mermaid ER diagram); `src/normalization/canonical.py`; `sql/001_schema_draft.sql`; report §8 |
| Ingestion contract, historical and incremental (3) | `docs/ingestion_contract.md`; `config/sources.yaml`; report §6 and §9 |
| Project definition, user stories, repository (2) | Report §10–15; repository root `README.md` |

## Samples

| File | Source | Notes |
|---|---|---|
| `noaa_storm_events_TX_2025_<ts>.csv` | H1 | every row with `STATE_FIPS=48` of `StormEvents_details-ftp_v1.0_d2025_c20260819.csv.gz`, original lines. The full 12 MB file is in `data/raw/` (git-ignored); re-download with `python workshop_1/scripts/fetch_samples.py historical`. |
| `noaa_storm_events_listing_<ts>.html` | H1 | directory listing (file names, compilation dates, sizes) used for the volume estimate |
| `ghcn_daily_<ts>.json`, `ghcn_daily_csv_<ts>.csv` | H2 | 10 stations, 2025-01-01 → today, JSON; one station in CSV to document the alternative format |
| `poll1/`, `poll2/` | I1, I2 | two polls ≥ 1 h apart: active alerts, recent alerts (≈7 days), observations per station (`.geojson.gz`, byte-identical gzip of the response) |
| `*.headers.json` | all | URL, HTTP status, retrieval time and caching headers of each response |
