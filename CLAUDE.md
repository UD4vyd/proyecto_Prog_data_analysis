# CLAUDE.md: Team 5, Extreme Weather Monitoring (Texas)

Course: Data Analysis Programming 2026-II, Universidad Distrital (Prof. Carlos Andrés Sierra).
Owner of this branch: David Martínez. Team: Andrés Mendoza, David Martínez, Juan Escallón.

You are finishing **Workshop 1** on the `main` branch, continuing the work David already did.
Read this whole file before touching anything.

**Hard deadline: the PDF report and the repo link are submitted at 19:30 Bogotá time (00:30 UTC).**
Work in the priority order of section 8. A finished, simple deliverable beats an unfinished ambitious one.

---

## 1. How to work

- **Continue David's approach. Do not restart it.** The notebook `notebooks/01_source_profiling.ipynb`
  already has his cells (station selection, crosswalk, first GHCN query). Keep them, and add the new
  cells after them.
- **Do not copy from the `workshop-1` branch.** A teammate generated it with a different scope
  (2010 window, 10 hand-picked stations, pip). You may read it as a checklist of topics, but every
  decision in `main` follows section 3 of this file.
- **Never invent numbers.** Every figure in the report, README or mapping must come from an
  executed notebook output or a file in the repo. If something cannot be verified (for example the
  meaning of a GHCN source flag that is not in the documentation you have), say so plainly or drop
  it. Never write "to be confirmed".
- **Stay in the scope of Workshop 1.** Do NOT implement: Docker, Airflow, a database, real
  ingestion logic in `ingest.py`, `src/` modules, or tests. Workshop 1 needs evidence, profiling,
  mapping, a contract and a project definition. `ingest.py` stays a stub that only parses
  `--mode historical|incremental|status`.
- **Dependencies only with Poetry** (`poetry add <pkg>`; dev tools with `--group dev`). There is no
  `requirements.txt` and you must not create one. Runtime libraries (pandas, requests) are main
  dependencies; jupyter and nbconvert go in the dev group.
- Run the notebook with the project venv (`.venv`, created by Poetry in the project folder).

## 2. Writing style (all deliverables are in English)

The documents must read like the work of this team: undergraduate engineers explaining what they
did and why.

- Plain technical English, short sentences, active voice ("We filtered...", "We chose...").
- Explain the reasoning behind each decision in one or two sentences, including the trade-off.
- Keep punctuation simple: commas and periods. Avoid long dashes, arrows and "≈" in running text.
  Write "about 1,000 rows", not "≈1k rows".
- No marketing tone, no filler intros or summaries that repeat the section.
- Tables are fine where the content is a comparison or a per-source contract.
- Report length: about 6 to 9 pages, not more.
- Notebook: short markdown cells that say what each step checks and what the result means,
  written after seeing the output. Code cells should be readable, not clever one-liners.
- Git: one commit per real piece of work with a clear message (for example
  "Add F1 Storm Events profiling", "Write canonical mapping"). Do not squash everything into one
  commit, and do not rewrite or fake history.

## 3. Decisions already taken (do not change them)

### Source labels (use them everywhere: notebook, report, mapping)
| Label | Source | Role |
|---|---|---|
| F1 | NOAA Storm Events Database, yearly bulk CSV.gz | historical (impact reports) |
| F2 | GHCN-Daily via NCEI Access Data Service, `dataset=daily-summaries` | historical (daily station measurements) |
| F3 | NWS API alerts (`/alerts`, `/alerts/active?area=TX`), GeoJSON with CAP fields | incremental (warnings by zone) |
| F4 | NWS API station observations (`/stations/{icao}/observations`) | incremental (current station measurements) |

How they relate: F2 vs F4 = same station, history vs now (this is the indicator). F1 vs F2 = past
impacts used to validate the indicator (station inside the county of the event, same local day).
F3 vs F4 = is there an active alert in the zone where the station is (match by NWS zone/county
code). F1 and F4 never overlap in time (F1 has months of delay, F4 keeps 7 days).

### Scope
- Region: Texas (required US scope because F1, F3, F4 are US services).
- Historical windows: **F2 from 1991 to present** (the percentile baseline is the WMO standard
  normal period 1991-2020). **F1 from 1996 to present** (before 1996 F1 only recorded a few event
  types, so earlier frequencies are not comparable).
- "Current situation" = F4 observations of the last 24 hours plus F3 alerts active now; the last
  7 days are shown for context.

### Station selection (David's work, keep it and explain it in the report)
Final list: `config/stations.csv` (46 stations). Criterion, in one sentence:
"All first-order (USW) GHCN-Daily stations in Texas with TMAX, TMIN and PRCP records from 1991 or
earlier to 2025 or later, that have an ICAO identifier in the NOAA ISD station history and are
published by the NWS API."

Steps (all reproducible in the notebook from files in `data/raw/`):
1. `ghcnd-stations.txt` (saved as `S2_stations.txt`, fixed width) filtered to ID prefix `USW` and
   state `TX`: 171 stations.
2. `ghcnd-inventory.txt` (`S2_inventory.txt`): keep stations that have all three elements
   TMAX, TMIN, PRCP with first_year <= 1991 and last_year >= 2025 (semi-join). Note: the inventory
   only gives first and last year, not gaps, so real completeness is checked on the daily data.
3. Crosswalk F2 -> F4: the last 5 digits of the USW id are the WBAN number. Join with
   `isd-history.csv` (read WBAN/USAF as strings to keep leading zeros) to get the ICAO code.
   Left join with indicator to report unmatched stations.
4. Check that each ICAO exists in the NWS stations catalogue for Texas (9 paged JSON files
   `data/raw/nws_stations_tx_*`), which also gives forecast zone, county zone and time zone.
5. Assert `ghcn_id` and `icao` are unique before writing `config/stations.csv`.

Problems found during the selection (they go into the data-quality section):
- ISD uses the sentinel `99999` for a missing WBAN (and `999999` for USAF). Converted to NULL.
- The same WBAN appears in several ISD rows (station history). This first produced duplicated
  stations in the config, which we detected because a GHCN request returned one station twice
  (730 rows for one station, 365 duplicated station-days). Fixed by keeping the latest row per
  WBAN (sort by END) and asserting uniqueness.
- The NWS catalogue paginates with a cursor; the last `next` link returns an empty page, so the
  loop must stop on an empty `features` list.

### Evidence rules
- Raw responses are saved byte-for-byte (`r.content`) before parsing, named
  `source_YYYYMMDD_HHMMSS.ext` in UTC, with a `.headers.json` next to them (URL, status, time,
  HTTP headers). `data/raw/` is the landing zone. Small samples go to `workshop_1/samples/`.
- Never modify or overwrite a file in `data/raw/`. Derived files go to `data/staging/`.
- Poll 1 of F3/F4 was taken at **2026-11-16 23:12:34 UTC** (`workshop_1/samples/poll1/`) for
  KDFW, KIAH, KAUS. **Poll 2 must be taken at or after 00:13 UTC** into `workshop_1/samples/poll2/`
  with the same code, then compared (new alert ids, alerts no longer active, new observation
  timestamps per station). If poll 2 is not possible before the deadline, say so honestly in the
  report and describe the update behaviour that poll 1 already shows (timestamps, `messageType`
  Alert/Update/Cancel, `references`).

### Findings already confirmed (use them, with the example records from the outputs)
- F2: numeric values arrive as strings in the JSON (`"TMAX": "15.0"`); convert explicitly with
  `pd.to_numeric(errors="coerce")`, never rely on implicit inference.
- F2: missing station-days are absent rows, not nulls (USW00003902 has no data on 2025-08-30 and
  2025-08-31). Completeness must be checked against a full calendar.
- F2: `_ATTRIBUTES` = measurement, quality, source flags. Precipitation trace days have
  `PRCP = 0.0` with measurement flag `T`. Decision needed and documented: we keep the value 0.0 and
  a boolean `is_trace` flag, so wet-day statistics can include or exclude them.
- F2: source flag `W` = WBAN/ASOS summary of the day from ISD (documented in the GHCN-Daily PDF).
  Values can be re-published from another source stream later, so F2 is not frozen: the contract
  re-reads a recent window.
- F1 (to measure in the notebook): local standard time with an offset field (`CZ_TIMEZONE` like
  `CST-6`, `MST-7` for El Paso), 2-digit year in `BEGIN_DATE_TIME`, damage as text with K/M/B
  suffix, `MAGNITUDE` units depend on event type (hail inches, wind knots, empty for heat/flood),
  zone-based rows (`CZ_TYPE=Z`) have no coordinates. Filter Texas with `STATE_FIPS == "48"`, not
  with text matching.
- F3/F4: GeoJSON coordinates are `[lon, lat]` (opposite order to F2); timestamps carry local
  offsets in F3; F4 values come as `{value, unitCode, qualityControl}` objects with nulls as
  `value: null`; F4 observations are kept only 7 days by NWS (risk: an outage longer than 7 days
  loses data permanently).

## 4. Notebook: what is still missing

Add these sections after David's existing cells, in this order, each with a short markdown intro
and a short conclusion. Execute the whole notebook top to bottom at the end so all outputs are
saved (`poetry run jupyter nbconvert --to notebook --execute --inplace notebooks/01_source_profiling.ipynb`).

1. F2 profiling of the sample (3 stations, 2025, TMAX/TMIN/PRCP): shape, duplicates on
   (STATION, DATE), missing days against the calendar, null %, ranges, TMIN > TMAX check, flags
   split into M/Q/S columns with counts, trace days.
2. F1: find the latest `d2025` details file in the bulk listing, download once to `data/raw/`,
   keep Texas rows (`STATE_FIPS == "48"`), save the Texas sample in `workshop_1/samples/`.
   Profile: rows and fields, EVENT_ID duplicates, CZ_TYPE, CZ_TIMEZONE, date format example,
   coordinates missing by CZ_TYPE, damage parsing, MAGNITUDE by EVENT_TYPE.
3. F3: profile poll 1 (count, id duplicates, offsets in `sent`, share without geometry,
   messageType, severity, references, events).
4. F4: profile poll 1 (records and hours covered per station, duplicates on
   (station, timestamp), null % per variable, units, QC codes, temperature range).
5. Overlap: at least 5 candidate pairs F1 <-> F2 (county FIPS of the station from
   `county_zone`, same local day, compatible variable) with what differs between them.
   Mention that F2 <-> F4 is the same physical station by construction (crosswalk).
6. Data-quality table with at least 8 problems, each with extent and one example record,
   saved to `workshop_1/data_quality_issues.csv`.
7. Poll 2 and comparison (see section 3).
8. Volume estimate: F2 = 46 stations x years x 365 x elements; F1 = yearly file sizes from the
   listing; partitioning (F1 per year file, F2 per station and year request).

## 5. Other files to write

- `workshop_1/mapping.md`: table per source mapping native fields to the canonical `event`
  fields (event_id, source, event_type, timestamp UTC, latitude, longitude, severity + unit,
  raw_reference), the unit and time rules, the proposed domain tables
  (`station`, `daily_observation` for F2, `observation` for F4, `storm_event` for F1, `alert` for
  F3) and a first ER draft as a Mermaid diagram. Draft level only.
- `config/sources.yaml`: one entry per source with owner, url, docs, format, auth, rate limits
  (only what is documented), license, historical strategy, schedule, watermark field, overlap
  window and duplicate key. Plus a `scope` block (Texas, windows, stations file path).
- `docs/ingestion_contract.md`: per source, historical strategy, incremental strategy,
  watermark, overlap window, duplicate key, revisions. Decisions:
  F1 one file per year, re-load a year when its `_cYYYYMMDD` suffix changes, key EVENT_ID.
  F2 one request per station and year; incremental per station from max DATE minus 30 days
  (late data and re-published values), key (station, date, element).
  F3 every 15 minutes, watermark max `sent` minus 2 hours, key alert `id`, Update/Cancel linked via
  `references`.
  F4 every hour per station, watermark max `timestamp` minus 3 hours, key (station, timestamp);
  the 7-day retention is the main risk.
  Failure rule: raw file first, parse from disk, watermark only moves after a successful load.
- `docs/source_status_log.md`: one line per source with the date it was validated.
- `.gitignore`: it is currently empty. Ignore `.venv/`, `__pycache__/`, `.ipynb_checkpoints/`,
  `.env`, and large raw files (`data/raw/*.csv.gz`). Keep small evidence files.
- `README.md` (currently empty): project, team, hazard and scope, the 4 sources, repo structure,
  how to set up with Poetry and run the notebook, and links to the Workshop 1 deliverables.
- `.env.example`: `NWS_USER_AGENT="(extreme-weather-monitoring, contact email)"`.
- `ingest.py`: argparse stub with the three modes; it prints that ingestion arrives in Workshop 2.

## 6. Report: `docs/Workshop1_Report.pdf`

Sections (match the workshop rubric):
1. Project overview: title, motivation, goal, scope, exclusions, constraints.
2. Hazard definition: phenomena, severity measures with units, Texas scope, windows and why,
   what "current situation" means.
3. Sources and validation evidence: for F1-F4 owner, URL, access, auth, limits, license,
   format, coverage, update policy, plus one short real response snippet each.
4. Station selection and crosswalk (section 3 of this file, with the real counts).
5. Profiling results and data-quality findings (table with examples).
6. Incremental behaviour from the two polls.
7. Cross-source overlap (the candidate pairs).
8. Canonical mapping and domain tables (summary, link to mapping.md, ER figure).
9. Ingestion contract (summary table, link to the md).
10. Functional user: an emergency-management analyst in a Texas state or county office who
    monitors current conditions and needs to know if today is unusual for each station.
11. At least 6 user stories with MoSCoW priority covering monitoring (what is new), analysis
    (is it unusual) and trust (how fresh and complete the data are).
12. Indicator proposal: for each station, element and day of year, percentiles from the 1991-2020
    baseline using a window of +/-7 days. NORMAL between p10 and p90, UNUSUAL outside p10-p90,
    EXTREME below p1 or above p99. Thresholds are parameters. Validation: check whether days marked
    EXTREME in the past match F1 events in the station county (examples to test: summer 2011 heat,
    Hurricane Harvey rain in August 2017). Monitoring aid only, not a prediction.
13. Risks and mitigation (at least 5): F4 7-day retention, rate limits / User-Agent, schema
    changes, F1 delay and recompilations, ambiguous identifiers (crosswalk), data volume.
14. Architecture overview: sources -> raw landing zone -> normalize/validate/deduplicate
    (quarantine) -> PostgreSQL canonical model -> analytics -> Dash dashboard. Figure as image.
15. Repository and references (cite NOAA NCEI, GHCN-Daily DOI 10.7289/V5D21VHZ, NWS API).

Build it as Markdown (`docs/report/Workshop1_Report.md`) and convert to PDF. Use pandoc if it is
installed; otherwise use a small Python script with a PDF library added through Poetry's dev group.
If the conversion fails close to the deadline, stop and tell David so he can export the Markdown
manually.

## 7. Before finishing

- Run the notebook top to bottom with no errors.
- `git status` must not include `.venv/` or secrets.
- Check that every number in the report appears in a notebook output.
- Commit and push to `main`.
- Give David a short list of the decisions he must be ready to explain in the defense.

## 8. Priority order if time runs out

1. Notebook sections 1 to 6 executed (largest share of the grade: sources, profiling).
2. `mapping.md` with ER draft and `ingestion_contract.md`.
3. Report PDF.
4. Poll 2 comparison (run it as soon as it is 00:13 UTC, whatever you are doing).
5. README, sources.yaml, .gitignore, stub.
