# Extreme Weather Monitoring System — Texas (Team 5)

Data Analysis Programming, semester 2026-II — Universidad Distrital Francisco José de Caldas
(Prof. Carlos Andrés Sierra).

A multi-source data ingestion and analysis system that integrates historical and continuously
updated weather information for **Texas, USA**, detects new extreme-weather events and
observations, and classifies current measurements as **NORMAL / UNUSUAL / EXTREME** against the
historical distribution of the same station and season.

> Monitoring and correlation aid only — the system does **not** forecast or predict weather hazards.

## Team

| Student ID | Name |
|---|---|
| 20222020152 | Andrés Mendoza |
| 20222020167 | David Martínez |
| 20231020002 | Juan Escallón |

## Hazard and scope

* **Hazard:** extreme heat, extreme cold, heavy precipitation / flash flooding, severe wind, hail,
  tornadoes and winter storms.
* **Region:** State of Texas (US scope is required because the NWS and Storm Events sources are US-only).
  10 first-order ASOS stations published both by GHCN-Daily and by the NWS API:
  KDFW, KIAH, KAUS, KSAT, KELP, KLBB, KAMA, KCRP, KMAF, KBRO.
* **Historical window:** 2010-01-01 → present (≥ 16 years).
* **Current situation:** active NWS alerts now + station observations of the last 24 h, compared with the
  station-season baseline; plus the last 7 days for context.

## Sources

| Id | Source | Role | Format / access | License |
|---|---|---|---|---|
| H1 | [NOAA NCEI Storm Events Database](https://www.ncei.noaa.gov/stormevents/) | historical | yearly CSV.gz bulk files | Public domain (US Gov.) |
| H2 | [GHCN-Daily via NCEI Access Data Service](https://www.ncei.noaa.gov/access/services/data/v1) | historical | REST API, JSON/CSV | Public domain; cite Menne et al. 2012 |
| I1 | [NWS API — alerts](https://www.weather.gov/documentation/services-web-api) | incremental | REST, GeoJSON (CAP) | Public domain (US Gov.) |
| I2 | [NWS API — station observations](https://www.weather.gov/documentation/services-web-api) | incremental | REST, GeoJSON | Public domain (US Gov.) |

Details (URLs, schedule, watermark field, duplicate key) live in [`config/sources.yaml`](config/sources.yaml).

## Repository structure

```
├── ingest.py                  # CLI entry point (stub in Workshop 1)
├── config/sources.yaml        # one entry per source + region/stations
├── src/
│   ├── sources/               # raw readers (and API clients from Workshop 2)
│   ├── normalization/         # canonical mapping rules
│   └── storage/               # database access (Workshop 2)
├── sql/                       # schema definitions (draft)
├── dashboard/                 # Plotly Dash app (Workshop 4)
├── data/raw/                  # raw landing zone (git-ignored)
├── notebooks/01_source_profiling.ipynb
├── tests/ + tests/fixtures/   # pytest + recorded real responses
├── docs/                      # ingestion contract, source status log, report, figures
└── workshop_1/                # mapping.md, samples/, scripts/, data-quality findings
```

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                  # set NWS_USER_AGENT with your contact

python ingest.py --mode status                        # stub
python -m pytest -q                                   # normalisation tests on fixtures
```

### Reproducing the Workshop 1 evidence

```bash
python workshop_1/scripts/fetch_samples.py historical          # Storm Events 2025 + GHCN-Daily sample
python workshop_1/scripts/fetch_samples.py incremental poll1   # NWS alerts + observations, poll 1
# wait at least one hour
python workshop_1/scripts/fetch_samples.py incremental poll2   # poll 2
python workshop_1/scripts/build_notebook.py
jupyter nbconvert --to notebook --execute --inplace notebooks/01_source_profiling.ipynb
```

The full Storm Events yearly file (≈12 MB gz) is stored in `data/raw/` (git-ignored); the Texas rows
are kept byte-for-byte in `workshop_1/samples/`. NWS observation responses are committed gzip-compressed
(`.geojson.gz`, identical content).

## Workshop deliverables

* Workshop 1 — report: [`docs/Workshop1_Report.pdf`](docs/Workshop1_Report.pdf) · mapping:
  [`workshop_1/mapping.md`](workshop_1/mapping.md) · contract: [`docs/ingestion_contract.md`](docs/ingestion_contract.md)
  · profiling: [`notebooks/01_source_profiling.ipynb`](notebooks/01_source_profiling.ipynb)

## Attribution

Storm Events Database and GHCN-Daily: NOAA National Centers for Environmental Information.
GHCN-Daily: Menne, M.J., I. Durre, B. Korzeniewski, S. McNeill, K. Thomas, X. Yin, S. Anthony, R. Ray,
R.S. Vose, B.E. Gleason, and T.G. Houston (2012): Global Historical Climatology Network - Daily (GHCN-Daily),
Version 3. NOAA National Climatic Data Center. doi:10.7289/V5D21VHZ.
Alerts and observations: NOAA National Weather Service, api.weather.gov.
