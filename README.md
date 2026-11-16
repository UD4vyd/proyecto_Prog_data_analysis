# Extreme Weather Monitoring (Texas)

Team 5, Data Analysis Programming 2026-II, Universidad Distrital.
Andrés Mendoza, David Martínez, Juan Escallón.

## Project

We monitor extreme heat, cold and heavy precipitation at 46 first-order weather stations in Texas.
For each station we compare current observations with the 1991-2020 history and flag days that are
unusual or extreme. Past storm reports are used to check that the flags make sense.

Hazard and scope: Texas, daily temperature (TMAX, TMIN) and precipitation (PRCP). Station history
from 1991, storm reports from 1996. The current situation is the last 24 hours of station
observations plus the alerts active now.

## Sources

| Label | Source | Role |
|---|---|---|
| F1 | NOAA Storm Events Database, yearly bulk CSV.gz | historical impact reports |
| F2 | GHCN-Daily via NCEI Access Data Service (`daily-summaries`) | historical daily station data |
| F3 | NWS API alerts (`/alerts`, `/alerts/active?area=TX`) | incremental warnings by zone |
| F4 | NWS API station observations (`/stations/{icao}/observations`) | incremental current station data |

## Repository structure

```
config/         stations.csv (46 selected stations), sources.yaml (source catalogue)
data/raw/       landing zone, raw responses saved byte for byte with .headers.json
data/staging/   derived files
docs/           ingestion contract, source status log, Workshop 1 report
notebooks/      01_source_profiling.ipynb (station selection and profiling)
workshop_1/     samples, poll script, mapping.md, data_quality_issues.csv
ingest.py       command-line stub (--mode historical|incremental|status)
```

## Setup

Requires Python 3.12 and Poetry.

```bash
poetry install                       # creates .venv in the project folder
cp .env.example .env                 # set NWS_USER_AGENT with a contact
poetry run jupyter nbconvert --to notebook --execute --inplace notebooks/01_source_profiling.ipynb
poetry run python workshop_1/poll_nws.py workshop_1/samples/pollN   # one F3/F4 evidence poll
poetry run python ingest.py --mode status
```

The F1 yearly file (`data/raw/*.csv.gz`) is not committed because of its size. The notebook
downloads it again from the bulk listing if it is missing.

## Workshop 1 deliverables

- Report: [docs/Workshop1_Report.pdf](docs/Workshop1_Report.pdf) (source: [docs/report/Workshop1_Report.md](docs/report/Workshop1_Report.md))
- Profiling notebook: [notebooks/01_source_profiling.ipynb](notebooks/01_source_profiling.ipynb)
- Data-quality table: [workshop_1/data_quality_issues.csv](workshop_1/data_quality_issues.csv)
- Canonical mapping and ER draft: [workshop_1/mapping.md](workshop_1/mapping.md)
- Ingestion contract: [docs/ingestion_contract.md](docs/ingestion_contract.md)
- Source catalogue: [config/sources.yaml](config/sources.yaml)
- Source status log: [docs/source_status_log.md](docs/source_status_log.md)
- Evidence: [workshop_1/samples/](workshop_1/samples/)
