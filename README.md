# Extreme Weather Monitoring (Texas)

Team 5, Data Analysis Programming 2026-I, Universidad Distrital.
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

## Licenses and attribution

- F1: US Government work, public domain. Cite "NOAA National Centers for Environmental Information (NCEI), Storm Events Database".
- F2: public domain. Cite Menne et al. (2012), Global Historical Climatology Network - Daily (GHCN-Daily), Version 3, NOAA NCEI, doi:10.7289/V5D21VHZ.
- F3 and F4: US Government work, public domain. Credit "NOAA National Weather Service (api.weather.gov)". A descriptive User-Agent header is required.

## Repository structure

```
airflow/dags/     bronze_ingestion, silver_processing, gold_processing
src/              pipeline code: sources, Bronze storage, Silver normalizers, Gold PySpark summaries
dashboard/        the two Plotly Dash apps (Workshop 4), see dashboard/README.md
config/           stations.csv, sources.yaml, pipeline.yaml, governance_thresholds.yaml
datalake_bronze/  raw JSON per run (samples committed)
datalake_silver/  clean Parquet per topic (samples committed)
datalake_gold/    governance_*.parquet and storytelling_*.parquet (sample committed)
data/raw/         Workshop 1 landing zone, raw responses saved byte for byte with .headers.json
docs/             ingestion contract, source status log, Workshop 1 report
notebooks/        profiling (01), Silver exploration (02), data quality findings (03), Gold preview (04)
tests/            pytest: pipeline, Gold (needs Java) and dashboards
workshop_1..4/    reports, evidence and samples of each workshop
ingest.py         command line: --mode historical|incremental|status
```

## Setup

Requires Python 3.11 to 3.13 and Poetry.

```bash
poetry install                       # creates .venv in the project folder
cp .env.example .env                 # set NWS_USER_AGENT with a contact
poetry run jupyter nbconvert --to notebook --execute --inplace notebooks/01_source_profiling.ipynb
poetry run python workshop_1/poll_nws.py workshop_1/samples/pollN   # one F3/F4 evidence poll
poetry run python ingest.py --mode status
```

The F1 yearly file (`data/raw/*.csv.gz`) is not committed because of its size. The notebook
downloads it again from the bulk listing if it is missing.

Three reference files used by the station selection are also not committed (52 MB in total).
Download them into `data/raw/` with these names before running the notebook:

| File in `data/raw/` | Download from |
|---|---|
| `S2_stations.txt` | https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt |
| `S2_inventory.txt` | https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt |
| `isd-history.csv` | https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv |

## Workshop 1 deliverables

- Report: [docs/Workshop1_Report.pdf](docs/Workshop1_Report.pdf) (source: [docs/report/Workshop1_Report.md](docs/report/Workshop1_Report.md))
- Profiling notebook: [notebooks/01_source_profiling.ipynb](notebooks/01_source_profiling.ipynb)
- Data-quality table: [workshop_1/data_quality_issues.csv](workshop_1/data_quality_issues.csv)
- Canonical mapping and ER draft: [workshop_1/mapping.md](workshop_1/mapping.md)
- Ingestion contract: [docs/ingestion_contract.md](docs/ingestion_contract.md)
- Source catalogue: [config/sources.yaml](config/sources.yaml)
- Source status log: [docs/source_status_log.md](docs/source_status_log.md)
- Evidence: [workshop_1/samples/](workshop_1/samples/)

## Workshop 2: Bronze and Silver pipeline

Status: the Airflow stack runs locally with Docker Compose. The DAG `bronze_ingestion` saves raw JSON
files of the four sources in `datalake_bronze/` every hour, and the DAG `silver_processing` detects them
with a FileSensor and writes clean Parquet files in `datalake_silver/`. Gold is described below (Workshop 3).

Dependencies are managed with Poetry (`pyproject.toml` and `poetry.lock`). The Airflow image installs the
dependency list of `pyproject.toml`, and `requirements.txt` is the export of the lock
(`poetry export --only main --without-hashes -o requirements.txt`).

```bash
cp .env.example .env            # fill FERNET_KEY and NWS_USER_AGENT
docker compose build            # image with the dependencies of pyproject.toml
docker compose up -d            # Airflow UI on http://localhost:8080 (user and password from .env)
```

To add a library: `poetry add <name>`, export `requirements.txt` again and run `docker compose build`.
Tests and notebooks run in the Poetry environment:

```bash
poetry install
poetry run pytest
poetry run jupyter nbconvert --to notebook --execute --inplace notebooks/02_silver_exploration.ipynb
```

In the UI turn on `silver_processing` and `bronze_ingestion`. To start a run by hand use the Trigger
button or `docker compose exec airflow-scheduler airflow dags trigger bronze_ingestion`.
If a new DAG file does not appear, run `docker compose restart airflow-dag-processor`.

The same code runs from the command line:

```bash
docker compose exec airflow-scheduler python /opt/airflow/project/ingest.py --mode status
docker compose exec airflow-scheduler python /opt/airflow/project/ingest.py --mode historical --source ghcn_daily --start-year 2025 --end-year 2025 --skip-silver
```

Deliverables: [report](workshop_2/Workshop2_Report.pdf), DAGs in `airflow/dags/`, pipeline code in `src/`,
[Silver notebook](notebooks/02_silver_exploration.ipynb), samples in `datalake_bronze/processed/` and
`datalake_silver/`, screenshots in `workshop_2/evidence/`. The Storm Events Bronze file (11.5 MB) is not
committed; the Bronze DAG rebuilds it.

## Workshop 3: Gold layer with PySpark

Status: the three layers run in Airflow.

| Layer | DAG | Schedule | Output |
|---|---|---|---|
| Bronze | `bronze_ingestion` | `@hourly` | raw JSON in `datalake_bronze/` |
| Silver | `silver_processing` | `@continuous` + FileSensor | clean Parquet per topic in `datalake_silver/` |
| Gold | `gold_processing` | `15 6 * * *` (00:15 in Texas) | `governance_*.parquet` and `storytelling_*.parquet` in `datalake_gold/` |

The Gold DAG runs PySpark in local mode (`local[*]`, 2 GB driver) inside the Airflow containers. It reads all
the Silver files of each topic as one DataFrame (`spark.read.parquet("datalake_silver/<topic>_*.parquet")`),
keeps the latest version of each record and writes two summaries in a long layout
(`section, topic, period, dim_1, dim_2, metric, value, label, computed_at`):

- governance: volume, duplicate rate, null rate per field, schema compliance, outlier rate, text length,
  ingestion compliance, freshness and station completeness (`src/gold/governance.py`)
- storytelling: NORMAL / UNUSUAL / EXTREME class of each station-day against 1991-2020, class distribution,
  current situation with active alerts, alert severity per day and event type, top keywords and bigrams,
  keyword-severity association, storm impacts and the match of EXTREME days with Storm Events reports
  (`src/gold/storytelling.py`)

The image now includes a Java runtime (`default-jre-headless`) for Spark, so build it again after pulling:

```bash
docker compose build
docker compose up -d
```

In the UI turn on `gold_processing`, or trigger it by hand:

```bash
docker compose exec airflow-scheduler airflow dags trigger gold_processing
```

The indicator needs the 1991-2020 baseline of GHCN-Daily, loaded once with the historical mode:

```bash
docker compose exec airflow-scheduler python /opt/airflow/project/ingest.py --mode historical --source ghcn_daily --start-year 1991 --end-year 2026 --skip-silver
```

Storm Events is loaded for the five years 2021-2025 (the Course Project asks for at least five years of each
historical source):

```bash
docker compose exec airflow-scheduler python /opt/airflow/project/ingest.py --mode historical --source storm_events --start-year 2021 --end-year 2025 --skip-silver
```

Notebooks: [data quality findings](notebooks/03_data_quality_findings.ipynb) and
[Gold preview](notebooks/04_gold_preview.ipynb). Run them with `poetry run python -m nbconvert --to notebook --execute --inplace <notebook>`.

Deliverables: [report](workshop_3/Workshop3_Report.pdf), Gold DAG `airflow/dags/gold_processing_dag.py`,
PySpark modules in `src/gold/`, Silver and Bronze improvements in `src/normalization/`, `src/storage/silver.py`
and `src/sources/http.py`, sample Gold files in `datalake_gold/`, screenshots in `workshop_3/evidence/`.

## Workshop 4: dashboards and end-to-end run

Status: the three DAGs feed two independent Plotly Dash apps that read only the Gold files.

| Service | URL | Reads |
|---|---|---|
| Airflow UI | http://localhost:8080 | DAGs `bronze_ingestion`, `silver_processing`, `gold_processing` |
| Governance dashboard | http://localhost:8050 | newest `datalake_gold/governance_*.parquet` |
| Storytelling dashboard | http://localhost:8051 | newest `datalake_gold/storytelling_*.parquet` |

### From a fresh clone

Requirements: Docker Desktop (8 GB of memory for the VM), Git. Python 3.11-3.13 and Poetry only for the
local runs, tests and notebooks.

```bash
git clone https://github.com/UD4vyd/proyecto_Prog_data_analysis.git
cd proyecto_Prog_data_analysis
git checkout v1.0-workshop4
cp .env.example .env              # fill FERNET_KEY and NWS_USER_AGENT (on Linux also AIRFLOW_UID=$(id -u))
docker compose build              # Airflow image (with Java for Spark) and the dashboard image
docker compose up -d              # Airflow on :8080, governance dashboard on :8050, storytelling on :8051
```

The repository includes sample Bronze, Silver and Gold files, so both dashboards show data right away.

### Run the pipeline end to end

1. Open http://localhost:8080 (user and password from `.env`) and turn on the three DAGs.
2. One time only, load the history the indicator needs (1991-2020 baseline and five years of storm reports):

   ```bash
   docker compose exec airflow-scheduler python /opt/airflow/project/ingest.py --mode historical --source ghcn_daily --start-year 1991 --end-year 2026 --skip-silver
   docker compose exec airflow-scheduler python /opt/airflow/project/ingest.py --mode historical --source storm_events --start-year 2021 --end-year 2025 --skip-silver
   ```

3. Trigger the DAGs in order, waiting for each run to finish (Trigger button in the UI, or):

   ```bash
   docker compose exec airflow-scheduler airflow dags trigger bronze_ingestion
   # silver_processing starts by itself: its FileSensor sees the new Bronze files
   docker compose exec airflow-scheduler airflow dags trigger gold_processing
   ```

4. Press **Refresh data** in both dashboards (they also reload every 5 minutes). The header shows the time
   of the Gold run that produced the data.

On a schedule, Bronze runs every hour, Silver right after each Bronze file and Gold every day at 06:15 UTC
(00:15 in Texas).

### Start each dashboard on its own

```bash
docker compose up -d dashboard-governance          # only the governance dashboard
docker compose up -d dashboard-storytelling        # only the storytelling dashboard
```

Without Docker (Poetry group `dashboard`):

```bash
poetry install --with dashboard
poetry run python dashboard/governance_app.py       # http://localhost:8050
poetry run python dashboard/storytelling_app.py     # http://localhost:8051
```

Dashboard libraries are pinned in the Poetry group `dashboard` and in `dashboard/requirements.txt` (used by the
dashboard image). They are not installed in the Airflow image, which pins its own Flask. `requirements.txt` is
exported with `poetry export --only "main,dashboard" --without-hashes -o requirements.txt`.

Notes for Windows (checked on a fresh Windows 11 laptop):

- Docker Desktop needs WSL 2: run `wsl --install` in an administrator PowerShell and restart.
- Use Python 3.11-3.13 (3.14 is not supported by the project). With several versions installed:
  `poetry env use (py -3.13 -c "import sys; print(sys.executable)")`.
- If Smart App Control blocks `pipx.exe` or `poetry.exe`, install and run Poetry as a module:
  `py -3.13 -m pip install --user poetry`, then `py -3.13 -m poetry install --with dashboard`.
- In PowerShell, quote comma lists: `--only "main,dashboard"`.
- The PySpark tests (`tests/test_gold.py`) need Spark with Hadoop binaries and fail on plain Windows; the Gold
  code runs inside the Airflow containers.

Tests: `poetry run pytest` (the dashboard figure test runs when the `dashboard` group is installed).

Deliverables: [report](workshop_4/Workshop4_Report.pdf), apps in `dashboard/`, thresholds in
`config/governance_thresholds.yaml`, Gold fix in `src/gold/storytelling.py`, screenshots in `workshop_4/evidence/`.

