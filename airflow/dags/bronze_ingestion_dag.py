"""Bronze ingestion DAG.

Every hour it asks the four sources and saves each answer as one raw JSON file in datalake_bronze/,
named source_YYYYMMDD_HHMMSS.json. Each source follows the same three steps:

    extract (to a staging folder) -> validate (basic schema check) -> persist (move to Bronze)

Only file paths travel between tasks (XCom), never the data.
"""
import logging
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, get_current_context, task

from src import pipeline
from src.storage import bronze

log = logging.getLogger(__name__)


def _run_moment():
    """Moment the run was scheduled for. Using it in file names makes a re-run overwrite its own file."""
    dag_run = get_current_context()["dag_run"]
    return (dag_run.logical_date or dag_run.run_after).astimezone(timezone.utc)


def _staged_or_skip(path, source):
    if path is None:
        raise AirflowSkipException(f"{source}: nothing new to ingest in this run")
    return path


@dag(
    dag_id="bronze_ingestion",
    description="Extract F1-F4 and save raw JSON files in datalake_bronze/",
    schedule="@hourly",                      # F4 is polled every hour (ingestion contract of Workshop 1)
    start_date=datetime(2026, 11, 16, tzinfo=timezone.utc),
    catchup=False,                           # the APIs only return current data, old runs cannot be rebuilt
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["bronze", "workshop-2"],
)
def bronze_ingestion():

    @task
    def extract_nws_alerts():
        """API extraction, F3: alerts of the last 7 days for Texas."""
        moment = _run_moment()
        return pipeline.extract_alerts(pipeline.run_stamp(moment))

    @task
    def extract_nws_observations():
        """API extraction, F4: station observations since the watermark of each station."""
        moment = _run_moment()
        return pipeline.extract_observations(pipeline.run_stamp(moment), moment)

    @task
    def extract_ghcn_daily():
        """API extraction, F2: daily summaries of the last 30 days, once per day."""
        moment = _run_moment()
        return _staged_or_skip(pipeline.extract_daily(pipeline.run_stamp(moment), moment), "ghcn_daily")

    @task
    def extract_storm_events():
        """Web scraping extraction, F1: read the HTML listing and download a year file if it is new."""
        moment = _run_moment()
        return _staged_or_skip(pipeline.extract_storm_events(pipeline.run_stamp(moment)), "storm_events")

    @task
    def validate(path: str):
        """Basic schema check. A file that fails stays in staging and never reaches Bronze."""
        try:
            return bronze.validate_file(path)
        except ValueError:
            log.exception("Validation failed for %s", path)
            raise

    @task
    def persist(path: str):
        """Move the validated file to datalake_bronze/ and advance the watermark of its source."""
        return bronze.persist_file(path)

    extractions = {
        "nws_alerts": extract_nws_alerts(),
        "nws_observations": extract_nws_observations(),
        "ghcn_daily": extract_ghcn_daily(),
        "storm_events": extract_storm_events(),
    }
    for source, staged in extractions.items():
        checked = validate.override(task_id=f"validate_{source}")(staged)
        persist.override(task_id=f"persist_{source}")(checked)


bronze_ingestion()
