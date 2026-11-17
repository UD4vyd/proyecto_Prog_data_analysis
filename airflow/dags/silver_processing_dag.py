"""Silver processing DAG.

A FileSensor waits for a new JSON file in datalake_bronze/. When one arrives, the pending Bronze files
are cleaned and saved as Parquet in datalake_silver/, named topic_YYYYMMDD_HHMMSS.parquet. Processed
Bronze files are moved to datalake_bronze/processed/, so the sensor only reacts to new files.

    wait_for_bronze_file -> list_pending -> process (one task per source) -> check -> archive_bronze
"""
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import dag, task

from src import pipeline
from src.storage import silver

log = logging.getLogger(__name__)

BRONZE_GLOB = os.path.join(os.environ.get("BRONZE_DIR", "/opt/airflow/datalake_bronze"), "*.json")


@dag(
    dag_id="silver_processing",
    description="Clean new Bronze JSON files and save them as Parquet in datalake_silver/",
    schedule="@continuous",                  # a new run starts waiting as soon as the previous one ends
    start_date=datetime(2026, 11, 16, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,                       # required by @continuous, and avoids two runs on the same files
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["silver", "workshop-2"],
)
def silver_processing():

    wait_for_bronze_file = FileSensor(
        task_id="wait_for_bronze_file",
        fs_conn_id="fs_default",
        filepath=BRONZE_GLOB,                # only the top folder: staging and processed files do not match
        poke_interval=30,                    # look every 30 seconds
        timeout=60 * 60,                     # give up after one hour, the period of the Bronze DAG
        soft_fail=True,                      # no file in one hour is not an error: the run is skipped
        deferrable=True,                     # the wait runs in the triggerer and does not hold a worker slot
        retries=0,
    )

    @task
    def list_pending():
        """Bronze files waiting to be processed, grouped by source, and the timestamp of this processing run.

        The timestamp is taken once here and shared through XCom, so every Parquet file of the run has the
        same one and a retried task writes the same file name again.
        """
        files = silver.pending_files()
        for source, paths in files.items():
            log.info("%s: %s pending files", source, len(paths))
        return {"stamp": pipeline.run_stamp(datetime.now(timezone.utc)), "files": files}

    @task
    def process(source: str, pending: dict):
        """Apply the preprocessing pipeline to the files of one source and write one Parquet file."""
        paths = pending["files"].get(source, [])
        if not paths:
            raise AirflowSkipException(f"{source}: no pending Bronze files")
        return silver.process_source(source, paths, pending["stamp"])

    @task
    def check(summary: dict):
        """Read the Parquet file back and confirm the schema, the required fields and the unique key."""
        if summary["silver_file"]:
            summary["rows_checked"] = silver.check_silver_file(summary["silver_file"], summary["source"])
        return summary

    @task(trigger_rule="none_failed_min_one_success")
    def archive_bronze(pending: dict):
        """Move the processed Bronze files out of the watched folder. Runs only if no source failed."""
        return silver.archive([path for paths in pending["files"].values() for path in paths])

    pending = list_pending()
    wait_for_bronze_file >> pending
    checks = []
    for source in pipeline.SOURCES:
        summary = process.override(task_id=f"process_{source}")(source, pending)
        checks.append(check.override(task_id=f"check_{source}")(summary))
    checks >> archive_bronze(pending)


silver_processing()
