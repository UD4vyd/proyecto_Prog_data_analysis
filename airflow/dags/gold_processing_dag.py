"""Gold processing DAG.

Once a day PySpark reads every Silver Parquet file of each topic as one DataFrame and writes two summaries
to datalake_gold/, the only data source of the Workshop 4 dashboards:

    governance_YYYYMMDD_HHMMSS.parquet     data quality KPIs (src/gold/governance.py)
    storytelling_YYYYMMDD_HHMMSS.parquet   analytical aggregations (src/gold/storytelling.py)

    check_silver -> build_governance -> build_storytelling -> check_gold

The two build tasks run one after the other on purpose: each one starts its own Spark driver with 2 GB, and
the Docker VM also has to keep the Airflow services and the hourly Bronze and Silver runs alive.
PySpark is imported inside the tasks, so the DAG processor can parse this file without starting a JVM.
"""
import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task

log = logging.getLogger(__name__)


@dag(
    dag_id="gold_processing",
    description="Aggregate all Silver Parquet files with PySpark into the governance and storytelling summaries",
    # 06:15 UTC is 00:15 in Texas (CST): the local day that just ended is complete in Silver.
    schedule="15 6 * * *",
    start_date=datetime(2026, 11, 20, tzinfo=timezone.utc),
    catchup=False,                           # each run reads the whole Silver layer, old runs add nothing
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["gold", "pyspark", "workshop-3"],
)
def gold_processing():

    @task
    def check_silver():
        """Count the Silver files of each topic and fix the timestamp shared by both Gold files of the run."""
        from src import pipeline
        from src.gold.spark import TOPICS, silver_files

        counts = {topic: len(silver_files(topic)) for topic in TOPICS}
        log.info("Silver files per topic: %s", counts)
        missing = [topic for topic, n in counts.items() if n == 0]
        if missing:
            raise ValueError(f"No Silver files for {missing}: run the Bronze and Silver DAGs first")
        run_id = get_current_context()["dag_run"].run_id
        return {"stamp": pipeline.run_stamp(datetime.now(timezone.utc)), "run_id": run_id, "silver_files": counts}

    @task
    def build_governance(run: dict):
        """Data quality KPIs per topic: volume, duplicates, nulls, schema, outliers, text, ingestion, freshness."""
        from src.gold import governance
        from src.gold.spark import get_spark, write_gold

        spark = get_spark("gold_governance")
        try:
            now = datetime.strptime(run["stamp"], "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
            return write_gold(governance.build(spark, now), "governance", run["stamp"])
        finally:
            spark.stop()

    @task
    def build_storytelling(run: dict):
        """Indicator classes, alert severity, keywords, storm impacts and the current situation."""
        from src.gold import storytelling
        from src.gold.spark import get_spark, write_gold

        spark = get_spark("gold_storytelling")
        try:
            now = datetime.strptime(run["stamp"], "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
            return write_gold(storytelling.build(spark, now), "storytelling", run["stamp"])
        finally:
            spark.stop()

    @task
    def check_gold(governance_path: str, storytelling_path: str):
        """Read both files back and confirm the layout and that every expected section has rows."""
        from src.gold.check import check_gold_file

        return {"governance": check_gold_file(governance_path, "governance"),
                "storytelling": check_gold_file(storytelling_path, "storytelling")}

    run = check_silver()
    governance_path = build_governance(run)
    storytelling_path = build_storytelling(run)
    governance_path >> storytelling_path
    check_gold(governance_path, storytelling_path)


gold_processing()
