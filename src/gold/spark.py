"""Spark session, Silver readers and the Gold writer shared by the governance and storytelling summaries.

Gold files use one long ("tidy") layout, so a single Parquet file can hold every KPI or aggregation:

    section   name of the KPI or aggregation, for example null_rate or alert_severity_daily
    topic     Silver topic the row describes (weather_alerts, station_observations, ...)
    period    day or month the value refers to, null when it covers the whole data set
    dim_1     first breakdown (column name, station, severity, keyword, ...)
    dim_2     second breakdown (element, event type, ...)
    metric    what the value measures (records, pct, mean, p90, ...)
    value     the number
    label     a text result (NORMAL / EXTREME class, timestamp, list of alert events)
    computed_at  moment of the Gold run

The dashboard filters by section and pivots the metrics it needs.
"""
import logging
import os
import shutil
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src import settings
from src.storage.silver import NORMALIZERS

log = logging.getLogger(__name__)

TOPICS = {module.TOPIC: module for module in NORMALIZERS.values()}
GOLD_SCHEMA = T.StructType([
    T.StructField("section", T.StringType(), False),
    T.StructField("topic", T.StringType(), True),
    T.StructField("period", T.DateType(), True),
    T.StructField("dim_1", T.StringType(), True),
    T.StructField("dim_2", T.StringType(), True),
    T.StructField("metric", T.StringType(), False),
    T.StructField("value", T.DoubleType(), True),
    T.StructField("label", T.StringType(), True),
])


def get_spark(app_name):
    """Spark in local mode: driver and executors live in the task process, one thread per CPU core.

    Memory: 2 GB for the driver is enough for the Silver data (tens of thousands of rows) and leaves room for
    the Airflow services in the Docker VM (about 8 GB). Shuffle partitions go down from 200 to 8 because the
    data is small and 200 tiny tasks would cost more than the work itself. The session time zone is UTC,
    like every timestamp in Silver. The web UI is off: nobody can reach it inside the container.
    """
    # Python workers use the same interpreter as the driver (on Windows "python" may be the Store alias).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder
             .master("local[*]")
             .appName(app_name)
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             # The progress bar goes to stderr, which Airflow shows as ERROR lines in the task log.
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def silver_files(topic):
    return sorted(settings.SILVER_DIR.glob(f"{topic}_*.parquet"))


def read_topic_raw(spark, topic) -> DataFrame:
    """All the Silver files of one topic as a single DataFrame, plus the name of the file of each row.

    This is the folder read pattern of a cloud data lake: spark.read.parquet("datalake_silver/"), as one would
    read s3://bucket/silver/. The folder holds four topics with different schemas, and Spark would take the
    schema of one file and drop or null the columns of the others, so pathGlobFilter keeps the files of one
    topic. (A glob inside the path also works, but Spark then logs a FileNotFoundException stack trace while
    it looks for a _spark_metadata folder.)
    """
    return (spark.read.option("pathGlobFilter", f"{topic}_*.parquet").parquet(str(settings.SILVER_DIR))
            .withColumn("silver_file", F.regexp_extract(F.input_file_name(), r"([^/]+\.parquet)$", 1)))


def latest_version(df, key) -> DataFrame:
    """Keep one row per key: the version ingested last.

    Silver keeps every version of a record whose content changed (and, before the Workshop 3 hash fix, a full
    copy of Storm Events for every NOAA compilation). Gold always works on the latest version.
    """
    order = Window.partitionBy(*key).orderBy(F.col("ingested_at").desc(), F.col("processed_at").desc(),
                                             F.col("silver_file").desc())
    return df.withColumn("_rank", F.row_number().over(order)).filter("_rank = 1").drop("_rank")


def read_topic(spark, topic) -> DataFrame:
    return latest_version(read_topic_raw(spark, topic), TOPICS[topic].KEY)


def rows(df, section, topic=None, period=None, dim_1=None, dim_2=None, metric=None, value=None, label=None):
    """Select the columns of a DataFrame into the Gold layout. Arguments are column expressions or literals."""
    def column(item, cast):
        if item is None:
            return F.lit(None).cast(cast)
        if isinstance(item, str):
            return F.col(item).cast(cast) if item in df.columns else F.lit(item).cast(cast)
        return item.cast(cast)

    return df.select(
        F.lit(section).alias("section"),
        column(topic, "string").alias("topic"),
        column(period, "date").alias("period"),
        column(dim_1, "string").alias("dim_1"),
        column(dim_2, "string").alias("dim_2"),
        column(metric, "string").alias("metric"),
        column(value, "double").alias("value"),
        column(label, "string").alias("label"),
    )


def union_all(parts):
    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part)
    return result


def write_gold(df, name, stamp):
    """Write one Parquet file datalake_gold/<name>_<stamp>.parquet.

    Spark always writes a folder of part files. We write one part (coalesce(1), the summaries are small) to a
    hidden temporary folder and move it to the final name, so the dashboard never sees a half-written file.
    """
    settings.GOLD_DIR.mkdir(parents=True, exist_ok=True)
    computed_at = datetime.strptime(stamp, settings.STAMP_FORMAT).replace(tzinfo=timezone.utc)
    df = df.withColumn("computed_at", F.lit(computed_at).cast("timestamp"))
    temporary = settings.GOLD_DIR / f".{name}_{stamp}"
    final = settings.GOLD_DIR / f"{name}_{stamp}.parquet"
    df.orderBy("section", "topic", "period", "dim_1", "dim_2", "metric").coalesce(1) \
        .write.mode("overwrite").parquet(str(temporary))
    part = next(temporary.glob("part-*.parquet"))
    os.replace(part, final)
    shutil.rmtree(temporary, ignore_errors=True)
    log.info("Wrote %s", final.name)
    return str(final)
