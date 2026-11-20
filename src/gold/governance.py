"""Governance summary: data quality KPIs of the Silver layer, computed with PySpark.

Each KPI answers a finding of the Workshop 3 data quality review (notebooks/03_data_quality_findings.ipynb):

    volume_daily / volume_total   how much each topic received, per day and in total
    duplicate_rate                Storm Events was copied once per NOAA compilation (67 % duplicated rows)
    null_rate                     several F4 fields are always null (wind gust, precipitation)
    schema_compliance             Gold reads all files of a topic together, so every file must share a schema
    outlier_rate                  share of rows flagged by the Silver range, IQR and Z-score checks
    text_length                   alert descriptions and storm narratives feed the keyword analysis
    ingestion_compliance          hourly runs that really reached Silver (Docker was off on some days)
    freshness                     time since the last record of each topic (user story 5)
    station_completeness          days and values present per station and year (user story 6)
    alert_status                  alerts per CAP status and message type (a Test message was found)
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.gold.spark import TOPICS, latest_version, read_topic_raw, rows, silver_files, union_all

OUTLIER_FLAGS = {
    "station_observations": ["temperature_outlier"],
    "daily_summaries": ["tmax_outlier", "tmin_outlier", "prcp_outlier", "tmin_above_tmax"],
    "storm_events": ["damage_outlier"],
}
# Text columns whose non-empty content lists the columns a check marked.
MARK_COLUMNS = {"station_observations": ["out_of_range"], "daily_summaries": ["qc_failed"]}
TEXT_COLUMNS = {"weather_alerts": "description_clean", "storm_events": "narrative_clean"}
STATION_COLUMN = {"station_observations": "icao", "daily_summaries": "icao"}
# Hourly sources: the Bronze DAG runs every hour, so 24 runs per day are expected.
HOURLY_TOPICS = ["station_observations", "weather_alerts"]
METADATA = {"record_hash", "bronze_file", "ingested_at", "processed_at", "silver_file"}
# In these columns an empty text means "no problem found", so they are not part of the null rate.
EMPTY_MEANS_OK = {"out_of_range", "qc_failed"}


def volume(raw, topic):
    """Rows received per UTC day of ingestion and the running total."""
    daily = raw.groupBy(F.to_date("ingested_at").alias("day")).count()
    cumulative = F.sum("count").over(Window.orderBy("day").rowsBetween(Window.unboundedPreceding, 0))
    daily = daily.withColumn("cumulative", cumulative)
    return [rows(daily, "volume_daily", topic, period="day", metric=F.lit("records"), value="count"),
            rows(daily, "volume_daily", topic, period="day", metric=F.lit("records_cumulative"),
                 value="cumulative")]


def totals_and_duplicates(raw, latest, topic):
    """Files, rows, distinct keys and the duplicate rate before and after the Gold de-duplication."""
    key = TOPICS[topic].KEY
    n_files = len(silver_files(topic))
    n_raw = raw.count()
    n_keys = raw.select(*key).distinct().count()
    n_latest = latest.count()
    n_latest_dup = n_latest - latest.select(*key).distinct().count()
    spark = raw.sparkSession
    values = [("silver_files", float(n_files)), ("rows_raw", float(n_raw)), ("distinct_keys", float(n_keys)),
              ("rows_after_dedup", float(n_latest))]
    total = spark.createDataFrame(values, "metric string, value double")
    dup = spark.createDataFrame([
        ("duplicate_rate_before_pct", 100.0 * (n_raw - n_keys) / n_raw if n_raw else 0.0),
        ("duplicate_rate_after_pct", 100.0 * n_latest_dup / n_latest if n_latest else 0.0),
        ("duplicated_rows", float(n_raw - n_keys)),
    ], "metric string, value double")
    return [rows(total, "volume_total", topic, metric="metric", value="value"),
            rows(dup, "duplicate_rate", topic, metric="metric", value="value")]


def null_rate(latest, topic):
    """Percentage of null or empty values per column (empty text and empty lists count as empty)."""
    checks = []
    for field in latest.schema.fields:
        if field.name in METADATA or field.name in EMPTY_MEANS_OK:
            continue
        column = F.col(field.name)
        if isinstance(field.dataType, T.StringType):
            empty = column.isNull() | (F.trim(column) == "")
        elif isinstance(field.dataType, T.ArrayType):
            empty = column.isNull() | (F.size(column) == 0)
        elif isinstance(field.dataType, (T.DoubleType, T.FloatType)):
            empty = column.isNull() | F.isnan(column)
        else:
            empty = column.isNull()
        checks.append(F.round(100 * F.avg(empty.cast("double")), 3).alias(field.name))
    wide = latest.agg(*checks)
    stacked = wide.select(F.explode(F.array(*[
        F.struct(F.lit(c).alias("column"), F.col(c).alias("pct")) for c in wide.columns])).alias("s"))
    return [rows(stacked.select("s.*"), "null_rate", topic, dim_1="column", metric=F.lit("null_pct"),
                 value="pct")]


def schema_compliance(spark, topic, latest):
    """Files whose columns match the target schema of the normalizer, and rows with every required field."""
    expected = list(TOPICS[topic].SCHEMA)
    files = silver_files(topic)
    compliant = sum(1 for path in files if spark.read.parquet(str(path)).columns == expected)
    required = TOPICS[topic].REQUIRED
    complete = F.lit(True)
    for name in required:
        complete = complete & F.col(name).isNotNull()
    row_pct = latest.agg(100 * F.avg(complete.cast("double"))).first()[0]
    result = spark.createDataFrame([
        ("files_compliant_pct", 100.0 * compliant / len(files) if files else 0.0),
        ("files_checked", float(len(files))),
        ("rows_with_required_fields_pct", float(row_pct or 0.0)),
    ], "metric string, value double")
    return [rows(result, "schema_compliance", topic, metric="metric", value="value")]


def outlier_rate(latest, topic):
    """Share of rows flagged by each Silver quality check, for all stations and per station."""
    flags = [(name, F.col(name).cast("double")) for name in OUTLIER_FLAGS.get(topic, [])]
    flags += [(name, (F.coalesce(F.col(name), F.lit("")) != "").cast("double"))
              for name in MARK_COLUMNS.get(topic, [])]
    if not flags:
        return []
    aggregations = [F.round(100 * F.avg(expr), 3).alias(name) for name, expr in flags]
    station = STATION_COLUMN.get(topic)
    grouped = [latest.agg(*aggregations).withColumn("station", F.lit("ALL"))]
    if station:
        grouped.append(latest.groupBy(F.col(station).alias("station")).agg(*aggregations))
    parts = []
    for frame in grouped:
        stacked = frame.select("station", F.explode(F.array(*[
            F.struct(F.lit(name).alias("flag"), F.col(name).alias("pct")) for name, _ in flags])).alias("s"))
        parts.append(rows(stacked.select("station", "s.*"), "outlier_rate", topic, dim_1="flag",
                          dim_2="station", metric=F.lit("flagged_pct"), value="pct"))
    return parts


def text_length(latest, topic):
    """Length in characters of the cleaned text: overall statistics and the daily mean."""
    column = TEXT_COLUMNS.get(topic)
    if not column:
        return []
    length = F.length(F.coalesce(F.col(column), F.lit("")))
    stats = latest.agg(F.mean(length).alias("mean"), F.percentile_approx(length, 0.5).alias("median"),
                       F.min(length).alias("min"), F.max(length).alias("max"),
                       (100 * F.avg((length == 0).cast("double"))).alias("empty_pct"))
    stacked = stats.select(F.explode(F.array(*[
        F.struct(F.lit(c).alias("metric"), F.col(c).cast("double").alias("value")) for c in stats.columns]))
        .alias("s")).select("s.*")
    time_column = "sent_utc" if topic == "weather_alerts" else "begin_utc"
    daily = (latest.groupBy(F.to_date(time_column).alias("day"))
             .agg(F.mean(length).alias("mean_length"), F.count("*").alias("texts")))
    return [rows(stacked, "text_length", topic, dim_1=F.lit(column), metric="metric", value="value"),
            rows(daily, "text_length_daily", topic, period="day", dim_1=F.lit(column),
                 metric=F.lit("mean_length"), value="mean_length")]


def ingestion_compliance(raw, topic):
    """Hourly Bronze runs that reached Silver per UTC day, against the 24 expected.

    The run time is read from the Bronze file name (source_YYYYMMDD_HHMMSS.json). The first and the last day
    only expect the hours between the first run and the last one.
    """
    runs = (raw.select(F.regexp_extract("bronze_file", r"_(\d{8}_\d{2})\d{4}\.json$", 1).alias("hour"))
            .filter("hour != ''").distinct()
            .withColumn("run_hour", F.to_timestamp("hour", "yyyyMMdd_HH"))
            .withColumn("day", F.to_date("run_hour")))
    bounds = runs.agg(F.min("run_hour").alias("first"), F.max("run_hour").alias("last"))
    # Every calendar day between the first and the last run, so a day without any run shows up with 0.
    calendar = bounds.select("first", "last", F.explode(F.sequence(F.to_date("first"), F.to_date("last")))
                             .alias("day"))
    counts = runs.groupBy("day").agg(F.count("*").alias("actual"))
    daily = calendar.join(counts, "day", "left").fillna({"actual": 0})
    expected = (F.when(F.col("day") == F.to_date("first"), 24 - F.hour("first"))
                .when(F.col("day") == F.to_date("last"), F.hour("last") + 1)
                .otherwise(24))
    # If first and last fall on the same day, only the hours between them are expected.
    expected = F.when(F.to_date("first") == F.to_date("last"), F.hour("last") - F.hour("first") + 1) \
        .otherwise(expected)
    daily = daily.withColumn("expected", expected.cast("double")) \
        .withColumn("pct", F.round(100 * F.col("actual") / F.col("expected"), 2))
    return [rows(daily, "ingestion_compliance", topic, period="day", metric=F.lit("runs_actual"), value="actual"),
            rows(daily, "ingestion_compliance", topic, period="day", metric=F.lit("runs_expected"),
                 value="expected"),
            rows(daily, "ingestion_compliance", topic, period="day", metric=F.lit("compliance_pct"), value="pct")]


def freshness(latest, topic, now):
    """Last ingestion time of the topic and hours since then (user story 5)."""
    last = latest.agg(F.max("ingested_at").alias("last"))
    last = last.withColumn("hours", (F.lit(now).cast("timestamp").cast("long") - F.col("last").cast("long")) / 3600)
    return [rows(last, "freshness", topic, metric=F.lit("hours_since_last_ingestion"), value="hours",
                 label=F.date_format("last", "yyyy-MM-dd'T'HH:mm:ss'Z'"))]


def station_completeness(latest):
    """Per station and year: share of calendar days present and of null values per element (user story 6)."""
    data = latest.withColumn("year", F.year("date"))
    days_in_year = F.when(F.year(F.current_date()) == F.col("year"), F.dayofyear(F.current_date())) \
        .otherwise(F.dayofyear(F.make_date("year", F.lit(12), F.lit(31))))
    grouped = data.groupBy("icao", "year").agg(
        F.countDistinct("date").alias("days"),
        *[(100 * F.avg(F.col(c).isNull().cast("double"))).alias(c) for c in ["tmax_c", "tmin_c", "prcp_mm"]])
    grouped = grouped.withColumn("days_present_pct", F.round(100 * F.col("days") / days_in_year, 2)) \
        .withColumn("period", F.make_date("year", F.lit(1), F.lit(1)))
    stacked = grouped.select("icao", "period", F.explode(F.array(
        F.struct(F.lit("days_present_pct").alias("metric"), F.col("days_present_pct").alias("value")),
        F.struct(F.lit("tmax_null_pct").alias("metric"), F.col("tmax_c").alias("value")),
        F.struct(F.lit("tmin_null_pct").alias("metric"), F.col("tmin_c").alias("value")),
        F.struct(F.lit("prcp_null_pct").alias("metric"), F.col("prcp_mm").alias("value")),
    )).alias("s")).select("icao", "period", "s.*")
    return [rows(stacked, "station_completeness", "daily_summaries", period="period", dim_1="icao",
                 metric="metric", value="value")]


def alert_status(latest):
    """Alerts per CAP status and message type. A TEST Tsunami Warning reached the Texas feed on 2026-11-17."""
    counts = latest.groupBy("status", "message_type").count()
    total = latest.count()
    counts = counts.withColumn("pct", F.round(100 * F.col("count") / F.lit(total), 2))
    return [rows(counts, "alert_status", "weather_alerts", dim_1="status", dim_2="message_type",
                 metric=F.lit("alerts"), value="count"),
            rows(counts, "alert_status", "weather_alerts", dim_1="status", dim_2="message_type",
                 metric=F.lit("alerts_pct"), value="pct")]


def build(spark, now):
    """Compute every governance KPI and return one DataFrame in the Gold layout."""
    parts = []
    for topic, module in TOPICS.items():
        if not silver_files(topic):
            continue
        raw = read_topic_raw(spark, topic).cache()
        latest = latest_version(raw, module.KEY).cache()
        parts += volume(raw, topic)
        parts += totals_and_duplicates(raw, latest, topic)
        parts += null_rate(latest, topic)
        parts += schema_compliance(spark, topic, latest)
        parts += outlier_rate(latest, topic)
        parts += text_length(latest, topic)
        parts += freshness(latest, topic, now)
        if topic in HOURLY_TOPICS:
            parts += ingestion_compliance(raw, topic)
        if topic == "daily_summaries":
            parts += station_completeness(latest)
        if topic == "weather_alerts":
            parts += alert_status(latest)
    return union_all(parts)
