"""Storytelling summary: the aggregations behind the functional user dashboard, computed with PySpark.

Narrative: "How unusual is the weather at our Texas stations compared with their own 1991-2020 history,
when does the National Weather Service warn about it, and do our EXTREME days match real storm impacts?"

The course template speaks of sentiment, because most teams analyse opinions. Our texts are official
bulletins with no opinion, so the role of sentiment is taken by two ordinal scores:
    - the station-day class NORMAL / UNUSUAL / EXTREME of the percentile indicator (Workshop 1, section 12)
    - the CAP severity of each alert: Minor 1, Moderate 2, Severe 3, Extreme 4

Sections and the user story (Workshop 1, section 11) each one serves:
    station_day_class        US3  value of each recent day, its class and the baseline percentiles
    class_distribution       US3  days per class, station, element and month
    current_situation        US2, US4  latest observation per station and the active alerts on its zone
    alert_severity_daily     US1  alerts per local day and severity, and the mean severity score
    alert_event_breakdown    US1  alerts and mean severity per event type
    top_keywords             US1  most frequent words and bigrams of alert texts and storm narratives
    keyword_severity         US1  mean severity of the alerts that contain each top keyword
    source_comparison        US5  records and covered period per source (API vs web scraping)
    storm_impact_by_type     US7  events, casualties and damage per Storm Events type
    storm_monthly_volume     US7  storm reports per month and type
    extreme_vs_storms        US7  share of station-days with a storm report in the same county, per class
"""
from datetime import date

from pyspark.ml.feature import NGram
from pyspark.sql import Window
from pyspark.sql import functions as F

from src import settings
from src.gold.spark import read_topic, rows, union_all
from src.normalization import text

BASELINE = (1991, 2020)          # WMO climate normal period used by the indicator
CLASSIFY_FROM = date(2021, 1, 1)  # days after the baseline that get a class (the five years of Storm Events)
WINDOW_DAYS = 7                   # +/- days around each day of year (15 days x 30 years, about 450 values)
LOCAL_TZ = "America/Chicago"      # the three stations are in Central time
SEVERITY_SCORE = {"Minor": 1, "Moderate": 2, "Severe": 3, "Extreme": 4}
TOP_WORDS, TOP_BIGRAMS = 25, 15


def _severity_score():
    mapping = F.create_map(*[x for k, v in SEVERITY_SCORE.items() for x in (F.lit(k), F.lit(v))])
    return mapping[F.col("severity")].cast("double")


def _issued(alerts):
    """Real alerts only. Cancel messages are not new alerts, and NWS also sends Test messages (a TEST Tsunami
    Warning reached our Texas feed on 2026-11-17), which must not count as weather."""
    return alerts.filter((F.col("message_type") != "Cancel") & (F.col("status") == "Actual"))


def _day_index(column):
    """Day of year from 1 to 365. In leap years 29 February shares the index of 28 February."""
    doy = F.dayofyear(column)
    # In a leap year day 60 is 29 February and day 61 is 1 March, so from day 60 on we subtract one.
    leap_from_feb_29 = (F.dayofmonth(F.last_day(F.make_date(F.year(column), F.lit(2), F.lit(1)))) == 29) & \
        (doy >= 60)
    return F.when(leap_from_feb_29, doy - 1).otherwise(doy)


def daily_long(daily):
    """One row per station, day and element (TMAX, TMIN, PRCP)."""
    return daily.select("icao", F.col("date").cast("date").alias("date"), F.explode(F.array(
        F.struct(F.lit("TMAX").alias("element"), F.col("tmax_c").alias("value")),
        F.struct(F.lit("TMIN").alias("element"), F.col("tmin_c").alias("value")),
        F.struct(F.lit("PRCP").alias("element"), F.col("prcp_mm").alias("value")),
    )).alias("e")).select("icao", "date", "e.*").filter(F.col("value").isNotNull())


def baseline_percentiles(long):
    """p1, p10, p90 and p99 per station, element and day of year, from the +/-7 day window of 1991-2020.

    Each baseline value is copied to the 15 target days it belongs to (explode of the offsets) and the
    percentiles are computed per target day with percentile_approx.
    """
    base = long.filter(F.year("date").between(*BASELINE)).withColumn("doy", _day_index(F.col("date")))
    offsets = F.array(*[F.lit(o) for o in range(-WINDOW_DAYS, WINDOW_DAYS + 1)])
    spread = base.withColumn("offset", F.explode(offsets)) \
        .withColumn("target_doy", F.pmod(F.col("doy") - 1 + F.col("offset"), 365) + 1)
    pct = F.percentile_approx("value", F.array(F.lit(0.01), F.lit(0.10), F.lit(0.90), F.lit(0.99)), 10000)
    return spread.groupBy("icao", "element", F.col("target_doy").alias("doy")).agg(
        pct.alias("p"), F.count("*").alias("baseline_n"),
        F.countDistinct(F.year("date")).alias("baseline_years"))


def classify(long, percentiles):
    """NORMAL inside p10-p90, UNUSUAL outside, EXTREME outside p1-p99. PRCP only has the high side."""
    recent = long.filter(F.col("date") >= F.lit(CLASSIFY_FROM)).withColumn("doy", _day_index(F.col("date")))
    joined = recent.join(percentiles, ["icao", "element", "doy"], "left") \
        .select("*", F.col("p")[0].alias("p01"), F.col("p")[1].alias("p10"),
                F.col("p")[2].alias("p90"), F.col("p")[3].alias("p99"))
    high_only = F.col("element") == "PRCP"
    label = (F.when(F.col("p10").isNull(), "NO_BASELINE")
             .when(F.col("value") > F.col("p99"), "EXTREME_HIGH")
             .when(~high_only & (F.col("value") < F.col("p01")), "EXTREME_LOW")
             .when(F.col("value") > F.col("p90"), "UNUSUAL_HIGH")
             .when(~high_only & (F.col("value") < F.col("p10")), "UNUSUAL_LOW")
             .otherwise("NORMAL"))
    return joined.withColumn("class", label)


def station_day_rows(classified):
    parts = [rows(classified, "station_day_class", "daily_summaries", period="date", dim_1="icao",
                  dim_2="element", metric=F.lit("observed"), value="value", label="class")]
    for name in ["p01", "p10", "p90", "p99", "baseline_n"]:
        parts.append(rows(classified, "station_day_class", "daily_summaries", period="date", dim_1="icao",
                          dim_2="element", metric=F.lit(name), value=name))
    return parts


def class_distribution(classified):
    """Days per class for each station, element and month, with the share of the month."""
    monthly = classified.withColumn("month", F.trunc("date", "month")) \
        .groupBy("icao", "element", "month", "class").count()
    total = F.sum("count").over(Window.partitionBy("icao", "element", "month"))
    monthly = monthly.withColumn("pct", F.round(100 * F.col("count") / total, 2))
    return [rows(monthly, "class_distribution", "daily_summaries", period="month", dim_1="icao",
                 dim_2="element", metric=F.lit("days"), value="count", label="class"),
            rows(monthly, "class_distribution", "daily_summaries", period="month", dim_1="icao",
                 dim_2="element", metric=F.lit("days_pct"), value="pct", label="class")]


def current_situation(spark, observations, alerts, now):
    """Latest observation of each station and the alerts active now on its forecast zone or county (US2, US4)."""
    latest = observations.withColumn("_rank", F.row_number().over(
        Window.partitionBy("icao").orderBy(F.col("timestamp_utc").desc()))).filter("_rank = 1")
    stations = spark.createDataFrame([(s["icao"], s["forecast_zone"], s["county_zone"], s["name"])
                                      for s in settings.load_stations()],
                                     "icao string, forecast_zone string, county_zone string, name string")
    active = alerts.filter((F.coalesce("ends_utc", "expires_utc") > F.lit(now).cast("timestamp")) &
                           (F.col("message_type") != "Cancel") & (F.col("status") == "Actual")) \
        .select("event", "severity", F.explode("ugc_codes").alias("ugc"))
    on_station = stations.join(active, (active.ugc == stations.forecast_zone) | (active.ugc == stations.county_zone),
                               "left") \
        .groupBy("icao").agg(F.concat_ws("; ", F.array_sort(F.collect_set("event"))).alias("events"),
                             F.max(_severity_score()).alias("max_severity"))
    joined = latest.join(stations, "icao").join(on_station, "icao", "left") \
        .withColumn("local_day", F.to_date(F.from_utc_timestamp("timestamp_utc", LOCAL_TZ)))
    parts = []
    for metric in ["temperature_c", "dewpoint_c", "relative_humidity_pct", "wind_speed_ms", "pressure_hpa",
                   "latitude", "longitude", "max_severity"]:
        parts.append(rows(joined, "current_situation", "station_observations", period="local_day", dim_1="icao",
                          dim_2="name", metric=F.lit(metric), value=metric,
                          label=F.date_format("timestamp_utc", "yyyy-MM-dd'T'HH:mm'Z'")))
    parts.append(rows(joined, "current_situation", "weather_alerts", period="local_day", dim_1="icao", dim_2="name",
                      metric=F.lit("active_alerts"), value=F.size(F.split(F.col("events"), "; ")).cast("double")
                      * (F.col("events") != "").cast("double"), label="events"))
    return parts


def alert_rows(alerts):
    """Alert volume and severity per local day and per event type."""
    issued = _issued(alerts) \
        .withColumn("day", F.to_date(F.from_utc_timestamp("sent_utc", LOCAL_TZ))) \
        .withColumn("score", _severity_score())
    per_severity = issued.groupBy("day", "severity").count()
    per_day = issued.groupBy("day").agg(F.count("*").alias("alerts"), F.mean("score").alias("mean_score"))
    per_event = issued.groupBy("event", "severity").count()
    event_score = issued.groupBy("event").agg(F.count("*").alias("alerts"), F.mean("score").alias("mean_score"))
    return [
        rows(per_severity, "alert_severity_daily", "weather_alerts", period="day", dim_1="severity",
             metric=F.lit("alerts"), value="count"),
        rows(per_day, "alert_severity_daily", "weather_alerts", period="day", dim_1=F.lit("ALL"),
             metric=F.lit("alerts"), value="alerts"),
        rows(per_day, "alert_severity_daily", "weather_alerts", period="day", dim_1=F.lit("ALL"),
             metric=F.lit("mean_severity_score"), value="mean_score"),
        rows(per_event, "alert_event_breakdown", "weather_alerts", dim_1="event", dim_2="severity",
             metric=F.lit("alerts"), value="count"),
        rows(event_score, "alert_event_breakdown", "weather_alerts", dim_1="event", dim_2=F.lit("ALL"),
             metric=F.lit("mean_severity_score"), value="mean_score"),
    ]


def _tokens(df, column):
    """Tokens of a cleaned text with the same stop words and noise words as Silver (src/normalization/text)."""
    drop = F.array(*[F.lit(w) for w in sorted(text.STOP_WORDS | text.NOISE_WORDS)])
    words = F.split(F.trim(F.coalesce(F.col(column), F.lit(""))), r"\s+")
    kept = F.filter(words, lambda w: (F.length(w) > 1) & ~w.rlike(r"^\d+$") & ~F.array_contains(drop, w))
    return df.withColumn("tokens", kept)


def keyword_rows(alerts, storms):
    """Top words and bigrams of the alert descriptions and storm narratives, and keyword-severity association.

    The tokens are computed again here from the cleaned text, so files written before the Workshop 3 text
    improvements get the same treatment as new ones.

    Workshop 4: the keyword-severity association is computed for the same 25 alert words as top_keywords. Before,
    it ranked words by the number of alerts that contain them, a different top 25, so 9 of the words in the
    dashboard keyword chart (rip, high, beach, island, ...) had no severity to colour them.
    """
    parts = []
    alert_words = None
    issued = _tokens(_issued(alerts), "description_clean") \
        .withColumn("score", _severity_score())
    narratives = _tokens(storms, "narrative_clean")
    for topic, frame in [("weather_alerts", issued), ("storm_events", narratives)]:
        words = frame.select(F.explode("tokens").alias("term")).groupBy("term").count() \
            .orderBy(F.desc("count"), "term").limit(TOP_WORDS)
        if topic == "weather_alerts":
            alert_words = words
        bigrams = NGram(n=2, inputCol="tokens", outputCol="bigrams").transform(frame) \
            .select(F.explode("bigrams").alias("term")).groupBy("term").count() \
            .orderBy(F.desc("count"), "term").limit(TOP_BIGRAMS)
        parts += [rows(words, "top_keywords", topic, dim_1="term", dim_2=F.lit("word"), metric=F.lit("count"),
                       value="count"),
                  rows(bigrams, "top_keywords", topic, dim_1="term", dim_2=F.lit("bigram"), metric=F.lit("count"),
                       value="count")]
    # Each alert counts once per keyword, however many times the word appears in it.
    per_alert = issued.select("score", F.explode(F.array_distinct("tokens")).alias("term"))
    association = per_alert.join(alert_words.select("term"), "term").groupBy("term") \
        .agg(F.mean("score").alias("mean_score"), F.count("*").alias("alerts"))
    parts += [rows(association, "keyword_severity", "weather_alerts", dim_1="term",
                   metric=F.lit("mean_severity_score"), value="mean_score"),
              rows(association, "keyword_severity", "weather_alerts", dim_1="term", metric=F.lit("alerts"),
                   value="alerts")]
    return parts


def source_rows(spark, frames):
    """Records and covered period of each source, split by extraction method (API or web scraping)."""
    info = [("weather_alerts", "F3 NWS alerts", "API", "sent_utc"),
            ("station_observations", "F4 NWS observations", "API", "timestamp_utc"),
            ("daily_summaries", "F2 GHCN-Daily", "API", "date"),
            ("storm_events", "F1 Storm Events", "web scraping", "begin_utc")]
    parts = []
    for topic, name, method, column in info:
        summary = frames[topic].agg(F.count("*").alias("records"),
                                    F.min(F.to_date(column)).alias("first"), F.max(F.to_date(column)).alias("last"))
        summary = summary.withColumn("covered", F.concat_ws(" to ", F.col("first").cast("string"),
                                                            F.col("last").cast("string")))
        parts.append(rows(summary, "source_comparison", topic, dim_1=F.lit(name), dim_2=F.lit(method),
                          metric=F.lit("records"), value="records", label="covered"))
    return parts


def storm_rows(storms):
    """Impact per event type and reports per month (Storm Events, local standard time month)."""
    damage = F.coalesce("damage_property_usd", F.lit(0.0)) + F.coalesce("damage_crops_usd", F.lit(0.0))
    impact = storms.groupBy("event_type").agg(
        F.count("*").alias("events"), F.sum("deaths_direct").alias("deaths"),
        F.sum("injuries_direct").alias("injuries"), F.sum(damage).alias("damage_usd"))
    stacked = impact.select("event_type", F.explode(F.array(*[
        F.struct(F.lit(c).alias("metric"), F.col(c).cast("double").alias("value"))
        for c in ["events", "deaths", "injuries", "damage_usd"]])).alias("s")).select("event_type", "s.*")
    monthly = storms.withColumn("month", F.trunc(F.from_utc_timestamp("begin_utc", LOCAL_TZ), "month")) \
        .groupBy("month", "event_type").count()
    return [rows(stacked, "storm_impact_by_type", "storm_events", dim_1="event_type", metric="metric",
                 value="value"),
            rows(monthly, "storm_monthly_volume", "storm_events", period="month", dim_1="event_type",
                 metric=F.lit("events"), value="count")]


def extreme_vs_storms(spark, classified, storms):
    """For each class, share of station-days with a Storm Events report in the station county or zone.

    If EXTREME days have storm reports more often than NORMAL days, the indicator points at real impacts
    (user story 7). Storm reports use the county (cz_type C) or forecast zone (cz_type Z) FIPS number.
    """
    stations = spark.createDataFrame(
        [(s["icao"], int(s["county_zone"][3:]), int(s["forecast_zone"][3:])) for s in settings.load_stations()],
        "icao string, county int, zone int")
    reports = storms.withColumn("day", F.to_date(F.from_utc_timestamp("begin_utc", LOCAL_TZ))) \
        .select("day", "cz_type", "cz_fips")
    matches = stations.join(reports, ((reports.cz_type == "C") & (reports.cz_fips == stations.county)) |
                            ((reports.cz_type == "Z") & (reports.cz_fips == stations.zone))) \
        .groupBy("icao", "day").count().withColumnRenamed("count", "reports")
    years = storms.select(F.year(F.from_utc_timestamp("begin_utc", LOCAL_TZ)).alias("y")).distinct()
    days = classified.withColumn("broad", F.regexp_replace("class", "_(HIGH|LOW)$", "")) \
        .join(years, F.year("date") == years.y) \
        .groupBy("icao", "date", "broad").agg(F.count("*").alias("elements"))
    # A station-day takes its strongest class over the three elements.
    rank = F.create_map(F.lit("NORMAL"), F.lit(0), F.lit("UNUSUAL"), F.lit(1), F.lit("EXTREME"), F.lit(2))
    strongest = days.withColumn("r", rank[F.col("broad")]).groupBy("icao", "date").agg(F.max("r").alias("r")) \
        .withColumn("class", F.element_at(F.array(F.lit("NORMAL"), F.lit("UNUSUAL"), F.lit("EXTREME")),
                                          F.col("r") + 1))
    joined = strongest.join(matches, (strongest.icao == matches.icao) & (strongest.date == matches.day), "left") \
        .select(strongest.icao, "class", (F.coalesce("reports", F.lit(0)) > 0).cast("double").alias("hit"))
    result = joined.groupBy("icao", "class").agg(F.count("*").alias("days"), (100 * F.avg("hit")).alias("pct"))
    overall = joined.groupBy("class").agg(F.count("*").alias("days"), (100 * F.avg("hit")).alias("pct")) \
        .withColumn("icao", F.lit("ALL"))
    result = result.unionByName(overall)
    return [rows(result, "extreme_vs_storms", "storm_events", dim_1="icao", dim_2="class",
                 metric=F.lit("station_days"), value="days"),
            rows(result, "extreme_vs_storms", "storm_events", dim_1="icao", dim_2="class",
                 metric=F.lit("days_with_storm_report_pct"), value="pct")]


def build(spark, now):
    """Compute every storytelling aggregation and return one DataFrame in the Gold layout."""
    frames = {topic: read_topic(spark, topic).cache()
              for topic in ["daily_summaries", "station_observations", "weather_alerts", "storm_events"]}
    classified = classify(daily_long(frames["daily_summaries"]),
                          baseline_percentiles(daily_long(frames["daily_summaries"]))).cache()
    parts = []
    parts += station_day_rows(classified)
    parts += class_distribution(classified)
    parts += current_situation(spark, frames["station_observations"], frames["weather_alerts"], now)
    parts += alert_rows(frames["weather_alerts"])
    parts += keyword_rows(frames["weather_alerts"], frames["storm_events"])
    parts += source_rows(spark, frames)
    parts += storm_rows(frames["storm_events"])
    parts += extreme_vs_storms(spark, classified, frames["storm_events"])
    return union_all(parts)
