"""Check of a Gold file without Spark: pyarrow reads it back as the dashboard will."""
import logging
import os

import pyarrow.parquet as pq

log = logging.getLogger(__name__)

COLUMNS = ["section", "topic", "period", "dim_1", "dim_2", "metric", "value", "label", "computed_at"]
SECTIONS = {
    "governance": {"volume_daily", "volume_total", "duplicate_rate", "null_rate", "schema_compliance",
                   "outlier_rate", "text_length", "text_length_daily", "ingestion_compliance", "freshness",
                   "station_completeness", "alert_status"},
    "storytelling": {"station_day_class", "class_distribution", "current_situation", "alert_severity_daily",
                     "alert_event_breakdown", "top_keywords", "keyword_severity", "source_comparison", "source_severity",
                     "storm_impact_by_type", "storm_monthly_volume", "extreme_vs_storms"},
}


def check_gold_file(path, kind):
    """Return {section: rows}. Raises ValueError if a column or an expected section is missing."""
    table = pq.read_table(path)
    if table.column_names != COLUMNS:
        raise ValueError(f"{path}: columns {table.column_names} differ from {COLUMNS}")
    counts = table.group_by("section").aggregate([("section", "count")]).to_pydict()
    per_section = dict(zip(counts["section"], counts["section_count"]))
    missing = SECTIONS[kind] - set(per_section)
    if missing:
        raise ValueError(f"{path}: sections without rows {sorted(missing)}")
    log.info("Checked %s: %s rows in %s sections", os.path.basename(path), table.num_rows, len(per_section))
    return per_section
