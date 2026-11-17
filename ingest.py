"""Command-line entry point of the pipeline. It runs the same code as the Airflow DAGs.

Examples:
    python ingest.py --mode status
    python ingest.py --mode incremental
    python ingest.py --mode historical --source ghcn_daily --start-year 2024 --end-year 2025
    python ingest.py --mode historical --source storm_events --start-year 2023 --end-year 2025

Inside Docker: docker compose exec airflow-scheduler python /opt/airflow/project/ingest.py --mode status

By default a load writes Bronze and then Silver. With --skip-silver it only writes Bronze and the Silver DAG
does the rest. Use --skip-silver while Airflow is running, so the two do not process the same file.
"""
import argparse
import json
import logging
from datetime import datetime, timezone

from src import pipeline, settings
from src.storage import bronze, silver

log = logging.getLogger("ingest")


def _to_bronze(staged):
    """Validate and persist the staged files. None means the source had nothing new."""
    return [bronze.persist_file(bronze.validate_file(path)) for path in staged if path]


def _to_silver(stamp):
    pending = silver.pending_files()
    for source, paths in pending.items():
        summary = silver.process_source(source, paths, stamp)
        if summary["silver_file"]:
            silver.check_silver_file(summary["silver_file"], source)
    silver.archive([path for paths in pending.values() for path in paths])


def historical(args, now, stamp):
    if args.source == "ghcn_daily":
        staged = [pipeline.extract_daily_history(stamp, args.start_year, args.end_year)]
    elif args.source == "storm_events":
        years = list(range(args.start_year, args.end_year + 1))
        staged = [pipeline.extract_storm_events(stamp, years=years, mode="historical")]
    else:
        raise SystemExit("Historical mode needs --source ghcn_daily or storm_events. "
                         "The NWS API (alerts, observations) only keeps about 7 days.")
    log.info("Bronze files written: %s", _to_bronze(staged))
    if not args.skip_silver:
        _to_silver(stamp)


def incremental(args, now, stamp):
    staged = [pipeline.extract_alerts(stamp), pipeline.extract_observations(stamp, now),
              pipeline.extract_daily(stamp, now), pipeline.extract_storm_events(stamp)]
    log.info("Bronze files written: %s", _to_bronze(staged))
    if not args.skip_silver:
        _to_silver(stamp)


def status():
    print("Watermarks:")
    print(json.dumps({s: bronze.read_state(s) for s in pipeline.SOURCES}, indent=1, sort_keys=True))
    pending = sum(len(paths) for paths in silver.pending_files().values())
    processed = len(list(settings.PROCESSED_DIR.glob("*.json")))
    print(f"Bronze files: {pending} pending, {processed} processed")
    for module in silver.NORMALIZERS.values():
        files = sorted(settings.SILVER_DIR.glob(f"{module.TOPIC}_*.parquet"))
        latest = files[-1].name if files else "none"
        print(f"Silver {module.TOPIC}: {len(files)} files, latest {latest}")


def main():
    parser = argparse.ArgumentParser(description="Extreme weather monitoring (Texas) ingestion")
    parser.add_argument("--mode", choices=["historical", "incremental", "status"], required=True)
    parser.add_argument("--source", choices=pipeline.SOURCES, help="source of the historical load")
    parser.add_argument("--start-year", type=int, help="first year of the historical load")
    parser.add_argument("--end-year", type=int, help="last year of the historical load")
    parser.add_argument("--skip-silver", action="store_true", help="write Bronze only, the Silver DAG processes it")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    now = datetime.now(timezone.utc)
    stamp = pipeline.run_stamp(now)
    if args.mode == "status":
        status()
    elif args.mode == "incremental":
        incremental(args, now, stamp)
    else:
        if not (args.start_year and args.end_year):
            parser.error("--mode historical needs --start-year and --end-year")
        historical(args, now, stamp)


if __name__ == "__main__":
    main()
