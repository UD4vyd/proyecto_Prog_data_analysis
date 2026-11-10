"""Command-line entry point of the Extreme Weather Monitoring pipeline (Team 5).

    python ingest.py --mode historical [--start 2010-01-01 --end 2025-12-31]
    python ingest.py --mode incremental
    python ingest.py --mode status

Workshop 1: STUB. The contract (arguments, behaviour, exit codes) is fixed here; the
implementation arrives in Workshop 2 (historical) and Workshop 3 (incremental).
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
log = logging.getLogger("ingest")


def load_config() -> dict:
    return yaml.safe_load((ROOT / "config" / "sources.yaml").read_text(encoding="utf-8"))


def run_historical(cfg: dict, start: date, end: date) -> int:
    for key, src in cfg["sources"].items():
        if src["role"] == "historical":
            log.info("[stub] historical load %s %s..%s, partition: %s", key, start, end, src["partition"])
    return 0


def run_incremental(cfg: dict) -> int:
    for key, src in cfg["sources"].items():
        if src["role"] == "incremental":
            log.info("[stub] incremental poll %s, watermark field: %s", key, src["watermark_field"])
    return 0


def run_status(cfg: dict) -> int:
    print(f"{'source':20} {'role':12} {'schedule':30} watermark")
    for key, src in cfg["sources"].items():
        print(f"{key:20} {src['role']:12} {src['schedule'][:30]:30} (no database yet)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extreme Weather Monitoring - ingestion CLI")
    parser.add_argument("--mode", required=True, choices=["historical", "incremental", "status"])
    parser.add_argument("--start", type=date.fromisoformat, default=None)
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = load_config()
    if args.mode == "historical":
        window = cfg["project"]["historical_window"]
        start = args.start or date.fromisoformat(window["start"])
        end = args.end or date.today()
        return run_historical(cfg, start, end)
    if args.mode == "incremental":
        return run_incremental(cfg)
    return run_status(cfg)


if __name__ == "__main__":
    sys.exit(main())
