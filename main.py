#!/usr/bin/env python3
"""
NYC Yellow Taxi — filter trips above the 90th percentile by distance.

Percentile is computed per file (per month). Trips are included when
trip_distance is STRICTLY greater than the file's own P90 threshold.

Usage examples
--------------
  # Download the latest available monthly file and process it:
  python main.py

  # Specific month:
  python main.py --year 2024 --month 06

  # All months in a year:
  python main.py --year 2024

  # Skip download and (re-)process files already in data/raw/:
  python main.py --skip-download

  # Enable DEBUG-level output on the terminal:
  python main.py --verbose
"""
import argparse
import logging
import sys
from pathlib import Path

from src.utils.logger import setup_logging

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Build and return the CLI argument parser."""
    p = argparse.ArgumentParser(
        description=(
            "Filter NYC Yellow Taxi trips above the per-file "
            "90th percentile in trip distance."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--year",  type=int, help="Year to download (e.g. 2024). Omit for latest.")
    p.add_argument(
        "--month", type=int,
        help="Month 1-12. Requires --year. Omit to download all months for that year.",
    )
    p.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step; process parquet files already in data/raw/.",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show DEBUG-level messages on the terminal (always written to logs/pipeline.log).",
    )
    return p.parse_args()


def main() -> None:
    """Entry point: configure logging, download, process, and report."""
    args = parse_args()
    log_file = setup_logging(
        console_level=logging.DEBUG if args.verbose else logging.INFO
    )
    logger.info(
        "Run started | year=%s month=%s skip_download=%s | log=%s",
        args.year, args.month, args.skip_download, log_file.name,
    )

    if args.month and not args.year:
        logger.error("--month requires --year.")
        sys.exit(1)

    try:
        _run(args)
        logger.info("Run complete. Log: %s", log_file)
    except SystemExit:
        raise
    except Exception:
        logger.exception("Unexpected error. Full traceback written to %s", log_file)
        sys.exit(1)


def _run(args: argparse.Namespace) -> None:
    """Core pipeline: download -> process -> report."""
    # -- Download -------------------------------------------------------------
    if args.skip_download:
        raw_dir = Path("data/raw")
        files = sorted(raw_dir.glob("*.parquet"))
        if not files:
            logger.error("No parquet files in data/raw/. Run without --skip-download first.")
            sys.exit(1)
        logger.info("Using %d existing file(s) from data/raw/", len(files))
    else:
        from src.downloader import download
        files = download(year=args.year, month=args.month)

    # -- Process --------------------------------------------------------------
    from src.processor import process
    summary = process(files)

    # -- Report ---------------------------------------------------------------
    totals = summary["totals"]
    sep = "=" * 60

    logger.info(sep)
    logger.info("  Results (per-file P90, trip_distance > threshold)")
    logger.info(sep)

    for r in summary["per_file"]:
        logger.info("  %s", r["file"])
        logger.info("    Valid trips      : %d", r["total_valid_trips"])
        logger.info("    P90 threshold    : %.4f miles", r["p90_threshold_miles"])
        logger.info(
            "    Trips above P90  : %d  (%.4f%%)",
            r["filtered_trips"], r["share_above_p90_pct"],
        )

    if totals["input_files"] > 1:
        logger.info("  TOTAL across %d files", totals["input_files"])
        logger.info("    Trips analyzed   : %d", totals["total_valid_trips"])
        logger.info(
            "    Trips above P90  : %d  (%.4f%%)",
            totals["filtered_trips"], totals["share_above_p90_pct"],
        )

    logger.info("  Outputs: data/output/filtered_trips.parquet  |  data/output/summary.json")
    logger.info(sep)


if __name__ == "__main__":
    main()
