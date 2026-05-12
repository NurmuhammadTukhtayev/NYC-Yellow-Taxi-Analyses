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
"""
import argparse
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Build and return the argument parser."""
    p = argparse.ArgumentParser(
        description=(
            "Filter NYC Yellow Taxi trips above the per-file "
            "90th percentile in trip distance."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--year",  type=int, help="Year to download (e.g. 2024). Omit for latest.")
    p.add_argument("--month", type=int, help="Month 1–12. Requires --year. Omit to get all months.")
    p.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step; process parquet files already in data/raw/.",
    )
    return p.parse_args()


def main() -> None:
    """Entry point: download, process, and report."""
    args = parse_args()

    if args.month and not args.year:
        sys.exit("--month requires --year.")

    # ── Download ──────────────────────────────────────────────────────────────
    if args.skip_download:
        raw_dir = Path("data/raw")
        files = sorted(raw_dir.glob("*.parquet"))
        if not files:
            sys.exit("No parquet files found in data/raw/. Run without --skip-download first.")
        print(f"Using {len(files)} existing file(s) in data/raw/")
    else:
        from src.downloader import download
        files = download(year=args.year, month=args.month)

    # ── Process ───────────────────────────────────────────────────────────────
    from src.processor import process
    summary = process(files)

    # ── Report ────────────────────────────────────────────────────────────────
    totals = summary["totals"]

    sep = "=" * 60
    header = "  Results (per-file 90th percentile, trip_distance > P90)"
    print()
    print(sep)
    print(header)
    print(sep)

    for r in summary["per_file"]:
        print(f"  {r['file']}")
        print(f"    Valid trips      : {r['total_valid_trips']:>12,}")
        print(f"    P90 threshold    : {r['p90_threshold_miles']:>12.4f} miles")
        print(f"    Trips above P90  : {r['filtered_trips']:>12,}  ({r['share_above_p90_pct']}%)")
        print()

    if totals["input_files"] > 1:
        print(f"  TOTAL across {totals['input_files']} files")
        print(f"    Trips analyzed   : {totals['total_valid_trips']:>12,}")
        print(f"    Trips above P90  : {totals['filtered_trips']:>12,}  ({totals['share_above_p90_pct']}%)")
        print()

    print("  Outputs saved to data/output/")
    print("    filtered_trips.parquet")
    print("    summary.json")
    print(sep)


if __name__ == "__main__":
    main()
