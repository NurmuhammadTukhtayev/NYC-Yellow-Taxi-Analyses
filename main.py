#!/usr/bin/env python3
"""
NYC Yellow Taxi — filter trips above the 90th percentile by distance.

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
    p = argparse.ArgumentParser(
        description="Filter NYC Yellow Taxi trips above the 90th percentile in trip distance.",
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
    print()
    print("=" * 52)
    print("  Results")
    print("=" * 52)
    print(f"  Input files          : {len(files)}")
    print(f"  Valid trips analyzed : {summary['total_valid_trips']:>12,}")
    print(f"  P90 threshold        : {summary['p90_threshold_miles']:>12.4f} miles")
    print(f"  Trips above P90      : {summary['filtered_trips']:>12,}  ({summary['share_above_p90_pct']}%)")
    print(f"  Distance range       : {summary['filtered_distance_stats']['min_miles']} – "
          f"{summary['filtered_distance_stats']['max_miles']} miles")
    print("=" * 52)
    print(f"  Outputs in: data/output/")
    print(f"    filtered_trips.parquet")
    print(f"    summary.json")
    print("=" * 52)


if __name__ == "__main__":
    main()
