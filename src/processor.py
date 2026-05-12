"""
Public interface for processing NYC TLC Yellow Taxi parquet files.

For each file: computes its own 90th-percentile threshold, filters trips
strictly above it, merges results, and writes two outputs:

  data/output/filtered_trips.parquet  — all qualifying trips
  data/output/summary.json            — run metadata and per-file statistics

All DuckDB query logic lives in utils/compute.py.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .utils.compute import compute_file, merge_tmp_files

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/output")


def process(files: list[Path]) -> dict:
    """
    Process each parquet file independently and write combined outputs.

    Returns the summary dict written to summary.json.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()

    per_file_results = []
    tmp_files: list[Path] = []

    for f in files:
        logger.info("Processing %s ...", f.name)
        tmp = OUTPUT_DIR / f"_tmp_{f.stem}.parquet"
        total, p90, filtered = compute_file(conn, f, tmp)

        logger.info("  Valid trips      : %d", total)
        logger.info("  P90 threshold    : %.4f miles", p90)
        logger.info("  Trips above P90  : %d (%.2f%%)", filtered, filtered / total * 100)

        per_file_results.append({
            "file": f.name,
            "total_valid_trips": total,
            "p90_threshold_miles": round(p90, 6),
            "filtered_trips": filtered,
            "share_above_p90_pct": round(filtered / total * 100, 4),
        })
        tmp_files.append(tmp)

    out_parquet = OUTPUT_DIR / "filtered_trips.parquet"
    logger.info("Writing %s ...", out_parquet)
    merge_tmp_files(conn, tmp_files, out_parquet)

    total_all = sum(r["total_valid_trips"] for r in per_file_results)
    filtered_all = sum(r["filtered_trips"] for r in per_file_results)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "configuration": {
            "percentile_scope": "per_file",
            "threshold_operator": "strictly_greater_than",
            "distance_metric": "trip_distance_miles",
        },
        "per_file": per_file_results,
        "totals": {
            "input_files": len(files),
            "total_valid_trips": total_all,
            "filtered_trips": filtered_all,
            "share_above_p90_pct": round(filtered_all / total_all * 100, 4),
        },
        "output_parquet": str(out_parquet),
    }

    out_json = OUTPUT_DIR / "summary.json"
    out_json.write_text(json.dumps(summary, indent=2))
    logger.info("Summary written to %s", out_json)

    return summary
