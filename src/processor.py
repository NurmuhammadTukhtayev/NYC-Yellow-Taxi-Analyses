"""
Compute the 90th percentile of trip_distance globally across all input parquet
files, filter trips above that threshold, and write two outputs:

  data/output/filtered_trips.parquet  — all qualifying trips
  data/output/summary.json            — run metadata and statistics
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

OUTPUT_DIR = Path("data/output")


def _parquet_glob(files: list[Path]) -> str:
    """Return a DuckDB-compatible source expression for the given file list."""
    if len(files) == 1:
        return files[0].as_posix()
    # Multiple files: use glob on the raw dir (assumes all .parquet there are ours)
    return (files[0].parent / "*.parquet").as_posix()


def process(files: list[Path]) -> dict:
    """
    1. Compute the global 90th percentile of trip_distance (nulls and zeros excluded).
    2. Write all trips above that threshold to a parquet file.
    3. Write a JSON summary with run metadata and distance statistics.

    Returns the summary dict.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    source = _parquet_glob(files)
    conn = duckdb.connect()

    # Step 1: global percentile
    print("Computing 90th percentile of trip_distance across all files...")
    row = conn.execute(f"""
        SELECT
            COUNT(*)                              AS total_trips,
            quantile_cont(trip_distance, 0.9)     AS p90
        FROM read_parquet('{source}')
        WHERE trip_distance IS NOT NULL
          AND trip_distance > 0
    """).fetchone()

    total_trips: int = row[0]
    p90: float = row[1]

    print(f"  Trips with valid distance : {total_trips:>12,}")
    print(f"  P90 threshold (miles)     : {p90:>12.4f}")

    # Step 2: filter and write parquet
    out_parquet = OUTPUT_DIR / "filtered_trips.parquet"
    print(f"Writing filtered trips to {out_parquet} ...")
    conn.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{source}')
            WHERE trip_distance > {p90}
        ) TO '{out_parquet.as_posix()}' (FORMAT PARQUET)
    """)

    # Step 3: summary statistics on the filtered set
    stats = conn.execute(f"""
        SELECT
            COUNT(*)                          AS filtered_trips,
            ROUND(MIN(trip_distance),  4)     AS min_distance,
            ROUND(MAX(trip_distance),  4)     AS max_distance,
            ROUND(AVG(trip_distance),  4)     AS avg_distance,
            ROUND(MEDIAN(trip_distance), 4)   AS median_distance
        FROM read_parquet('{out_parquet.as_posix()}')
    """).fetchone()

    filtered_trips = stats[0]

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_files": [f.name for f in files],
        "p90_threshold_miles": round(p90, 6),
        "total_valid_trips": total_trips,
        "filtered_trips": filtered_trips,
        "share_above_p90_pct": round(filtered_trips / total_trips * 100, 4),
        "filtered_distance_stats": {
            "min_miles": stats[1],
            "max_miles": stats[2],
            "mean_miles": stats[3],
            "median_miles": stats[4],
        },
        "output_parquet": str(out_parquet),
    }

    out_json = OUTPUT_DIR / "summary.json"
    out_json.write_text(json.dumps(summary, indent=2))
    print(f"Summary written to {out_json}")

    return summary
