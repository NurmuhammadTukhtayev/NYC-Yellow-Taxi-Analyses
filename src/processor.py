"""
For each input parquet file:
  1. Compute the 90th percentile of trip_distance for that file.
  2. Filter trips where trip_distance is STRICTLY greater than that threshold.
  3. Write all filtered trips (across all files) to data/output/filtered_trips.parquet.
  4. Write per-file and aggregate stats to data/output/summary.json.

Why per-file percentile?
  Each monthly file represents a distinct operational period. Computing the threshold
  per file means outliers are relative to that month's own distribution, which is more
  meaningful for anomaly detection than a cross-month global threshold that can be
  skewed by seasonal demand shifts.

Why strictly greater than (>)?
  The 90th percentile value is the boundary between normal and outlier territory — it
  is by definition the last "normal" data point, not itself an outlier. Using > ensures
  we only flag trips that unambiguously exceed the threshold.

Why trip_distance (meter reading) rather than geographic distance?
  trip_distance is what the taxi meter recorded — the actual business metric the
  customer cares about. Geographic straight-line distance (haversine from lat/lon)
  ignores routing, traffic, and detours, so it would miss trips that are long by fare
  but short geographically (circuitous routes, bridge detours). It would also require
  valid GPS coordinates, which not all records have.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

OUTPUT_DIR = Path("data/output")


def _compute_file(conn: duckdb.DuckDBPyConnection, f: Path) -> tuple[int, float, int, Path]:
    """
    Process a single parquet file.
    Returns (total_valid_trips, p90, filtered_trips, tmp_output_path).
    """
    source = f.as_posix()

    row = conn.execute(f"""
        SELECT
            COUNT(*)                          AS total_trips,
            quantile_cont(trip_distance, 0.9) AS p90
        FROM read_parquet('{source}')
        WHERE trip_distance IS NOT NULL
          AND trip_distance > 0
    """).fetchone()

    total: int = row[0]
    p90: float = row[1]

    tmp = OUTPUT_DIR / f"_tmp_{f.stem}.parquet"
    conn.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{source}')
            WHERE trip_distance > {p90}
        ) TO '{tmp.as_posix()}' (FORMAT PARQUET)
    """)

    filtered: int = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{tmp.as_posix()}')"
    ).fetchone()[0]

    return total, p90, filtered, tmp


def process(files: list[Path]) -> dict:
    """
    Compute per-file P90, filter, and write outputs.
    Returns the full summary dict.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()

    per_file_results = []
    tmp_files: list[Path] = []

    for f in files:
        print(f"\nProcessing {f.name} ...")
        total, p90, filtered, tmp = _compute_file(conn, f)

        print(f"  Valid trips      : {total:>12,}")
        print(f"  P90 threshold    : {p90:>12.4f} miles")
        print(f"  Trips above P90  : {filtered:>12,}  ({filtered / total * 100:.2f}%)")

        per_file_results.append({
            "file": f.name,
            "total_valid_trips": total,
            "p90_threshold_miles": round(p90, 6),
            "filtered_trips": filtered,
            "share_above_p90_pct": round(filtered / total * 100, 4),
        })
        tmp_files.append(tmp)

    # Merge all per-file filtered outputs into one parquet
    out_parquet = OUTPUT_DIR / "filtered_trips.parquet"
    print(f"\nWriting {out_parquet} ...")

    if len(tmp_files) == 1:
        tmp_files[0].replace(out_parquet)
    else:
        tmp_glob = (OUTPUT_DIR / "_tmp_*.parquet").as_posix()
        conn.execute(f"""
            COPY (SELECT * FROM read_parquet('{tmp_glob}'))
            TO '{out_parquet.as_posix()}' (FORMAT PARQUET)
        """)
        for t in tmp_files:
            if t.exists():
                t.unlink()

    # Aggregate totals
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
    print(f"Summary written to {out_json}")

    return summary
