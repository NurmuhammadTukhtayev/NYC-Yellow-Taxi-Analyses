"""
DuckDB query helpers for per-file percentile computation.

Named compute.py (not duckdb.py) to avoid shadowing the installed duckdb package
when using absolute imports inside this file.
"""
import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def compute_file(
    conn: duckdb.DuckDBPyConnection,
    source: Path,
    dest: Path,
) -> tuple[int, float, int]:
    """
    For a single parquet file:
      1. Compute the 90th percentile of trip_distance (nulls and zeros excluded).
      2. Write all trips where trip_distance > p90 to dest.

    Returns:
      total_valid_trips  — rows with a positive, non-null trip_distance
      p90                — the computed threshold in miles
      filtered_trips     — rows written to dest
    """
    src = source.as_posix()
    logger.debug("Computing P90 for %s", source.name)

    row = conn.execute(f"""
        SELECT
            COUNT(*)                          AS total_trips,
            quantile_cont(trip_distance, 0.9) AS p90
        FROM read_parquet('{src}')
        WHERE trip_distance IS NOT NULL
          AND trip_distance > 0
    """).fetchone()

    total: int = row[0]
    p90: float = row[1]
    logger.debug("P90 = %.4f miles for %s (%d valid rows)", p90, source.name, total)

    conn.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{src}')
            WHERE trip_distance > {p90}
        ) TO '{dest.as_posix()}' (FORMAT PARQUET)
    """)

    filtered: int = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{dest.as_posix()}')"
    ).fetchone()[0]

    logger.debug("Wrote %d filtered rows to %s", filtered, dest.name)
    return total, p90, filtered


def merge_tmp_files(
    conn: duckdb.DuckDBPyConnection,
    tmp_files: list[Path],
    out_parquet: Path,
) -> None:
    """
    Merge per-file temporary parquets into a single output file
    and clean up the temp files afterwards.
    """
    logger.debug("Merging %d temp file(s) into %s", len(tmp_files), out_parquet.name)

    if len(tmp_files) == 1:
        tmp_files[0].replace(out_parquet)
    else:
        tmp_glob = (out_parquet.parent / "_tmp_*.parquet").as_posix()
        conn.execute(f"""
            COPY (SELECT * FROM read_parquet('{tmp_glob}'))
            TO '{out_parquet.as_posix()}' (FORMAT PARQUET)
        """)
        for tmp in tmp_files:
            tmp.unlink(missing_ok=True)

    logger.debug("Merge complete: %s", out_parquet.name)
