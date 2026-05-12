"""
Public interface for downloading NYC TLC Yellow Taxi parquet files.

Accepts optional year/month parameters; defaults to the latest available file.
All HTTP and path logic lives in utils/http.py.
"""
from pathlib import Path

from .utils.http import RAW_DIR, detect_latest, download_file, download_year


def download(year: int | None = None, month: int | None = None) -> list[Path]:
    """
    Download Yellow Taxi parquet file(s) and return their local paths.

      No args          → latest available single file
      --year only      → all available months for that year
      --year --month   → that specific file
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if year is None and month is None:
        y, m = detect_latest()
        return [download_file(y, m)]

    if month is not None:
        if not (1 <= month <= 12):
            raise SystemExit("--month must be between 1 and 12.")
        return [download_file(year, month)]

    return download_year(year)
