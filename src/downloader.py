"""
Download NYC TLC Yellow Taxi parquet files by year/month parameter.
Files are fetched from the TLC CloudFront distribution and saved to data/raw/.
"""
import sys
from datetime import date
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DIR = Path("data/raw")

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB


def _file_url(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def _file_path(year: int, month: int) -> Path:
    return RAW_DIR / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def _detect_latest() -> tuple[int, int]:
    """
    Probe recent months (newest first) to find the latest published file.
    TLC typically publishes data with a 2–3 month lag.
    """
    today = date.today()
    year, month = today.year, today.month

    for _ in range(8):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        url = _file_url(year, month)
        try:
            r = requests.head(url, timeout=15)
            if r.status_code == 200:
                print(f"Latest available dataset: {year}-{month:02d}")
                return year, month
        except requests.RequestException:
            continue

    raise RuntimeError("Could not detect the latest available TLC dataset. Try specifying --year and --month.")


def _download_one(year: int, month: int) -> Path:
    dest = _file_path(year, month)
    if dest.exists():
        print(f"Already downloaded: {dest.name} — skipping.")
        return dest

    url = _file_url(year, month)
    print(f"Downloading {url}")

    try:
        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()
    except requests.HTTPError as e:
        raise SystemExit(f"HTTP {e.response.status_code} for {url}. The file may not exist yet.") from e

    total = int(r.headers.get("content-length", 0))
    downloaded = 0

    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                mb_done = downloaded / 1e6
                mb_total = total / 1e6
                print(f"\r  {pct:5.1f}%  {mb_done:.1f} / {mb_total:.1f} MB", end="", flush=True)

    print(f"\r  Done. Saved to {dest}              ")
    return dest


def download(year: int | None = None, month: int | None = None) -> list[Path]:
    """
    Download Yellow Taxi parquet file(s) and return their local paths.

    - No args          → latest available single file
    - --year only      → all months for that year (skips missing files)
    - --year --month   → that specific file
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if year is None and month is None:
        y, m = _detect_latest()
        return [_download_one(y, m)]

    if month is not None:
        if not (1 <= month <= 12):
            raise SystemExit("--month must be between 1 and 12.")
        return [_download_one(year, month)]

    # year only → all months
    files: list[Path] = []
    print(f"Downloading all available months for {year}...")
    for m in range(1, 13):
        url = _file_url(year, m)
        try:
            r = requests.head(url, timeout=15)
            if r.status_code != 200:
                print(f"  {year}-{m:02d}: not available, skipping.")
                continue
        except requests.RequestException as e:
            print(f"  {year}-{m:02d}: network error ({e}), skipping.")
            continue
        files.append(_download_one(year, m))

    if not files:
        raise SystemExit(f"No files found for year {year}.")
    return files
