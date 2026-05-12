"""
HTTP utilities for the TLC CloudFront CDN:
  - URL and local-path construction
  - Availability probing (HEAD requests)
  - Streaming file download with progress output
"""
from datetime import date
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DIR = Path("data/raw")
CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB


def file_url(year: int, month: int) -> str:
    """Return the CDN URL for a Yellow Taxi monthly parquet file."""
    return f"{BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def file_path(year: int, month: int) -> Path:
    """Return the local destination path for a monthly parquet file."""
    return RAW_DIR / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def is_available(year: int, month: int) -> bool:
    """Return True if the CDN has a file for the given year/month."""
    try:
        r = requests.head(file_url(year, month), timeout=15)
        return r.status_code == 200
    except requests.RequestException:
        return False


def detect_latest() -> tuple[int, int]:
    """
    Probe recent months (newest first) to find the latest published file.
    TLC typically publishes data with a 2–3 month lag.
    """
    today = date.today()
    year, month = today.year, today.month

    for _ in range(8):
        month -= 1
        if month == 0:
            month, year = 12, year - 1
        if is_available(year, month):
            print(f"Latest available dataset: {year}-{month:02d}")
            return year, month

    raise RuntimeError(
        "Could not detect the latest available TLC dataset. "
        "Try specifying --year and --month explicitly."
    )


def download_year(year: int) -> list[Path]:
    """Download all available monthly files for a given year."""
    print(f"Downloading all available months for {year}...")
    files: list[Path] = []

    for month in range(1, 13):
        if not is_available(year, month):
            print(f"  {year}-{month:02d}: not available, skipping.")
            continue
        files.append(download_file(year, month))

    if not files:
        raise SystemExit(f"No files found for year {year}.")
    return files


def download_file(year: int, month: int) -> Path:
    """
    Download a single TLC parquet file to RAW_DIR with a progress indicator.
    Skips the download if the file already exists locally.
    """
    dest = file_path(year, month)
    if dest.exists():
        print(f"Already downloaded: {dest.name} — skipping.")
        return dest

    url = file_url(year, month)
    print(f"Downloading {url}")

    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise SystemExit(
            f"HTTP {exc.response.status_code} for {url}. "
            "The file may not exist yet."
        ) from exc

    total = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(dest, "wb") as fh:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            fh.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(
                    f"\r  {pct:5.1f}%  {downloaded / 1e6:.1f} / {total / 1e6:.1f} MB",
                    end="",
                    flush=True,
                )

    print(f"\r  Done. Saved to {dest}              ")
    return dest
