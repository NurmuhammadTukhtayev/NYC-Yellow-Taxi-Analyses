"""
HTTP utilities for the TLC CloudFront CDN:
  - URL and local-path construction
  - Availability probing (HEAD requests)
  - Streaming file download with progress logging
"""
import logging
import time
from datetime import date
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DIR = Path("data/raw")
CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB
_PROGRESS_MILESTONES = {25, 50, 75}


def file_url(year: int, month: int) -> str:
    """Return the CDN URL for a Yellow Taxi monthly parquet file."""
    return f"{BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def file_path(year: int, month: int) -> Path:
    """Return the local destination path for a monthly parquet file."""
    return RAW_DIR / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def is_available(year: int, month: int) -> bool:
    """Return True if the CDN has a file for the given year/month."""
    url = file_url(year, month)
    try:
        response = requests.head(url, timeout=15)
        available = response.status_code == 200
        logger.debug("HEAD %s → %d", url, response.status_code)
        return available
    except requests.RequestException as exc:
        logger.warning("Availability check failed for %d-%02d: %s", year, month, exc)
        return False


def detect_latest() -> tuple[int, int]:
    """
    Probe recent months (newest first) to find the latest published file.
    TLC typically publishes data with a 2–3 month lag.
    """
    logger.info("Detecting latest available TLC dataset...")
    today = date.today()
    year, month = today.year, today.month

    for _ in range(8):
        month -= 1
        if month == 0:
            month, year = 12, year - 1
        logger.debug("Probing %d-%02d ...", year, month)
        if is_available(year, month):
            logger.info("Latest available dataset: %d-%02d", year, month)
            return year, month

    msg = (
        "Could not detect the latest available TLC dataset. "
        "Try specifying --year and --month explicitly."
    )
    logger.error(msg)
    raise RuntimeError(msg)


def download_year(year: int) -> list[Path]:
    """Download all available monthly files for a given year."""
    logger.info("Fetching all available months for %d...", year)
    files: list[Path] = []

    for month in range(1, 13):
        if not is_available(year, month):
            logger.warning("File not available: %d-%02d — skipping.", year, month)
            continue
        files.append(download_file(year, month))

    if not files:
        msg = f"No files found for year {year}."
        logger.error(msg)
        raise SystemExit(msg)

    return files


def download_file(year: int, month: int) -> Path:
    """
    Download a single TLC parquet file to RAW_DIR.
    Skips the download if the file already exists locally.
    Progress milestones (25 %, 50 %, 75 %) are logged at DEBUG level.
    """
    dest = file_path(year, month)
    if dest.exists():
        logger.info("Already downloaded: %s — skipping.", dest.name)
        return dest

    url = file_url(year, month)
    logger.info("Starting download: %s", url)

    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
    except requests.HTTPError as exc:
        msg = (
            f"HTTP {exc.response.status_code} fetching {url}. "
            "The file may not exist yet."
        )
        logger.error(msg)
        raise SystemExit(msg) from exc

    total = int(response.headers.get("content-length", 0))
    size_mb = total / 1e6
    logger.info("Downloading %s (%.1f MB)...", dest.name, size_mb)

    downloaded = 0
    logged_milestones: set[int] = set()
    start = time.monotonic()

    with open(dest, "wb") as fh:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            fh.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                for milestone in _PROGRESS_MILESTONES - logged_milestones:
                    if pct >= milestone:
                        logger.debug(
                            "%s: %d%% complete (%.1f / %.1f MB)",
                            dest.name, milestone, downloaded / 1e6, size_mb,
                        )
                        logged_milestones.add(milestone)

    elapsed = time.monotonic() - start
    logger.info(
        "Download complete: %s — %.1f MB in %.1fs", dest.name, size_mb, elapsed
    )
    return dest
