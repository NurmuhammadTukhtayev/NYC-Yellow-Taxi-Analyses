# NYC Yellow Taxi — Trips Above the 90th Percentile in Distance

Filter NYC Yellow Taxi trips that exceed the **per-file 90th percentile of `trip_distance`**
from monthly parquet files published by the
[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

---

## Approach

### Engine: DuckDB

[DuckDB](https://duckdb.org/) is an in-process analytical database that reads parquet files
natively via SQL. It was chosen because:

- **No server required** — runs in-process, zero infrastructure overhead.
- **Parquet-native** — scans columnar data efficiently without loading everything into RAM.
- **SQL percentile function** — `quantile_cont(col, 0.9)` computes the exact continuous
  90th percentile in a single pass.
- **Minimal dependencies** — compute and filter use only `duckdb`; downloads use only
  `requests`. Both are in stdlib-level simplicity.

### Pipeline steps

```
Download          Process (DuckDB, per file)          Output          Logging
--------   ------------------------------------   ---------------   ----------
TLC CDN -> data/raw/*.parquet -> P90 -> filter -> filtered_trips  -> logs/
                                                   .parquet           pipeline_
                                                   summary.json       TIMESTAMP
                                                                      .log
```

1. **Download** — fetch the requested monthly parquet file(s) from the TLC CloudFront CDN
   and save to `data/raw/`. Already-downloaded files are skipped (idempotent).
2. **Compute threshold per file** — for each parquet file independently, one SQL query
   calculates the 90th percentile of `trip_distance`, excluding nulls and zero-distance rows.
3. **Filter** — trips where `trip_distance > p90` for that file are written to a temporary
   parquet, then all temp files are merged into `data/output/filtered_trips.parquet`.
4. **Summarise** — `data/output/summary.json` records per-file thresholds, row counts,
   and aggregate totals.
5. **Log** — every run writes a dedicated log file to `logs/` (see [Logging](#logging)).

---

## Scope

These points were confirmed during the requirements discussion and are not assumptions:

| Point | Decision |
|-------|----------|
| Dataset | **Yellow Taxi** monthly parquet files only. Green Taxi, FHV, and HVFHV are out of scope. |
| Default input | **One file per run** — the latest available monthly file. Use `--year` to process a full year. |
| Percentile | **90th percentile** (top 10% by distance). |
| Percentile scope | **Per file** — each monthly file gets its own threshold (see Design decisions). |
| Filter operator | **Strictly greater than (`>`)** the threshold (see Design decisions). |
| Distance metric | **`trip_distance`** — the meter reading in miles (see Design decisions). |
| Output format | **Parquet** for filtered trips + **JSON** for the run summary. |
| Analysis mode | **One-time analysis** by design. See [How to productionise](#how-to-productionise-this-ongoing-repeatable-process) for extending it. |
| Data exclusions | Rows where `trip_distance` is `NULL` or `<= 0` are excluded from percentile computation and from the output (cannot exceed a positive threshold). |
| Units | `trip_distance` is in **miles**, as per the TLC data dictionary. |
| Versioning | Raw data and pipeline outputs are **gitignored**; only code is committed. |

---

## Design decisions

### Per-file vs global percentile

Percentile is computed independently per file (per month). Each monthly file represents a
distinct operational period; seasonal demand shifts (summer vs winter) mean a global
threshold computed across multiple months would be skewed by whichever period contributes
more rows. Per-file percentiles make outlier detection relative to each month's own
distribution, which gives the customer more actionable signals.

### Strictly greater than (`>`) vs greater-or-equal (`>=`)

The 90th percentile value is the *boundary* between normal and outlier territory — it is
by definition the last "normal" data point, not itself an outlier. Using `>` ensures only
trips that unambiguously exceed the threshold are flagged. Using `>=` would include the
boundary value, inflating the flagged set without adding meaningful signal.

### `trip_distance` (meter reading) vs geographic distance

`trip_distance` is what the taxi meter recorded — the actual business metric the customer
cares about. Computing geographic straight-line distance from pickup/dropoff coordinates
would introduce two problems:

1. **Semantic mismatch** — a long trip via a circuitous route or bridge detour looks short
   geographically but is a genuine long trip. The customer wants outliers in their *recorded*
   data, not in point-to-point geography.
2. **Data quality** — a meaningful fraction of TLC records have missing or zero-valued
   coordinates; geographic distance cannot be computed for those rows, causing silent data loss.

---

## Repository structure

```
.
├── src/
│   ├── __init__.py
│   ├── downloader.py          # public: download() — orchestrates fetch by year/month
│   ├── processor.py           # public: process() — orchestrates per-file P90 and merge
│   └── utils/
│       ├── __init__.py
│       ├── http.py            # URL/path helpers, CDN probing, streaming download
│       ├── compute.py         # DuckDB helpers: compute_file(), merge_tmp_files()
│       └── logger.py          # logging setup: per-run file + console handlers
├── data/
│   ├── raw/                   # downloaded parquet files         (gitignored)
│   └── output/                # filtered_trips.parquet + summary.json  (gitignored)
├── logs/                      # one log file per run             (gitignored)
├── main.py                    # CLI entry point
├── requirements.txt
├── .gitignore
└── README.md
```

`src/downloader.py` and `src/processor.py` contain only public orchestration logic.
All helper functions live in `src/utils/` — this keeps the main modules readable and
each utility independently testable.

---

## Prerequisites

- Python 3.11 or later
- Internet access (to download TLC data on the first run)
- ~100 MB free disk space per monthly parquet file (recent files are ~70–150 MB)

---

## Quickstart

```bash
# 1. Clone / unzip the repo
cd NYC-Yellow-Taxi-Analyses

# 2. Create and activate a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run — auto-detects and downloads the latest monthly file, then processes it
python main.py
```

The first run downloads a ~70 MB parquet file; subsequent runs with `--skip-download` are
fast (seconds). Each run writes a log file to `logs/`.

---

## CLI reference

```
python main.py [--year YEAR] [--month MONTH] [--skip-download] [--verbose]
```

| Flag | Description |
|------|-------------|
| *(no flags)* | Auto-detect and download the latest available monthly file |
| `--year 2024` | Download all available months for 2024 |
| `--year 2024 --month 06` | Download only June 2024 |
| `--skip-download` | Skip download; process files already in `data/raw/` |
| `--verbose` / `-v` | Show DEBUG-level messages on the terminal (always in the log file) |

### Examples

```bash
# Latest file (default)
python main.py

# Specific month
python main.py --year 2024 --month 03

# All of 2023
python main.py --year 2023

# Re-run processing without re-downloading
python main.py --skip-download

# Verbose terminal output for debugging
python main.py --skip-download --verbose
```

---

## Outputs

### `data/output/filtered_trips.parquet`

Full trip records (same schema as the source) for all trips where
`trip_distance > p90_threshold` within their respective monthly file.

### `data/output/summary.json`

Per-file thresholds and aggregate totals. Example from the March 2026 dataset:

```json
{
  "generated_at": "2026-05-12T14:39:09+00:00",
  "configuration": {
    "percentile_scope": "per_file",
    "threshold_operator": "strictly_greater_than",
    "distance_metric": "trip_distance_miles"
  },
  "per_file": [
    {
      "file": "yellow_tripdata_2026-03.parquet",
      "total_valid_trips": 3831241,
      "p90_threshold_miles": 8.8,
      "filtered_trips": 381585,
      "share_above_p90_pct": 9.9598
    }
  ],
  "totals": {
    "input_files": 1,
    "total_valid_trips": 3831241,
    "filtered_trips": 381585,
    "share_above_p90_pct": 9.9598
  },
  "output_parquet": "data/output/filtered_trips.parquet"
}
```

---

## Logging

Each run of `main.py` creates a dedicated log file:

```
logs/pipeline_YYYY-MM-DD_HH-MM-SS.log
```

The timestamp in the filename makes it immediately clear which log belongs to which run —
no need to search through a shared append-only file.

### Handlers

| Handler | Level | Format |
|---------|-------|--------|
| Console (stderr) | INFO and above | `timestamp | LEVEL | module | message` |
| Per-run file | DEBUG and above | `timestamp | LEVEL | module:line | message` |

The file handler captures DEBUG-level detail (CDN probes, download progress milestones,
exact DuckDB row counts) that would be too noisy on the terminal during normal use.

### Log levels used

| Level | Examples |
|-------|---------|
| DEBUG | CDN HEAD probes, download progress at 25/50/75%, DuckDB internal row counts, merge steps |
| INFO | Download start/complete, P90 threshold, filtered row counts, output paths, run start/end |
| WARNING | Skipped months (file not on CDN), failed availability checks |
| ERROR | All `SystemExit` cases logged before raising |
| EXCEPTION | Any unexpected error — full traceback captured automatically |

### First line of every log

The run parameters are logged at the top of each file:

```
2026-05-12 14:39:09 | INFO | __main__:71 | Run started | year=None month=None skip_download=True | log=pipeline_2026-05-12_14-39-09.log
```

This makes each log file self-contained — you can identify what was run without opening
any other file.

### Auto-cleanup

Old log files are pruned automatically on startup. By default the last **30 runs** are
kept. This is configurable via `MAX_LOG_FILES` at the top of `src/utils/logger.py`.

### Verbose mode

Pass `--verbose` (or `-v`) to promote DEBUG messages to the terminal as well:

```bash
python main.py --verbose
```

---

## How to productionise this (ongoing repeatable process)

The solution is intentionally designed as a one-time analysis tool. To run it reliably
as a monthly pipeline, the following changes would be needed:

### 1. Scheduling

Add a monthly trigger that calls `main.py --year YYYY --month MM` once TLC publishes new
data (typically around the 10th of each month, 2-3 months in arrears).

```bash
# Example crontab: run on the 10th of every month
0 6 10 * * cd /app && python main.py --year $(date +%Y) --month $(date -d '-2 months' +%m)
```

Tools that scale better than cron: **Apache Airflow**, **Prefect**, or **GitHub Actions**
scheduled workflows.

### 2. Idempotency

The downloader already skips files that exist in `data/raw/`. Add a matching check in
the processor to skip re-processing a file whose entry already exists in `summary.json`,
so accidental re-runs do not overwrite previous results.

### 3. Output storage

Replace the local parquet write with a persistent store:

- **Object storage** (S3 / GCS / Azure Blob) — write new monthly files as
  `filtered_trips/year=YYYY/month=MM/part-0.parquet` (Hive-partitioned for easy querying).
- **Analytical database** — write directly to a DuckDB persistent file, ClickHouse, or
  BigQuery for long-term querying across months.

### 4. Alerting and monitoring

After each run, compare `share_above_p90_pct` against historical values. A sudden jump
(e.g., above 12% or below 8%) could indicate a data quality issue in the source file
worth investigating before the output is consumed downstream.

### 5. Containerisation

Wrap the pipeline in a `Dockerfile` so the execution environment is fully reproducible
across machines and CI runners:

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python", "main.py"]
```
