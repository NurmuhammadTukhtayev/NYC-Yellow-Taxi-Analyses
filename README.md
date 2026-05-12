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
- **SQL percentile function** — `quantile_cont(col, 0.9)` computes the exact 90th percentile
  in a single pass.
- **Minimal dependencies** — the entire pipeline is `duckdb` + `requests`.

### Pipeline steps

```
Download (requests)         Process (DuckDB, per file)             Output
─────────────────   ──────────────────────────────────────   ──────────────────
TLC CloudFront  →  data/raw/  →  per-file P90  →  filter  →  data/output/
*.parquet                         threshold                   filtered_trips.parquet
                                                              summary.json
```

1. **Download** — fetch the requested monthly parquet file(s) from the TLC CloudFront CDN
   and save them to `data/raw/`. Already-downloaded files are skipped.
2. **Compute threshold per file** — for each parquet file, one SQL query calculates the
   90th percentile of `trip_distance`, excluding nulls and zero-distance records.
3. **Filter** — trips where `trip_distance > p90` for that file are written to a temp
   parquet, then all temp files are merged into `data/output/filtered_trips.parquet`.
4. **Summarise** — `data/output/summary.json` records per-file thresholds, row counts,
   and aggregate totals.

---

## Design decisions

### Per-file vs global percentile

The percentile is computed independently per file (per month). Each monthly file
represents a distinct operational period; seasonal demand shifts (summer vs winter,
pre- vs post-COVID) mean that a global threshold computed across years would be skewed
by whichever period dominates the dataset. Per-file percentiles make outlier detection
relative to each month's own distribution, which is more actionable for the customer.

### Strictly greater than (`>`) vs greater-or-equal (`>=`)

The 90th percentile value is the *boundary* between normal and outlier territory — it is
by definition the last "normal" data point, not itself an outlier. Using `>` ensures we
flag only trips that unambiguously exceed the threshold. Using `>=` would include the
boundary value, which would inflate the flagged set without adding meaningful signal.

### `trip_distance` vs geographic distance

`trip_distance` is what the taxi meter recorded — the actual business metric the customer
cares about. Computing geographic (straight-line) distance from pickup/dropoff coordinates
would introduce two problems:

1. **Semantic mismatch** — a high-distance trip via a circuitous route, bridge detour,
   or traffic diversion would look short geographically but is a genuine long trip. The
   customer wants outliers in their *recorded* data, not in straight-line geography.
2. **Data quality** — a meaningful fraction of TLC records have missing or zero-valued
   coordinates; geographic distance cannot be computed for those rows, causing silent
   data loss.

---

## Assumptions

| # | Assumption |
|---|-----------|
| 1 | **"Any of the parquet files"** = Yellow Taxi monthly files. Green Taxi, FHV, and HVFHV datasets are out of scope. |
| 2 | **"0.9 percentile"** = 90th percentile (top 10% by distance). |
| 3 | Percentile is computed **per file** (per month), not globally across files. |
| 4 | Rows where `trip_distance` is `NULL` or `≤ 0` are excluded from percentile computation (data quality). They are also excluded from the filtered output because they cannot exceed any positive threshold. |
| 5 | `trip_distance` is measured in **miles**, as stated in the TLC data dictionary. |
| 6 | "Latest dataset" = the most recently published monthly file (TLC lags ~2–3 months). |
| 7 | Raw files and outputs are **not committed** to the repo (`.gitignore`); only the pipeline code is versioned. |

---

## Repository structure

```
.
├── src/
│   ├── __init__.py
│   ├── downloader.py      # fetch parquet from TLC CDN
│   └── processor.py       # DuckDB: per-file P90, filter, merge, summarise
├── data/
│   ├── raw/               # downloaded parquet files  (gitignored)
│   └── output/            # filtered_trips.parquet + summary.json  (gitignored)
├── main.py                # CLI entry point
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Prerequisites

- Python 3.11 or later
- Internet access (to download TLC data on the first run)
- ~500 MB free disk space per monthly parquet file

---

## Quickstart

```bash
# 1. Clone / unzip the repo
cd nyc-taxi-p90

# 2. Create and activate a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# 3. Install dependencies  (only 2 packages)
pip install -r requirements.txt

# 4. Run — downloads the latest available monthly file and processes it
python main.py
```

---

## CLI reference

```
python main.py [--year YEAR] [--month MONTH] [--skip-download]
```

| Flag | Description |
|------|-------------|
| *(no flags)* | Auto-detect and download the latest available monthly file |
| `--year 2024` | Download all available months for 2024 |
| `--year 2024 --month 06` | Download only June 2024 |
| `--skip-download` | Skip download; process whatever is already in `data/raw/` |

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
```

---

## Outputs

### `data/output/filtered_trips.parquet`

A parquet file with the same schema as the source, containing only trips where
`trip_distance > p90_threshold` for their respective monthly file.

### `data/output/summary.json`

Per-file thresholds plus aggregate totals, e.g.:

```json
{
  "generated_at": "2026-05-11T12:00:00+00:00",
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
  "output_parquet": "data\\output\\filtered_trips.parquet"
}
```

---

## How to productionise this (ongoing repeatable process)

The current solution is designed as a one-time analysis tool. To run it reliably as a
monthly pipeline, the following changes would be needed:

### 1. Scheduling

Add a monthly cron job (or orchestration step) that triggers `main.py --year YYYY --month MM`
once TLC publishes new data (typically the 10th of each month, 2–3 months in arrears).

```
# Example crontab: run on the 10th of every month
0 6 10 * * cd /app && python main.py --year $(date +%Y) --month $(date -d '-2 months' +%m)
```

Tools that scale better than cron: **Apache Airflow**, **Prefect**, or **GitHub Actions**
scheduled workflows.

### 2. Idempotency

The downloader already skips files that exist in `data/raw/`. Add a check in the processor
to skip re-processing a file whose summary entry already exists, so accidental re-runs are
safe.

### 3. Output storage

Replace the local parquet write with a persistent store:

- **Object storage** (S3 / GCS / Azure Blob) — append new monthly filtered files as
  `filtered_trips/year=YYYY/month=MM/part-0.parquet` (Hive-partitioned).
- **Analytical database** — write directly to DuckDB persistent file, ClickHouse, or
  BigQuery for long-term querying.

### 4. Alerting / monitoring

After each run, compare `share_above_p90_pct` against historical values. A sudden jump
(e.g., > 12% or < 8%) could indicate a data quality issue in the source file worth
investigating.

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
