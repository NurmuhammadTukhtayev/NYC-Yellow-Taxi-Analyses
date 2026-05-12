# NYC Yellow Taxi — Distance Outlier Detection
### Technical Overview

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Context](#2-business-context)
   - [The Ask](#21-the-ask)
   - [Requirements](#22-requirements)
   - [Discovery Q&A](#23-discovery-qa)
   - [Solution Overview](#24-solution-overview-business-perspective)
3. [Technical Design](#3-technical-design)
   - [Architecture](#31-architecture)
   - [Technology Stack](#32-technology-stack)
   - [Module Structure](#33-module-structure)
   - [Data Flow](#34-data-flow)
   - [Key Design Decisions](#35-key-design-decisions)
   - [Logging & Observability](#36-logging--observability)
4. [How to Run](#4-how-to-run)
   - [Prerequisites](#41-prerequisites)
   - [Installation](#42-installation)
   - [CLI Reference](#43-cli-reference)
   - [Expected Outputs](#44-expected-outputs)
5. [Live Results](#5-live-results)
6. [Productionising](#6-productionising)

---

## 1. Executive Summary

A customer needs to identify **distance outliers** in NYC Yellow Taxi trip data — specifically,
trips that are unusually long relative to other trips in the same period. The solution is a
**parameterisable Python pipeline** that downloads monthly parquet files from the NYC TLC
open dataset, computes the 90th percentile of trip distance per file, and outputs all trips
that exceed that threshold.

The pipeline is designed for **one-time analysis** with a clear path to automation. It produces
two artefacts per run: a filtered Parquet file of outlier trips and a JSON summary with
statistics. Every run is fully logged to a dedicated, timestamped log file.

---

## 2. Business Context

### 2.1 The Ask

> *"Using NYC Yellow Taxi Trips Data, give me all the trips over 0.9 percentile in distance
> travelled for any of the parquet files you can find there."*

The customer wants to surface **distance outliers** in their taxi dataset. This kind of
analysis helps to:

- Identify unusually long trips that may indicate data entry errors, GPS faults, or
  genuinely anomalous routes.
- Provide a starting point for deeper investigation into billing irregularities or driver
  behaviour patterns.
- Establish a repeatable baseline for ongoing data quality monitoring.

### 2.2 Requirements

| # | Requirement | Source |
|---|-------------|--------|
| R1 | Process NYC TLC Yellow Taxi monthly parquet files | Original ask |
| R2 | Filter trips above the 90th percentile in trip distance | Original ask |
| R3 | Download configurable by year and/or month parameter | Clarification |
| R4 | Percentile computed per file (per month), not globally | Clarification |
| R5 | Output: filtered Parquet file of qualifying trips | Clarification |
| R6 | Output: JSON summary with thresholds and statistics | Clarification |
| R7 | Solution must be easily reproducible (pip + venv) | Original ask |
| R8 | No third-party managed services | Original ask |

### 2.3 Discovery Q&A

Before implementation, a structured set of questions was raised to remove ambiguity.
The table below documents every question, the answer received, and how it was actioned.

| Question | Answer | Decision |
|----------|--------|----------|
| Process all available files or a specific date range? | One file is sufficient. The goal is to help the customer identify outliers. | Default to latest available file; `--year`/`--month` flags allow targeting any period. |
| Per-file or global percentile? | Per file is fine. | P90 computed independently for each monthly file. |
| Strictly `>` or `>=` the threshold? | Customer wants to find outliers — your judgement. | Chose `>`. The P90 value itself is the boundary, not an outlier. `>=` would inflate results without adding signal. |
| `trip_distance` (meter) or geographic distance (coordinates)? | Customer wants outliers in their data — your judgement. | Chose `trip_distance`. It is the actual business metric (what was billed). Geographic distance ignores routing and has GPS null issues. |
| Full trip records or selected fields? | Full records are fine. | All columns from the source parquet are preserved in the output. |
| Output format? | Parquet is fine. | Filtered trips in Parquet; run metadata in JSON. |
| One-time or repeatable pipeline? | One-time analysis; talk through how to make it repeatable. | Designed as a one-time tool. See [Productionising](#6-productionising) for the path to automation. |

### 2.4 Solution Overview (Business Perspective)

The pipeline follows a simple three-step process:

```
1. FETCH          2. ANALYSE            3. DELIVER
-----------   -------------------   ------------------
Download  ->  Compute per-month  ->  Filtered Parquet
monthly       90th percentile        (outlier trips)
parquet       of trip_distance
file(s)
              Flag all trips         JSON summary
              above threshold   ->   (stats + metadata)
```

**What the customer receives:**

- A Parquet file containing every trip that exceeded the 90th percentile of trip distance
  for its respective month. These are the candidates for further investigation.
- A JSON summary that tells them exactly what threshold was applied, how many trips were
  flagged, and the distance range of those trips.
- A timestamped log file for every run, so there is a full audit trail of what was
  processed, when, and what the results were.

---

## 3. Technical Design

### 3.1 Architecture

The solution is a **single-host Python pipeline** with no external services. All compute
runs in-process using DuckDB. Data is read from the public TLC CloudFront CDN over HTTPS
and written to the local filesystem.

```
+------------------+        HTTPS        +---------------------+
|   main.py (CLI)  | ------------------> |  TLC CloudFront CDN |
|                  | <-- parquet file -- |  (public dataset)   |
+--------+---------+                     +---------------------+
         |
         v
+--------+---------+       SQL queries      +------------------+
|  src/downloader  | -----> data/raw/ ----> |  src/processor   |
|  (fetch & save)  |                        |  (DuckDB engine) |
+------------------+                        +--------+---------+
                                                     |
                              +----------------------+-------------------+
                              |                                          |
                    +---------v----------+                  +------------v--------+
                    | filtered_trips     |                  | summary.json        |
                    | .parquet           |                  | (per-file P90,      |
                    | (outlier trips)    |                  |  row counts, stats) |
                    +--------------------+                  +---------------------+
                              |
                    +---------v----------+
                    | logs/              |
                    | pipeline_TIMESTAMP |
                    | .log               |
                    +--------------------+
```

### 3.2 Technology Stack

| Component | Library | Version | Reason |
|-----------|---------|---------|--------|
| Compute engine | `duckdb` | >= 1.1.0 | Reads parquet natively, SQL percentile functions, in-process (no server), handles files larger than RAM via streaming columnar scans |
| HTTP download | `requests` | >= 2.31.0 | Mature, well-tested HTTP client; streaming support for large file downloads without memory spikes |
| CLI interface | `argparse` | stdlib | No extra dependency for a straightforward CLI |
| Logging | `logging` | stdlib | Structured, configurable, two-handler setup (console + file) |
| Path handling | `pathlib` | stdlib | Cross-platform path operations |
| Output metadata | `json` | stdlib | Human-readable run summaries |

**Total external dependencies: 2** (`duckdb`, `requests`).

#### Why DuckDB over alternatives?

| Alternative | Why not used |
|-------------|-------------|
| **pandas + pyarrow** | Loads entire file into RAM before processing; DuckDB streams columns on demand. Slower for filter-then-write workloads. |
| **Spark / PySpark** | Massive operational overhead (JVM, cluster setup) for a dataset that fits comfortably in a single-node query engine. |
| **ClickHouse / PostgreSQL** | Requires a running server, defeating the "no infrastructure" requirement. |
| **Tinybird** | Third-party managed service; excluded per requirements. Noted that this problem is trivially solvable there, but the goal is to demonstrate problem-solving ability. |

### 3.3 Module Structure

```
.
├── main.py                    # CLI entry point — argument parsing, logging setup,
│                              # top-level orchestration, error handling
└── src/
    ├── downloader.py          # Public API: download(year, month)
    │                          # Delegates to utils; contains only orchestration logic
    ├── processor.py           # Public API: process(files)
    │                          # Delegates to utils; contains only orchestration logic
    └── utils/
        ├── http.py            # All HTTP/CDN logic:
        │                      #   file_url(), file_path(), is_available(),
        │                      #   detect_latest(), download_year(), download_file()
        ├── compute.py         # All DuckDB logic:
        │                      #   compute_file(), merge_tmp_files()
        │                      #   DuckDB errors translated to RuntimeError here
        └── logger.py          # Logging setup:
                               #   setup_logging(), _prune_old_logs()
```

**Design principle:** `src/downloader.py` and `src/processor.py` are intentionally thin —
they contain only orchestration (what to call, in what order) and no implementation detail.
All helper functions live in `src/utils/`, making them independently readable and testable.

### 3.4 Data Flow

```
main.py
  |
  +--> setup_logging()               Creates logs/pipeline_TIMESTAMP.log
  |
  +--> download(year, month)
  |      |
  |      +--> detect_latest()        HEAD requests to CDN to find latest file
  |      +--> download_file()        Streams parquet to data/raw/ (skips if exists)
  |
  +--> process(files)
         |
         +--> [for each file]
         |      |
         |      +--> compute_file()
         |             |
         |             +--> DuckDB: COUNT + quantile_cont(trip_distance, 0.9)
         |             |           WHERE trip_distance IS NOT NULL AND > 0
         |             |
         |             +--> DuckDB: COPY filtered rows to _tmp_<name>.parquet
         |                         WHERE trip_distance > p90
         |
         +--> merge_tmp_files()     Rename (1 file) or COPY glob (n files)
         |
         +--> Write summary.json
```

**Two-pass query design:** The percentile scan and the filter write are separate queries.
DuckDB caches parquet metadata after the first scan, so the second pass is fast and the
logic stays simple and auditable.

### 3.5 Key Design Decisions

#### Percentile scope: per-file

Each monthly file is treated independently. A global threshold computed across months
would be skewed by seasonal variation (e.g., summer months have more long airport runs)
and period-over-period demand changes. Per-file percentiles keep the outlier threshold
relative to that month's own distribution, producing more actionable results.

#### Filter operator: strictly greater than (`>`)

The 90th percentile value marks the boundary between "normal" and "outlier" territory.
A trip exactly at P90 is by definition the last normal trip, not an outlier. Using `>`
ensures we flag only trips that unambiguously exceed the threshold.

#### Distance metric: `trip_distance` (meter reading)

`trip_distance` is what the taxi meter recorded — the direct business metric. Geographic
straight-line distance (haversine from lat/lon) was rejected for two reasons:

1. **Semantic mismatch:** A long trip via a detour or bridge looks short geographically
   but IS a long trip in every business sense.
2. **Data quality:** A significant share of TLC records have null or zero coordinates;
   geographic distance cannot be computed for those rows, causing silent data loss.

#### Exception handling: boundary translation

DuckDB-specific errors (`duckdb.Error`) are caught inside `src/utils/compute.py` and
re-raised as `RuntimeError` with the original cause preserved via `from exc`. This
means `main.py` handles only `(RuntimeError, OSError)` — it never needs to import
`duckdb` just for exception handling, and the module boundary stays clean.

#### Download idempotency

`download_file()` checks whether the destination file already exists before making any
HTTP request. Running the pipeline twice with the same parameters is safe — the download
step is skipped and only the compute step runs.

### 3.6 Logging & Observability

#### Per-run log files

Every invocation of `main.py` creates a dedicated log file:

```
logs/pipeline_YYYY-MM-DD_HH-MM-SS.log
```

This makes it trivial to locate the log for a specific run by timestamp, without grepping
through a shared append-only file. Old logs are pruned automatically (default: keep last
30 runs, configurable via `MAX_LOG_FILES` in `src/utils/logger.py`).

#### Two-handler setup

| Handler | Level | Content |
|---------|-------|---------|
| Console (stderr) | INFO+ | Clean pipeline progress — no line numbers |
| Per-run file | DEBUG+ | Full detail with module name and line number |

#### What each level captures

| Level | Examples |
|-------|---------|
| `DEBUG` | CDN HEAD probes, download progress at 25/50/75%, exact DuckDB row counts, merge steps |
| `INFO` | Download start/complete with size and duration, P90 threshold, filtered counts, output paths |
| `WARNING` | Month files not yet published on CDN, failed availability checks |
| `ERROR` | All `SystemExit` paths logged before raising; `(RuntimeError, OSError)` with full traceback |

#### Self-describing logs

The first line of every log file records the exact parameters used for that run:

```
2026-05-12 14:39:09 | INFO | __main__:71 | Run started | year=None month=None skip_download=True | log=pipeline_2026-05-12_14-39-09.log
```

No other file needs to be opened to understand the context of a log.

---

## 4. How to Run

### 4.1 Prerequisites

- Python **3.11** or later
- Internet access (for the initial download; not needed with `--skip-download`)
- ~150 MB free disk space per monthly parquet file

### 4.2 Installation

```bash
# Clone or unzip the repository
cd nyc-taxi-p90

# Create a virtual environment
python -m venv .venv

# Activate it
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # macOS / Linux

# Install dependencies (2 packages)
pip install -r requirements.txt
```

### 4.3 CLI Reference

```
python main.py [--year YEAR] [--month MONTH] [--skip-download] [--verbose]
```

| Flag | Description |
|------|-------------|
| *(no flags)* | Auto-detect and download the latest available monthly file |
| `--year 2024` | Download all available months for 2024 |
| `--year 2024 --month 06` | Download only June 2024 |
| `--skip-download` | Skip download; process files already in `data/raw/` |
| `--verbose` / `-v` | Push DEBUG messages to the terminal (always written to the log file) |

```bash
# Default: latest file
python main.py

# Specific month
python main.py --year 2024 --month 06

# Full year
python main.py --year 2023

# Re-process without re-downloading
python main.py --skip-download

# Verbose for debugging
python main.py --verbose
```

### 4.4 Expected Outputs

```
data/
  raw/
    yellow_tripdata_YYYY-MM.parquet     # source file (downloaded once)
  output/
    filtered_trips.parquet              # outlier trips (trip_distance > P90)
    summary.json                        # thresholds, counts, configuration

logs/
  pipeline_YYYY-MM-DD_HH-MM-SS.log     # one file per run, auto-pruned
```

**`summary.json` structure:**

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
  }
}
```

---

## 5. Live Results

The pipeline was run against the **March 2026** Yellow Taxi dataset — the most recent
available at time of writing (TLC publishes data ~2-3 months in arrears).

| Metric | Value |
|--------|-------|
| Source file | `yellow_tripdata_2026-03.parquet` |
| File size | 67.9 MB |
| Valid trips analysed | 3,831,241 |
| P90 threshold | **8.80 miles** |
| Trips above P90 | **381,585** (9.96%) |
| Min distance (flagged) | 8.81 miles |
| Max distance (flagged) | 288,381.68 miles |

> **Note on max distance:** The 288,381-mile value is a known data quality artefact
> in the TLC dataset (GPS or meter recording error). The pipeline correctly includes
> it — our job is to surface outliers, not to define what is or isn't a valid trip.
> The customer can apply further business rules on top of the filtered output.

**Terminal output from the live run:**

```
2026-05-12 14:39:09 | INFO | __main__     | Run started | year=None month=None skip_download=False
2026-05-12 14:39:09 | INFO | src.utils.http | Detecting latest available TLC dataset...
2026-05-12 14:39:11 | INFO | src.utils.http | Latest available dataset: 2026-03
2026-05-12 14:39:11 | INFO | src.utils.http | Downloading yellow_tripdata_2026-03.parquet (67.9 MB)...
2026-05-12 14:39:38 | INFO | src.utils.http | Download complete: yellow_tripdata_2026-03.parquet
2026-05-12 14:39:38 | INFO | src.processor  | Processing yellow_tripdata_2026-03.parquet ...
2026-05-12 14:39:39 | INFO | src.processor  |   Valid trips      :    3,831,241
2026-05-12 14:39:39 | INFO | src.processor  |   P90 threshold    :       8.8000 miles
2026-05-12 14:39:39 | INFO | src.processor  |   Trips above P90  :      381,585 (9.96%)
2026-05-12 14:39:39 | INFO | src.processor  | Writing data\output\filtered_trips.parquet ...
2026-05-12 14:39:39 | INFO | src.processor  | Summary written to data\output\summary.json
2026-05-12 14:39:39 | INFO | __main__       | Run complete. Log: logs\pipeline_2026-05-12_14-39-09.log
```

---

## 6. Productionising

The current solution is intentionally scoped as a **one-time analysis tool**. Below is the
roadmap for converting it into a reliable recurring pipeline.

### Step 1 — Scheduling

Trigger `main.py --year YYYY --month MM` monthly, once TLC publishes new data (typically
around the 10th of each month, 2-3 months in arrears).

```bash
# Crontab example: run on the 10th of each month at 06:00
0 6 10 * * cd /app && python main.py --year $(date +%Y) --month $(date -d '-2 months' +%m)
```

For more robust orchestration: **Apache Airflow**, **Prefect**, or **GitHub Actions**
scheduled workflows are all suitable replacements.

### Step 2 — Idempotency

The downloader already skips files that exist in `data/raw/`. The processor should
additionally check whether a `summary.json` entry for the target file already exists,
so accidental re-runs do not overwrite valid outputs.

### Step 3 — Output storage

Replace local filesystem writes with a persistent store:

- **Object storage** (S3 / GCS / Azure Blob): write outputs as
  `filtered_trips/year=YYYY/month=MM/part-0.parquet` (Hive-partitioned).
- **Analytical database**: append to a DuckDB persistent file, ClickHouse, or BigQuery
  for long-term cross-month querying.

### Step 4 — Monitoring & alerting

After each run, compare `share_above_p90_pct` against historical baselines. A drift
of more than ~2 percentage points could indicate a source data quality issue worth
investigating before results are consumed downstream.

### Step 5 — Containerisation

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python", "main.py"]
```

A Docker image makes the environment fully reproducible across local machines, CI
runners, and cloud compute without managing Python versions or virtual environments.

---

*Document prepared by the engineering team. For questions, refer to the inline code
documentation or the project `README.md`.*
