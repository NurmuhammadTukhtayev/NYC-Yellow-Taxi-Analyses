# NYC Yellow Taxi — Trips Above the 90th Percentile in Distance

Filter NYC Yellow Taxi trips that exceed the **global 90th percentile of `trip_distance`**
across any set of monthly parquet files from the
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
- **Multi-file glob** — `read_parquet('data/raw/*.parquet')` treats multiple files as one
  logical table, so the percentile is always *global* (not per-file).
- **Minimal dependencies** — the entire pipeline is `duckdb` + `requests`.

### Pipeline steps

```
Download (requests)          Process (DuckDB)               Output
─────────────────   ──────────────────────────────────   ──────────────
TLC CloudFront  →  data/raw/  →  p90 threshold  →  filter  →  data/output/
*.parquet                         (global)                   filtered_trips.parquet
                                                             summary.json
```

1. **Download** — fetch the requested monthly parquet file(s) from the TLC CloudFront CDN
   and save them to `data/raw/`. Already-downloaded files are skipped.
2. **Compute threshold** — one SQL query calculates the 90th percentile of `trip_distance`
   across all files in `data/raw/`, excluding nulls and zero-distance records.
3. **Filter** — a second query writes every trip where `trip_distance > p90` to
   `data/output/filtered_trips.parquet`.
4. **Summarise** — a JSON file (`data/output/summary.json`) records the threshold, row counts,
   and distance statistics for the filtered set.

---

## Assumptions

| # | Assumption |
|---|-----------|
| 1 | **"Any of the parquet files"** = Yellow Taxi monthly files. Green Taxi, FHV, and HVFHV datasets are out of scope. |
| 2 | **"0.9 percentile"** = 90th percentile (top 10 % by distance). |
| 3 | The percentile is computed **globally** across all downloaded files combined, not per-file. |
| 4 | Rows where `trip_distance` is `NULL` or `≤ 0` are excluded from percentile computation (data quality). They are also excluded from the filtered output because they cannot be "above" any positive threshold. |
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
│   └── processor.py       # DuckDB: compute p90, filter, summarise
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
`trip_distance > p90_threshold`.

### `data/output/summary.json`

Run metadata and statistics, e.g.:

```json
{
  "generated_at": "2026-05-11T10:00:00+00:00",
  "input_files": ["yellow_tripdata_2025-02.parquet"],
  "p90_threshold_miles": 4.2,
  "total_valid_trips": 2845312,
  "filtered_trips": 284532,
  "share_above_p90_pct": 10.0,
  "filtered_distance_stats": {
    "min_miles": 4.2001,
    "max_miles": 162.5,
    "mean_miles": 7.83,
    "median_miles": 6.1
  },
  "output_parquet": "data/output/filtered_trips.parquet"
}
```

---

## Design decisions & trade-offs

| Decision | Rationale |
|----------|-----------|
| DuckDB over pandas | DuckDB reads only the needed columns from parquet (columnar scan), uses streaming aggregation, and handles files larger than RAM. pandas would load everything into memory. |
| Two-pass query (count + filter) | Keeps logic simple and auditable. DuckDB caches the parquet scan metadata so the second pass re-reads the file efficiently. |
| `quantile_cont` (continuous) | Returns the exact interpolated value, matching the mathematical definition of the 90th percentile. |
| No Tinybird | The task asks to evaluate problem-solving ability, not use of managed services. DuckDB provides equivalent analytical power locally. |
| Streaming download | Files are ~100–500 MB; streaming with chunked writes avoids memory spikes during download. |
| Glob for multi-file | When multiple months are present in `data/raw/`, DuckDB's `read_parquet('data/raw/*.parquet')` computes the threshold globally across all of them in one query — no Python loop or intermediate concatenation needed. |
