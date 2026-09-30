# MIBEL Intelligence

**End-to-end data pipeline for the Iberian electricity market**, producing an hourly country-level panel for econometric analysis of the *Iberian Exception* (the gas-price cap applied to Spain and Portugal from 15 June 2022 to 31 December 2023).

The pipeline ingests day-ahead prices, cross-border flows and weather data from three public sources (OMIE, ENTSO-E Transparency Platform, Open-Meteo), loads them into a local DuckDB analytical database, and constructs a clean, analysis-ready panel. The current build covers market days **2022-01-01 to 2023-12-31** for **eight countries** (ES and PT from OMIE; FR, DE, IT, NL, BE, AT from ENTSO-E), with a further six ENTSO-E donor countries (DK, SE, NO, FI, PL, CZ) stored in the database.

For the full build history, design decisions and issue log, see [`docs/PIPELINE_MANUAL.md`](docs/PIPELINE_MANUAL.md). For the fixes applied to the defects it lists, see [`docs/FIX_LOG.txt`](docs/FIX_LOG.txt).

---

## Table of Contents

- [Project Goals](#project-goals)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Quickstart](#quickstart)
  - [1. Clone and Set Up the Environment](#1-clone-and-set-up-the-environment)
  - [2. Configure API Keys](#2-configure-api-keys)
  - [3. Download Raw Data](#3-download-raw-data)
  - [4. Create Database and Load Data](#4-create-database-and-load-data)
  - [5. Build the Analysis Panel](#5-build-the-analysis-panel)
  - [6. Run Exploratory Analysis](#6-run-exploratory-analysis)
  - [7. Run the Tests](#7-run-the-tests)
- [Design Choices and Rationale](#design-choices-and-rationale)
- [Example Usage](#example-usage)
- [Extending the Pipeline](#extending-the-pipeline)
- [Troubleshooting](#troubleshooting)

---

## Project Goals

1. **Build a reproducible data pipeline** that downloads, validates, and stores Iberian and European electricity-market data from three independent public APIs.
2. **Construct an hourly panel dataset** (one row per hour per country) combining prices, weather, cross-border flows, and calendar features — ready for causal-inference and structural econometric analysis.
3. **Provide economics-driven exploratory analysis** of the Iberian Exception: price dynamics, duration curves, cross-border flow leakage, and market coupling.

---

## Repository Structure

```text
mibel-intelligence/
│
├── data/                          # Not committed to git (in .gitignore)
│   ├── raw/                       # Original downloaded files, unmodified
│   │   ├── omie/                  #   OMIE day-ahead prices (quarterly files)
│   │   ├── entsoe/                #   ENTSO-E prices (per country), flows (per pair)
│   │   │   └── combined/          #     convenience files, never loaded
│   │   └── weather/               #   Open-Meteo weather (per location)
│   │       └── combined/          #     convenience file, never loaded
│   ├── processed/                 # Panel Parquet files
│   └── mibel.duckdb               # DuckDB analytical database
│
├── src/
│   ├── data/                      # Pipeline stages
│   │   ├── omie_ingest.py         # OMIE market data ingestion
│   │   ├── entsoe_ingest.py       # ENTSO-E Transparency Platform ingestion
│   │   ├── weather_ingest.py      # Open-Meteo weather ingestion
│   │   ├── load_to_db.py          # Load raw Parquet -> DuckDB tables (the only loader)
│   │   └── build_panel.py         # Construct hourly country panel
│   └── utils/                     # Shared utilities
│       ├── db_utils.py            # DuckDB connection helpers (session pinned to UTC)
│       ├── db_schema.py           # Schema definitions, indexes, migration
│       └── timezone_utils.py      # UTC normalisation, OMIE hour decoding, DST
│
├── mlops/models/training/
│   └── feature_engineering.py     # ML features + chronological train/test split
├── notebooks/
│   ├── 01_exploratory_analysis.ipynb   # EDA on the 7-day pilot panel
│   └── full_panel.ipynb                # EDA + Power BI exports on the full panel
├── docs/                          # Build manual and fix log
├── tests/                         # pytest suite (runs against a temporary DB)
├── diagnose_pipeline.py           # 10-layer end-to-end health check
├── diagnose_duplicates.py         # Raw OMIE key/duplicate check
├── download_2year_dataset.py      # Shortcut: OMIE prices 2022-2023, quarterly
├── create_env.py                  # Writes ENTSOE_API_KEY to .env
├── requirements.txt               # Pinned Python dependencies
└── README.md                      # <- You are here
```

**Key principle:** `data/` is never committed to version control. Only the code needed to regenerate the data lives in git, ensuring reproducibility without bloating the repository.

---

## Prerequisites

| Requirement | Version | Purpose |
|---|---|---|
| Python | ≥ 3.10 | Runtime |
| git | ≥ 2.30 | Version control |
| ENTSO-E API key | — | Pan-European electricity data ([register here](https://transparency.entsoe.eu/)) |

> **Note:** OMIE data is accessed via the open-source `OMIEData` library and does not require an API key. Open-Meteo is a free API with no authentication.

---

## Quickstart

### 1. Clone and Set Up the Environment

```bash
git clone https://github.com/<your-username>/mibel-intelligence.git
cd mibel-intelligence

# Create isolated virtual environment
python3 -m venv venv
source venv/bin/activate        # macOS / Linux
# venv\Scripts\activate          # Windows

# Install pinned dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

All dependency versions are pinned in `requirements.txt` (e.g. `duckdb==0.10.0`, `pandas==2.2.0`) to guarantee reproducibility across machines and over time.

### 2. Configure API Keys

```bash
python create_env.py            # prompts for the key, writes .env
```

or create `.env` in the project root by hand:

```bash
# .env — never commit this file (it is listed in .gitignore)
ENTSOE_API_KEY=your_actual_key_here
```

To obtain an ENTSO-E key:
1. Register at [transparency.entsoe.eu](https://transparency.entsoe.eu/).
2. Email `transparency@entsoe.eu` to request API access.
3. You will receive a token — paste it into `.env`.

### 3. Download Raw Data

Each ingestion script downloads data from its API in chunks, sleeps between requests, and saves raw Parquet files under `data/raw/`. None of them prompts for input, so they run unattended (cron, CI, background jobs). All dates are inclusive days.

```bash
# OMIE — Iberian day-ahead prices (default 2022-01-01..2023-12-31, quarterly files)
python -m src.data.omie_ingest
#   --start/--end, --chunk-months N
#   --bid-curves sample          # 7-day bid-curve sample from 2022-06-15 (~1 h)
#   --bid-curves full --yes      # every hour of the window (24-48 h)

# ENTSO-E — Day-ahead prices for 12 donor countries + ES<->PT, ES<->FR flows
python -m src.data.entsoe_ingest            # --start/--end

# Open-Meteo — Hourly weather for 7 Iberian locations (UTC)
python -m src.data.weather_ingest           # --start/--end (default 2021-12-31..2023-12-31)
```

**Output after this step:**

```text
data/raw/
├── omie/
│   ├── day_ahead_prices_2022-01-01_2022-03-31.parquet
│   ├── ...                                          # 8 quarterly files
│   └── day_ahead_prices_2023-10-01_2023-12-31.parquet
├── entsoe/
│   ├── prices_FR_2022-01-01_2023-12-31.parquet
│   ├── ...                                          # one file per country
│   ├── cross_border_flows_ES_FR_2022-01-01_2023-12-31.parquet
│   ├── ...                                          # one file per directed pair
│   └── combined/prices_all_countries_*.parquet      # convenience only
└── weather/
    ├── weather_Madrid_2021-12-31_2023-12-31.parquet
    ├── ...                                          # one file per location
    └── combined/weather_all_*.parquet               # convenience only
```

> **Tip:** Chunks never overlap. If a chunk fails, the script logs it, continues, and lists every failed chunk at the end — re-run to retry.

### 4. Create Database and Load Data

```bash
python -m src.data.load_to_db
```

**What happens:**

1. `create_schema()` creates five tables — `prices_day_ahead`, `generation`, `cross_border_flows`, `weather`, `bid_curves` — plus indexes. Every `timestamp` column is `TIMESTAMPTZ` and every connection runs with `SET TimeZone='UTC'`. A database built by an older version (naive `TIMESTAMP`) is migrated automatically; old rows are kept in `<table>_legacy_naive`.
2. Each loader reads the per-unit raw files (never the `combined/` ones), reshapes them, asserts one row per key per hour in UTC, and replaces the table contents in a single transaction using an **explicit column list**.
3. OMIE's `H1..H25` hours are decoded as sequential hours of the CET/CEST market day, so DST days have 23 or 25 prices and every one of them is kept.

**Verify the load:**

```python
from src.utils.db_schema import describe_schema
describe_schema()
```

or run the health check: `python diagnose_pipeline.py`.

### 5. Build the Analysis Panel

```bash
python -m src.data.build_panel                          # 2022-01-01..2023-12-31, 8 countries
python -m src.data.build_panel --start 2022-06-15 --end 2022-06-22 --countries ES PT
```

**What happens:**

1. A **complete hourly UTC index** is generated for the requested market days (day boundaries at Iberian local midnight), so there are no gaps across DST transitions. Two years = 17,520 hours.
2. **Prices**, **weather** and **flows** are queried with the *same* half-open UTC window and parameterised SQL.
3. **Weather** is aggregated from city level to country level (ES, PT) by an **unweighted mean** across locations (Madrid + Barcelona + Seville + Bilbao → ES; Lisbon + Porto + Faro → PT).
4. **Cross-border flows** are pivoted to one column per directed pair (e.g. `ES_to_FR_mw`) and attached to every country-hour.
5. Every source is left-joined onto the `(timestamp, country)` skeleton; the match rate of each join is logged and a zero match rate raises.
6. **Time features** are appended in Iberian market local time: `hour`, `day_of_week`, `month`, `year`, `quarter`, `day_of_year`, `is_weekend`, and `is_iberian_exception` (market days 2022-06-15 to 2023-12-31).
7. The row count is asserted (countries × hours) and the panel is saved as Snappy-compressed Parquet.

**Output:**

```text
data/processed/main_panel_2022-01-01_2023-12-31.parquet
```

140,160 rows (8 countries × 17,520 hours) with columns for price, weather, flows, and calendar features.

### 6. Run Exploratory Analysis

```bash
jupyter notebook notebooks/full_panel.ipynb
```

The notebooks contain economics-driven analyses including price dynamics with the treatment window shaded, price duration curves, hourly profiles, ES–PT market coupling and price splitting, weekday/weekend comparisons, and rolling volatility. `full_panel.ipynb` also writes the Power BI extracts used by `dashboard.pbix`.

> The notebooks and Power BI extracts were produced before the timestamp fix (manual D-01). Re-run them to regenerate figures on the corrected panel.

### 7. Run the Tests

```bash
python -m pytest tests/
```

The suite runs against a temporary database and temporary data directories (`tests/conftest.py`), so it never touches `data/`. Three tests call the live OMIE, ENTSO-E and Open-Meteo APIs.

---

## Design Choices and Rationale

### Why DuckDB?

| Criterion | pandas (in-memory) | SQLite | **DuckDB** |
|---|---|---|---|
| Storage model | Row-oriented (DataFrame) | Row-oriented | **Column-oriented** |
| 50M-row aggregation | Slow (4+ GB RAM) | Slow (full row scan) | **Fast (reads only needed columns)** |
| Server required | No | No | **No** (single file) |
| Pandas integration | Native | Manual conversion | **Native (`SELECT ... FROM df`)** |
| Parquet support | Via `read_parquet` | None | **Direct SQL on Parquet files** |

### Project Layout

- **`data/` excluded from git:** Raw data files can exceed several GB (especially bid curves at 50M+ rows). Only the code to generate data is versioned — anyone can reproduce the dataset by running the pipeline.
- **`src/` as a Python package:** clean imports like `from src.data.omie_ingest import download_day_ahead_prices` from any script or notebook.
- **Separation of ingestion → loading → panel construction:** Each stage is idempotent. You can re-download one data source without rebuilding everything, or rebuild the panel without re-downloading.

### Schema Design

- **Composite primary keys** on `(timestamp, country)` or similar: the key *is* the deduplication mechanism.
- **Two-letter ISO country codes** (`ES`, `PT`, `FR`, …). Countries with several bidding zones are stored under the zone that represents them (DE = DE-LU, IT = IT-North, DK = DK1, SE = SE3, NO = NO1).
- **Separate tables by granularity:** hourly prices and per-generator bid curves live in different tables.
- **Indexes on `timestamp` and `country`** for the date-window filters every query uses.

### Timezone Handling

OMIE publishes in CET/CEST market time on an `H1..H25` grid, ENTSO-E returns tz-aware timestamps, Open-Meteo returns UTC when asked, and DST creates 23-hour (spring) and 25-hour (autumn) days. The pipeline:

- **Stores UTC instants only** (`TIMESTAMPTZ`, session pinned to UTC), so results do not depend on the machine's time zone.
- **Decodes OMIE hours** as `local midnight of the market day + (n − 1) hours`, which handles both DST days exactly.
- **Uses one window convention** everywhere: inclusive market days → half-open UTC interval (`timezone_utils.market_window_utc`).
- **Asserts** UTC, on-the-hour, unique keys at load time (`assert_utc_hourly`) rather than assuming them.

### Panel Construction Strategy

- **Hour-index-first approach:** build the complete time skeleton, then merge data onto it. Missing data becomes explicit `NaN`, never a missing row.
- **Compound-key left joins** on `(timestamp, country)`, with `validate=` cardinality checks and logged match rates.
- **Weather aggregation:** an unweighted mean of the locations in each country — a transparent approximation; capacity weighting would need regional renewable-capacity data not yet acquired.

---

## Example Usage

### Load the panel and compute summary statistics

```python
import pandas as pd

df = pd.read_parquet("data/processed/main_panel_2022-01-01_2023-12-31.parquet")

print(f"Rows: {len(df):,}")
print(f"Date range (UTC): {df['timestamp'].min()} -> {df['timestamp'].max()}")
print(f"Countries: {sorted(df['country'].unique())}")
```

### Average price by country and year

```python
summary = (
    df.groupby(["country", "year"])["price_eur_mwh"]
      .mean()
      .unstack("country")
)
print(summary.round(2))
```

### Query the database directly

```python
from src.utils.db_utils import execute_query
from src.utils.timezone_utils import market_window_utc

start, end = market_window_utc("2022-06-15", "2023-12-31")

# Average price during the Iberian Exception, by country
result = execute_query("""
    SELECT country,
           AVG(price_eur_mwh) AS avg_price,
           COUNT(*) AS hours
    FROM prices_day_ahead
    WHERE timestamp >= ? AND timestamp < ?
    GROUP BY country
    ORDER BY avg_price
""", [start, end])
print(result)
```

### Quick plot: Spain vs. France monthly prices

```python
import matplotlib.pyplot as plt

monthly = (df[df["country"].isin(["ES", "FR"])]
           .groupby(["country", "year", "month"])["price_eur_mwh"].mean()
           .unstack("country"))
monthly.index = [f"{y}-{m:02d}" for y, m in monthly.index]

ax = monthly.plot(figsize=(14, 6), linewidth=2)
ax.set_ylabel("Monthly Avg Price (EUR/MWh)")
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
```

---

## Extending the Pipeline

| Extension | Where to Start |
|---|---|
| **Add bid-curve data** (50M+ rows) | `python -m src.data.omie_ingest --bid-curves sample`; the `bid_curves` table and indexes already exist, a loader does not yet. |
| **Add more countries** | Append codes to `DONOR_COUNTRIES` (and `PRICE_BIDDING_ZONES` if the country has several zones) in `entsoe_ingest.py`, and pass them to `build_panel --countries`. |
| **Add generation data to panel** | `download_generation_by_country()` exists in `entsoe_ingest.py`; add a loader for the `generation` table and a `build_generation_panel()` following `build_weather_panel()`. |
| **Causal inference** | Use the panel's `is_iberian_exception` flag with synthetic control or difference-in-differences, using the ENTSO-E countries as the donor pool. |
| **Structural modelling** | Query `bid_curves` to reconstruct supply/demand curves and estimate supply-function equilibria. |

---

## Troubleshooting

| Issue | Symptom | Solution |
|---|---|---|
| `ENTSOE_API_KEY not found` | Error on ENTSO-E download | Run `python create_env.py` or create `.env` with `ENTSOE_API_KEY=your_key`. Restart Python session. |
| Timezone warning | `"Timestamp column has no timezone. Assuming UTC."` | Raised when loading raw files written before the UTC fix. Harmless for Open-Meteo (requested in UTC); investigate for any other source. |
| Legacy tables | `prices_day_ahead_legacy_naive` exists | Left behind by the automatic schema migration. Holds the pre-fix, mislabelled rows; drop it once you no longer need to reproduce old figures. |
| Memory errors on large queries | `MemoryError` or system freeze | Filter in SQL before loading to pandas (`WHERE timestamp >= ? AND country = ?`). |
| Merge match rate 0% | `AssertionError: ... no panel row matched` | Timestamp dtype or window mismatch; see manual D-06. |
| OMIE HTTP 403 | Permission/rate-limit error | Wait 1 hour, check [omie.es](https://www.omie.es) is up, and re-run. |
| ENTSO-E chunk failures | `N chunk(s) failed, re-run to retry` | Re-run the same command; completed files are simply rewritten. |
