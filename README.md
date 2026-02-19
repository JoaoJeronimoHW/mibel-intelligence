# MIBEL Intelligence

**End-to-end data pipeline for the Iberian electricity market**, producing an hourly country-level panel for econometric analysis of the *Iberian Exception* (the gas-price cap applied to Spain and Portugal from June 2022 to December 2023).

The pipeline ingests day-ahead prices, cross-border flows, generation mix, and weather data from three public sources (OMIE, ENTSO-E Transparency Platform, Open-Meteo), loads them into a local DuckDB analytical database, and constructs a clean, analysis-ready panel covering 2019–2024 for up to eight European countries.

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
- [Design Choices and Rationale](#design-choices-and-rationale)
- [Example Usage](#example-usage)
- [Extending the Pipeline](#extending-the-pipeline)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Project Goals

1. **Build a reproducible data pipeline** that downloads, validates, and stores Iberian and European electricity-market data from three independent public APIs.
2. **Construct an hourly panel dataset** (one row per hour per country) combining prices, weather, cross-border flows, and calendar features — ready for causal-inference and structural econometric analysis.
3. **Provide economics-driven exploratory analysis** of the Iberian Exception: price dynamics, duration curves, cross-border flow leakage, merit-order shifts, and renewable-generation patterns.

---

## Repository Structure

```text
mibel-intelligence/
│
├── data/                          # ⛔ Not committed to git (in .gitignore)
│   ├── raw/                       # Original downloaded files, unmodified
│   │   ├── omie/                  #   OMIE day-ahead prices & generation
│   │   ├── entsoe/                #   ENTSO-E prices, flows, generation
│   │   └── weather/               #   Open-Meteo historical weather
│   ├── processed/                 # Cleaned panel Parquet files
│   └── mibel.duckdb              # DuckDB analytical database
│
├── src/
│   ├── __init__.py
│   ├── data/                      # Data pipeline scripts
│   │   ├── __init__.py
│   │   ├── omie_ingest.py         # OMIE market data ingestion
│   │   ├── entsoe_ingest.py       # ENTSO-E Transparency Platform ingestion
│   │   ├── weather_ingest.py      # Open-Meteo weather ingestion
│   │   ├── load_to_db.py          # Load raw Parquet → DuckDB tables
│   │   └── build_panel.py         # Construct hourly country panel
│   └── utils/                     # Shared utilities
│       ├── __init__.py
│       ├── dbutils.py             # DuckDB connection helpers
│       ├── dbschema.py            # Schema definitions & index creation
│       └── timezoneutils.py       # UTC normalisation, DST handling
│
├── notebooks/
│   └── 01_exploratory_analysis.ipynb   # Economics-driven EDA
│
├── docs/                          # Additional documentation
├── tests/                         # Unit tests
├── .env                           # ⛔ Not committed (API keys)
├── .gitignore
├── requirements.txt               # Pinned Python dependencies
└── README.md                      # ← You are here
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

Create a `.env` file in the project root:

```bash
# .env — do NOT commit this file (it is already in .gitignore)
ENTSOE_API_KEY=your_actual_key_here
```

To obtain an ENTSO-E key:
1. Register at [transparency.entsoe.eu](https://transparency.entsoe.eu/).
2. Email `transparency@entsoe.eu` to request API access.
3. You will receive a token — paste it into `.env`.

### 3. Download Raw Data

Each ingestion script downloads data from its respective API, chunks requests to respect rate limits, and saves raw Parquet files under `data/raw/`.

```bash
# OMIE — Iberian day-ahead prices & generation by technology
python -m src.data.omie_ingest

# ENTSO-E — Day-ahead prices for 15 EU countries, cross-border flows, generation
python -m src.data.entsoe_ingest

# Open-Meteo — Hourly weather for 7 Iberian/European locations (2019-2024)
python -m src.data.weather_ingest
```

**Output after this step:**

```text
data/raw/
├── omie/
│   ├── dayahead_prices_2019-01-01_2024-12-31.parquet
│   └── generation_2019-01-01_2024-12-31.parquet
├── entsoe/
│   ├── prices_ES_2019-01-01_2024-12-31.parquet
│   ├── prices_FR_2019-01-01_2024-12-31.parquet
│   ├── ...                              # one file per country
│   └── crossborder_flows_*.parquet
└── weather/
    ├── weather_Madrid_2019-01-01_2024-12-31.parquet
    ├── weather_Lisbon_2019-01-01_2024-12-31.parquet
    ├── ...                              # one file per location
    └── weather_all_2019-01-01_2024-12-31.parquet
```

> **Tip:** Downloads are chunked by time (3–6 months for ENTSO-E, 1 year for weather) with `time.sleep()` delays between requests. If a chunk fails, the script continues with the next chunk and logs the error — you can re-run safely without re-downloading successful chunks.

### 4. Create Database and Load Data

This step creates the DuckDB schema (tables + indexes) and inserts all raw Parquet data into structured tables.

```bash
python -m src.data.load_to_db
```

**What happens:**

1. `create_schema()` executes DDL to create five tables: `prices_dayahead`, `generation`, `crossborder_flows`, `weather`, and `bidcurves`.
2. Each loader function reads Parquet files from `data/raw/`, standardises column names, and inserts rows into the corresponding table using DuckDB's native DataFrame ingestion (`INSERT INTO table SELECT * FROM df`).
3. Indexes are created on `timestamp` and `country` columns for fast analytical queries.

**Verify the load:**

```python
from src.utils.dbschema import describe_schema
describe_schema()
```

### 5. Build the Analysis Panel

```bash
python -m src.data.build_panel
```

**What happens:**

1. A **complete hourly UTC index** is generated (every hour from 2019-01-01 to 2025-01-01), ensuring no gaps even across DST transitions.
2. **Prices** are queried from DuckDB and left-joined onto the hourly index for each country.
3. **Weather** data is aggregated from city-level to country-level (ES, PT) by averaging across locations (e.g., Madrid + Barcelona + Seville + Bilbao → Spain).
4. **Cross-border flows** are pivoted to wide format (one column per country pair, e.g., `ES_to_FR_mw`).
5. **Time features** are appended: `hour`, `day_of_week`, `month`, `year`, `quarter`, `is_weekend`, and `is_iberian_exception` (a binary flag for the policy period 2022-06-15 to 2023-12-31).
6. Data-quality diagnostics are logged (row counts, date range, missing-value rates).
7. The final panel is saved as a **Snappy-compressed Parquet** file.

**Output:**

```text
data/processed/main_panel_2019-01-01_2025-01-01.parquet
```

The panel has one row per hour per country (≈ 420,000 rows for 8 countries × 6 years) with columns for price, weather, flows, and calendar features.

### 6. Run Exploratory Analysis

```bash
jupyter notebook notebooks/01_exploratory_analysis.ipynb
```

The notebook contains economics-driven analyses including:

- **Price dynamics** — Time-series plots of Spanish and Portuguese prices with the Iberian Exception period shaded.
- **Price duration curves** — Distribution of prices before, during, and after the gas cap, showing whether peak pricing was compressed.
- **Spain vs. France divergence** — Monthly average prices comparing the capped Iberian market with uncapped France.
- **Cross-border flow leakage** — Spain → France flow volumes before and during the cap, quantifying subsidy leakage.
- **Market coupling / price splitting** — Frequency and timing of ES-PT price divergence.
- **Weather and renewable patterns** — Seasonal wind speed, solar radiation, and temperature patterns driving generation mix.
- **Hourly price profiles** — Typical daily price shape before, during, and after the Iberian Exception.

---

## Design Choices and Rationale

### Why DuckDB?

| Criterion | pandas (in-memory) | SQLite | **DuckDB** |
|---|---|---|---|
| Storage model | Row-oriented (DataFrame) | Row-oriented | **Column-oriented** |
| 50M-row aggregation | Slow (4+ GB RAM) | Slow (full row scan) | **Fast (reads only needed columns)** |
| Server required | No | No | **No** (single file) |
| Pandas integration | Native | Manual conversion | **Native (`SELECT * FROM df`)** |
| Parquet support | Via `read_parquet` | None | **Direct SQL on Parquet files** |

DuckDB is 10–100× faster than pandas for filtering, aggregating, and joining datasets at this scale, while requiring zero server configuration — it stores everything in a single `mibel.duckdb` file.

### Project Layout

- **`data/` excluded from git:** Raw data files can exceed several GB (especially bid curves at 50M+ rows). Only the code to generate data is versioned — anyone can reproduce the dataset by running the pipeline.
- **`src/` as a Python package:** Every directory contains `__init__.py`, enabling clean imports like `from src.data.omie_ingest import download_day_ahead_prices` from any script or notebook.
- **Separation of ingestion → loading → panel construction:** Each stage is idempotent. You can re-download one data source without rebuilding everything, or rebuild the panel without re-downloading.

### Schema Design

- **Timestamps as primary keys:** Every table uses `(timestamp, country)` or similar composite keys, making time-based analytical queries fast.
- **Two-letter ISO country codes** (`ES`, `PT`, `FR`, …) instead of full names — compact, standardised, and join-friendly.
- **Separate tables by granularity:** Hourly price aggregates and per-generator bid curves (multiple rows per hour) live in different tables, keeping queries clean and performant.
- **Indexes on `timestamp` and `country`:** Like a book's index, these let DuckDB jump directly to relevant rows instead of scanning entire tables.

### Timezone Handling

European electricity markets are a timezone minefield: OMIE uses CET/CEST, ENTSO-E returns UTC, and DST creates 23-hour days (spring) and 25-hour days (fall). The pipeline:

- **Normalises everything to UTC** once, in `timezoneutils.py`.
- **Generates a gapless hourly index** first, then left-joins all data sources onto it — so missing hours from DST transitions appear as explicit `NaN` rather than silent gaps.
- **Validates and warns** about duplicate or missing timestamps before panel construction.

### Panel Construction Strategy

- **Hour-index-first approach:** Build the complete time skeleton, then merge data onto it. This guarantees no hours are silently dropped and makes missing data visible.
- **Left joins throughout:** `panel.merge(prices, how="left")` keeps all hours even when a data source has gaps, producing `NaN` rather than dropping observations.
- **Weather aggregation:** City-level weather (Madrid, Barcelona, Seville, Bilbao → ES; Lisbon, Porto, Faro → PT) is averaged to country level — a pragmatic approximation weighting population centres.

---

## Example Usage

### Load the panel and compute summary statistics

```python
import pandas as pd
from pathlib import Path

# Load the processed panel
panel_file = next(Path("data/processed").glob("main_panel_*.parquet"))
df = pd.read_parquet(panel_file)

print(f"Rows: {len(df):,}")
print(f"Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")
print(f"Countries: {sorted(df['country'].unique())}")
```

### Average price by country and year

```python
summary = (
    df.groupby(["country", df["timestamp"].dt.year])["price_eur_mwh"]
      .mean()
      .reset_index(name="avg_price")
      .pivot(index="timestamp", columns="country", values="avg_price")
)
print(summary.round(2))
```

### Query the database directly

```python
from src.utils.dbutils import execute_query

# Average price during the Iberian Exception, by country
result = execute_query("""
    SELECT country,
           AVG(price_eur_mwh) AS avg_price,
           COUNT(*) AS hours
    FROM prices_dayahead
    WHERE timestamp >= '2022-06-15'
      AND timestamp <  '2024-01-01'
    GROUP BY country
    ORDER BY avg_price
""")
print(result)
```

### Quick plot: Spain vs. France monthly prices

```python
import matplotlib.pyplot as plt

es = df[df["country"] == "ES"].set_index("timestamp")["price_eur_mwh"].resample("M").mean()
fr = df[df["country"] == "FR"].set_index("timestamp")["price_eur_mwh"].resample("M").mean()

plt.figure(figsize=(14, 6))
plt.plot(es.index, es.values, label="Spain (capped)", linewidth=2)
plt.plot(fr.index, fr.values, label="France (no cap)", linewidth=2)
plt.axvspan("2022-06-15", "2023-12-31", alpha=0.1, color="red", label="Iberian Exception")
plt.ylabel("Monthly Avg Price (EUR/MWh)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
```

---

## Extending the Pipeline

| Extension | Where to Start |
|---|---|
| **Add bid-curve data** (50M+ rows) | Implement `download_bid_curves()` in `src/data/omie_ingest.py`; the `bidcurves` table and indexes already exist in the schema. |
| **Add more countries** | Append country codes to `COUNTRY_CODES` in `entsoe_ingest.py` and to the `countries` list in `build_panel()`. |
| **Add generation data to panel** | Create `build_generation_panel()` in `build_panel.py` following the same pattern as `build_weather_panel()`. |
| **Causal inference (Week 2)** | Use the panel's `is_iberian_exception` flag with synthetic control or difference-in-differences estimators. |
| **Structural modelling (Week 3)** | Query `bidcurves` to reconstruct supply/demand curves and estimate supply-function equilibria. |

---

## Troubleshooting

| Issue | Symptom | Solution |
|---|---|---|
| `ENTSOE_API_KEY not found` | Error on ENTSO-E download | Create `.env` in project root with `ENTSOE_API_KEY=your_key`. Restart Python session. |
| Timezone warnings | `"Timestamp column has no timezone"` | Informational only — the code assumes UTC and converts. Safe to ignore. |
| Memory errors on large queries | `MemoryError` or system freeze | Always filter in SQL before loading to pandas: `WHERE timestamp >= '2022-06-01' AND country = 'ES'`. |
| Duplicate timestamps in panel | More rows than expected | `handle_dst_transitions()` removes duplicates. Ensure you call it, or use UTC throughout. |
| OMIE HTTP 403 | Permission/rate-limit error | Wait 1 hour (rate limit resets), check [omie.es](https://www.omie.es) is up, add `time.sleep(5)` between requests. |
| 30%+ NaN in panel prices | Missing data after merge | Re-run download for the failing country/date range. Verify all timestamps are UTC before merging. |

---

## License

[MIT](LICENSE) — or specify your preferred licence here.
