# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

End-to-end data pipeline for the **Iberian electricity market**, producing an hourly country-level panel for econometric analysis of the *Iberian Exception* (gas-price cap applied to Spain and Portugal, June 15 2022 – December 31 2023). The pipeline downloads data from OMIE, ENTSO-E, and Open-Meteo, stores it in DuckDB, and builds a clean panel for causal inference.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
python create_env.py             # Interactive script to set ENTSOE_API_KEY in .env
```

## Commands

### Run the pipeline

```bash
python -m src.pipeline 30              # Last 30 days end to end (Windows: mibel 30)
```

Or stage by stage, in order:

```bash
python -m src.data.omie_ingest        # Download OMIE prices 2022-2023 (no API key; --bid-curves sample|full --yes)
python -m src.data.entsoe_ingest      # Download ENTSO-E data (API key required)
python -m src.data.weather_ingest     # Download Open-Meteo weather (free)
python -m src.data.load_to_db         # Load all raw Parquet → DuckDB
python -m src.data.build_panel        # Build hourly country panel → data/processed/ (--start --end --countries)
python diagnose_pipeline.py           # End-to-end health check
```

### Tests

```bash
python -m pytest tests/                       # All tests (conftest.py isolates them in a temp DB)
python -m pytest tests/test_timezone_utils.py # Single test file
```

### Linting and formatting

```bash
black src/
ruff check src/
```

## Architecture

### Pipeline stages

1. **Ingest** (`src/data/*_ingest.py`): Download raw data from APIs → save as Snappy-compressed Parquet in `data/raw/`
2. **Load** (`src/data/load_to_db.py`, the only loader): Transform Parquet files, dedupe and assert unique UTC keys, then DELETE+INSERT per table in one transaction with explicit column lists
3. **Build panel** (`src/data/build_panel.py`): Create gapless hourly UTC index → left-join all sources → save to `data/processed/`

Each stage is idempotent and independent.

### Database layer (`src/utils/`)

- `db_utils.py`: Central DuckDB connection management. DB at `data/mibel.duckdb`. Use `get_connection()` / `execute_query(sql, params)`. Every connection runs `SET TimeZone='UTC'`.
- `db_schema.py`: Creates 5 tables: `prices_day_ahead`, `generation`, `cross_border_flows`, `weather`, `bid_curves`. All `timestamp` columns are `TIMESTAMPTZ`; old naive tables are migrated automatically.
- `timezone_utils.py`: All timestamps normalized to UTC once at ingestion. Use `market_window_utc()` (the one date-window convention), `omie_hours_to_utc()`, `create_hour_index()`, `add_time_features()` (calendar features in CET/CEST market time), `assert_utc_hourly()`.

### Panel structure

Output: `data/processed/main_panel_YYYY-MM-DD_YYYY-MM-DD.parquet`
Rows: one per hour per country (8 countries × 17,520 hours = 140,160 rows for 2022-01-01..2023-12-31)
Key column: `is_iberian_exception` (binary flag for policy analysis)

### Data sources

| Source | Countries | Requires key |
|--------|-----------|--------------|
| OMIE | Spain, Portugal | No |
| ENTSO-E | 12 donor countries + ES/PT/FR flows | Yes (register at transparency.entsoe.eu) |
| Open-Meteo | 65 locations across the 8 panel countries (`src/data/weather_locations.py`) | No |

### Key design decisions

- **UTC-only storage**: all timezone normalization happens once in `timezone_utils.py`; DST creates 23/25-hour days that must be handled explicitly
- **Gapless index first**: `create_hour_index()` builds the complete time skeleton; data is left-joined onto it so missing hours become explicit NaN rather than dropped rows
- **DuckDB**: columnar, serverless (single file), native Pandas integration — chosen for 50M+ row bid curve aggregations
- **Bid curves are optional**: `download_all_bid_curves()` takes ~24 hours; use `download_bid_curves_sample()` for testing
- **Weather aggregated to country level**: levels = unweighted mean of each country's `main` cities; dispersion (`<var>_sd`, `<var>_range`) = across all its `main` + `reference` cities
