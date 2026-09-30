"""
Load raw data files into DuckDB database.

This script reads the Parquet files created by the ingestion scripts
and loads them into the structured database tables.

It is the single supported loader (the root-level rescue scripts it
replaced are in git history). Every loader:
- inserts with an explicit column list, never positional SELECT * (D-10)
- globs only per-unit raw files, never the combined convenience files (D-09)
- replaces a table's contents inside one transaction, so a failure
  part-way leaves the previous contents in place instead of an empty
  table (D-05)
- stores tz-aware UTC timestamps into TIMESTAMPTZ columns (D-01)

Run: python -m src.data.load_to_db
"""

import pandas as pd
from pathlib import Path
import logging

from src.utils.db_utils import get_connection
from src.utils.db_schema import create_schema
from src.utils.timezone_utils import (
    omie_hours_to_utc,
    hours_in_market_day,
    assert_utc_hourly,
    normalize_to_utc,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"

OMIE_CONCEPTS = {'PRICE_SP': 'ES', 'PRICE_PT': 'PT'}

WEATHER_COLUMNS = [
    'timestamp', 'location', 'latitude', 'longitude', 'temperature_c',
    'wind_speed_10m', 'wind_speed_100m', 'wind_direction_100m',
    'solar_radiation', 'dni', 'diffuse_radiation', 'cloud_cover',
]
FLOW_COLUMNS = ['timestamp', 'country_from', 'country_to', 'flow_mw']
PRICE_COLUMNS = ['timestamp', 'country', 'price_eur_mwh', 'energy_mwh']
# Primary keys, as declared in src/utils/db_schema.py
PRICE_KEYS = ['timestamp', 'country']
WEATHER_KEYS = ['timestamp', 'location']
FLOW_KEYS = ['timestamp', 'country_from', 'country_to']


def _raw_files(directory: Path, pattern: str) -> list:
    """Per-unit raw files only: combined files ('*_all_*') are never loaded."""
    return sorted(f for f in directory.glob(pattern) if '_all_' not in f.name)


def _replace_rows(conn, table: str, columns: list, keys: list, df: pd.DataFrame,
                  scope_sql: str = "TRUE", scope_params: list = None) -> None:
    """
    Make the rows of `table` matching `scope_sql` equal to `df`, atomically.

    DuckDB 0.10 rejects deleting and re-inserting the same primary key inside
    one transaction (a documented index limitation), so a plain DELETE +
    INSERT only works on an empty table. Instead, within one transaction:
    delete in-scope rows whose key is absent from `df`, then upsert `df` with
    an explicit column list. No key is ever deleted and re-inserted.

    Callers deduplicate and assert key uniqueness first, so `df` can never
    carry two values for one key (the silent-loss mode of D-04).
    """
    cols = ', '.join(columns)
    key_match = ' AND '.join(f"f.{k} = t.{k}" for k in keys)
    frame = df[columns]  # noqa: F841 -- referenced by name in the SQL below
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f"""
            DELETE FROM {table} t
            WHERE ({scope_sql})
              AND NOT EXISTS (SELECT 1 FROM frame f WHERE {key_match})
        """, scope_params or [])
        updates = ', '.join(f"{c} = excluded.{c}" for c in columns if c not in keys)
        conn.execute(f"""
            INSERT INTO {table} ({cols}) SELECT {cols} FROM frame
            ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}
        """)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def omie_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape a raw OMIE file (DATE, CONCEPT, H1..H25) to long UTC prices.

    H{n} is the n-th hour of the market day in CET/CEST, so H25 on the
    autumn-fallback day is a real, distinct hour and is kept (D-03/D-04).
    """
    prices = df[df['CONCEPT'].isin(OMIE_CONCEPTS)]
    hour_cols = [c for c in prices.columns if c.startswith('H') and c[1:].isdigit()]
    long = prices.melt(id_vars=['DATE', 'CONCEPT'], value_vars=hour_cols,
                       var_name='hour_col', value_name='price_eur_mwh')
    long = long.dropna(subset=['price_eur_mwh']).reset_index(drop=True)
    hour_num = long['hour_col'].str[1:].astype(int)

    # Every market day must have exactly as many prices as it has hours (23/24/25)
    per_day = long.groupby(['DATE', 'CONCEPT']).size()
    expected = hours_in_market_day(per_day.index.get_level_values('DATE').to_series()).values
    bad = per_day[per_day.values != expected]
    if not bad.empty:
        logger.warning(f"  {len(bad)} OMIE day/concept pairs have an unexpected hour count: "
                       f"{bad.head().to_dict()}")

    return pd.DataFrame({
        'timestamp': omie_hours_to_utc(long['DATE'], hour_num),
        'country': long['CONCEPT'].map(OMIE_CONCEPTS),
        'price_eur_mwh': long['price_eur_mwh'].astype(float),
        'energy_mwh': float('nan'),
    })


def load_omie_prices(conn):
    """
    Load OMIE day-ahead prices into database.

    OMIE data comes in WIDE format (H1, H2, H3... columns).
    We transform to LONG format, in UTC, for the database.
    """
    logger.info("Loading OMIE prices...")

    omie_dir = RAW_DIR / "omie"
    price_files = _raw_files(omie_dir, "day_ahead_prices_*.parquet") if omie_dir.exists() else []

    if not price_files:
        logger.warning(f"No OMIE price files found in {omie_dir}")
        return

    frames = [omie_wide_to_long(pd.read_parquet(f)) for f in price_files]
    df_long = pd.concat(frames, ignore_index=True)

    # Chunk files can overlap at their boundaries. Identical rows are expected;
    # a conflicting price for the same hour is a data problem and must be loud.
    df_long = df_long.drop_duplicates()
    conflicts = df_long.duplicated(subset=['timestamp', 'country'], keep=False)
    if conflicts.any():
        raise ValueError(f"{int(conflicts.sum())} OMIE rows have conflicting prices "
                         f"for the same (timestamp, country):\n{df_long[conflicts].head(10)}")
    assert_utc_hourly(df_long, group_col='country')

    _replace_rows(conn, 'prices_day_ahead', PRICE_COLUMNS, PRICE_KEYS, df_long,
                  "t.country IN ('ES', 'PT')")

    logger.info(f"  [OK] Loaded {len(df_long):,} OMIE price records from {len(price_files)} files")


def load_entsoe_prices(conn):
    """Load ENTSO-E prices into database."""
    logger.info("Loading ENTSO-E prices...")

    entsoe_dir = RAW_DIR / "entsoe"
    price_files = _raw_files(entsoe_dir, "prices_*.parquet") if entsoe_dir.exists() else []

    if not price_files:
        logger.warning(f"No ENTSO-E price files found in {entsoe_dir}")
        return

    df = pd.concat([pd.read_parquet(f) for f in price_files], ignore_index=True)
    df = normalize_to_utc(df)
    df = df.dropna(subset=['price_eur_mwh'])
    # ENTSO-E can publish sub-hourly resolution for some zones; the panel is hourly
    df = (df.groupby(['country', df['timestamp'].dt.floor('h')])['price_eur_mwh'].mean()
            .reset_index())
    df['energy_mwh'] = float('nan')
    assert_utc_hourly(df, group_col='country')

    countries = sorted(df['country'].unique())
    placeholders = ', '.join('?' for _ in countries)
    _replace_rows(conn, 'prices_day_ahead', PRICE_COLUMNS, PRICE_KEYS, df,
                  f"t.country IN ({placeholders})", countries)

    logger.info(f"  [OK] Loaded {len(df):,} ENTSO-E price records for {countries}")


def load_weather_data(conn):
    """Load weather data into database."""
    logger.info("Loading weather data...")

    weather_dir = RAW_DIR / "weather"
    weather_files = _raw_files(weather_dir, "weather_*.parquet") if weather_dir.exists() else []

    if not weather_files:
        logger.warning(f"No weather files found in {weather_dir}")
        return

    df = pd.concat([pd.read_parquet(f) for f in weather_files], ignore_index=True)
    # Open-Meteo is requested with timezone=UTC; raw files written before the
    # D-01 fix hold that UTC as tz-naive values, which normalize_to_utc assumes
    df = normalize_to_utc(df)
    # Yearly chunks share their boundary day
    df = df.drop_duplicates(subset=['timestamp', 'location'])
    assert_utc_hourly(df, group_col='location')

    _replace_rows(conn, 'weather', WEATHER_COLUMNS, WEATHER_KEYS, df)

    logger.info(f"  [OK] Loaded {len(df):,} weather records for {df['location'].nunique()} locations")


def load_cross_border_flows(conn):
    """Load cross-border flow data."""
    logger.info("Loading cross-border flows...")

    entsoe_dir = RAW_DIR / "entsoe"
    flow_files = _raw_files(entsoe_dir, "cross_border_flows_*.parquet") if entsoe_dir.exists() else []

    if not flow_files:
        logger.warning(f"No cross-border flow files found in {entsoe_dir}")
        return

    df = pd.concat([pd.read_parquet(f) for f in flow_files], ignore_index=True)
    df = df.rename(columns={'from_country': 'country_from', 'to_country': 'country_to'})
    df = normalize_to_utc(df)
    df = df.dropna(subset=['flow_mw'])
    # Hourly resolution, one row per directed pair and hour
    df = (df.groupby(['country_from', 'country_to', df['timestamp'].dt.floor('h')])['flow_mw']
            .mean().reset_index())
    assert_utc_hourly(df.assign(pair=df['country_from'] + df['country_to']), group_col='pair')

    _replace_rows(conn, 'cross_border_flows', FLOW_COLUMNS, FLOW_KEYS, df)

    logger.info(f"  [OK] Loaded {len(df):,} cross-border flow records")


def load_all_data():
    """
    Master function to load all raw data into database.

    Run this after downloading all data to populate the database.
    """
    logger.info("="*60)
    logger.info("LOADING ALL DATA INTO DATABASE")
    logger.info("="*60)

    # Create schema if it doesn't exist (and migrate a timezone-naive one)
    create_schema()

    with get_connection(readonly=False) as conn:
        load_omie_prices(conn)
        load_entsoe_prices(conn)
        load_weather_data(conn)
        load_cross_border_flows(conn)

        logger.info("="*60)
        logger.info("DATA LOADING COMPLETE")
        logger.info("="*60)

        # Show database stats
        summary = conn.execute("""
            SELECT country, COUNT(*) AS row_count,
                   MIN(timestamp) AS start_utc, MAX(timestamp) AS end_utc
            FROM prices_day_ahead
            GROUP BY country
            ORDER BY country
        """).fetchdf()
        logger.info("Database summary (prices_day_ahead):\n" + summary.to_string(index=False))
        for table in ['weather', 'cross_border_flows']:
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"  {table}: {n:,} rows")


if __name__ == "__main__":
    load_all_data()
