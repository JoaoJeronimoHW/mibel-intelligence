"""
Build the main hourly panel for analysis.

A "panel" in econometrics is a dataset with:
- Multiple entities (countries)
- Multiple time periods (hours)
- Variables measured for each entity-time combination

Our panel structure:
- Rows: every UTC hour of the requested market days, per country
- Columns: Prices, weather, flows, time features
- Multiple countries: Each country gets its own rows

Window convention (one, used everywhere): start_date and end_date are
inclusive Iberian market days; every query and the hour skeleton use the
same half-open UTC window from timezone_utils.market_window_utc().

Run: python -m src.data.build_panel --start 2022-01-01 --end 2023-12-31
"""

import argparse
import pandas as pd
from pathlib import Path
import logging

from src.data.weather_locations import LOCATIONS as WEATHER_LOCATIONS
from src.utils.db_utils import execute_query
from src.utils.timezone_utils import (
    create_hour_index,
    add_time_features,
    market_window_utc,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_COUNTRIES = ['ES', 'PT', 'FR', 'DE', 'IT', 'NL', 'BE', 'AT']

WEATHER_VARIABLES = ['temperature_c', 'wind_speed_100m', 'solar_radiation', 'dni', 'cloud_cover']
# Panel column order: levels, then dispersion (sd, range) per variable
WEATHER_COLUMNS = (WEATHER_VARIABLES
                   + [f'{v}_sd' for v in WEATHER_VARIABLES]
                   + [f'{v}_range' for v in WEATHER_VARIABLES])


def fix_timestamp_dtype(df: pd.DataFrame, col: str = 'timestamp') -> pd.DataFrame:
    """
    Ensure timestamp column is datetime64[ns, UTC].
    DuckDB returns datetime64[us, UTC], and pandas merges keys of different
    dtypes into all-NaN without complaint (D-06).
    """
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], utc=True).astype('datetime64[ns, UTC]')
    return df


def _window_query(sql: str, start_date: str, end_date: str) -> pd.DataFrame:
    start, end_exclusive = market_window_utc(start_date, end_date)
    return fix_timestamp_dtype(execute_query(sql, [start, end_exclusive]))


def build_price_panel(start_date: str, end_date: str) -> pd.DataFrame:
    """Build panel with prices for all countries."""

    logger.info("Building price panel...")

    df = _window_query("""
        SELECT timestamp, country, price_eur_mwh
        FROM prices_day_ahead
        WHERE timestamp >= ? AND timestamp < ?
        ORDER BY timestamp, country
    """, start_date, end_date)

    logger.info(f"Price panel: {len(df):,} rows, {df['country'].nunique()} countries")
    return df


def build_weather_panel(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Build panel with weather data: one row per (timestamp, country).

    Levels (e.g. temperature_c): unweighted mean across the country's 'main'
    locations. Capacity weighting would be the principled choice but needs
    regional renewable-capacity data we have not acquired.

    Intra-country dispersion, per variable, across ALL the country's
    locations ('main' + 'reference'):
      <var>_sd:    sample standard deviation across locations (ddof=1)
      <var>_range: max - min across locations
    Locations are listed in weather_locations.py.
    """
    logger.info("Building weather panel...")

    df = _window_query(f"""
        SELECT timestamp, location, {', '.join(WEATHER_VARIABLES)}
        FROM weather
        WHERE timestamp >= ? AND timestamp < ?
        ORDER BY timestamp, location
    """, start_date, end_date)

    meta = pd.DataFrame.from_dict(WEATHER_LOCATIONS, orient='index')[['country', 'role']]
    df = df.join(meta, on='location', how='inner')  # drops locations no longer registered

    missing = sorted(set(WEATHER_LOCATIONS) - set(df['location']))
    if missing:
        logger.warning(f"No weather rows in window for {len(missing)} registered location(s): "
                       f"{missing} -- run weather_ingest for this window")

    keys = ['timestamp', 'country']
    levels = (df[df['role'] == 'main']
              .groupby(keys)[WEATHER_VARIABLES].mean())
    grouped = df.groupby(keys)[WEATHER_VARIABLES]
    sd = grouped.std().add_suffix('_sd')
    spread = (grouped.max() - grouped.min()).add_suffix('_range')

    weather_panel = levels.join([sd, spread], how='outer').reset_index()
    weather_panel = weather_panel[keys + WEATHER_COLUMNS]

    logger.info(f"Weather panel: {len(weather_panel):,} rows, "
                f"{weather_panel['country'].nunique()} countries")
    return weather_panel


def build_flows_panel(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Build panel with cross-border flows.

    We'll pivot this so each country pair gets its own column.
    """
    logger.info("Building flows panel...")

    df = _window_query("""
        SELECT timestamp, country_from, country_to, flow_mw
        FROM cross_border_flows
        WHERE timestamp >= ? AND timestamp < ?
        ORDER BY timestamp
    """, start_date, end_date)

    if df.empty:
        logger.info("Flows panel: no flow data in window")
        return pd.DataFrame({'timestamp': pd.Series(dtype='datetime64[ns, UTC]')})

    # Create flow column names
    df['flow_name'] = df['country_from'] + '_to_' + df['country_to'] + '_mw'

    # Pivot to wide format (the loader guarantees one row per pair and hour)
    flows_wide = df.pivot(index='timestamp', columns='flow_name', values='flow_mw').reset_index()
    flows_wide.columns.name = None

    logger.info(f"Flows panel: {len(flows_wide):,} rows, {len(flows_wide.columns)-1} flow pairs")
    return flows_wide


def _merge_checked(left: pd.DataFrame, right: pd.DataFrame, on: list, validate: str,
                   name: str) -> pd.DataFrame:
    """Left-join and report the match rate, so a silent all-NaN join is visible (D-06)."""
    merged = left.merge(right, on=on, how='left', validate=validate, indicator=True)
    assert len(merged) == len(left), f"{name} merge changed the row count"
    matched = (merged['_merge'] == 'both').mean()
    logger.info(f"  {name}: {matched:.1%} of panel rows matched")
    if len(right) and matched == 0:
        raise AssertionError(f"{name}: {len(right):,} source rows but no panel row matched "
                             "-- timestamp dtype or window mismatch")
    return merged.drop(columns='_merge')


def build_main_panel(start_date: str = '2022-01-01',
                     end_date: str = '2023-12-31',
                     countries: list = None) -> pd.DataFrame:
    """
    Build the main analysis panel combining all data sources.

    This is the key dataset for your entire analysis!

    Args:
        start_date: 'YYYY-MM-DD' (inclusive market day)
        end_date: 'YYYY-MM-DD' (inclusive market day)
        countries: List of country codes to include (None = DEFAULT_COUNTRIES)

    Returns:
        DataFrame with one row per hour per country
    """
    logger.info("="*60)
    logger.info("BUILDING MAIN ANALYSIS PANEL")
    logger.info("="*60)

    if countries is None:
        countries = DEFAULT_COUNTRIES

    # 1. Skeleton: every hour x every country (ensures no gaps)
    logger.info("1. Creating hour index...")
    hour_index = create_hour_index(start_date, end_date)
    skeleton = hour_index.merge(pd.DataFrame({'country': countries}), how='cross')

    # 2-4. Sources
    logger.info("2. Loading prices...")
    prices = build_price_panel(start_date, end_date)
    prices = prices[prices['country'].isin(countries)]

    logger.info("3. Loading weather...")
    weather = build_weather_panel(start_date, end_date)

    logger.info("4. Loading cross-border flows...")
    flows = build_flows_panel(start_date, end_date)

    # 5. Merge on the compound key; flows are a property of the hour (D-08)
    logger.info("5. Merging all data sources...")
    main_panel = _merge_checked(skeleton, prices, ['timestamp', 'country'], 'one_to_one', 'prices')
    main_panel = _merge_checked(main_panel, weather, ['timestamp', 'country'], 'one_to_one', 'weather')
    main_panel = _merge_checked(main_panel, flows, ['timestamp'], 'many_to_one', 'flows')

    # 6. Add time features
    logger.info("6. Adding time features...")
    main_panel = add_time_features(main_panel)

    # 7. Sort
    main_panel = main_panel.sort_values(['country', 'timestamp']).reset_index(drop=True)

    # 8. Data quality checks
    logger.info("7. Data quality checks...")
    expected_rows = len(hour_index) * len(countries)
    assert len(main_panel) == expected_rows, (
        f"panel has {len(main_panel):,} rows, expected {expected_rows:,}")
    logger.info(f"  Total rows: {len(main_panel):,} ({len(countries)} countries x {len(hour_index):,} hours)")
    logger.info(f"  Date range (UTC): {main_panel['timestamp'].min()} to {main_panel['timestamp'].max()}")
    missing = main_panel.groupby('country')['price_eur_mwh'].apply(lambda s: s.isna().mean())
    for country, rate in missing.items():
        logger.info(f"  Missing prices {country}: {rate:.2%}")

    # 9. Save
    output_file = PROCESSED_DIR / f"main_panel_{start_date}_{end_date}.parquet"
    main_panel.to_parquet(output_file, compression='snappy', index=False)
    logger.info(f"Saved to: {output_file}")

    logger.info("="*60)
    logger.info("PANEL CONSTRUCTION COMPLETE")
    logger.info("="*60)

    return main_panel


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build the hourly country panel.")
    parser.add_argument('--start', default='2022-01-01', help="first market day (inclusive)")
    parser.add_argument('--end', default='2023-12-31', help="last market day (inclusive)")
    parser.add_argument('--countries', nargs='+', default=DEFAULT_COUNTRIES)
    args = parser.parse_args()

    panel = build_main_panel(args.start, args.end, args.countries)

    # Show summary
    print("\n" + "="*60)
    print("PANEL SUMMARY")
    print("="*60)
    panel.info()
    print("\nPrice statistics by country:")
    print(panel.groupby('country')['price_eur_mwh'].describe())
