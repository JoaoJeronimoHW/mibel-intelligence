"""
ENTSO-E Transparency Platform data ingestion.

ENTSO-E provides pan-European electricity data. The entsoe-py library
makes API access easier, but there are quirks to handle.

IMPORTANT SETUP:
1. Register at transparency.entsoe.eu
2. Email transparency@entsoe.eu requesting API access
3. You'll receive an API key
4. Create a .env file in project root with:
   ENTSOE_API_KEY=your_key_here
   (or run: python create_env.py)

Run: python -m src.data.entsoe_ingest --start 2022-01-01 --end 2023-12-31
"""

import argparse
import pandas as pd
from pathlib import Path
import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor
import time

from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError

# Load environment variables (for API key)
from dotenv import load_dotenv
import os

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Data directory
RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw" / "entsoe"
RAW_DIR.mkdir(parents=True, exist_ok=True)
# Combined convenience files live in a subdirectory so the loader's
# prices_*.parquet glob can never pick them up (D-09)
COMBINED_DIR = RAW_DIR / "combined"

# Request bounds are expressed in the platform's reference zone (CET/CEST)
REQUEST_TZ = 'Europe/Brussels'


# ENTSO-E uses country codes. Here are the main ones for donor pool:
COUNTRY_CODES = {
    'Spain': 'ES',
    'Portugal': 'PT',
    'France': 'FR',
    'Germany': 'DE',  # Germany has multiple bidding zones
    'Italy': 'IT',
    'Netherlands': 'NL',
    'Belgium': 'BE',
    'Austria': 'AT',
    'Denmark': 'DK',
    'Sweden': 'SE',
    'Norway': 'NO',
    'Finland': 'FI',
    'Poland': 'PL',
    'Czech Republic': 'CZ',
    'Switzerland': 'CH',
}

# Day-ahead prices are published per *bidding zone*. These countries are not
# a single zone, so the country code returns no prices; query the zone below
# and store the result under the two-letter country code.
PRICE_BIDDING_ZONES = {
    'DE': 'DE_LU',    # Germany-Luxembourg zone (since Oct 2018)
    'IT': 'IT_NORD',  # Northern Italy, the zone coupled with FR/AT/CH
    'DK': 'DK_1',     # West Denmark
    'SE': 'SE_3',     # Stockholm zone
    'NO': 'NO_1',     # Oslo zone
}

DONOR_COUNTRIES = ['FR', 'DE', 'IT', 'NL', 'BE', 'AT',
                   'DK', 'SE', 'NO', 'FI', 'PL', 'CZ']

# Directed pairs: physical flows are reported per direction, never netted
FLOW_PAIRS = [('ES', 'PT'), ('PT', 'ES'), ('ES', 'FR'), ('FR', 'ES')]

DEFAULT_START = '2022-01-01'
DEFAULT_END = '2023-12-31'


def get_entsoe_client() -> EntsoePandasClient:
    """
    Initialize ENTSO-E API client with your API key.

    Why separate function?
    - API key loading is centralized
    - Easier to add error handling for missing keys
    - Can be reused across multiple functions
    """
    api_key = os.getenv('ENTSOE_API_KEY')

    if not api_key:
        raise ValueError(
            "ENTSOE_API_KEY not found in environment variables.\n"
            "Create a .env file in project root with:\n"
            "ENTSOE_API_KEY=your_key_here"
        )

    return EntsoePandasClient(api_key=api_key)


def _request_bounds(start_date: str, end_date: str):
    """Inclusive 'YYYY-MM-DD' days -> half-open tz-aware request window."""
    start = pd.Timestamp(start_date, tz=REQUEST_TZ)
    end = pd.Timestamp(end_date, tz=REQUEST_TZ) + pd.Timedelta(days=1)
    return start, end


def _chunks(start_date: str, end_date: str, months: int):
    """Split inclusive [start_date, end_date] into non-overlapping inclusive chunks."""
    current = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    while current <= end:
        chunk_end = min(current + pd.DateOffset(months=months) - pd.Timedelta(days=1), end)
        yield current.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')
        current = chunk_end + pd.Timedelta(days=1)


def download_day_ahead_prices(country_code: str,
                               start_date: str,
                               end_date: str,
                               client: EntsoePandasClient = None) -> pd.DataFrame:
    """
    Download day-ahead prices for a single country.

    Args:
        country_code: Two-letter code (e.g., 'FR' for France)
        start_date: 'YYYY-MM-DD' (inclusive)
        end_date: 'YYYY-MM-DD' (inclusive)
        client: optional shared client

    Returns:
        DataFrame with columns: timestamp (UTC), price_eur_mwh, country.
        Empty if the platform has no data for the window.
    """
    area = PRICE_BIDDING_ZONES.get(country_code, country_code)
    logger.info(f"Downloading prices for {country_code} ({area}): {start_date} to {end_date}")

    client = client or get_entsoe_client()
    start, end = _request_bounds(start_date, end_date)

    try:
        prices = client.query_day_ahead_prices(area, start=start, end=end)
    except NoMatchingDataError:
        logger.warning(f"No data available for {country_code} in date range")
        return pd.DataFrame()

    # entsoe-py includes the end hour; keep the half-open window
    prices = prices[prices.index < end]

    # Convert Series to DataFrame
    df = prices.reset_index()
    df.columns = ['timestamp', 'price_eur_mwh']
    df['country'] = country_code

    # Convert timestamp to UTC (standard timezone)
    # Why UTC? All our data should use one timezone to avoid confusion
    df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

    logger.info(f"Downloaded {len(df)} price records for {country_code}")
    return df


def download_cross_border_flows(country_from: str,
                                 country_to: str,
                                 start_date: str,
                                 end_date: str,
                                 client: EntsoePandasClient = None) -> pd.DataFrame:
    """
    Download physical electricity flows between two countries.

    Why important?
    The Iberian Exception caused Spain to export more to France.
    We need to quantify this "leakage" of the subsidy.

    Args:
        country_from: Origin country code (e.g., 'ES')
        country_to: Destination country code (e.g., 'FR')
        start_date: 'YYYY-MM-DD' (inclusive)
        end_date: 'YYYY-MM-DD' (inclusive)
        client: optional shared client

    Returns:
        DataFrame with columns: timestamp (UTC), flow_mw (positive = from->to)
    """
    logger.info(f"Downloading cross-border flows {country_from} -> {country_to}: "
                f"{start_date} to {end_date}")

    client = client or get_entsoe_client()
    start, end = _request_bounds(start_date, end_date)

    # ENTSO-E sends flows as curve type A03 (repeated values omitted), which
    # entsoe-py < 0.8 could not parse ("Length mismatch"); 0.8.1 is pinned.
    try:
        flows = client.query_crossborder_flows(
            country_code_from=country_from,
            country_code_to=country_to,
            start=start,
            end=end
        )
    except NoMatchingDataError:
        logger.warning(f"No flow data for {country_from}->{country_to}")
        return pd.DataFrame()

    flows = flows[(flows.index >= start) & (flows.index < end)]
    df = flows.rename('flow_mw').rename_axis('timestamp').reset_index()

    df['from_country'] = country_from
    df['to_country'] = country_to
    df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

    logger.info(f"Downloaded {len(df)} flow records")
    return df


def download_generation_by_country(country_code: str,
                                   start_date: str,
                                   end_date: str) -> pd.DataFrame:
    """
    Download actual generation by fuel type for a country.

    Returns:
        DataFrame with: timestamp, fuel_type, generation_mw, country
        Fuel types: wind, solar, hydro, gas, coal, nuclear, etc.
    """
    logger.info(f"Downloading generation data for {country_code}")

    client = get_entsoe_client()
    start, end = _request_bounds(start_date, end_date)

    try:
        gen = client.query_generation(country_code, start=start, end=end)
    except NoMatchingDataError:
        logger.warning(f"No generation data for {country_code}")
        return pd.DataFrame()

    # Convert to long format (one row per timestamp-fuel combination)
    df = gen.reset_index().melt(
        id_vars=['index'],
        var_name='fuel_type',
        value_name='generation_mw'
    )
    df.rename(columns={'index': 'timestamp'}, inplace=True)
    df['country'] = country_code
    df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

    logger.info(f"Downloaded generation data: {len(df)} records")
    return df


# Requests are mostly server wait time (~5 s each), so a few run in parallel.
# 4 workers stays far below ENTSO-E's limit of 400 requests per minute.
MAX_WORKERS = 4


def _download_chunked(fetch, start_date: str, end_date: str, chunk_months: int,
                      label: str, failed: list) -> pd.DataFrame:
    """Run fetch(chunk_start, chunk_end) per chunk; log and continue on failure."""
    parts = []
    chunks = list(_chunks(start_date, end_date, chunk_months))
    for i, (chunk_start, chunk_end) in enumerate(chunks):
        try:
            df = fetch(chunk_start, chunk_end)
            if not df.empty:
                parts.append(df)
        except Exception as e:
            logger.error(f"Failed chunk {chunk_start}..{chunk_end} for {label}: {e}")
            failed.append((label, chunk_start))
        # Rate limiting: pause between consecutive requests of the same series
        if i < len(chunks) - 1:
            time.sleep(1)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _parallel(tasks: dict) -> dict:
    """Run {label: zero-arg function} on a small thread pool; returns {label: result}."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {label: pool.submit(fn) for label, fn in tasks.items()}
        return {label: future.result() for label, future in futures.items()}


def download_all_countries_prices(country_codes: List[str],
                                  start_date: str,
                                  end_date: str,
                                  chunk_months: int = 3,
                                  failed: list = None) -> pd.DataFrame:
    """
    Download prices for multiple countries, handling rate limits.

    Countries are fetched in parallel (MAX_WORKERS); within a country, time
    chunks are fetched one after another with a pause in between.

    Args:
        country_codes: List of country codes to download
        start_date: 'YYYY-MM-DD' (inclusive)
        end_date: 'YYYY-MM-DD' (inclusive)
        chunk_months: Download this many months at a time per country
        failed: list that collects (label, chunk_start) of failed chunks

    Returns:
        Combined DataFrame with all countries
    """
    logger.info(f"Downloading prices for {len(country_codes)} countries")

    get_entsoe_client()  # fail fast if the API key is missing
    failed = failed if failed is not None else []

    def fetch_country(country):
        client = get_entsoe_client()  # one client (HTTP session) per thread
        return _download_chunked(
            lambda s, e: download_day_ahead_prices(country, s, e, client=client),
            start_date, end_date, chunk_months, f"prices {country}", failed)

    results = _parallel({c: (lambda c=c: fetch_country(c)) for c in country_codes})

    all_data = []
    for country, country_df in results.items():
        if country_df.empty:
            logger.error(f"No prices downloaded for {country}")
            continue
        all_data.append(country_df)
        # Save individual country file (the unit the loader reads)
        output_file = RAW_DIR / f"prices_{country}_{start_date}_{end_date}.parquet"
        country_df.to_parquet(output_file, compression='snappy', index=False)

    if not all_data:
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total records: {len(combined)}")

    COMBINED_DIR.mkdir(exist_ok=True)
    output_file = COMBINED_DIR / f"prices_all_countries_{start_date}_{end_date}.parquet"
    combined.to_parquet(output_file, compression='snappy', index=False)

    return combined


def download_all_flows(start_date: str, end_date: str, chunk_months: int = 3,
                       failed: list = None) -> pd.DataFrame:
    """Download every directed pair in FLOW_PAIRS (in parallel), one raw file per pair."""
    get_entsoe_client()  # fail fast if the API key is missing
    failed = failed if failed is not None else []

    def fetch_pair(country_from, country_to):
        client = get_entsoe_client()
        return _download_chunked(
            lambda s, e: download_cross_border_flows(country_from, country_to, s, e, client=client),
            start_date, end_date, chunk_months, f"flows {country_from}->{country_to}", failed)

    results = _parallel({pair: (lambda pair=pair: fetch_pair(*pair)) for pair in FLOW_PAIRS})

    all_flows = []
    for (country_from, country_to), pair_df in results.items():
        if pair_df.empty:
            logger.error(f"No flows downloaded for {country_from}->{country_to}")
            continue
        all_flows.append(pair_df)
        output_file = RAW_DIR / (f"cross_border_flows_{country_from}_{country_to}_"
                                 f"{start_date}_{end_date}.parquet")
        pair_df.to_parquet(output_file, compression='snappy', index=False)

    return pd.concat(all_flows, ignore_index=True) if all_flows else pd.DataFrame()


def download_all_entsoe_data(start_date: str = DEFAULT_START,
                             end_date: str = DEFAULT_END):
    """
    Download all ENTSO-E data needed for project.

    This takes a while due to rate limits. Be patient!
    Expected time: 15-30 minutes for two years of data.
    """
    logger.info("="*60)
    logger.info("ENTSO-E DATA DOWNLOAD STARTING")
    logger.info("="*60)

    failed = []

    # 1. Day-ahead prices for the donor pool (ES/PT come from OMIE)
    logger.info("\n1. Downloading day-ahead prices for donor pool...")
    download_all_countries_prices(DONOR_COUNTRIES, start_date, end_date, failed=failed)

    # 2. Cross-border flows (critical for network analysis / leakage)
    logger.info("\n2. Downloading cross-border flows...")
    download_all_flows(start_date, end_date, failed=failed)

    # 3. Generation data (optional - heavy download, no loader yet)
    logger.info("\n3. Generation data: skipped (no loader/table population yet)")

    if failed:
        logger.error(f"{len(failed)} chunk(s) failed, re-run to retry: {failed}")

    logger.info("\n" + "="*60)
    logger.info("ENTSO-E DATA DOWNLOAD COMPLETE")
    logger.info("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download ENTSO-E prices and flows.")
    parser.add_argument('--start', default=DEFAULT_START, help="first day (inclusive)")
    parser.add_argument('--end', default=DEFAULT_END, help="last day (inclusive)")
    args = parser.parse_args()
    download_all_entsoe_data(args.start, args.end)
