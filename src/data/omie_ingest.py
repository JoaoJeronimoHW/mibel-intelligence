"""
OMIE data ingestion module.

OMIE provides Spanish and Portuguese electricity market data.
The challenge: Their data comes in multiple formats (CSV, TXT with weird encoding).

This module handles:
1. Downloading day-ahead prices (simple)
2. Parsing bid curves (complex - step function offers from each generator)
3. Extracting generation by technology
"""

import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from tqdm import tqdm
import time

# OMIEData is a community library for accessing OMIE's API
from OMIEData.DataImport.omie_marginalprice_importer import OMIEMarginalPriceFileImporter

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Data directories
RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw" / "omie"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def download_day_ahead_prices(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download day-ahead hourly prices for Spain and Portugal.

    Args:
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'

    Returns:
        DataFrame with columns: DATE, HOUR, price columns for Spain/Portugal

    Example:
        prices = download_day_ahead_prices('2022-01-01', '2022-12-31')
    """
    logger.info(f"Downloading OMIE day-ahead prices: {start_date} to {end_date}")

    try:
        # Convert string dates to datetime objects
        date_ini = datetime.strptime(start_date, '%Y-%m-%d')
        date_end = datetime.strptime(end_date, '%Y-%m-%d')

        # Initialize OMIE importer
        importer = OMIEMarginalPriceFileImporter(
            date_ini=date_ini,
            date_end=date_end
        )

        # Download data
        df = importer.read_to_dataframe(verbose=True)

        logger.info(f"Downloaded {len(df)} hourly price records")

        # Save raw data (good practice: always keep original)
        output_file = RAW_DIR / f"day_ahead_prices_{start_date}_{end_date}.parquet"
        df.to_parquet(output_file, compression='snappy', index=False)
        logger.info(f"Saved to {output_file}")

        return df

    except Exception as e:
        logger.error(f"Error downloading OMIE prices: {e}")
        raise


def download_generation_by_technology(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download hourly generation breakdown by technology (wind, solar, gas, hydro, etc.)

    This tells us HOW electricity was generated each hour.

    Returns:
        DataFrame with generation data by technology
    """
    logger.info(f"Downloading generation by technology: {start_date} to {end_date}")

    try:
        # Note: OMIEData might not have a direct generation importer
        # You may need to use ENTSO-E for this data instead

        logger.warning("Generation by technology download not fully implemented")
        logger.info("Consider using ENTSO-E data for generation by technology")

        # Return empty DataFrame for now
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error downloading generation data: {e}")
        raise


def download_bid_curves(start_date: str, end_date: str, hour: int = 1) -> pd.DataFrame:
    """
    Download bid/supply-demand curves for a date range using OMIEData library.

    **What are bid curves?**
    Each hour, generators submit offers: "I'll provide X MW at price Y EUR/MWh".
    The market operator stacks these offers by price to create the supply curve.
    Where this supply curve meets demand determines the clearing price.

    **Why this data matters for your project:**
    - You have 50M+ rows here (24 hours x 365 days x 5 years x many bids per hour)
    - This lets you analyze HOW bidding strategies changed under the gas cap
    - Essential for structural auction modeling in Week 3

    **IMPORTANT: This downloads data hour by hour, which is slow!**
    For 5 years x 24 hours/day = 43,800 API calls. At 2 seconds per call,
    that's ~24 hours of continuous downloading. Plan accordingly.

    Args:
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
        hour: Which hour to download (1-24). For full data, loop over all 24.

    Returns:
        DataFrame with columns:
        - DATE: Date
        - HOUR: Hour (1-24)
        - COUNTRY: 'Spain' or 'Portugal'
        - TYPE: 'buy' or 'sell'
        - PRICE: EUR/MWh
        - ENERGY: MW
    """
    logger.info(f"Downloading bid curves: {start_date} to {end_date}, hour {hour}")

    # Import the specific OMIEData class for supply/demand curves
    from OMIEData.DataImport.omie_supply_demand_curve_importer import OMIESupplyDemandCurvesImporter

    try:
        # Convert string dates to datetime objects
        date_ini = datetime.strptime(start_date, '%Y-%m-%d')
        date_end = datetime.strptime(end_date, '%Y-%m-%d')

        # Download bid curves using OMIEData
        # NOTE: This library downloads one hour at a time!
        importer = OMIESupplyDemandCurvesImporter(
            date_ini=date_ini,
            date_end=date_end,
            hour=hour  # Hour 1-24 (Spanish convention: 1 = 00:00-01:00)
        )

        # Read to DataFrame
        df = importer.read_to_dataframe(verbose=True)

        # Sort for consistency
        df = df.sort_values(by=['DATE', 'HOUR'], axis=0).reset_index(drop=True)

        logger.info(f"Downloaded {len(df):,} bid curve records")

        # Save to parquet (good practice: always save raw data)
        output_file = RAW_DIR / f"bid_curves_hour{hour}_{start_date}_{end_date}.parquet"
        df.to_parquet(output_file, compression='snappy', index=False)
        logger.info(f"Saved to {output_file}")

        return df

    except Exception as e:
        logger.error(f"Error downloading bid curves for hour {hour}: {e}")
        raise


def download_all_bid_curves(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download bid curves for ALL 24 hours of each day.

    **WARNING: This is VERY slow!**
    - 5 years x 365 days x 24 hours = 43,800 downloads
    - At ~2 seconds per download = ~24 hours of runtime

    **Recommendation:** Start with a small date range to test (1 week).
    Then expand to full 5 years and let it run overnight (or over several days).

    Strategy:
    - Download each hour separately
    - Save intermediate results (so you can resume if interrupted)
    - Use progress bars to track status
    - Add delays to avoid overwhelming OMIE servers

    Args:
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'

    Returns:
        Combined DataFrame with all hours
    """
    logger.info("="*60)
    logger.info(f"DOWNLOADING ALL BID CURVES: {start_date} to {end_date}")
    logger.info("WARNING: This will take a LONG time (potentially days)")
    logger.info("="*60)

    all_dataframes = []

    # Loop through each hour (H25, the extra autumn-fallback hour, is not
    # requested; see manual D-16 before relying on DST days here)
    for hour in tqdm(range(1, 25), desc="Hours"):
        logger.info(f"\nProcessing hour {hour}/24...")

        try:
            # Download this hour for the entire date range
            df_hour = download_bid_curves(start_date, end_date, hour=hour)
            all_dataframes.append(df_hour)

            # Be nice to the server - pause between hours
            time.sleep(3)

        except Exception as e:
            logger.error(f"Failed to download hour {hour}: {e}")
            logger.info("Continuing with next hour...")
            continue

    # Combine all hours
    if all_dataframes:
        combined = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"\nTotal bid curve records: {len(combined):,}")

        # Save combined file
        output_file = RAW_DIR / f"bid_curves_all_hours_{start_date}_{end_date}.parquet"
        combined.to_parquet(output_file, compression='snappy', index=False)
        logger.info(f"Saved combined file to: {output_file}")

        return combined
    else:
        logger.error("No bid curve data downloaded successfully")
        return pd.DataFrame()


def download_bid_curves_sample(start_date: str, num_days: int = 7) -> pd.DataFrame:
    """
    Download a SAMPLE of bid curves for testing (recommended first step).

    Downloads all 24 hours for just a few days, so you can:
    1. Test that the code works
    2. Explore the data structure
    3. Build your analysis pipeline
    4. THEN download the full 5 years

    Args:
        start_date: 'YYYY-MM-DD'
        num_days: Number of days to download (default: 1 week)

    Returns:
        DataFrame with bid curves for sample period
    """
    end_date = (pd.to_datetime(start_date) + pd.Timedelta(days=num_days)).strftime('%Y-%m-%d')

    logger.info(f"Downloading SAMPLE bid curves: {start_date} to {end_date}")
    logger.info(f"This is {num_days} days x 24 hours = {num_days * 24} downloads")
    logger.info(f"Estimated time: ~{num_days * 24 * 2 / 60:.1f} minutes")

    return download_all_bid_curves(start_date, end_date)


def batch_download_prices(start_date: str, end_date: str, chunk_months: int = 6):
    """
    Download prices in chunks to avoid memory issues and handle API failures gracefully.

    Why chunks?
    - Downloading 5 years at once might timeout or run out of memory
    - If download fails halfway, you don't lose everything
    - Easier to resume after errors

    Args:
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
        chunk_months: Download this many months at a time
    """
    logger.info(f"Starting batch download: {start_date} to {end_date}")

    # Convert to datetime
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    # Generate date chunks
    current = start
    all_dataframes = []

    # Progress bar for user feedback
    total_days = (end - start).days + 1
    pbar = tqdm(total=total_days, desc="Downloading OMIE data")

    # OMIE's date_end is inclusive, so chunks must not share a day
    while current <= end:
        # Calculate chunk end date
        chunk_end = current + pd.DateOffset(months=chunk_months) - pd.Timedelta(days=1)
        chunk_end = min(chunk_end, end)

        try:
            # Download this chunk
            df = download_day_ahead_prices(
                current.strftime('%Y-%m-%d'),
                chunk_end.strftime('%Y-%m-%d')
            )
            all_dataframes.append(df)

            # Update progress
            days_downloaded = (chunk_end - current).days + 1
            pbar.update(days_downloaded)

            # Be nice to the API - don't hammer it
            time.sleep(2)

        except Exception as e:
            logger.error(f"Failed to download chunk {current} to {chunk_end}: {e}")
            logger.info("Continuing with next chunk...")

        # Move to next chunk
        current = chunk_end + pd.Timedelta(days=1)

    pbar.close()

    # Combine all chunks
    if all_dataframes:
        combined = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Total records downloaded: {len(combined)}")
        return combined
    else:
        logger.error("No data downloaded successfully")
        return None


# Convenience function to download everything
def download_all_omie_data(start_date: str = '2022-01-01',
                            end_date: str = '2023-12-31',
                            download_bid_curves: bool = False,
                            bid_curves_sample_only: bool = True,
                            bid_curves_sample_days: int = 7,
                            chunk_months: int = 3):
    """
    Download all OMIE data needed for the project.

    This is your main entry point. Run this once to get all data.

    Args:
        start_date: Start of date range
        end_date: End of date range
        download_bid_curves: Whether to download bid curves (WARNING: VERY slow!)
        bid_curves_sample_only: If True, only download a sample (recommended for first run)
        bid_curves_sample_days: Number of days to sample if bid_curves_sample_only=True
        chunk_months: months per price request / raw file

    Returns:
        Tuple of (prices, generation, bid_curves_df or None)

    Examples:
        # First run - get prices and generation only (fast)
        prices, gen, _ = download_all_omie_data()

        # Second run - test bid curves with small sample
        prices, gen, bids = download_all_omie_data(download_bid_curves=True, bid_curves_sample_only=True)

        # Final run - download ALL bid curves (takes 24+ hours!)
        prices, gen, bids = download_all_omie_data(
            download_bid_curves=True,
            bid_curves_sample_only=False
        )
    """
    logger.info("="*60)
    logger.info("OMIE DATA DOWNLOAD STARTING")
    logger.info("="*60)

    # 1. Day-ahead prices (always download - this is fast)
    logger.info("\n1. Downloading day-ahead prices...")
    prices = batch_download_prices(start_date, end_date, chunk_months=chunk_months)

    # 2. Generation by technology (always download - this is fast)
    logger.info("\n2. Downloading generation by technology...")
    generation = download_generation_by_technology(start_date, end_date)

    # 3. Bid curves (optional - this is SLOW)
    bid_curves_df = None

    if download_bid_curves:
        logger.info("\n3. Downloading bid curves...")

        if bid_curves_sample_only:
            logger.info(f"   -> SAMPLE MODE: Downloading {bid_curves_sample_days} days only")
            logger.info("   -> This is for testing. Set bid_curves_sample_only=False for full data.")

            # Download sample starting from the Iberian Exception start date
            # (most interesting period for analysis)
            sample_start = '2022-06-15'  # Iberian Exception start
            bid_curves_df = download_bid_curves_sample(
                start_date=sample_start,
                num_days=bid_curves_sample_days
            )

        else:
            logger.warning("   -> FULL MODE: Downloading ALL bid curves")
            logger.warning(f"   -> Date range: {start_date} to {end_date}")
            logger.warning("   -> This will take 24-48 HOURS!")
            logger.warning("   -> Make sure your computer won't sleep/disconnect")

            # Give user a chance to cancel
            logger.info("   -> Starting in 10 seconds... (Ctrl+C to cancel)")
            time.sleep(10)

            # Download all bid curves for all hours
            bid_curves_df = download_all_bid_curves(start_date, end_date)

    else:
        logger.info("\n3. Bid curves: SKIPPED")
        logger.info("   -> Set download_bid_curves=True to download")
        logger.info("   -> Recommended: Start with bid_curves_sample_only=True first")

    # Summary
    logger.info("\n" + "="*60)
    logger.info("OMIE DATA DOWNLOAD COMPLETE")
    logger.info("="*60)
    logger.info(f"Files saved in: {RAW_DIR}")
    logger.info("\nData downloaded:")
    logger.info(f"  [OK] Prices: {len(prices):,} rows" if prices is not None else "  [FAIL] Prices: Failed")
    logger.info(f"  [OK] Generation: {len(generation):,} rows" if generation is not None else "  [FAIL] Generation: Failed")

    if bid_curves_df is not None:
        logger.info(f"  [OK] Bid curves: {len(bid_curves_df):,} rows")
        logger.info(f"    -> Date range: {bid_curves_df['DATE'].min()} to {bid_curves_df['DATE'].max()}")
    else:
        logger.info("  - Bid curves: Not downloaded")

    logger.info("\nNext steps:")
    if bid_curves_df is None:
        logger.info("  1. Run with download_bid_curves=True, bid_curves_sample_only=True to test")
    elif bid_curves_sample_only and download_bid_curves:
        logger.info("  1. Check the sample data looks correct")
        logger.info("  2. When ready, run with bid_curves_sample_only=False for full dataset")
    else:
        logger.info("  1. Load data into DuckDB: python -m src.data.load_to_db")

    return prices, generation, bid_curves_df


if __name__ == "__main__":
    # Explicit flags instead of input(): an interactive prompt hangs under
    # cron, CI or any runner without stdin (D-17). The default is the safe,
    # fast option; the 24-48h download needs both --bid-curves full and --yes.
    parser = argparse.ArgumentParser(
        description="Download OMIE day-ahead prices (and optionally bid curves).")
    parser.add_argument('--start', default='2022-01-01', help="first market day (inclusive)")
    parser.add_argument('--end', default='2023-12-31', help="last market day (inclusive)")
    parser.add_argument('--chunk-months', type=int, default=3,
                        help="months per request/raw file (default 3 = quarterly)")
    parser.add_argument('--bid-curves', choices=['none', 'sample', 'full'], default='none',
                        help="none (default); sample = 7 days from 2022-06-15 (~1h); "
                             "full = every hour of the window (24-48h)")
    parser.add_argument('--sample-days', type=int, default=7)
    parser.add_argument('--yes', action='store_true',
                        help="confirm the full bid-curve download")
    args = parser.parse_args()

    if args.bid_curves == 'full' and not args.yes:
        parser.error("--bid-curves full takes 24-48 hours; add --yes to confirm")

    download_all_omie_data(
        start_date=args.start,
        end_date=args.end,
        download_bid_curves=args.bid_curves != 'none',
        bid_curves_sample_only=args.bid_curves == 'sample',
        bid_curves_sample_days=args.sample_days,
        chunk_months=args.chunk_months,
    )
