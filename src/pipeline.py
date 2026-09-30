"""
Run the whole pipeline for the last N days in one command.

    python -m src.pipeline 30                 # last 30 complete days
    python -m src.pipeline 7 --end 2026-09-15 # 7 days ending 15 Sep 2026
    python -m src.pipeline 90 --skip entsoe   # OMIE + weather only
    mibel 30                                  # same, from cmd (mibel.bat)

Steps: download OMIE, ENTSO-E and Open-Meteo for the window -> load all raw
files into DuckDB -> build data/processed/main_panel_<start>_<end>.parquet.

Days are Iberian market days, inclusive. The default window ends yesterday,
the last day every source has complete data for (Open-Meteo's archive runs
about a day behind; OMIE and ENTSO-E already publish tomorrow's prices).
"""

import argparse
import logging
import os
import sys
import time

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("pipeline")

SOURCES = ['omie', 'entsoe', 'weather']


def market_window(days: int, end: str = None) -> tuple:
    """Inclusive (start, end) market days, 'YYYY-MM-DD', for the last `days` days."""
    if days < 1:
        raise ValueError("days must be at least 1")
    today = pd.Timestamp.now(tz='Europe/Madrid').normalize().tz_localize(None)
    end_day = pd.Timestamp(end) if end else today - pd.Timedelta(days=1)
    start_day = end_day - pd.Timedelta(days=days - 1)
    return start_day.strftime('%Y-%m-%d'), end_day.strftime('%Y-%m-%d')


def run(days: int, end: str = None, skip: list = (), countries: list = None,
        refresh: bool = False) -> pd.DataFrame:
    start_date, end_date = market_window(days, end)
    logger.info("=" * 60)
    logger.info(f"PIPELINE: {days} market day(s), {start_date} to {end_date}")
    logger.info("=" * 60)
    t0 = time.time()

    # 1. Ingest. Each source is independent: one failing does not stop the others.
    failures = []
    if 'omie' not in skip:
        from src.data import omie_ingest
        try:
            omie_ingest.download_prices(start_date, end_date, refresh=refresh)
        except Exception as e:
            logger.error(f"OMIE download failed: {e}")
            failures.append('omie')

    if 'entsoe' not in skip:
        from src.data import entsoe_ingest
        if not os.getenv('ENTSOE_API_KEY'):
            logger.warning("ENTSOE_API_KEY not set (run `python create_env.py`); skipping ENTSO-E")
            failures.append('entsoe')
        else:
            try:
                entsoe_ingest.download_all_entsoe_data(start_date, end_date)
            except Exception as e:
                logger.error(f"ENTSO-E download failed: {e}")
                failures.append('entsoe')

    if 'weather' not in skip:
        from src.data import weather_ingest
        # The panel's first market hour is 23:00 UTC the day before (22:00 in summer)
        weather_start = (pd.Timestamp(start_date) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        try:
            weather_ingest.download_all_locations(weather_start, end_date)
        except Exception as e:
            logger.error(f"Weather download failed: {e}")
            failures.append('weather')

    # 2. Load every raw file (idempotent, a few seconds)
    from src.data.load_to_db import load_all_data
    load_all_data()

    # 3. Panel for exactly this window
    from src.data.build_panel import build_main_panel
    panel = build_main_panel(start_date, end_date, countries)

    logger.info("=" * 60)
    logger.info(f"DONE in {time.time() - t0:.0f}s: {len(panel):,} rows -> "
                f"data/processed/main_panel_{start_date}_{end_date}.parquet")
    if failures:
        logger.warning(f"Sources that failed or were unavailable: {failures} "
                       "(their panel columns are empty for this window)")
    logger.info("=" * 60)
    return panel


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="mibel",
        description="Download the last N days of MIBEL data and build the hourly panel.")
    parser.add_argument('days', type=int, help="number of market days to fetch")
    parser.add_argument('--end', help="last market day, YYYY-MM-DD (default: yesterday)")
    parser.add_argument('--skip', nargs='+', choices=SOURCES, default=[],
                        help="sources not to download (already-loaded data is still used)")
    parser.add_argument('--countries', nargs='+', default=None,
                        help="panel countries (default: ES PT FR DE IT NL BE AT)")
    parser.add_argument('--refresh', action='store_true',
                        help="re-download OMIE daily files even if cached")
    args = parser.parse_args(argv)

    try:
        run(args.days, args.end, args.skip, args.countries, args.refresh)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
