"""
Weather data ingestion using Open-Meteo API.

Open-Meteo provides historical weather data based on ERA5 reanalysis.
It's free for non-commercial use with no API key required!

We need weather data for multiple locations in every panel country because
renewable generation varies by region. The locations (and whether each one
feeds the country's level or its dispersion variables) are defined in
weather_locations.py.
"""

import argparse
import pandas as pd
import requests
from pathlib import Path
import logging
from tqdm import tqdm
import time

from src.data.weather_locations import LOCATIONS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw" / "weather"
RAW_DIR.mkdir(parents=True, exist_ok=True)
# Combined convenience files live in a subdirectory so the loader's
# weather_*.parquet glob can never pick them up (D-09)
COMBINED_DIR = RAW_DIR / "combined"

# Default window: the panel's market days 2022-01-01..2023-12-31 begin at
# 2021-12-31 23:00 UTC, so the UTC download starts one day earlier
DEFAULT_START = '2021-12-31'
DEFAULT_END = '2023-12-31'


# Open-Meteo rate-limits by weighted calls (per minute/hour/day); with ~60
# locations a full download can hit it, so HTTP 429 (and transient 5xx) is
# retried after a pause
MAX_RETRIES = 5
RATE_LIMIT_PAUSE_S = 65


def download_historical_weather(location_name: str,
                                latitude: float,
                                longitude: float,
                                start_date: str,
                                end_date: str) -> pd.DataFrame:
    """
    Download historical weather data for a location.

    Args:
        location_name: Name for reference (e.g., 'Madrid')
        latitude: Decimal degrees
        longitude: Decimal degrees
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'

    Returns:
        DataFrame with hourly weather variables
    """
    logger.info(f"Downloading weather for {location_name}")

    # Open-Meteo API endpoint
    url = "https://archive-api.open-meteo.com/v1/archive"

    # Parameters for API request
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'start_date': start_date,
        'end_date': end_date,
        'hourly': [
            'temperature_2m',           # Temperature at 2m height (deg C)
            'wind_speed_10m',            # Wind speed at 10m (m/s)
            'wind_speed_100m',           # Wind speed at 100m (typical wind turbine hub height)
            'wind_direction_100m',       # Wind direction at 100m (degrees)
            'shortwave_radiation',       # Solar radiation (W/m2)
            'direct_normal_irradiance',  # Direct solar radiation (for PV)
            'diffuse_radiation',         # Diffuse solar radiation
            'cloud_cover',               # Cloud cover percentage
        ],
        'timezone': 'UTC',  # Always use UTC for consistency
    }

    try:
        # Make API request, waiting out the rate limit if we hit it
        for attempt in range(1, MAX_RETRIES + 1):
            response = requests.get(url, params=params, timeout=60)
            retryable = response.status_code == 429 or response.status_code >= 500
            if not retryable or attempt == MAX_RETRIES:
                break
            logger.warning(f"Open-Meteo HTTP {response.status_code} ({location_name}); "
                           f"retry {attempt}/{MAX_RETRIES - 1} in {RATE_LIMIT_PAUSE_S}s")
            time.sleep(RATE_LIMIT_PAUSE_S)
        response.raise_for_status()  # Raise error for bad status codes

        data = response.json()

        # Parse response into DataFrame
        df = pd.DataFrame({
            # Requested in UTC; store it tz-aware so nothing downstream guesses
            'timestamp': pd.to_datetime(data['hourly']['time']).tz_localize('UTC'),
            'temperature_c': data['hourly']['temperature_2m'],
            'wind_speed_10m': data['hourly']['wind_speed_10m'],
            'wind_speed_100m': data['hourly']['wind_speed_100m'],
            'wind_direction_100m': data['hourly']['wind_direction_100m'],
            'solar_radiation': data['hourly']['shortwave_radiation'],
            'dni': data['hourly']['direct_normal_irradiance'],  # Direct Normal Irradiance
            'diffuse_radiation': data['hourly']['diffuse_radiation'],
            'cloud_cover': data['hourly']['cloud_cover'],
        })

        df['location'] = location_name
        df['latitude'] = latitude
        df['longitude'] = longitude

        logger.info(f"Downloaded {len(df)} hourly weather records for {location_name}")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading weather for {location_name}: {e}")
        return pd.DataFrame()


def download_all_locations(start_date: str = DEFAULT_START,
                           end_date: str = DEFAULT_END,
                           chunk_years: int = 1,
                           overwrite: bool = False):
    """
    Download weather data for all locations.

    We chunk by year because Open-Meteo has a 10,000 request/day limit.
    Each location x year = 1 request.

    A location whose file for this exact window already exists is skipped
    unless overwrite=True, so an interrupted run can simply be re-run. A
    location file is only written when every chunk succeeded, so a skipped
    file is always complete.

    Args:
        start_date: 'YYYY-MM-DD' (inclusive, UTC day)
        end_date: 'YYYY-MM-DD' (inclusive, UTC day)
        chunk_years: Download this many years at a time
        overwrite: Re-download locations whose file already exists
    """
    logger.info("="*60)
    logger.info("WEATHER DATA DOWNLOAD STARTING")
    logger.info("="*60)

    all_data = []
    failed = []

    for location_name, coords in tqdm(LOCATIONS.items(), desc="Locations"):

        output_file = RAW_DIR / f"weather_{location_name}_{start_date}_{end_date}.parquet"
        if output_file.exists() and not overwrite:
            logger.info(f"Skipping {location_name}: {output_file.name} already exists")
            all_data.append(pd.read_parquet(output_file))
            continue

        # Chunk by year
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        current = start

        location_data = []
        location_failed = False

        # Open-Meteo's end_date is inclusive, so chunks must not share a day
        while current <= end:
            chunk_end = min(current + pd.DateOffset(years=chunk_years) - pd.Timedelta(days=1), end)

            try:
                df = download_historical_weather(
                    location_name,
                    coords['lat'],
                    coords['lon'],
                    current.strftime('%Y-%m-%d'),
                    chunk_end.strftime('%Y-%m-%d')
                )

                if not df.empty:
                    location_data.append(df)
                else:
                    failed.append((location_name, current.date()))
                    location_failed = True

                # Small delay to be polite to the API
                time.sleep(1)

            except Exception as e:
                logger.error(f"Failed chunk for {location_name}: {e}")
                failed.append((location_name, current.date()))
                location_failed = True

            current = chunk_end + pd.Timedelta(days=1)

        # Combine chunks for this location; a partial location is not saved,
        # so the next run retries it instead of skipping an incomplete file
        if location_data and not location_failed:
            location_df = pd.concat(location_data, ignore_index=True)
            all_data.append(location_df)

            # Save individual location file
            location_df.to_parquet(output_file, compression='snappy', index=False)

    # Combine all locations
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total weather records: {len(combined)}")

        # Save combined file
        COMBINED_DIR.mkdir(exist_ok=True)
        output_file = COMBINED_DIR / f"weather_all_{start_date}_{end_date}.parquet"
        combined.to_parquet(output_file, compression='snappy', index=False)

        logger.info(f"Saved combined weather data to {output_file}")

    if failed:
        logger.error(f"{len(failed)} chunk(s) failed, re-run to retry: {failed}")

    logger.info("="*60)
    logger.info("WEATHER DATA DOWNLOAD COMPLETE")
    logger.info("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Open-Meteo hourly weather (UTC).")
    parser.add_argument('--start', default=DEFAULT_START, help="first UTC day (inclusive)")
    parser.add_argument('--end', default=DEFAULT_END, help="last UTC day (inclusive)")
    parser.add_argument('--overwrite', action='store_true',
                        help="re-download locations whose file already exists")
    args = parser.parse_args()
    download_all_locations(args.start, args.end, overwrite=args.overwrite)
