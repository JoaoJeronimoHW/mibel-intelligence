"""
Weather data ingestion using Open-Meteo API.

Open-Meteo provides historical weather data based on ERA5 reanalysis.
It's free for non-commercial use with no API key required!

We need weather data for multiple locations across Spain and Portugal
because renewable generation varies by region.
"""

import argparse
import pandas as pd
import requests
from pathlib import Path
import logging
from tqdm import tqdm
import time

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


# Representative locations across Iberian Peninsula
# These capture geographic diversity in weather patterns
LOCATIONS = {
    'Madrid': {'lat': 40.4168, 'lon': -3.7038},      # Central Spain
    'Barcelona': {'lat': 41.3874, 'lon': 2.1686},     # Northeast coast
    'Seville': {'lat': 37.3891, 'lon': -5.9845},      # South Spain
    'Bilbao': {'lat': 43.2630, 'lon': -2.9350},       # North coast
    'Lisbon': {'lat': 38.7223, 'lon': -9.1393},       # Portugal coast
    'Porto': {'lat': 41.1579, 'lon': -8.6291},        # North Portugal
    'Faro': {'lat': 37.0194, 'lon': -7.9322},         # South Portugal (Algarve)
}


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
        # Make API request
        response = requests.get(url, params=params, timeout=30)
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
                           chunk_years: int = 1):
    """
    Download weather data for all locations.

    We chunk by year because Open-Meteo has a 10,000 request/day limit.
    Each location x year = 1 request, so 7 locations x 5 years = 35 requests total.
    Well under the limit!

    Args:
        start_date: 'YYYY-MM-DD' (inclusive, UTC day)
        end_date: 'YYYY-MM-DD' (inclusive, UTC day)
        chunk_years: Download this many years at a time
    """
    logger.info("="*60)
    logger.info("WEATHER DATA DOWNLOAD STARTING")
    logger.info("="*60)

    all_data = []
    failed = []

    for location_name, coords in tqdm(LOCATIONS.items(), desc="Locations"):

        # Chunk by year
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        current = start

        location_data = []

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

                # Small delay to be polite to the API
                time.sleep(1)

            except Exception as e:
                logger.error(f"Failed chunk for {location_name}: {e}")
                failed.append((location_name, current.date()))

            current = chunk_end + pd.Timedelta(days=1)

        # Combine chunks for this location
        if location_data:
            location_df = pd.concat(location_data, ignore_index=True)
            all_data.append(location_df)

            # Save individual location file
            output_file = RAW_DIR / f"weather_{location_name}_{start_date}_{end_date}.parquet"
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
    args = parser.parse_args()
    download_all_locations(args.start, args.end)
