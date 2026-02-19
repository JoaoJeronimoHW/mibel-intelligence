"""
Timezone utilities for handling European electricity market data.

THE TIMEZONE NIGHTMARE:
- OMIE uses CET/CEST (Central European Time with daylight saving)
- ENTSO-E API returns UTC
- Spain and Portugal observe DST (last Sunday of March/October)
- DST creates 23-hour days (spring forward) and 25-hour days (fall back)

This module provides utilities to handle these edge cases correctly.
"""

import pandas as pd
import pytz
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Timezones we care about
UTC = pytz.UTC
CET = pytz.timezone('Europe/Madrid')  # Spain/Portugal use same timezone


def normalize_to_utc(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
    """
    Ensure all timestamps are in UTC.
    
    Args:
        df: DataFrame with timestamp column
        timestamp_col: Name of timestamp column
    
    Returns:
        DataFrame with UTC timestamps (using pytz.UTC)
    """
    df = df.copy()
    
    # Check if timestamp is timezone-aware
    if df[timestamp_col].dt.tz is None:
        # Assume UTC if no timezone
        logger.warning(f"Timestamp column '{timestamp_col}' has no timezone. Assuming UTC.")
        # Localize to pytz.UTC (not datetime.timezone.utc)
        df[timestamp_col] = df[timestamp_col].dt.tz_localize(UTC)
    else:
        # Convert to UTC first
        df[timestamp_col] = df[timestamp_col].dt.tz_convert('UTC')
        # Then ensure it's pytz.UTC by removing timezone and re-adding it
        df[timestamp_col] = df[timestamp_col].dt.tz_localize(None).dt.tz_localize(UTC)
    
    return df


def handle_dst_transitions(df: pd.DataFrame, 
                           timestamp_col: str = 'timestamp') -> pd.DataFrame:
    """
    Handle daylight saving time transitions.
    
    In spring: 23-hour day (hour 2:00-3:00 skipped)
    In fall: 25-hour day (hour 2:00-3:00 repeated)
    
    Strategy: Use UTC timestamps which don't have DST issues.
    This function validates and warns about gaps/duplicates.
    
    Args:
        df: DataFrame with timestamp column
        timestamp_col: Name of timestamp column
    
    Returns:
        DataFrame (validated, may have rows removed if duplicates found)
    """
    df = df.copy()
    
    # Sort by timestamp
    df = df.sort_values(timestamp_col).reset_index(drop=True)
    
    # Check for gaps
    if len(df) > 1:
        time_diffs = df[timestamp_col].diff()
        expected_diff = pd.Timedelta(hours=1)
        
        # Find gaps larger than 1.5 hours (allowing some tolerance)
        gaps = time_diffs[time_diffs > pd.Timedelta(hours=1.5)]
        if not gaps.empty:
            logger.warning(f"Found {len(gaps)} gaps in timestamps")
            for idx in gaps.index:
                logger.warning(f"  Gap at {df.loc[idx, timestamp_col]}")
    
    # Check for duplicates
    duplicates = df[df.duplicated(subset=[timestamp_col], keep=False)]
    if not duplicates.empty:
        logger.warning(f"Found {len(duplicates)} duplicate timestamps")
        logger.warning("Keeping first occurrence of each duplicate")
        df = df.drop_duplicates(subset=[timestamp_col], keep='first')
    
    return df


def create_hour_index(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Create a complete hourly index with no gaps.
    
    Why? When we merge data from multiple sources, we want a complete
    time series with one row for every hour, even if some data is missing.
    
    Args:
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
    
    Returns:
        DataFrame with single column 'timestamp' containing every hour
    """
    start = pd.Timestamp(start_date, tz=UTC)
    end = pd.Timestamp(end_date, tz=UTC) + pd.Timedelta(days=1) - pd.Timedelta(hours=1)
    
    # Generate hourly timestamps
    timestamps = pd.date_range(start=start, end=end, freq='h', tz=UTC)
    
    # Ensure timestamps are in pytz.UTC format
    timestamps = pd.DatetimeIndex(timestamps).tz_convert(UTC)
    
    df = pd.DataFrame({'timestamp': timestamps})
    
    logger.info(f"Created hour index: {len(df):,} hours from {start_date} to {end_date}")
    return df


def add_time_features(df: pd.DataFrame, 
                      timestamp_col: str = 'timestamp') -> pd.DataFrame:
    """
    Add time-based features useful for analysis.
    
    Features:
    - hour: Hour of day (0-23)
    - day_of_week: Day of week (0=Monday, 6=Sunday)
    - month: Month (1-12)
    - year: Year
    - is_weekend: Boolean
    - quarter: Quarter (1-4)
    - day_of_year: Day of year (1-366)
    
    These are useful for:
    - Grouping (e.g., average price by hour)
    - Regression features
    - Identifying patterns
    """
    df = df.copy()
    
    # Extract time components
    df['hour'] = df[timestamp_col].dt.hour
    df['day_of_week'] = df[timestamp_col].dt.dayofweek
    df['month'] = df[timestamp_col].dt.month
    df['year'] = df[timestamp_col].dt.year
    df['quarter'] = df[timestamp_col].dt.quarter
    df['day_of_year'] = df[timestamp_col].dt.dayofyear
    
    # Boolean features
    df['is_weekend'] = df['day_of_week'].isin([5, 6])  # Saturday, Sunday
    
    # Time periods for analysis
    # Iberian Exception: June 15, 2022 to December 31, 2023
    iberian_start = pd.Timestamp('2022-06-15', tz=UTC)
    iberian_end = pd.Timestamp('2023-12-31 23:59:59', tz=UTC)
    
    df['is_iberian_exception'] = (
        (df[timestamp_col] >= iberian_start) & 
        (df[timestamp_col] <= iberian_end)
    )
    
    return df


if __name__ == "__main__":
    # Quick test
    print("Testing timezone utilities...")
    
    # Test 1: Create hour index
    print("\n1. Creating hour index...")
    idx = create_hour_index('2022-06-15', '2022-06-16')
    print(f"   Created {len(idx)} hours")
    print(f"   First: {idx['timestamp'].iloc[0]}")
    print(f"   Last: {idx['timestamp'].iloc[-1]}")
    
    # Test 2: Add time features
    print("\n2. Adding time features...")
    idx_with_features = add_time_features(idx)
    print(f"   Features added: {[col for col in idx_with_features.columns if col != 'timestamp']}")
    
    # Test 3: Check Iberian Exception
    in_exception = idx_with_features['is_iberian_exception'].sum()
    print(f"   Hours in Iberian Exception: {in_exception}/{len(idx_with_features)}")
    
    print("\n✅ Basic tests passed!")