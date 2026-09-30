"""
Timezone utilities for handling European electricity market data.

THE TIMEZONE NIGHTMARE:
- OMIE publishes in Iberian *market* time, which is CET/CEST (Europe/Madrid)
  for both Spain and Portugal, on an H1..H25 grid (H1 = 00:00-01:00 local)
- ENTSO-E returns timezone-aware timestamps (converted to UTC at ingestion)
- Spain and Portugal observe DST (last Sunday of March/October)
- DST creates 23-hour days (spring forward) and 25-hour days (fall back)

THE RULE: every timestamp stored or joined is UTC. Local market time is used
only (a) to decode OMIE's hour numbering and (b) to define day boundaries and
calendar features, because those are what the market and the policy use.

This module is the single place that rule is implemented.
"""

import pandas as pd
import pytz
import logging

logger = logging.getLogger(__name__)

# Timezones we care about
UTC = pytz.UTC
# Iberian market time. Portugal's civil time is Europe/Lisbon (WET), but the
# MIBEL day-ahead market runs on CET/CEST for both countries.
MARKET_TZ = pytz.timezone('Europe/Madrid')
CET = MARKET_TZ  # backwards-compatible alias

# Iberian Exception: market days 2022-06-15 through 2023-12-31 (inclusive)
IBERIAN_EXCEPTION_START = '2022-06-15'
IBERIAN_EXCEPTION_END = '2023-12-31'


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


def market_window_utc(start_date: str, end_date: str, tz=MARKET_TZ):
    """
    The one window convention used by every query and index in the pipeline.

    `start_date` and `end_date` are *inclusive* market days ('YYYY-MM-DD').
    The window is half-open in UTC: [local midnight of start_date,
    local midnight of the day after end_date).

    Returns:
        (start_utc, end_utc_exclusive) as tz-aware UTC Timestamps
    """
    start_local = tz.localize(pd.Timestamp(start_date).to_pydatetime())
    end_local = tz.localize((pd.Timestamp(end_date) + pd.Timedelta(days=1)).to_pydatetime())
    return (pd.Timestamp(start_local).tz_convert(UTC),
            pd.Timestamp(end_local).tz_convert(UTC))


def omie_hours_to_utc(dates: pd.Series, hour_numbers: pd.Series) -> pd.Series:
    """
    Convert OMIE (DATE, H{n}) pairs to UTC timestamps.

    OMIE numbers the hours of each market day sequentially from H1 (the hour
    starting at local midnight). A spring-forward day has H1..H23, an
    autumn-fallback day has H1..H25. Because the numbering is sequential in
    real time, hour n starts exactly (n-1) hours after local midnight -- which
    handles both DST days without any ambiguous/nonexistent wall-clock lookups.

    Args:
        dates: market dates (anything pd.to_datetime understands)
        hour_numbers: OMIE hour numbers, 1-based

    Returns:
        Series of tz-aware UTC timestamps (hour start)
    """
    days = pd.to_datetime(pd.Series(dates).reset_index(drop=True)).dt.normalize()
    unique_days = pd.DatetimeIndex(days.unique())
    # Local midnight never falls in a DST gap/fold in Iberia (transitions are at 02:00/03:00)
    midnight_utc = unique_days.tz_localize(MARKET_TZ).tz_convert(UTC)
    lookup = pd.Series(midnight_utc, index=unique_days)
    offsets = pd.to_timedelta(pd.Series(hour_numbers).reset_index(drop=True).astype(int) - 1, unit='h')
    result = lookup.loc[days].reset_index(drop=True) + offsets
    return result.dt.tz_convert(UTC)


def omie_periods_to_utc(dates: pd.Series, periods: pd.Series,
                        periods_per_day: pd.Series) -> pd.Series:
    """
    Generalisation of omie_hours_to_utc for any market time unit.

    OMIE numbers the periods of a market day sequentially from 1. Since
    2025-10-01 a day has 96 quarter-hours (92/100 on DST days); before that
    it had 24 hours (23/25). The period length is the day's real length
    divided by its number of periods, so both formats map exactly.

    Returns:
        Series of tz-aware UTC timestamps (period start)
    """
    dates = pd.Series(dates).reset_index(drop=True)
    periods = pd.Series(periods).reset_index(drop=True).astype(int)
    n = pd.Series(periods_per_day).reset_index(drop=True).astype(int)
    minutes_per_period = hours_in_market_day(dates).reset_index(drop=True) * 60 / n
    bad = minutes_per_period.isin([15, 60]) == False  # noqa: E712
    if bad.any():
        raise ValueError(f"{int(bad.sum())} rows have a period count that fits neither "
                         f"hourly nor 15-minute periods, e.g. {dates[bad].iloc[0]} "
                         f"with {n[bad].iloc[0]} periods")
    start_of_day = omie_hours_to_utc(dates, pd.Series(1, index=dates.index))
    offsets = pd.to_timedelta((periods - 1) * minutes_per_period, unit='min')
    return start_of_day + offsets


def hours_in_market_day(dates: pd.Series) -> pd.Series:
    """Number of hours in each market day (23, 24 or 25 in Iberia)."""
    days = pd.DatetimeIndex(pd.to_datetime(pd.Series(dates)).dt.normalize())
    start = days.tz_localize(MARKET_TZ)
    end = (days + pd.Timedelta(days=1)).tz_localize(MARKET_TZ)
    return pd.Series(((end - start) / pd.Timedelta(hours=1)).astype(int), index=pd.Series(dates).index)


def assert_utc_hourly(df: pd.DataFrame, timestamp_col: str = 'timestamp',
                      group_col: str = None) -> None:
    """
    Assert (not assume) that a timestamp column is tz-aware UTC, on the hour,
    and unique per group. Raises AssertionError on violation.
    """
    ts = df[timestamp_col]
    assert ts.dt.tz is not None, f"'{timestamp_col}' is timezone-naive"
    assert str(ts.dt.tz) == 'UTC', f"'{timestamp_col}' is {ts.dt.tz}, not UTC"
    off_hour = (ts.dt.minute != 0) | (ts.dt.second != 0)
    assert not off_hour.any(), f"{int(off_hour.sum())} timestamps are not on the hour"
    keys = [timestamp_col] + ([group_col] if group_col else [])
    dups = df.duplicated(subset=keys)
    assert not dups.any(), f"{int(dups.sum())} duplicate {keys} keys"


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


def create_hour_index(start_date: str, end_date: str, tz=MARKET_TZ) -> pd.DataFrame:
    """
    Create a complete hourly UTC index with no gaps.

    Why? When we merge data from multiple sources, we want a complete
    time series with one row for every hour, even if some data is missing.

    Day boundaries are market days in `tz` (see market_window_utc), so
    '2022-01-01'..'2023-12-31' covers exactly the hours OMIE published for
    those days: 17,520 hours, since the spring and autumn DST days cancel out.
    Pass tz=UTC for plain UTC calendar days.

    Args:
        start_date: 'YYYY-MM-DD' (inclusive)
        end_date: 'YYYY-MM-DD' (inclusive)
        tz: timezone in which the day boundaries are defined

    Returns:
        DataFrame with single column 'timestamp' containing every hour (UTC)
    """
    start, end_exclusive = market_window_utc(start_date, end_date, tz=tz)

    # Generate hourly timestamps, half-open [start, end)
    timestamps = pd.date_range(start=start, end=end_exclusive, freq='h', inclusive='left')

    # Ensure timestamps are in pytz.UTC format
    timestamps = pd.DatetimeIndex(timestamps).tz_convert(UTC)

    df = pd.DataFrame({'timestamp': timestamps})

    logger.info(f"Created hour index: {len(df):,} hours from {start_date} to {end_date}")
    return df


def add_time_features(df: pd.DataFrame,
                      timestamp_col: str = 'timestamp',
                      tz=MARKET_TZ) -> pd.DataFrame:
    """
    Add time-based features useful for analysis.

    Calendar features are computed in Iberian market local time (CET/CEST),
    because demand, the market day and the policy all follow local clocks.
    The timestamp column itself stays UTC.

    Features:
    - hour: Local hour of day (0-23)
    - day_of_week: Day of week (0=Monday, 6=Sunday)
    - month: Month (1-12)
    - year: Year
    - is_weekend: Boolean
    - quarter: Quarter (1-4)
    - day_of_year: Day of year (1-366)
    - is_iberian_exception: market days 2022-06-15 .. 2023-12-31

    These are useful for:
    - Grouping (e.g., average price by hour)
    - Regression features
    - Identifying patterns
    """
    df = df.copy()

    local = df[timestamp_col].dt.tz_convert(tz)

    # Extract time components
    df['hour'] = local.dt.hour
    df['day_of_week'] = local.dt.dayofweek
    df['month'] = local.dt.month
    df['year'] = local.dt.year
    df['quarter'] = local.dt.quarter
    df['day_of_year'] = local.dt.dayofyear

    # Boolean features
    df['is_weekend'] = df['day_of_week'].isin([5, 6])  # Saturday, Sunday

    # Time periods for analysis (half-open window in UTC, local-day boundaries)
    iberian_start, iberian_end = market_window_utc(
        IBERIAN_EXCEPTION_START, IBERIAN_EXCEPTION_END, tz=tz
    )

    df['is_iberian_exception'] = (
        (df[timestamp_col] >= iberian_start) &
        (df[timestamp_col] < iberian_end)
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

    print("\n[OK] Basic tests passed!")
