"""
Feature Engineering for Electricity Price Forecasting

WHY THIS MODULE:
- Transforms raw hourly prices into ML-ready features
- Captures temporal patterns (hour, day, season)
- Creates lag features (past prices predict future prices)
- Encodes the Iberian Exception policy effect
"""

import pandas as pd
import numpy as np
from typing import Tuple, List
from pathlib import Path


class PriceFeatureEngineer:
    """
    Feature engineering for next-day electricity price forecasting.

    INTERVIEW TALKING POINTS:
    - We use lag features because prices exhibit autocorrelation
    - Rolling statistics capture market volatility
    - Calendar features handle seasonality and peak demand patterns
    - Policy dummy captures structural breaks (Iberian Exception)
    """

    def __init__(self, forecast_horizon: int = 24):
        """
        Args:
            forecast_horizon: Hours ahead to forecast (24 = next day)

        WHY 24 HOURS:
        - Industry standard for day-ahead markets
        - Matches OMIE auction structure
        - Allows utilities to plan generation schedules
        """
        self.forecast_horizon = forecast_horizon
        self.feature_names = []

    def create_lag_features(self, df: pd.DataFrame,
                           lags: List[int] = [1, 2, 3, 24, 48, 168]) -> pd.DataFrame:
        """
        Create lagged price features.

        INTERVIEW EXPLANATION:
        "We use lags at 1-3 hours (recent trends), 24 hours (yesterday same time),
        48 hours (day-before-yesterday), and 168 hours (last week same time).
        This captures both immediate patterns and weekly seasonality."

        Args:
            df: DataFrame with 'price_eur_mwh' column
            lags: List of lag hours

        Returns:
            DataFrame with lag features added
        """
        df = df.copy()

        # Shift within each country: without the groupby, the first week of
        # the second country would be filled with the first country's prices
        price = df.groupby('country')['price_eur_mwh']
        for lag in lags:
            df[f'price_lag_{lag}h'] = price.shift(lag)
            self.feature_names.append(f'price_lag_{lag}h')

        return df

    def create_rolling_features(self, df: pd.DataFrame,
                               windows: List[int] = [24, 168]) -> pd.DataFrame:
        """
        Create rolling statistics (mean, std, min, max).

        INTERVIEW EXPLANATION:
        "Rolling features capture market volatility. A high rolling std
        indicates volatile period. Rolling mean shows trending direction.
        24h window = daily average, 168h = weekly average."

        Args:
            df: DataFrame with 'price_eur_mwh' column
            windows: List of window sizes (in hours)

        Returns:
            DataFrame with rolling features added
        """
        df = df.copy()

        # shift(1) before rolling keeps the target out of its own window;
        # both are done per country so windows never span two countries
        past = df.groupby('country')['price_eur_mwh'].shift(1)
        by_country = past.groupby(df['country'])

        for window in windows:
            rolling = by_country.rolling(window)
            # Rolling mean (trend), std (volatility), min/max (price range)
            for stat in ['mean', 'std', 'min', 'max']:
                df[f'price_rolling_{stat}_{window}h'] = (
                    getattr(rolling, stat)().reset_index(level=0, drop=True)
                )

            self.feature_names.extend([
                f'price_rolling_mean_{window}h',
                f'price_rolling_std_{window}h',
                f'price_rolling_min_{window}h',
                f'price_rolling_max_{window}h'
            ])

        return df

    def create_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create calendar and time-based features.

        INTERVIEW EXPLANATION:
        "Hour captures daily patterns (peak demand 18:00-21:00).
        Day of week captures weekday/weekend differences.
        Month captures seasonal patterns (heating/cooling demand).
        We use cyclic encoding to preserve continuity (hour 23 -> hour 0)."

        Args:
            df: DataFrame with 'timestamp' column

        Returns:
            DataFrame with time features added
        """
        df = df.copy()

        # Cyclic encoding for hour (preserves continuity)
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)

        # Cyclic encoding for day of week
        df['dayofweek_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
        df['dayofweek_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)

        # Cyclic encoding for month
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

        # Binary features
        df['is_weekend'] = df['is_weekend'].astype(int)
        df['is_peak_hour'] = ((df['hour'] >= 18) & (df['hour'] <= 21)).astype(int)

        self.feature_names.extend([
            'hour_sin', 'hour_cos',
            'dayofweek_sin', 'dayofweek_cos',
            'month_sin', 'month_cos',
            'is_weekend', 'is_peak_hour'
        ])

        return df

    def create_policy_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create Iberian Exception policy features.

        INTERVIEW EXPLANATION:
        "We encode the policy intervention as a binary treatment.
        This allows both the LightGBM model AND the diff-in-diff benchmark
        to explicitly model the structural break on June 15, 2022."

        Args:
            df: DataFrame with 'is_iberian_exception' column

        Returns:
            DataFrame with policy features added
        """
        df = df.copy()

        # Binary treatment indicator
        df['iberian_exception'] = df['is_iberian_exception'].astype(int)

        # Days since policy implementation (captures adaptation dynamics)
        df['days_since_policy'] = 0
        # 00:00 Iberian market time on 15 June 2022, not 00:00 UTC
        policy_start = pd.Timestamp('2022-06-15', tz='Europe/Madrid').tz_convert('UTC')

        mask = df['timestamp'] >= policy_start
        df.loc[mask, 'days_since_policy'] = (
            (df.loc[mask, 'timestamp'] - policy_start).dt.days
        )

        self.feature_names.extend(['iberian_exception', 'days_since_policy'])

        return df

    def create_all_features(self, df: pd.DataFrame, min_rows: int = 1,
                            max_nan_rate: float = 0.5) -> pd.DataFrame:
        """
        Apply full feature engineering pipeline.

        PIPELINE ORDER:
        1. Lag features (depend on raw prices only)
        2. Rolling features (depend on raw prices only)
        3. Time features (independent)
        4. Policy features (independent)

        Args:
            df: Raw panel data
            min_rows: raise if fewer rows than this survive the NaN drop
            max_nan_rate: warn about any column missing more than this share

        Returns:
            DataFrame with all features
        """
        print("Starting feature engineering pipeline...")

        # Reset so calling this twice does not duplicate feature names
        self.feature_names = []

        # Sort by country and timestamp (crucial for lag/rolling features)
        df = df.sort_values(['country', 'timestamp']).reset_index(drop=True)

        # Apply feature creation
        df = self.create_lag_features(df)
        df = self.create_rolling_features(df)
        df = self.create_time_features(df)
        df = self.create_policy_features(df)

        # Audit missingness before dropping anything: a column that is mostly
        # NaN (e.g. weather before it is downloaded) must be visible, not
        # silently empty the dataset (D-14)
        model_columns = self.feature_names + ['price_eur_mwh']
        nan_rates = df.isna().mean()
        for col, rate in nan_rates[nan_rates > max_nan_rate].items():
            used = "USED BY MODEL" if col in model_columns else "not used, ignored"
            print(f"[WARN] {col}: {rate:.0%} missing ({used})")

        # Drop rows with NaN only in the columns the model uses
        # (lag/rolling warm-up and genuinely missing prices)
        initial_rows = len(df)
        df = df.dropna(subset=model_columns).reset_index(drop=True)
        dropped_rows = initial_rows - len(df)

        print(f"[OK] Created {len(self.feature_names)} features")
        print(f"[OK] Dropped {dropped_rows} rows with NaN in model columns")
        print(f"[OK] Final dataset: {len(df):,} rows")

        if len(df) < min_rows:
            raise ValueError(
                f"Feature engineering produced {len(df):,} rows (< {min_rows:,}); "
                f"{dropped_rows:,} of {initial_rows:,} rows were dropped for NaN"
            )

        return df

    def prepare_train_test_split(self, df: pd.DataFrame,
                                  test_size: int = 30) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Time-based train/test split.

        INTERVIEW EXPLANATION:
        "We use time-based split, not random split, because:
        1. Preserves temporal ordering (crucial for time series)
        2. Tests forecasting ability on truly unseen future data
        3. Prevents data leakage from future to past

        We hold out last 30 days as test set, which gives us enough data
        to evaluate performance across different market conditions."

        Args:
            df: Full dataset with features
            test_size: Number of days for test set

        Returns:
            (train_df, test_df)
        """
        if df.empty:
            raise ValueError("Cannot split an empty dataset")

        # One split date for all countries (last test_size days)
        split_date = df['timestamp'].max() - pd.Timedelta(days=test_size)

        train_df = df[df['timestamp'] < split_date].reset_index(drop=True)
        test_df = df[df['timestamp'] >= split_date].reset_index(drop=True)

        if train_df.empty or test_df.empty:
            raise ValueError(f"Empty split: {len(train_df)} train rows, {len(test_df)} test rows")

        print(f"\nTrain/Test Split:")
        print(f"   Train: {train_df['timestamp'].min().date()} to {train_df['timestamp'].max().date()}")
        print(f"   Test:  {test_df['timestamp'].min().date()} to {test_df['timestamp'].max().date()}")
        print(f"   Train rows: {len(train_df):,}")
        print(f"   Test rows:  {len(test_df):,}")

        return train_df, test_df

    def get_feature_matrix(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """
        Extract X (features) and y (target) for modeling.

        Args:
            df: DataFrame with all features and target

        Returns:
            (X, y) as numpy arrays
        """
        X = df[self.feature_names].values
        y = df['price_eur_mwh'].values

        return X, y


if __name__ == "__main__":
    """
    Test feature engineering pipeline.

    RUN THIS TO VERIFY:
    python mlops/models/training/feature_engineering.py
    """
    import argparse
    import sys
    from pathlib import Path

    # Add project root to path
    project_root = Path(__file__).parents[3]
    sys.path.insert(0, str(project_root))

    parser = argparse.ArgumentParser(description="Build ML train/test feature sets.")
    parser.add_argument('--panel', type=Path, default=None,
                        help="panel parquet (default: the panel covering the most hours)")
    parser.add_argument('--countries', nargs='+', default=['ES', 'PT'])
    args = parser.parse_args()

    # Load panel data. Pick explicitly: glob order is arbitrary, and the old
    # `[-1]` could select the 7-day pilot panel, too short for 168h lags.
    processed_dir = project_root / 'data' / 'processed'
    panel_file = args.panel or max(processed_dir.glob("main_panel_*.parquet"),
                                   key=lambda f: len(pd.read_parquet(f, columns=['timestamp'])))

    print(f"Loading panel: {panel_file.name}")
    df = pd.read_parquet(panel_file)
    df = df[df['country'].isin(args.countries)]

    # Initialize feature engineer
    fe = PriceFeatureEngineer(forecast_horizon=24)

    # Create features
    df_features = fe.create_all_features(df)

    # Train/test split
    train_df, test_df = fe.prepare_train_test_split(df_features, test_size=30)

    # Extract feature matrices
    X_train, y_train = fe.get_feature_matrix(train_df)
    X_test, y_test = fe.get_feature_matrix(test_df)

    print(f"\n[OK] Feature engineering complete!")
    print(f"   X_train shape: {X_train.shape}")
    print(f"   X_test shape:  {X_test.shape}")
    print(f"   Features: {len(fe.feature_names)}")

    # Save processed data
    output_dir = project_root / 'mlops' / 'models' / 'training'
    output_dir.mkdir(parents=True, exist_ok=True)

    train_df.to_parquet(output_dir / 'train_data.parquet')
    test_df.to_parquet(output_dir / 'test_data.parquet')

    print(f"\nSaved processed data to:")
    print(f"   {output_dir / 'train_data.parquet'}")
    print(f"   {output_dir / 'test_data.parquet'}")