"""
Load downloaded OMIE data into DuckDB.
Run: python load_omie_to_db.py
"""

import pandas as pd
from pathlib import Path
from src.utils.db_utils import get_connection
from src.utils.db_schema import create_schema

print("="*70)
print("LOADING OMIE DATA INTO DATABASE")
print("="*70)

# Create schema
create_schema()

# Get connection
conn = get_connection(readonly=False)

# Clear existing data
print("\n1. Clearing existing data...")
conn.execute("DELETE FROM prices_day_ahead")
conn.commit()
print("   ✓ Table cleared")

# Find OMIE files
raw_dir = Path("data/raw/omie")
price_files = list(raw_dir.glob("day_ahead_prices_*.parquet"))

if not price_files:
    print(f"\n❌ No price files found in {raw_dir}")
    print("   Run download script first!")
    exit(1)

print(f"\n2. Found {len(price_files)} price files")

total_rows = 0

for file in price_files:
    print(f"\n   Loading {file.name}...")
    df = pd.read_parquet(file)
    
    print(f"      Raw shape: {df.shape}")
    print(f"      Unique concepts: {df['CONCEPT'].unique() if 'CONCEPT' in df.columns else 'N/A'}")
    
    # Filter for price rows only
    price_spain = df[df['CONCEPT'] == 'PRICE_SP'].copy()
    price_portugal = df[df['CONCEPT'] == 'PRICE_PT'].copy()
    
    print(f"      Spain price rows: {len(price_spain)}")
    print(f"      Portugal price rows: {len(price_portugal)}")
    
    if len(price_spain) == 0 and len(price_portugal) == 0:
        print("      ⚠️ No price data found, skipping...")
        continue
    
    # Get hour columns (H1, H2, ... H24, excluding H25 which is DST overflow)
    hour_cols = [f'H{i}' for i in range(1, 25)]
    
    # Transform Spain prices
    long_data = []
    
    for _, row in price_spain.iterrows():
        date = pd.Timestamp(row['DATE'])
        
        for i, hour_col in enumerate(hour_cols):
            if hour_col in row and pd.notna(row[hour_col]):
                timestamp = date + pd.Timedelta(hours=i)
                timestamp = timestamp.tz_localize('UTC')
                
                long_data.append({
                    'timestamp': timestamp,
                    'country': 'ES',
                    'price_eur_mwh': float(row[hour_col]),
                    'energy_mwh': None
                })
    
    # Transform Portugal prices
    for _, row in price_portugal.iterrows():
        date = pd.Timestamp(row['DATE'])
        
        for i, hour_col in enumerate(hour_cols):
            if hour_col in row and pd.notna(row[hour_col]):
                timestamp = date + pd.Timedelta(hours=i)
                timestamp = timestamp.tz_localize('UTC')
                
                long_data.append({
                    'timestamp': timestamp,
                    'country': 'PT',
                    'price_eur_mwh': float(row[hour_col]),
                    'energy_mwh': None
                })
    
    df_long = pd.DataFrame(long_data)
    
    print(f"      Transformed to {len(df_long):,} rows")
    
    if len(df_long) > 0:
        # Show sample
        print(f"      Sample transformed data:")
        print(df_long.head(3))
        
        # Check for NaN prices
        nan_prices = df_long['price_eur_mwh'].isna().sum()
        if nan_prices > 0:
            print(f"      ⚠️ Found {nan_prices} NaN prices, dropping...")
            df_long = df_long[df_long['price_eur_mwh'].notna()]
        
        # Insert - duplicates will be skipped
        try:
            conn.execute("INSERT OR IGNORE INTO prices_day_ahead SELECT * FROM df_long")
            conn.commit()
            
            # Count actual inserts (difficult to get exact count with OR IGNORE)
            total_rows += len(df_long)
            print(f"      ✓ Processed {len(df_long):,} rows (duplicates ignored)")
            
        except Exception as e:
            print(f"      ❌ Insert failed: {e}")
            raise

print(f"\n3. Loading complete!")
print(f"   Total rows loaded: {total_rows:,}")

# Verify
result = conn.execute("""
    SELECT 
        COUNT(*) as count,
        MIN(timestamp) as start,
        MAX(timestamp) as end,
        COUNT(DISTINCT country) as countries
    FROM prices_day_ahead
""").fetchdf()

print(f"\n4. Verification:")
print(f"   Rows: {result['count'].iloc[0]:,}")
print(f"   Date range: {result['start'].iloc[0]} to {result['end'].iloc[0]}")
print(f"   Countries: {result['countries'].iloc[0]}")

conn.close()

print("\n✅ DATA LOADED INTO DATABASE!")
print("="*70)