"""
Fix data loading issues and reload everything correctly.
Run: python fix_and_reload_data.py
"""

import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from src.utils.db_utils import get_connection
from src.utils.db_schema import create_schema

print("="*80)
print(" FIXING AND RELOADING DATA")
print("="*80)

# 1. Create schema
create_schema()

# 2. Clear existing data
conn = get_connection(readonly=False)
print("\n1. Clearing all existing data...")
conn.execute("DELETE FROM prices_day_ahead")
conn.commit()
print("   ✓ Cleared")

# 3. Load OMIE data with CORRECT transformation
raw_dir = Path("data/raw/omie")
price_files = list(raw_dir.glob("day_ahead_prices_*.parquet"))

print(f"\n2. Found {len(price_files)} OMIE files to load")

all_data = []

for file in price_files:
    print(f"\n   Processing {file.name}...")
    df = pd.read_parquet(file)
    
    print(f"     Raw shape: {df.shape}")
    print(f"     Concepts: {df['CONCEPT'].unique()}")
    
    # Filter for prices only
    price_spain = df[df['CONCEPT'] == 'PRICE_SP'].copy()
    price_portugal = df[df['CONCEPT'] == 'PRICE_PT'].copy()
    
    # Hour columns (H1 = 00:00-01:00, H2 = 01:00-02:00, etc.)
    hour_cols = [f'H{i}' for i in range(1, 25)]
    
    long_data = []
    
    # Process Spain
    for _, row in price_spain.iterrows():
        date = pd.Timestamp(row['DATE'])
        
        for i, hour_col in enumerate(hour_cols):
            if hour_col in row and pd.notna(row[hour_col]):
                # CRITICAL: H1 = hour 0 (00:00), H2 = hour 1 (01:00), etc.
                # So we use i directly, not i+1
                timestamp = pd.Timestamp(f"{date.date()} {i:02d}:00:00", tz='UTC')
                
                long_data.append({
                    'timestamp': timestamp,
                    'country': 'ES',
                    'price_eur_mwh': float(row[hour_col]),
                    'energy_mwh': None
                })
    
    # Process Portugal
    for _, row in price_portugal.iterrows():
        date = pd.Timestamp(row['DATE'])
        
        for i, hour_col in enumerate(hour_cols):
            if hour_col in row and pd.notna(row[hour_col]):
                timestamp = pd.Timestamp(f"{date.date()} {i:02d}:00:00", tz='UTC')
                
                long_data.append({
                    'timestamp': timestamp,
                    'country': 'PT',
                    'price_eur_mwh': float(row[hour_col]),
                    'energy_mwh': None
                })
    
    df_long = pd.DataFrame(long_data)
    all_data.append(df_long)
    print(f"     Transformed to {len(df_long)} rows")
    print(f"     First timestamp: {df_long['timestamp'].min()}")
    print(f"     Last timestamp: {df_long['timestamp'].max()}")

# Combine all files
combined = pd.concat(all_data, ignore_index=True)

# Remove duplicates (from overlapping files)
print(f"\n3. Combined data: {len(combined)} rows")
combined = combined.drop_duplicates(subset=['timestamp', 'country'])
print(f"   After deduplication: {len(combined)} rows")

# Sort
combined = combined.sort_values(['country', 'timestamp']).reset_index(drop=True)

# Insert
print(f"\n4. Inserting into database...")
conn.execute("INSERT INTO prices_day_ahead SELECT * FROM combined")
conn.commit()
print(f"   ✓ Inserted {len(combined)} rows")

# Verify
print(f"\n5. Verification:")
result = conn.execute("""
    SELECT 
        COUNT(*) as count,
        MIN(timestamp) as start,
        MAX(timestamp) as end,
        COUNT(DISTINCT country) as countries,
        COUNT(DISTINCT DATE_TRUNC('day', timestamp)) as days
    FROM prices_day_ahead
""").fetchdf()

print(f"   Rows: {result['count'].iloc[0]}")
print(f"   Start: {result['start'].iloc[0]}")
print(f"   End: {result['end'].iloc[0]}")
print(f"   Countries: {result['countries'].iloc[0]}")
print(f"   Days: {result['days'].iloc[0]}")

# Show hourly breakdown for first day
print(f"\n6. First day hourly breakdown:")
first_day = conn.execute("""
    SELECT 
        timestamp,
        country,
        price_eur_mwh
    FROM prices_day_ahead
    WHERE DATE_TRUNC('day', timestamp) = '2022-06-15'
    ORDER BY country, timestamp
    LIMIT 5
""").fetchdf()

print(first_day)

conn.close()

print("\n" + "="*80)
print(" DATA RELOAD COMPLETE")
print("="*80)
print("\nNext steps:")
print("  1. Rebuild panel: python -c \"from src.data.build_panel import build_main_panel; build_main_panel('2022-06-15', '2022-06-22', ['ES', 'PT'])\"")
print("  2. Or in notebook: from src.data.build_panel import build_main_panel; panel = build_main_panel('2022-06-15', '2022-06-22', ['ES', 'PT'])")