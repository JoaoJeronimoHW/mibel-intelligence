"""
Load OMIE data with bulletproof duplicate handling using INSERT OR REPLACE.
Run: python fix_midnight_hour.py
"""

import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from src.utils.db_utils import get_connection
from src.utils.db_schema import create_schema

print("="*80)
print(" LOADING ALL OMIE DATA (BULLETPROOF VERSION)")
print("="*80)

# Create schema
create_schema()

# Get connection
conn = get_connection(readonly=False)

# Clear existing data
print("\n1. Clearing existing data...")
conn.execute("DELETE FROM prices_day_ahead")
conn.commit()
print("   ✓ Cleared")

# Load OMIE files
raw_dir = Path("data/raw/omie")
price_files = sorted(raw_dir.glob("day_ahead_prices_*.parquet"))

if not price_files:
    print("\n❌ No OMIE files found!")
    sys.exit(1)

print(f"\n2. Found {len(price_files)} OMIE files")

# Process each file and insert directly with OR REPLACE
total_inserted = 0

for file_idx, file in enumerate(price_files, 1):
    print(f"\n   [{file_idx}/{len(price_files)}] {file.name}")
    
    try:
        df = pd.read_parquet(file)
        
        # Filter for price rows
        price_spain = df[df['CONCEPT'] == 'PRICE_SP'].copy()
        price_portugal = df[df['CONCEPT'] == 'PRICE_PT'].copy()
        
        rows_processed = 0
        
        # Process Spain - insert row by row with OR REPLACE
        for _, row in price_spain.iterrows():
            date_str = str(row['DATE'])
            base_date = pd.Timestamp(date_str).date()
            
            # Only H1-H24 (skip H25 for DST)
            for hour_num in range(1, 25):
                hour_col = f'H{hour_num}'
                
                if hour_col in row and pd.notna(row[hour_col]):
                    hour_of_day = hour_num - 1
                    timestamp = pd.Timestamp(f"{base_date} {hour_of_day:02d}:00:00", tz='UTC')
                    price = float(row[hour_col])
                    
                    # INSERT OR REPLACE - will overwrite if duplicate
                    conn.execute("""
                        INSERT OR REPLACE INTO prices_day_ahead 
                        (timestamp, country, price_eur_mwh, energy_mwh)
                        VALUES (?, ?, ?, ?)
                    """, [timestamp, 'ES', price, None])
                    
                    rows_processed += 1
        
        # Process Portugal
        for _, row in price_portugal.iterrows():
            date_str = str(row['DATE'])
            base_date = pd.Timestamp(date_str).date()
            
            for hour_num in range(1, 25):
                hour_col = f'H{hour_num}'
                
                if hour_col in row and pd.notna(row[hour_col]):
                    hour_of_day = hour_num - 1
                    timestamp = pd.Timestamp(f"{base_date} {hour_of_day:02d}:00:00", tz='UTC')
                    price = float(row[hour_col])
                    
                    conn.execute("""
                        INSERT OR REPLACE INTO prices_day_ahead 
                        (timestamp, country, price_eur_mwh, energy_mwh)
                        VALUES (?, ?, ?, ?)
                    """, [timestamp, 'PT', price, None])
                    
                    rows_processed += 1
        
        # Commit after each file
        conn.commit()
        total_inserted += rows_processed
        
        print(f"     ✓ Processed {rows_processed} rows")
        
    except Exception as e:
        print(f"     ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        continue

print(f"\n3. Total operations: {total_inserted:,}")

# Verify final count
print(f"\n4. Verification:")
result = conn.execute("""
    SELECT 
        COUNT(*) as count,
        MIN(timestamp) as start,
        MAX(timestamp) as end,
        COUNT(DISTINCT country) as countries,
        COUNT(DISTINCT DATE_TRUNC('day', timestamp)) as days
    FROM prices_day_ahead
""").fetchdf()

print(f"   Rows in database: {result['count'].iloc[0]:,}")
print(f"   Date range: {result['start'].iloc[0]} to {result['end'].iloc[0]}")
print(f"   Countries: {result['countries'].iloc[0]}")
print(f"   Days: {result['days'].iloc[0]}")

# Check for any remaining duplicates (shouldn't be any)
dups = conn.execute("""
    SELECT timestamp, country, COUNT(*) as count
    FROM prices_day_ahead
    GROUP BY timestamp, country
    HAVING COUNT(*) > 1
""").fetchdf()

if len(dups) > 0:
    print(f"\n   ⚠️ Still have {len(dups)} duplicates (this shouldn't happen!):")
    print(dups)
else:
    print(f"\n   ✅ No duplicates in database")

conn.close()

print("\n" + "="*80)
print(" LOAD COMPLETE")
print("="*80)
print("\nNote: This script used INSERT OR REPLACE")
print("Duplicates from overlapping files were automatically handled")
print("="*80)

































