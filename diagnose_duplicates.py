"""
Diagnose exactly where duplicates are coming from.
Run: python diagnose_duplicates.py
"""

import pandas as pd
from pathlib import Path

raw_dir = Path("data/raw/omie")
price_files = sorted(raw_dir.glob("day_ahead_prices_*.parquet"))

print("="*80)
print(" DUPLICATE DIAGNOSIS")
print("="*80)

all_data = []

for file in price_files:
    print(f"\nFile: {file.name}")
    df = pd.read_parquet(file)
    
    # Get date range
    if 'DATE' in df.columns:
        dates = pd.to_datetime(df['DATE']).dt.date
        print(f"  Date range: {dates.min()} to {dates.max()}")
        print(f"  Unique dates: {dates.nunique()}")
    
    # Filter for prices
    price_spain = df[df['CONCEPT'] == 'PRICE_SP'].copy()
    
    # Check for October 30, 2022 specifically
    oct30_rows = price_spain[price_spain['DATE'].astype(str).str.contains('2022-10-30')]
    
    if len(oct30_rows) > 0:
        print(f"\n  ⚠️ Contains October 30, 2022!")
        print(f"  Columns: {oct30_rows.columns.tolist()}")
        
        # Check what hours exist
        hour_cols = [col for col in oct30_rows.columns if col.startswith('H')]
        print(f"  Hour columns: {hour_cols}")
        
        # Show the actual data
        print(f"\n  October 30, 2022 data:")
        for col in hour_cols:
            val = oct30_rows[col].iloc[0] if len(oct30_rows) > 0 else None
            if pd.notna(val):
                print(f"    {col}: {val}")

print("\n" + "="*80)
print("Now processing all files to find duplicates...")
print("="*80)

# Process all files
for file in price_files:
    df = pd.read_parquet(file)
    
    price_spain = df[df['CONCEPT'] == 'PRICE_SP'].copy()
    
    long_data = []
    
    for _, row in price_spain.iterrows():
        date_str = str(row['DATE'])
        base_date = pd.Timestamp(date_str).date()
        
        # Only H1-H24
        for hour_num in range(1, 25):
            hour_col = f'H{hour_num}'
            
            if hour_col in row and pd.notna(row[hour_col]):
                hour_of_day = hour_num - 1
                timestamp = pd.Timestamp(f"{base_date} {hour_of_day:02d}:00:00", tz='UTC')
                
                long_data.append({
                    'timestamp': timestamp,
                    'country': 'ES',
                    'price_eur_mwh': float(row[hour_col]),
                    'file': file.name
                })
    
    if long_data:
        all_data.extend(long_data)

# Convert to DataFrame
combined = pd.DataFrame(all_data)

print(f"\nTotal rows created: {len(combined)}")

# Find duplicates
print("\nLooking for duplicates...")
duplicates = combined[combined.duplicated(subset=['timestamp', 'country'], keep=False)]

if len(duplicates) > 0:
    print(f"Found {len(duplicates)} duplicate rows\n")
    
    # Group by timestamp to see which timestamps are duplicated
    dup_groups = duplicates.groupby(['timestamp', 'country'])
    
    print("Duplicate timestamp-country pairs:")
    for (ts, country), group in dup_groups:
        print(f"\n  {ts} {country}:")
        print(f"    Appears in {len(group)} rows")
        print(f"    From files: {group['file'].unique().tolist()}")
        print(f"    Prices: {group['price_eur_mwh'].unique().tolist()}")
else:
    print("✅ No duplicates found!")

# Specifically check October 30, 2022 01:00
oct30_01 = combined[(combined['timestamp'] == pd.Timestamp('2022-10-30 01:00:00', tz='UTC')) & 
                     (combined['country'] == 'ES')]

print(f"\n" + "="*80)
print("Specific check: 2022-10-30 01:00:00 ES")
print("="*80)
print(f"Occurrences: {len(oct30_01)}")
if len(oct30_01) > 0:
    print("\nDetails:")
    print(oct30_01[['timestamp', 'country', 'price_eur_mwh', 'file']])