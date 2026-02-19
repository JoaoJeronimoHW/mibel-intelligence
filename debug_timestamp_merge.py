"""
Debug script to diagnose timestamp merge issue.
Run: python debug_timestamp_merge.py
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from src.utils.db_utils import get_connection
from src.utils.db_schema import create_schema

# Create schema and insert test data
create_schema()
conn = get_connection(readonly=False)

# Clean and insert sample data
conn.execute("DELETE FROM prices_day_ahead WHERE country IN ('ES', 'PT')")
conn.commit()

sample_prices = pd.DataFrame({
    'timestamp': pd.date_range('2022-06-15', periods=48, freq='h', tz='UTC').tolist() * 2,
    'country': ['ES'] * 48 + ['PT'] * 48,
    'price_eur_mwh': [100 + i for i in range(48)] + [105 + i for i in range(48)],
    'energy_mwh': [25000 + i*100 for i in range(48)] * 2
})

conn.execute("INSERT INTO prices_day_ahead SELECT * FROM sample_prices")
conn.commit()

# Query back
panel = conn.execute("""
    SELECT timestamp, country, price_eur_mwh, energy_mwh
    FROM prices_day_ahead
    WHERE country = 'ES'
    ORDER BY timestamp
""").fetchdf()

# Create weather data
sample_weather = pd.DataFrame({
    'timestamp': pd.date_range('2022-06-15', periods=48, freq='h', tz='UTC'),
    'country': ['ES'] * 48,
    'temperature_c': [20 + i*0.5 for i in range(48)]
})

print("="*70)
print("TIMESTAMP DIAGNOSTIC")
print("="*70)

print(f"\n1. ORIGINAL DATA TYPES:")
print(f"   Panel: {panel['timestamp'].dtype}")
print(f"   Weather: {sample_weather['timestamp'].dtype}")

# Convert to same type
panel['timestamp'] = pd.to_datetime(panel['timestamp'], utc=True).astype('datetime64[ns, UTC]')
sample_weather['timestamp'] = pd.to_datetime(sample_weather['timestamp'], utc=True).astype('datetime64[ns, UTC]')

print(f"\n2. AFTER CONVERSION:")
print(f"   Panel: {panel['timestamp'].dtype}")
print(f"   Weather: {sample_weather['timestamp'].dtype}")

print(f"\n3. FIRST 5 TIMESTAMPS FROM EACH:")
print("\n   Panel timestamps:")
for i, ts in enumerate(panel['timestamp'].head(5)):
    print(f"      [{i}] {ts} | {repr(ts)} | {ts.value}")

print("\n   Weather timestamps:")
for i, ts in enumerate(sample_weather['timestamp'].head(5)):
    print(f"      [{i}] {ts} | {repr(ts)} | {ts.value}")

print(f"\n4. CHECKING EQUALITY:")
for i in range(min(5, len(panel))):
    panel_ts = panel['timestamp'].iloc[i]
    weather_ts = sample_weather['timestamp'].iloc[i]
    equal = panel_ts == weather_ts
    print(f"   [{i}] {panel_ts} == {weather_ts} ? {equal}")
    if not equal:
        print(f"        Difference: {panel_ts.value - weather_ts.value} nanoseconds")

print(f"\n5. ATTEMPTING MERGE:")
merged = panel.merge(
    sample_weather[['timestamp', 'country', 'temperature_c']],
    on=['timestamp', 'country'],
    how='left'
)

print(f"   Panel shape: {panel.shape}")
print(f"   Weather shape: {sample_weather.shape}")
print(f"   Merged shape: {merged.shape}")
print(f"   Non-null temperatures: {merged['temperature_c'].notna().sum()} / {len(merged)}")

print(f"\n6. ROWS WITH MISSING WEATHER:")
missing = merged[merged['temperature_c'].isna()]
if len(missing) > 0:
    print(f"   Found {len(missing)} rows with missing weather:")
    for idx, row in missing.iterrows():
        print(f"      {row['timestamp']} | Country: {row['country']}")
        
        # Find closest match in weather data
        weather_ts = sample_weather['timestamp']
        closest_idx = (weather_ts - row['timestamp']).abs().idxmin()
        closest_ts = weather_ts.iloc[closest_idx]
        diff = (row['timestamp'] - closest_ts).total_seconds()
        print(f"        Closest weather timestamp: {closest_ts}")
        print(f"        Difference: {diff} seconds")
else:
    print("   No missing weather data!")

print(f"\n7. UNIQUE TIMESTAMP COUNTS:")
print(f"   Panel unique: {panel['timestamp'].nunique()}")
print(f"   Weather unique: {sample_weather['timestamp'].nunique()}")
print(f"   Intersection: {len(set(panel['timestamp']) & set(sample_weather['timestamp']))}")

# Clean up
conn.execute("DELETE FROM prices_day_ahead WHERE country IN ('ES', 'PT')")
conn.commit()
conn.close()

print("\n" + "="*70)
print("DIAGNOSTIC COMPLETE")
print("="*70)