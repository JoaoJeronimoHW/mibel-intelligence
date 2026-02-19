"""
Download 2-year dataset for Iberian Exception analysis.
Coverage: Jan 2022 - Dec 2023 (24 months)

This gives you:
- 6 months PRE-treatment (Jan - Jun 14, 2022)
- 18 months TREATMENT (Jun 15, 2022 - Dec 31, 2023)
- Plus you can extend to 2024 for POST-treatment

Run: python download_2year_dataset.py
"""

from src.data.omie_ingest import download_day_ahead_prices
import time
from datetime import datetime

print("="*80)
print(" DOWNLOADING 2-YEAR IBERIAN EXCEPTION DATASET")
print("="*80)
print("\nDate range: January 1, 2022 - December 31, 2023 (24 months)")
print("Iberian Exception: June 15, 2022 - December 31, 2023")
print("\nThis will take approximately 2-3 hours")
print("You can let this run in the background or overnight")
print("="*80)

input("\nPress Enter to start download (or Ctrl+C to cancel)...")

start_time = time.time()

# Download in chunks to avoid timeouts and allow recovery if interrupted
chunks = [
    ('2022-01-01', '2022-03-31', 'Q1 2022'),
    ('2022-04-01', '2022-06-30', 'Q2 2022'),
    ('2022-07-01', '2022-09-30', 'Q3 2022'),
    ('2022-10-01', '2022-12-31', 'Q4 2022'),
    ('2023-01-01', '2023-03-31', 'Q1 2023'),
    ('2023-04-01', '2023-06-30', 'Q2 2023'),
    ('2023-07-01', '2023-09-30', 'Q3 2023'),
    ('2023-10-01', '2023-12-31', 'Q4 2023'),
]

print("\nDownloading in quarterly chunks for reliability...\n")

for start_date, end_date, label in chunks:
    print(f"\n{'='*80}")
    print(f"Downloading {label}: {start_date} to {end_date}")
    print(f"{'='*80}")
    
    try:
        prices = download_day_ahead_prices(start_date, end_date)
        
        if prices is not None and len(prices) > 0:
            print(f"✅ {label} complete: {len(prices)} rows downloaded")
        else:
            print(f"⚠️ {label}: No data returned (check for errors above)")
            
    except Exception as e:
        print(f"❌ {label} failed: {e}")
        print("Continuing with next chunk...")
        continue
    
    # Brief pause between chunks
    print("Pausing 5 seconds before next chunk...")
    time.sleep(5)

elapsed = time.time() - start_time
hours = int(elapsed // 3600)
minutes = int((elapsed % 3600) // 60)

print("\n" + "="*80)
print(" DOWNLOAD COMPLETE")
print("="*80)
print(f"Total time: {hours}h {minutes}m")
print("\nNext steps:")
print("  1. Run: python fix_midnight_hour.py")
print("  2. Then rebuild panel with full date range")
print("="*80)