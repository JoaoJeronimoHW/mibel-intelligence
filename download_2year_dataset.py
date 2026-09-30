"""
Download 2-year dataset for Iberian Exception analysis.
Coverage: Jan 2022 - Dec 2023 (24 months)

This gives you:
- 6 months PRE-treatment (Jan - Jun 14, 2022)
- 18 months TREATMENT (Jun 15, 2022 - Dec 31, 2023)
- Plus you can extend to 2024 for POST-treatment

Equivalent to `python -m src.data.omie_ingest` with its defaults: eight
quarterly raw files, no interactive prompt (D-17), so it can run unattended.

Run: python download_2year_dataset.py
"""

import time

from src.data.omie_ingest import batch_download_prices

print("=" * 80)
print(" DOWNLOADING 2-YEAR IBERIAN EXCEPTION DATASET")
print("=" * 80)
print("\nDate range: January 1, 2022 - December 31, 2023 (24 months)")
print("Iberian Exception: June 15, 2022 - December 31, 2023")
print("Downloading in quarterly chunks for reliability (Ctrl+C to cancel)...")
print("=" * 80)

start_time = time.time()

prices = batch_download_prices('2022-01-01', '2023-12-31', chunk_months=3)

elapsed = time.time() - start_time
print("\n" + "=" * 80)
print(" DOWNLOAD COMPLETE" if prices is not None else " DOWNLOAD FAILED")
print("=" * 80)
print(f"Total time: {int(elapsed // 3600)}h {int((elapsed % 3600) // 60)}m")
print("\nNext steps:")
print("  1. Run: python -m src.data.load_to_db")
print("  2. Run: python -m src.data.build_panel")
print("=" * 80)
