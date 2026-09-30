"""
Diagnose where duplicate (timestamp, country) keys come from in the raw OMIE files.

Uses the same wide->long transformation as the loader (src.data.load_to_db),
so what it reports is exactly what the loader would see. Expected result
after the D-01 fix: overlapping chunk files may produce byte-identical
duplicates (harmless), and there are NO conflicting prices -- the DST fold
hour is two distinct UTC hours, not a collision.

Run: python diagnose_duplicates.py
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from src.data.load_to_db import omie_wide_to_long, RAW_DIR

price_files = sorted((RAW_DIR / "omie").glob("day_ahead_prices_*.parquet"))

print("=" * 80)
print(" DUPLICATE DIAGNOSIS")
print("=" * 80)

frames = []
for file in price_files:
    long = omie_wide_to_long(pd.read_parquet(file))
    long['file'] = file.name
    frames.append(long)
    print(f"  {file.name}: {len(long):,} rows, "
          f"{long['timestamp'].min()} to {long['timestamp'].max()} (UTC)")

if not frames:
    print("[ERROR] No OMIE files found")
    sys.exit(1)

combined = pd.concat(frames, ignore_index=True)
print(f"\nTotal rows: {len(combined):,}")

dups = combined[combined.duplicated(subset=['timestamp', 'country'], keep=False)]
if dups.empty:
    print("[OK] No duplicate (timestamp, country) keys")
else:
    conflicting = dups.groupby(['timestamp', 'country'])['price_eur_mwh'].nunique() > 1
    print(f"[INFO] {len(dups):,} rows share a key with another row")
    print(f"       identical across files (harmless): {int((~conflicting).sum())} keys")
    if conflicting.any():
        print(f"[ERROR] conflicting prices for {int(conflicting.sum())} keys:")
        keys = conflicting[conflicting].index
        print(dups.set_index(['timestamp', 'country']).loc[keys])

# The autumn DST fold must yield 25 distinct UTC hours, all present
fold = combined[(combined['country'] == 'ES') &
                (combined['timestamp'] >= pd.Timestamp('2022-10-29 22:00', tz='UTC')) &
                (combined['timestamp'] < pd.Timestamp('2022-10-30 23:00', tz='UTC'))]
print(f"\nMarket day 2022-10-30 (25-hour day), ES: {fold['timestamp'].nunique()} distinct UTC hours")
print("[OK]" if fold['timestamp'].nunique() == 25 else "[ERROR] expected 25")
