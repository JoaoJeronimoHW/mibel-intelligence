"""
Test OMIE data downloads with tiny sample.
Run: python tests/test_omie_download.py
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))

from src.data.omie_ingest import download_day_ahead_prices

def test_omie_prices_download():
    """Test downloading just 1 day of OMIE prices."""
    
    print("Testing OMIE price download (1 day)...")
    print("This will take ~30 seconds\n")
    
    try:
        # Download just 1 day
        df = download_day_ahead_prices(
            start_date='2022-06-15',  # Iberian Exception start
            end_date='2022-06-15'     # Same day
        )
        
        # Validate data
        print("Validating downloaded data...")
        
        assert df is not None, "No data returned"
        assert len(df) > 0, "Empty DataFrame"
        assert 'DATE' in df.columns, "No DATE column"
        assert 'CONCEPT' in df.columns, "No CONCEPT column"
        
        # Check we have hour columns (H1, H2, etc.)
        hour_cols = [col for col in df.columns if col.startswith('H') and col[1:].isdigit()]
        assert len(hour_cols) >= 24, f"Expected 24 hour columns, got {len(hour_cols)}"
        
        print(f"  ✓ Downloaded {len(df)} rows (concepts)")
        print(f"  ✓ Found {len(hour_cols)} hour columns: {hour_cols[:5]}...{hour_cols[-2:]}")
        print(f"  ✓ Columns: {list(df.columns)}")
        
        print(f"\nFirst few rows:")
        print(df.head())
        
        # Extract Spanish prices to validate
        spain_prices = df[df['CONCEPT'] == 'PRICE_SP']
        
        if not spain_prices.empty:
            print(f"\n✅ Spanish prices found!")
            
            # Get prices from hour columns (H1-H24)
            price_cols = [col for col in spain_prices.columns if col.startswith('H') and len(col) <= 3]
            prices = spain_prices[price_cols].iloc[0].dropna()
            
            print(f"\n  Price statistics for {spain_prices['DATE'].iloc[0]}:")
            print(f"    Min: {prices.min():.2f} EUR/MWh")
            print(f"    Max: {prices.max():.2f} EUR/MWh")
            print(f"    Avg: {prices.mean():.2f} EUR/MWh")
            print(f"    Number of hours: {len(prices)}")
            
            # Sanity checks
            assert prices.min() >= 0, f"Negative prices found: {prices.min()}"
            assert prices.max() < 1000, f"Suspiciously high price: {prices.max()}"
            
            # Show hourly prices
            print(f"\n  Hourly prices:")
            for i, (hour, price) in enumerate(prices.items(), 1):
                print(f"    Hour {i:2d}: {price:7.2f} EUR/MWh")
                if i >= 6:  # Just show first 6 hours
                    print(f"    ... ({len(prices) - 6} more hours)")
                    break
        
        # Check Portuguese prices too
        portugal_prices = df[df['CONCEPT'] == 'PRICE_PT']
        if not portugal_prices.empty:
            print(f"\n  ✓ Portuguese prices also found")
            
            # Check if Spain and Portugal prices are the same (market coupling)
            pt_prices = portugal_prices[price_cols].iloc[0].dropna()
            if prices.equals(pt_prices):
                print(f"  ✓ Spain and Portugal prices are identical (market coupled)")
            else:
                print(f"  ℹ Spain and Portugal prices differ (market splitting occurred)")
        
        print("\n✅ OMIE download test passed!")
        print("\nData structure explanation:")
        print("  - Each row is a 'CONCEPT' (PRICE_SP, PRICE_PT, ENER_IB, etc.)")
        print("  - Each column H1-H24 represents one hour of the day")
        print("  - This is the native OMIE format (wide format)")
        print("  - Later, we'll reshape to long format for analysis")
        
        return True
        
    except Exception as e:
        print(f"\n❌ OMIE download test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_omie_prices_download()