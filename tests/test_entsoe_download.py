"""
Test ENTSO-E data downloads.
Run: python tests/test_entsoe_download.py
"""

import sys
from pathlib import Path
import os

sys.path.append(str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.data.entsoe_ingest import download_day_ahead_prices

def test_entsoe_prices():
    """Test downloading prices from ENTSO-E."""
    
    print("Testing ENTSO-E download...")
    
    # Check API key exists
    api_key = os.getenv('ENTSOE_API_KEY')
    if not api_key:
        print("\n❌ ENTSOE_API_KEY not found!")
        print("\nSetup instructions:")
        print("  1. Register at https://transparency.entsoe.eu")
        print("  2. Email transparency@entsoe.eu for API access")
        print("  3. Create .env file with: ENTSOE_API_KEY=your_key")
        return False
    
    print(f"  ✓ API key found (starts with: {api_key[:8]}...)")
    
    # Try multiple countries and date ranges to find data
    test_cases = [
        ('DE', '2022-06-15', '2022-06-16', 'Germany'),  # Germany usually has complete data
        ('FR', '2023-01-01', '2023-01-02', 'France'),
        ('NL', '2022-06-15', '2022-06-16', 'Netherlands'),
    ]
    
    success = False
    
    for country_code, start_date, end_date, country_name in test_cases:
        print(f"\n  Trying {country_name} ({country_code}): {start_date} to {end_date}...")
        
        try:
            df = download_day_ahead_prices(
                country_code=country_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                print(f"    ⚠ No data for {country_name}")
                continue
            
            # We got data!
            print(f"    ✅ Success! Downloaded {len(df)} hourly records")
            print(f"    ✓ Columns: {list(df.columns)}")
            print(f"\n    Sample data:")
            print(df.head(3))
            
            # Check price range
            if 'price_eur_mwh' in df.columns:
                avg_price = df['price_eur_mwh'].mean()
                min_price = df['price_eur_mwh'].min()
                max_price = df['price_eur_mwh'].max()
                
                print(f"\n    Price statistics:")
                print(f"      Min: {min_price:.2f} EUR/MWh")
                print(f"      Max: {max_price:.2f} EUR/MWh")
                print(f"      Avg: {avg_price:.2f} EUR/MWh")
                
                # Sanity check
                if min_price < -500 or max_price > 5000:
                    print(f"    ⚠ Warning: Unusual price range")
            
            success = True
            break  # Found working data, no need to try other countries
            
        except Exception as e:
            print(f"    ✗ Error: {e}")
            continue
    
    if success:
        print("\n✅ ENTSO-E download test passed!")
        print("\nWhat this means:")
        print("  ✓ Your API key is valid and working")
        print("  ✓ You can download data from ENTSO-E")
        print("  ✓ Ready to proceed with full data download")
        return True
    else:
        print("\n⚠️  Could not download data from any test case")
        print("\nPossible reasons:")
        print("  1. API key might not be fully activated (can take 24-48 hours)")
        print("  2. ENTSO-E server might be temporarily down")
        print("  3. The specific dates/countries tested don't have data")
        print("\nNext steps:")
        print("  - Wait 24 hours and try again")
        print("  - Check https://transparency.entsoe.eu/dashboard/show manually")
        print("  - Try different dates (more recent data is more reliable)")
        return False


if __name__ == "__main__":
    test_entsoe_prices()