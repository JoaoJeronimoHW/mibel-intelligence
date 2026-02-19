"""
Test Open-Meteo weather downloads.
Run: python tests/test_weather_download.py
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from src.data.weather_ingest import download_historical_weather

def test_weather_download():
    """Test downloading 1 day of weather for Madrid."""
    
    print("Testing Open-Meteo weather download (1 day, Madrid)...")
    print("No API key needed - this is free!\n")
    
    try:
        df = download_historical_weather(
            location_name='Madrid',
            latitude=40.4168,
            longitude=-3.7038,
            start_date='2022-06-15',
            end_date='2022-06-15'
        )
        
        assert not df.empty, "No data returned"
        
        print(f"  ✓ Downloaded {len(df)} hourly records")
        print(f"  ✓ Columns: {list(df.columns)}")
        print(f"\nSample data:")
        print(df.head())
        
        # Check we have expected columns
        expected_cols = ['temperature_c', 'wind_speed_100m', 'solar_radiation']
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"
            print(f"  ✓ Column '{col}' present")
        
        # Check reasonable values
        temp = df['temperature_c'].mean()
        wind = df['wind_speed_100m'].mean()
        solar = df['solar_radiation'].mean()
        
        print(f"\n  Average values:")
        print(f"    Temperature: {temp:.1f} °C")
        print(f"    Wind speed: {wind:.1f} m/s")
        print(f"    Solar radiation: {solar:.1f} W/m²")
        
        assert -20 < temp < 50, f"Unrealistic temperature: {temp}"
        assert 0 <= wind < 50, f"Unrealistic wind speed: {wind}"
        assert 0 <= solar <= 1500, f"Unrealistic solar radiation: {solar}"
        
        print("\n✅ Weather download test passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Weather download failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_weather_download()