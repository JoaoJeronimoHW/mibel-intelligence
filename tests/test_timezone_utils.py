"""
Test timezone utility functions.
Run: python tests/test_timezone_utils.py
"""

import sys
from pathlib import Path
import pandas as pd
import pytz

sys.path.append(str(Path(__file__).parent.parent))

from src.utils.timezone_utils import (
    normalize_to_utc,
    create_hour_index,
    add_time_features,
    handle_dst_transitions
)

def test_normalize_to_utc():
    """Test converting different timezones to UTC."""
    
    print("="*60)
    print("TEST 1: Normalize to UTC")
    print("="*60)
    
    # Test 1a: CET to UTC
    print("\n1a. Converting CET (Madrid) to UTC...")
    df_cet = pd.DataFrame({
        'timestamp': pd.date_range('2022-06-15 12:00', periods=3, freq='h', tz='Europe/Madrid'),
        'value': [1, 2, 3]
    })
    
    print(f"  Original (CET): {df_cet['timestamp'].iloc[0]}")
    
    df_utc = normalize_to_utc(df_cet)
    
    print(f"  Converted (UTC): {df_utc['timestamp'].iloc[0]}")
    
    # CET is UTC+2 in summer, so 12:00 CET = 10:00 UTC
    assert df_utc['timestamp'].iloc[0].hour == 10, "CET to UTC conversion failed"
    assert df_utc['timestamp'].iloc[0].tz == pytz.UTC, "Not in UTC timezone"
    
    print("  ✓ CET to UTC works correctly")
    
    # Test 1b: Already UTC (should not change)
    print("\n1b. Testing already-UTC data...")
    df_already_utc = pd.DataFrame({
        'timestamp': pd.date_range('2022-06-15 12:00', periods=3, freq='h', tz='UTC'),
        'value': [1, 2, 3]
    })
    
    df_result = normalize_to_utc(df_already_utc)
    
    assert df_result['timestamp'].iloc[0].hour == 12, "UTC data changed incorrectly"
    print("  ✓ Already-UTC data unchanged")
    
    # Test 1c: Naive timestamps (no timezone)
    print("\n1c. Testing naive timestamps (no timezone)...")
    df_naive = pd.DataFrame({
        'timestamp': pd.date_range('2022-06-15 12:00', periods=3, freq='h'),  # No tz
        'value': [1, 2, 3]
    })
    
    df_result = normalize_to_utc(df_naive)
    
    assert df_result['timestamp'].iloc[0].tz == pytz.UTC, "Naive timestamps not converted to UTC"
    print("  ✓ Naive timestamps handled (assumed UTC)")
    
    print("\n✅ TEST 1 PASSED: normalize_to_utc works correctly\n")
    return True


def test_create_hour_index():
    """Test creating complete hourly index."""
    
    print("="*60)
    print("TEST 2: Create Hour Index")
    print("="*60)
    
    # Test 2a: Multi-day index
    print("\n2a. Creating 3-day hourly index...")
    hour_index = create_hour_index('2022-06-15', '2022-06-17')
    
    expected_hours = 3 * 24  # 3 days × 24 hours
    actual_hours = len(hour_index)
    
    print(f"  Expected hours: {expected_hours}")
    print(f"  Actual hours: {actual_hours}")
    print(f"  First timestamp: {hour_index['timestamp'].iloc[0]}")
    print(f"  Last timestamp: {hour_index['timestamp'].iloc[-1]}")
    
    assert actual_hours == expected_hours, f"Expected {expected_hours} hours, got {actual_hours}"
    
    # Test 2b: No gaps
    print("\n2b. Checking for gaps...")
    time_diffs = hour_index['timestamp'].diff()
    gaps = time_diffs[time_diffs > pd.Timedelta(hours=1)]
    
    if gaps.empty:
        print("  ✓ No gaps in hourly sequence")
    else:
        print(f"  ✗ Found {len(gaps)} gaps!")
        return False
    
    # Test 2c: All in UTC
    print("\n2c. Checking timezone...")
    assert hour_index['timestamp'].iloc[0].tz == pytz.UTC, "Timestamps not in UTC"
    print("  ✓ All timestamps in UTC")
    
    # Test 2d: Exactly hourly
    print("\n2d. Checking hourly frequency...")
    # All differences (except first NaT) should be exactly 1 hour
    diffs = time_diffs.dropna()
    assert all(diffs == pd.Timedelta(hours=1)), "Not all intervals are exactly 1 hour"
    print("  ✓ All intervals exactly 1 hour")
    
    print("\n✅ TEST 2 PASSED: create_hour_index works correctly\n")
    return True


def test_add_time_features():
    """Test adding time-based features."""
    
    print("="*60)
    print("TEST 3: Add Time Features")
    print("="*60)
    
    # Create sample data
    df = pd.DataFrame({
        'timestamp': pd.date_range('2022-06-15', periods=48, freq='h', tz='UTC'),
        'value': range(48)
    })
    
    print(f"\n3a. Adding time features to {len(df)} hours...")
    df_with_features = add_time_features(df)
    
    # Check all expected features exist
    expected_features = [
        'hour', 'day_of_week', 'month', 'year', 
        'is_weekend', 'quarter', 'day_of_year', 'is_iberian_exception'
    ]
    
    print("\n3b. Checking expected features...")
    for feature in expected_features:
        if feature in df_with_features.columns:
            print(f"  ✓ {feature}")
        else:
            print(f"  ✗ {feature} MISSING!")
            return False
    
    # Test 3c: Validate hour values
    print("\n3c. Validating feature values...")
    assert df_with_features['hour'].min() == 0, "Hour min should be 0"
    assert df_with_features['hour'].max() == 23, "Hour max should be 23"
    print(f"  ✓ Hours range from 0 to 23")
    
    # Test 3d: Validate month
    assert df_with_features['month'].iloc[0] == 6, "Month should be June (6)"
    print(f"  ✓ Month is correct (June = 6)")
    
    # Test 3e: Validate year
    assert df_with_features['year'].iloc[0] == 2022, "Year should be 2022"
    print(f"  ✓ Year is correct (2022)")
    
    # Test 3f: Validate weekend detection
    # June 15, 2022 was a Wednesday (not weekend)
    # June 18, 2022 was a Saturday (weekend)

    # Find Wednesday and Saturday in our data
    df_june = df_with_features[df_with_features['timestamp'].dt.month == 6]
    wednesday_rows = df_june[df_june['timestamp'].dt.day == 15]
    saturday_rows = df_june[df_june['timestamp'].dt.day == 18]

    if len(wednesday_rows) > 0:
        wednesday = wednesday_rows['is_weekend'].iloc[0]
        assert wednesday == False, "Wednesday incorrectly marked as weekend"
        print(f"  ✓ Wednesday correctly marked as not weekend")

    if len(saturday_rows) > 0:
        saturday = saturday_rows['is_weekend'].iloc[0]
        assert saturday == True, "Saturday not marked as weekend"
        print(f"  ✓ Saturday correctly marked as weekend")
    else:
        print(f"  ⚠ Saturday not in test data (test range too short)")
    
    # Test 3g: Validate Iberian Exception indicator
    iberian_hours = df_with_features['is_iberian_exception'].sum()
    print(f"\n3d. Iberian Exception indicator...")
    print(f"  Hours in exception period: {iberian_hours} / {len(df_with_features)}")
    
    # June 15, 2022 is AFTER start (June 15), so should be True
    assert iberian_hours == 48, "All test hours should be in Iberian Exception period"
    print(f"  ✓ Iberian Exception period detected correctly")
    
    print("\n✅ TEST 3 PASSED: add_time_features works correctly\n")
    return True


def test_handle_dst_transitions():
    """Test DST transition handling."""
    
    print("="*60)
    print("TEST 4: Handle DST Transitions")
    print("="*60)
    
    # Test 4a: Normal data (no issues)
    print("\n4a. Testing normal data (no DST issues)...")
    df_normal = pd.DataFrame({
        'timestamp': pd.date_range('2022-06-15', periods=24, freq='h', tz='UTC'),
        'value': range(24)
    })
    
    df_result = handle_dst_transitions(df_normal)
    
    assert len(df_result) == 24, "Normal data should be unchanged"
    print("  ✓ Normal data passes through unchanged")
    
    # Test 4b: Duplicate timestamps
    print("\n4b. Testing duplicate timestamp handling...")
    df_duplicates = pd.DataFrame({
        'timestamp': ['2022-06-15 01:00', '2022-06-15 02:00', '2022-06-15 02:00', '2022-06-15 03:00'],
        'value': [1, 2, 3, 4]
    })
    df_duplicates['timestamp'] = pd.to_datetime(df_duplicates['timestamp'], utc=True)
    
    df_result = handle_dst_transitions(df_duplicates)
    
    # Should remove one duplicate
    assert len(df_result) == 3, f"Should remove duplicates, got {len(df_result)} rows"
    print(f"  ✓ Duplicates removed (4 rows → 3 rows)")
    
    # Test 4c: Data with gaps
    print("\n4c. Testing gap detection...")
    df_gap = pd.DataFrame({
        'timestamp': ['2022-06-15 01:00', '2022-06-15 02:00', '2022-06-15 05:00'],  # 3-hour gap!
        'value': [1, 2, 3]
    })
    df_gap['timestamp'] = pd.to_datetime(df_gap['timestamp'], utc=True)
    
    df_result = handle_dst_transitions(df_gap)
    
    # Should detect and log the gap (check console output)
    print("  ✓ Gap detection runs (check for warning above)")
    
    print("\n✅ TEST 4 PASSED: handle_dst_transitions works correctly\n")
    return True


def run_all_timezone_tests():
    """Run all timezone utility tests."""
    
    print("\n" + "="*60)
    print(" TIMEZONE UTILITIES TEST SUITE")
    print("="*60 + "\n")
    
    results = []
    
    try:
        results.append(("normalize_to_utc", test_normalize_to_utc()))
    except Exception as e:
        print(f"❌ normalize_to_utc FAILED: {e}")
        results.append(("normalize_to_utc", False))
    
    try:
        results.append(("create_hour_index", test_create_hour_index()))
    except Exception as e:
        print(f"❌ create_hour_index FAILED: {e}")
        results.append(("create_hour_index", False))
    
    try:
        results.append(("add_time_features", test_add_time_features()))
    except Exception as e:
        print(f"❌ add_time_features FAILED: {e}")
        results.append(("add_time_features", False))
    
    try:
        results.append(("handle_dst_transitions", test_handle_dst_transitions()))
    except Exception as e:
        print(f"❌ handle_dst_transitions FAILED: {e}")
        results.append(("handle_dst_transitions", False))
    
    # Summary
    print("\n" + "="*60)
    print(" TEST SUMMARY")
    print("="*60)
    
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {name}")
    
    total = len(results)
    passed = sum(1 for _, p in results if p)
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TIMEZONE TESTS PASSED!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
    
    return passed == total


if __name__ == "__main__":
    success = run_all_timezone_tests()
    sys.exit(0 if success else 1)