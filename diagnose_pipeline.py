"""
Comprehensive Pipeline Diagnostic Tool
Run: python diagnose_pipeline.py

This will check every step of your data pipeline and identify issues.
"""

import sys
from pathlib import Path
import pandas as pd

print("="*80)
print(" MIBEL INTELLIGENCE PROJECT - COMPREHENSIVE DIAGNOSTICS")
print("="*80)

# Add project to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def section(title):
    """Print a section header."""
    print(f"\n{'='*80}")
    print(f" {title}")
    print("="*80)

def check(description, condition, details=""):
    """Print a check result."""
    status = "✅ PASS" if condition else "❌ FAIL"
    print(f"{status}: {description}")
    if details:
        print(f"     {details}")
    return condition

# ============================================================================
# 1. PROJECT STRUCTURE
# ============================================================================
section("1. PROJECT STRUCTURE")

required_dirs = {
    'src': project_root / 'src',
    'src/data': project_root / 'src' / 'data',
    'src/utils': project_root / 'src' / 'utils',
    'data': project_root / 'data',
    'data/raw': project_root / 'data' / 'raw',
    'data/processed': project_root / 'data' / 'processed',
    'notebooks': project_root / 'notebooks',
}

structure_ok = True
for name, path in required_dirs.items():
    exists = path.exists()
    check(f"Directory exists: {name}", exists, str(path))
    structure_ok = structure_ok and exists

required_files = {
    'src/__init__.py': project_root / 'src' / '__init__.py',
    'src/utils/db_utils.py': project_root / 'src' / 'utils' / 'db_utils.py',
    'src/utils/db_schema.py': project_root / 'src' / 'utils' / 'db_schema.py',
    'src/data/load_to_db.py': project_root / 'src' / 'data' / 'load_to_db.py',
    'src/data/build_panel.py': project_root / 'src' / 'data' / 'build_panel.py',
}

for name, path in required_files.items():
    exists = path.exists()
    check(f"File exists: {name}", exists, str(path))
    structure_ok = structure_ok and exists

# ============================================================================
# 2. PYTHON ENVIRONMENT
# ============================================================================
section("2. PYTHON ENVIRONMENT")

print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")

required_packages = {
    'pandas': 'pandas',
    'numpy': 'numpy',
    'duckdb': 'duckdb',
    'matplotlib': 'matplotlib',
    'pytz': 'pytz',
}

env_ok = True
for display_name, import_name in required_packages.items():
    try:
        module = __import__(import_name)
        version = getattr(module, '__version__', 'unknown')
        check(f"Package installed: {display_name}", True, f"version {version}")
    except ImportError:
        check(f"Package installed: {display_name}", False, "NOT INSTALLED")
        env_ok = False

# ============================================================================
# 3. DOWNLOADED DATA FILES
# ============================================================================
section("3. DOWNLOADED DATA FILES")

raw_dir = project_root / 'data' / 'raw'

# Check OMIE data
omie_dir = raw_dir / 'omie'
if omie_dir.exists():
    omie_files = list(omie_dir.glob("*.parquet"))
    check(f"OMIE directory exists", True, str(omie_dir))
    print(f"     Found {len(omie_files)} OMIE files:")
    for f in omie_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"       - {f.name} ({size_mb:.2f} MB)")
        
        # Check file content
        try:
            df = pd.read_parquet(f)
            print(f"         Shape: {df.shape}, Columns: {list(df.columns)[:5]}...")
        except Exception as e:
            print(f"         ERROR reading file: {e}")
else:
    check(f"OMIE directory exists", False, "Directory not found")

# Check ENTSO-E data
entsoe_dir = raw_dir / 'entsoe'
if entsoe_dir.exists():
    entsoe_files = list(entsoe_dir.glob("*.parquet"))
    check(f"ENTSO-E directory exists", True, str(entsoe_dir))
    print(f"     Found {len(entsoe_files)} ENTSO-E files")
else:
    check(f"ENTSO-E directory exists", False, "Directory not found - OK if not downloaded yet")

# Check weather data
weather_dir = raw_dir / 'weather'
if weather_dir.exists():
    weather_files = list(weather_dir.glob("*.parquet"))
    check(f"Weather directory exists", True, str(weather_dir))
    print(f"     Found {len(weather_files)} weather files")
else:
    check(f"Weather directory exists", False, "Directory not found - OK if not downloaded yet")

# ============================================================================
# 4. DATABASE CONNECTION
# ============================================================================
section("4. DATABASE CONNECTION")

try:
    from src.utils.db_utils import get_connection
    from src.utils.db_schema import create_schema
    
    check("Can import db_utils", True)
    
    # Try to connect
    try:
        conn = get_connection(readonly=True)
        check("Can connect to DuckDB", True)
        
        # Check database file
        db_path = project_root / 'data' / 'mibel.duckdb'
        db_exists = db_path.exists()
        if db_exists:
            size_mb = db_path.stat().st_size / (1024 * 1024)
            check("Database file exists", True, f"{db_path} ({size_mb:.2f} MB)")
        else:
            check("Database file exists", False, "Will be created on first use")
        
        conn.close()
        
    except Exception as e:
        check("Can connect to DuckDB", False, str(e))
        
except ImportError as e:
    check("Can import db_utils", False, str(e))

# ============================================================================
# 5. DATABASE SCHEMA
# ============================================================================
section("5. DATABASE SCHEMA")

try:
    from src.utils.db_utils import get_connection
    
    conn = get_connection(readonly=True)
    
    # Check if tables exist
    tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
    """
    
    tables = conn.execute(tables_query).fetchdf()
    
    expected_tables = ['prices_day_ahead', 'weather', 'generation', 'cross_border_flows', 'bid_curves']
    
    print(f"Tables in database: {len(tables)}")
    for table in expected_tables:
        exists = table in tables['table_name'].values
        check(f"Table exists: {table}", exists)
    
    conn.close()
    
except Exception as e:
    print(f"❌ Error checking schema: {e}")

# ============================================================================
# 6. DATABASE DATA
# ============================================================================
section("6. DATABASE DATA")

try:
    from src.utils.db_utils import execute_query
    
    # Check prices table
    try:
        price_count = execute_query("SELECT COUNT(*) as count FROM prices_day_ahead")
        count = price_count['count'].iloc[0]
        has_data = count > 0
        check(f"Prices table has data", has_data, f"{count:,} rows")
        
        if has_data:
            # Get details
            details = execute_query("""
                SELECT 
                    MIN(timestamp) as start,
                    MAX(timestamp) as end,
                    COUNT(DISTINCT country) as countries,
                    COUNT(DISTINCT DATE_TRUNC('day', timestamp)) as days
                FROM prices_day_ahead
            """)
            
            print(f"     Start: {details['start'].iloc[0]}")
            print(f"     End: {details['end'].iloc[0]}")
            print(f"     Countries: {details['countries'].iloc[0]}")
            print(f"     Days: {details['days'].iloc[0]}")
            
            # Check data types
            sample = execute_query("SELECT * FROM prices_day_ahead LIMIT 1")
            print(f"     Column types: {dict(sample.dtypes)}")
            
            # Check for nulls
            null_check = execute_query("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN price_eur_mwh IS NULL THEN 1 ELSE 0 END) as null_prices
                FROM prices_day_ahead
            """)
            print(f"     NULL prices: {null_check['null_prices'].iloc[0]} / {null_check['total'].iloc[0]}")
            
    except Exception as e:
        check(f"Can query prices table", False, str(e))
    
    # Check weather table
    try:
        weather_count = execute_query("SELECT COUNT(*) as count FROM weather")
        count = weather_count['count'].iloc[0]
        check(f"Weather table has data", count > 0, f"{count:,} rows")
    except:
        check(f"Weather table has data", False, "No data or table doesn't exist")
    
except Exception as e:
    print(f"❌ Error checking data: {e}")

# ============================================================================
# 7. PANEL CONSTRUCTION
# ============================================================================
section("7. PANEL CONSTRUCTION")

processed_dir = project_root / 'data' / 'processed'
if processed_dir.exists():
    panel_files = list(processed_dir.glob("main_panel_*.parquet"))
    check(f"Processed directory exists", True)
    print(f"     Found {len(panel_files)} panel files:")
    
    for f in panel_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"       - {f.name} ({size_mb:.2f} MB)")
        
        # Check panel content
        try:
            df = pd.read_parquet(f)
            print(f"         Shape: {df.shape}")
            print(f"         Columns: {list(df.columns)}")
            print(f"         Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            print(f"         Countries: {sorted(df['country'].unique())}")
            
            # Check for data
            null_prices = df['price_eur_mwh'].isna().sum()
            total = len(df)
            print(f"         Price data: {total - null_prices:,} / {total:,} non-null ({(1-null_prices/total)*100:.1f}%)")
            
        except Exception as e:
            print(f"         ERROR reading panel: {e}")
else:
    check(f"Processed directory exists", False)

# ============================================================================
# 8. MODULE IMPORTS TEST
# ============================================================================
section("8. MODULE IMPORTS TEST")

import_tests = [
    ('db_utils', 'src.utils.db_utils'),
    ('db_schema', 'src.utils.db_schema'),
    ('timezone_utils', 'src.utils.timezone_utils'),
    ('load_to_db', 'src.data.load_to_db'),
    ('build_panel', 'src.data.build_panel'),
]

for name, module_path in import_tests:
    try:
        __import__(module_path)
        check(f"Can import {name}", True)
    except ImportError as e:
        check(f"Can import {name}", False, str(e))

# ============================================================================
# 9. CRITICAL FUNCTIONS TEST
# ============================================================================
section("9. CRITICAL FUNCTIONS TEST")

try:
    from src.utils.timezone_utils import create_hour_index, add_time_features
    
    # Test hour index creation
    try:
        idx = create_hour_index('2022-06-15', '2022-06-16')
        expected_hours = 48
        actual_hours = len(idx)
        check(f"create_hour_index works", actual_hours == expected_hours, 
              f"Created {actual_hours} hours (expected {expected_hours})")
    except Exception as e:
        check(f"create_hour_index works", False, str(e))
    
    # Test time features
    try:
        test_df = pd.DataFrame({
            'timestamp': pd.date_range('2022-06-15', periods=10, freq='h', tz='UTC')
        })
        result = add_time_features(test_df)
        has_features = 'hour' in result.columns and 'is_iberian_exception' in result.columns
        check(f"add_time_features works", has_features, 
              f"Added {len(result.columns) - 1} features")
    except Exception as e:
        check(f"add_time_features works", False, str(e))
        
except ImportError as e:
    check(f"Can import timezone_utils", False, str(e))

# ============================================================================
# 10. SUMMARY & RECOMMENDATIONS
# ============================================================================
section("10. SUMMARY & RECOMMENDATIONS")

print("\nIssues found:")
issues = []

# Check for common issues
if not structure_ok:
    issues.append("❌ Project structure incomplete - missing directories or files")

if not env_ok:
    issues.append("❌ Python packages missing - run: pip install -r requirements.txt")

try:
    price_count = execute_query("SELECT COUNT(*) as count FROM prices_day_ahead")
    if price_count['count'].iloc[0] == 0:
        issues.append("❌ Database is empty - need to load data")
except:
    issues.append("❌ Cannot query database - schema may not be created")

if not (raw_dir / 'omie').exists() or len(list((raw_dir / 'omie').glob("*.parquet"))) == 0:
    issues.append("❌ No OMIE data downloaded")

if len(issues) == 0:
    print("✅ No critical issues found! Pipeline appears healthy.")
else:
    for issue in issues:
        print(issue)
    
    print("\n" + "="*80)
    print("RECOMMENDED ACTIONS:")
    print("="*80)
    
    if "structure incomplete" in str(issues):
        print("\n1. Fix project structure:")
        print("   - Ensure all directories exist")
        print("   - Check __init__.py files in src/ folders")
    
    if "packages missing" in str(issues):
        print("\n2. Install packages:")
        print("   pip install pandas numpy duckdb matplotlib pytz tqdm")
    
    if "No OMIE data" in str(issues):
        print("\n3. Download data:")
        print("   - Run: python -c \"from src.data.omie_ingest import download_day_ahead_prices; download_day_ahead_prices('2022-06-15', '2022-06-22')\"")
    
    if "Database is empty" in str(issues):
        print("\n4. Load data into database:")
        print("   - Run: python -m src.data.load_to_db")
        print("   - Or in notebook: from src.data.load_to_db import load_all_data; load_all_data()")

print("\n" + "="*80)
print(" DIAGNOSTIC COMPLETE")
print("="*80)
print("\nSave this output and share with Claude for debugging.")