"""
Test database utilities.
Run: python tests/test_database.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.db_utils import get_connection, table_exists
from src.utils.db_schema import create_schema

def test_database_connection():
    """Test that we can create and connect to DuckDB."""
    
    print("Testing database connection...")
    
    try:
        # Get connection
        conn = get_connection(readonly=False)
        
        # Test simple query
        result = conn.execute("SELECT 42 as answer").fetchdf()
        
        assert result['answer'].iloc[0] == 42, "Query returned wrong result"
        
        print("  ✓ Database connection works")
        print(f"  ✓ Database file: {Path('data/mibel.duckdb').absolute()}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"  ✗ Database connection failed: {e}")
        return False


def test_schema_creation():
    """Test that we can create tables."""
    
    print("\nTesting schema creation...")
    
    try:
        # Create schema
        create_schema()
        print("  ✓ Schema created")
        
        # Check that tables exist
        expected_tables = [
            'prices_day_ahead',
            'generation',
            'cross_border_flows',
            'weather',
            'bid_curves'
        ]
        
        conn = get_connection(readonly=True)
        
        for table in expected_tables:
            if table_exists(table):
                print(f"  ✓ Table '{table}' exists")
            else:
                print(f"  ✗ Table '{table}' missing")
                return False
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"  ✗ Schema creation failed: {e}")
        return False


if __name__ == "__main__":
    success = test_database_connection()
    
    if success:
        success = test_schema_creation()
    
    if success:
        print("\n✅ Database tests passed!")
    else:
        print("\n❌ Database tests failed!")
        print("\nTroubleshooting:")
        print("  1. Make sure data/ directory exists")
        print("  2. Check that src/utils/db_utils.py has no syntax errors")
        print("  3. Try running: python -c 'import duckdb; duckdb.connect(\":memory:\").execute(\"SELECT 1\")'")