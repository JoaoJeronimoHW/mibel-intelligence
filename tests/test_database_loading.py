"""
Test loading data into DuckDB.
Run: python tests/test_database_loading.py
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))

from src.utils.db_utils import get_connection, get_row_count
from src.utils.db_schema import create_schema

def test_database_insert():
    """Test inserting sample data into DuckDB."""
    
    print("Testing database insert operations...")
    
    try:
        # Create schema
        create_schema()
        
        # Create sample data with ALL 4 columns
        sample_data = pd.DataFrame({
            'timestamp': pd.date_range('2022-06-15', periods=24, freq='h', tz='UTC'),
            'country': ['ES'] * 24,
            'price_eur_mwh': [100 + i*5 for i in range(24)],
            'energy_mwh': [25000 + i*100 for i in range(24)]  # ← Added this column
        })
        
        print(f"  Creating sample data: {len(sample_data)} rows")
        print(f"  Columns: {list(sample_data.columns)}")
        
        # Insert into database
        conn = get_connection(readonly=False)
        
        # Method 1: Let DuckDB infer from DataFrame
        conn.execute("""
            INSERT INTO prices_day_ahead 
            SELECT * FROM sample_data
        """)
        
        conn.commit()
        
        # Query back
        result = conn.execute("""
            SELECT * FROM prices_day_ahead 
            WHERE country = 'ES' 
            ORDER BY timestamp
            LIMIT 5
        """).fetchdf()
        
        print(f"  ✓ Inserted {len(sample_data)} rows")
        print(f"  ✓ Queried back {len(result)} rows")
        print(f"\n  Sample query result:")
        print(result)
        
        # Verify data integrity
        assert len(result) > 0, "No data returned from query"
        assert 'timestamp' in result.columns, "Missing timestamp column"
        assert 'price_eur_mwh' in result.columns, "Missing price column"
        
        print(f"\n  ✓ Data integrity checks passed")
        
        # Clean up test data
        conn.execute("DELETE FROM prices_day_ahead WHERE country = 'ES'")
        conn.commit()
        print(f"  ✓ Cleaned up test data")
        
        conn.close()
        
        print("\n✅ Database loading test passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Database loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_database_insert()