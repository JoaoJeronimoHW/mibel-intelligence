"""
Database utilities for DuckDB connection and common operations.

Why a separate module? Database connections should be managed centrally
to avoid multiple connections to the same file (causes locking issues).
"""

import duckdb
from pathlib import Path
from typing import Optional

# Database file location
DB_PATH = Path(__file__).parent.parent.parent / "data" / "mibel.duckdb"


def get_connection(readonly: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Get a connection to the DuckDB database.
    
    Args:
        readonly: If True, open in read-only mode (prevents accidental modifications)
    
    Returns:
        DuckDB connection object
    
    Example:
        conn = get_connection()
        conn.execute("SELECT * FROM prices LIMIT 10").fetchdf()
    """
    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # DuckDB automatically creates the database file if it doesn't exist
    return duckdb.connect(str(DB_PATH), read_only=readonly)


def execute_query(query: str, readonly: bool = True) -> any:
    """
    Execute a query and return results as pandas DataFrame.
    
    Why this function? It handles connection management so you don't have
    to remember to close connections.
    
    Args:
        query: SQL query string
        readonly: Open database in read-only mode
    
    Returns:
        Query results as pandas DataFrame
    """
    with get_connection(readonly=readonly) as conn:
        return conn.execute(query).fetchdf()


def table_exists(table_name: str) -> bool:
    """Check if a table exists in the database."""
    query = f"""
        SELECT COUNT(*) as count 
        FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    """
    result = execute_query(query)
    return result['count'].iloc[0] > 0


def get_table_info(table_name: str) -> any:
    """
    Get information about a table's structure and size.
    
    Returns:
        DataFrame with columns: column_name, data_type, null_count
    """
    if not table_exists(table_name):
        raise ValueError(f"Table '{table_name}' does not exist")
    
    query = f"DESCRIBE {table_name}"
    return execute_query(query)


def get_row_count(table_name: str) -> int:
    """Get number of rows in a table."""
    query = f"SELECT COUNT(*) as count FROM {table_name}"
    result = execute_query(query)
    return int(result['count'].iloc[0])