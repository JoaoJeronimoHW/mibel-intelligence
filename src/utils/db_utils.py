"""
Database utilities for DuckDB connection and common operations.

Why a separate module? Database connections should be managed centrally
to avoid multiple connections to the same file (causes locking issues).
"""

import duckdb
from pathlib import Path
from typing import Optional, Sequence

# Database file location
DB_PATH = Path(__file__).parent.parent.parent / "data" / "mibel.duckdb"


def get_connection(readonly: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Get a connection to the DuckDB database.

    The session time zone is pinned to UTC. Without this DuckDB uses the
    operating system's zone (Europe/Lisbon on the original build machine) to
    convert tz-aware values, which silently rewrote every stored timestamp
    (manual issue D-01).

    Args:
        readonly: If True, open in read-only mode (prevents accidental modifications)

    Returns:
        DuckDB connection object

    Example:
        conn = get_connection()
        conn.execute("SELECT * FROM prices_day_ahead LIMIT 10").fetchdf()
    """
    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # DuckDB automatically creates the database file if it doesn't exist
    conn = duckdb.connect(str(DB_PATH), read_only=readonly)
    conn.execute("SET TimeZone = 'UTC'")
    return conn


def execute_query(query: str, params: Optional[Sequence] = None, readonly: bool = True) -> any:
    """
    Execute a query and return results as pandas DataFrame.

    Why this function? It handles connection management so you don't have
    to remember to close connections.

    Args:
        query: SQL query string, using ? placeholders for values
        params: values bound to the ? placeholders
        readonly: Open database in read-only mode

    Returns:
        Query results as pandas DataFrame
    """
    with get_connection(readonly=readonly) as conn:
        return conn.execute(query, params or []).fetchdf()


def table_exists(table_name: str) -> bool:
    """Check if a table exists in the database."""
    query = """
        SELECT COUNT(*) as count
        FROM information_schema.tables
        WHERE table_name = ?
    """
    result = execute_query(query, [table_name])
    return result['count'].iloc[0] > 0


def get_table_info(table_name: str) -> any:
    """
    Get information about a table's structure and size.

    Returns:
        DataFrame with columns: column_name, data_type, null_count
    """
    if not table_exists(table_name):
        raise ValueError(f"Table '{table_name}' does not exist")

    # Identifiers cannot be bound as parameters; table_exists() has validated the name
    query = f'DESCRIBE "{table_name}"'
    return execute_query(query)


def get_row_count(table_name: str) -> int:
    """Get number of rows in a table."""
    if not table_exists(table_name):
        raise ValueError(f"Table '{table_name}' does not exist")
    query = f'SELECT COUNT(*) as count FROM "{table_name}"'
    result = execute_query(query)
    return int(result['count'].iloc[0])
