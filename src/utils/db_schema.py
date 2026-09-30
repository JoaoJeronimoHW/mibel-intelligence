"""
Database schema definitions for DuckDB.

Why separate schema file?
- Documents table structures in one place
- Makes it easy to recreate database from scratch
- Serves as data dictionary for the project
"""

from src.utils.db_utils import get_connection
import logging

logger = logging.getLogger(__name__)


# SQL to create tables
SCHEMA_SQL = """
-- Day-ahead electricity prices
CREATE TABLE IF NOT EXISTS prices_day_ahead (
    timestamp TIMESTAMPTZ NOT NULL,
    country VARCHAR(2) NOT NULL,
    price_eur_mwh DOUBLE NOT NULL,
    energy_mwh DOUBLE,
    PRIMARY KEY (timestamp, country)
);

-- Generation by technology and country
CREATE TABLE IF NOT EXISTS generation (
    timestamp TIMESTAMPTZ NOT NULL,
    country VARCHAR(2) NOT NULL,
    technology VARCHAR(50) NOT NULL,
    generation_mw DOUBLE NOT NULL,
    PRIMARY KEY (timestamp, country, technology)
);

-- Cross-border electricity flows
CREATE TABLE IF NOT EXISTS cross_border_flows (
    timestamp TIMESTAMPTZ NOT NULL,
    country_from VARCHAR(2) NOT NULL,
    country_to VARCHAR(2) NOT NULL,
    flow_mw DOUBLE NOT NULL,
    PRIMARY KEY (timestamp, country_from, country_to)
);

-- Weather data by location
CREATE TABLE IF NOT EXISTS weather (
    timestamp TIMESTAMPTZ NOT NULL,
    location VARCHAR(50) NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    temperature_c DOUBLE,
    wind_speed_10m DOUBLE,
    wind_speed_100m DOUBLE,
    wind_direction_100m DOUBLE,
    solar_radiation DOUBLE,
    dni DOUBLE,
    diffuse_radiation DOUBLE,
    cloud_cover DOUBLE,
    PRIMARY KEY (timestamp, location)
);

-- Bid curves (if available)
CREATE TABLE IF NOT EXISTS bid_curves (
    timestamp TIMESTAMPTZ NOT NULL,
    country VARCHAR(2) NOT NULL,
    generator_id VARCHAR(100),
    price_eur_mwh DOUBLE NOT NULL,
    quantity_mw DOUBLE NOT NULL,
    bid_type VARCHAR(20),
    technology VARCHAR(50)
);

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices_day_ahead(timestamp);
CREATE INDEX IF NOT EXISTS idx_prices_country ON prices_day_ahead(country);
CREATE INDEX IF NOT EXISTS idx_generation_timestamp ON generation(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather(timestamp);
CREATE INDEX IF NOT EXISTS idx_flows_timestamp ON cross_border_flows(timestamp);
CREATE INDEX IF NOT EXISTS idx_bids_timestamp ON bid_curves(timestamp);
CREATE INDEX IF NOT EXISTS idx_bids_country ON bid_curves(country);
"""


TABLES = ['prices_day_ahead', 'generation', 'cross_border_flows', 'weather', 'bid_curves']


def migrate_naive_timestamps(conn) -> list:
    """
    Upgrade tables created by the old schema (timezone-naive TIMESTAMP).

    DuckDB cannot retype a primary-key column in place, so each outdated
    table is copied to `<table>_legacy_naive`, dropped, and recreated by
    create_schema(). The legacy copy keeps the old rows untouched (their
    labels are Europe/Lisbon wall time, see manual D-01) and is dropped
    straight away if it is empty. Every table except bid_curves is
    rebuilt from data/raw by load_to_db, so nothing is lost.

    Returns:
        list of migrated table names
    """
    outdated = conn.execute("""
        SELECT table_name
        FROM information_schema.columns
        WHERE column_name = 'timestamp'
          AND data_type = 'TIMESTAMP'
          AND table_schema = 'main'
          AND table_name NOT LIKE '%_legacy_naive'
    """).fetchall()
    migrated = []
    for (table,) in outdated:
        if table not in TABLES:
            continue
        legacy = f"{table}_legacy_naive"
        n_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.execute(f"DROP TABLE IF EXISTS {legacy}")
        if n_rows:
            conn.execute(f"CREATE TABLE {legacy} AS SELECT * FROM {table}")
            logger.warning(f"  {table}: timezone-naive schema, {n_rows:,} old rows kept in {legacy}")
        else:
            logger.info(f"  {table}: timezone-naive schema (empty), recreating")
        conn.execute(f"DROP TABLE {table}")
        migrated.append(table)
    return migrated


def create_schema():
    """
    Create all tables and indexes in the database.

    Run this once after setting up the project, or anytime you want to
    rebuild the database from scratch. Tables left over from the old
    timezone-naive schema are migrated first (see migrate_naive_timestamps).
    """
    logger.info("Creating database schema...")

    try:
        conn = get_connection(readonly=False)

        migrated = migrate_naive_timestamps(conn)
        if migrated:
            logger.warning(f"  Migrated to TIMESTAMPTZ: {migrated} -- reload with "
                           "`python -m src.data.load_to_db`")

        # Execute schema SQL (split by semicolon for multiple statements)
        for statement in SCHEMA_SQL.split(';'):
            statement = statement.strip()
            if statement:  # Skip empty statements
                conn.execute(statement)
        
        conn.commit()
        conn.close()
        logger.info("Schema created successfully")
        
    except Exception as e:
        logger.error(f"Error creating schema: {e}")
        raise


def describe_schema():
    """
    Print information about all tables in the database.
    
    Useful for checking what's in your database.
    """
    conn = get_connection(readonly=True)
    
    # Get list of tables
    tables = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
    """).fetchdf()
    
    print("\n" + "="*60)
    print("DATABASE SCHEMA")
    print("="*60)
    
    for table in tables['table_name']:
        # Get table info
        info = conn.execute(f"SELECT COUNT(*) as count FROM {table}").fetchdf()
        columns = conn.execute(f"DESCRIBE {table}").fetchdf()
        
        print(f"\n{table}:")
        print(f"  Rows: {info['count'].iloc[0]:,}")
        print("  Columns:")
        for _, col in columns.iterrows():
            print(f"    - {col['column_name']}: {col['column_type']}")
    
    conn.close()


if __name__ == "__main__":
    # Create schema
    create_schema()
    
    # Show what we created
    describe_schema()
