#!/usr/bin/env python3
"""
Script to load Snowplow events data into Snowflake database.
"""

import os
import sys
import snowflake.connector
from pathlib import Path


def create_snowflake_connection():
    """Create Snowflake connection with hardcoded atomic schema."""
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    database = os.getenv("SNOWFLAKE_DATABASE", "dbt_snowplow_web")
    schema = "atomic"  # Hardcoded to atomic schema
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    if not all([user, password, account]):
        raise ValueError("Missing one or more required Snowflake environment variables: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT")

    connect_args = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
        "role": role,
    }

    # First try to connect without specifying database
    connect_args_no_db = connect_args.copy()
    connect_args_no_db.pop('database', None)
    connect_args_no_db.pop('schema', None)
    
    conn = snowflake.connector.connect(**connect_args_no_db)
    
    # Create database and schema if they don't exist
    conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    conn.cursor().execute(f"USE DATABASE {database}")
    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
    conn.cursor().execute(f"USE SCHEMA {schema}")

    return conn


def execute_sql_script(conn, script_path, filename=None):
    """Execute SQL script against the database."""
    with open(script_path, 'r') as f:
        sql_content = f.read()
    
    # Replace filename placeholders if filename is provided
    if filename:
        sql_content = sql_content.replace('events_yesterday.csv', filename)
        sql_content = sql_content.replace('events_today.csv', filename)
    
    # Split by semicolon and execute each statement
    statements = []
    current_statement = ""
    
    for line in sql_content.split('\n'):
        line = line.strip()
        if line.startswith('--') or not line:  # Skip comments and empty lines
            continue
        current_statement += line + " "
        if line.endswith(';'):
            statements.append(current_statement.strip())
            current_statement = ""
    
    if current_statement.strip():
        statements.append(current_statement.strip())
    
    cursor = conn.cursor()
    
    for i, statement in enumerate(statements, 1):
        if statement and not statement.startswith('--'):
            print(f"Executing statement {i}/{len(statements)}: {statement[:50]}...")
            try:
                cursor.execute(statement)
                print("✓ Statement executed successfully")
            except Exception as e:
                print(f"⚠ Warning executing statement {i}: {e}")
                # Continue with next statement
    
    cursor.close()


def verify_data_load(conn):
    """Verify that data was loaded successfully."""
    cursor = conn.cursor()
    
    try:
        # Check total rows
        cursor.execute("SELECT COUNT(*) as total_rows FROM events")
        result = cursor.fetchone()
        if result and result[0] is not None:
            total_rows = result[0]
            print(f"✓ Data verification: {total_rows} rows loaded")
            
            if total_rows > 0:
                # Show sample data
                cursor.execute("""
                    SELECT event_id, event, user_id, collector_tstamp, page_url 
                    FROM events 
                    LIMIT 3
                """)
                sample_data = cursor.fetchall()
                print("✓ Sample data:")
                for row in sample_data:
                    print(f"  {row}")
            else:
                print("⚠ Warning: Table is empty - data may not have loaded correctly")
        else:
            print("⚠ Warning: Could not verify row count")
            
    except Exception as e:
        print(f"⚠ Warning during verification: {e}")
    
    cursor.close()


def manage_warehouse(conn, warehouse_name, action):
    """Simple warehouse management - resume or suspend."""
    try:
        if action == 'resume':
            print(f"Starting warehouse {warehouse_name}...")
            conn.cursor().execute(f"ALTER WAREHOUSE {warehouse_name} RESUME IF SUSPENDED")
            print("✓ Warehouse resume command sent")
        elif action == 'suspend':
            print(f"Suspending warehouse {warehouse_name}...")
            conn.cursor().execute(f"ALTER WAREHOUSE {warehouse_name} SUSPEND")
            print("✓ Warehouse suspend command sent")
    except Exception as e:
        print(f"⚠ Warning: Could not {action} warehouse: {e}")


def drop_schemas(conn):
    """Drop the specified schemas."""
    database = os.getenv("SNOWFLAKE_DATABASE", "dbt_snowplow_web")
    schemas_to_drop = ['PUBLIC_DERIVED', 'PUBLIC_SCRATCH', 'PUBLIC_SNOWPLOW_MANIFEST']
    
    cursor = conn.cursor()
    
    # Ensure we're in the correct database context
    cursor.execute(f"USE DATABASE {database}")
    
    for schema in schemas_to_drop:
        try:
            print(f"Dropping schema {database}.{schema}...")
            cursor.execute(f"DROP SCHEMA IF EXISTS {database}.{schema} CASCADE")
            print(f"✓ Schema {database}.{schema} dropped successfully")
        except Exception as e:
            print(f"⚠ Warning: Could not drop schema {database}.{schema}: {e}")
    cursor.close()


def print_usage():
    """Print usage information."""
    print("Usage:")
    print("  python load_events.py [OPTIONS]")
    print()
    print("Options:")
    print("  --yesterday          Load only events_yesterday.csv (first run)")
    print("  --combined           Load both events_yesterday.csv and events_today.csv (second run)")
    print()
    print("Schema Management:")
    print("  - First run (--yesterday): Drops PUBLIC_DERIVED, PUBLIC_SCRATCH, PUBLIC_SNOWPLOW_MANIFEST schemas")
    print("  - Second run (--combined): Preserves existing schemas (incremental mode)")
    print()
    print("Examples:")
    print("  python load_events.py --yesterday    # First run: load yesterday's data")
    print("  python load_events.py --combined     # Second run: load combined data incrementally")


def load_multiple_files(conn, files):
    """Load multiple CSV files into Snowflake without creating a combined file."""
    cursor = conn.cursor()

    # Create stage (this will drop any existing stage and its files)
    cursor.execute("CREATE OR REPLACE STAGE my_stage")

    total_rows_loaded = 0

    for file in files:
        # Extract just the filename for the stage path
        filename = Path(file).name
        print(f"Uploading {file} to stage...")
        cursor.execute(f"PUT file://{file} @my_stage")

        print(f"Loading {file} into events table...")
        result = cursor.execute(f"""
            COPY INTO events
            FROM @my_stage/{filename}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                RECORD_DELIMITER = '\\n'
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ESCAPE_UNENCLOSED_FIELD = NONE
                ESCAPE = NONE
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                REPLACE_INVALID_CHARACTERS = TRUE
                DATE_FORMAT = 'AUTO'
                TIMESTAMP_FORMAT = 'AUTO'
                BINARY_FORMAT = 'HEX'
                TRIM_SPACE = TRUE
            )
            ON_ERROR = 'CONTINUE'
        """)

        # Get row count from COPY result
        # Result format: (file, status, rows_parsed, rows_loaded, error_limit, errors_seen, first_error, first_error_line, first_error_character, first_error_column_name)
        copy_result = result.fetchone()
        if copy_result and len(copy_result) > 3:
            rows_loaded = int(copy_result[3])  # Fourth column is rows_loaded
            total_rows_loaded += rows_loaded
            print(f"✓ Loaded {rows_loaded:,} rows from {file}")
        else:
            print(f"⚠ Warning: Could not get row count from COPY result for {file}")

    # Clean up: Remove all files from stage to avoid storage costs
    print("Cleaning up stage files...")
    cursor.execute("REMOVE @my_stage")
    print("✓ Stage files removed")

    cursor.close()
    print(f"✓ Total rows loaded: {total_rows_loaded:,}")
    return total_rows_loaded


def main():
    """Main function to load events data into Snowflake."""
    # Parse command line arguments
    mode = None

    args = sys.argv[1:]
    for arg in args:
        if arg in ['-h', '--help']:
            print_usage()
            return
        elif arg == '--yesterday':
            mode = 'yesterday'
        elif arg == '--combined':
            mode = 'combined'

    if not mode:
        print("Error: Must specify either --yesterday or --combined")
        print()
        print_usage()
        sys.exit(1)

    # Determine files and incremental mode based on run type
    if mode == 'yesterday':
        input_files = ['events_yesterday.csv']
        is_incremental = False
        print("First run: Loading yesterday's data only")
    else:  # mode == 'combined'
        input_files = ['events_yesterday.csv', 'events_today.csv']
        is_incremental = True
        print("Second run: Loading combined data (yesterday + today) in memory")
    
    print(f"=== Loading Snowplow Events Data into Snowflake Database ===")

    # Configuration
    script_dir = Path(__file__).parent
    sql_script = script_dir / "create.sql"
    # CSV files are in the parent directory
    parent_dir = script_dir.parent
    input_files = [str(parent_dir / file) for file in input_files]

    # Check if required files exist
    for file in input_files:
        if not Path(file).exists():
            print(f"Error: {file} not found")
            sys.exit(1)

    if not sql_script.exists():
        print(f"Error: {sql_script} not found")
        sys.exit(1)
    
    # Connect to Snowflake
    print("Connecting to Snowflake...")
    
    try:
        conn = create_snowflake_connection()
        warehouse_name = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        manage_warehouse(conn, warehouse_name, 'resume')
        
        print("✓ Connected to Snowflake successfully")
        
        # Drop schemas unless this is an incremental run
        if not is_incremental:
            print("Full run: Dropping existing schemas...")
            drop_schemas(conn)
        else:
            print("Incremental run: Skipping schema drop")

        # Execute SQL script to create table structure (without loading data)
        print("Creating table structure...")
        execute_sql_script(conn, sql_script, None)

        # Load data files
        print("Loading data files...")
        load_multiple_files(conn, input_files)

        # Verify data load
        print("Verifying data load...")
        verify_data_load(conn)
        
        # Suspend warehouse
        manage_warehouse(conn, warehouse_name, 'suspend')
        
        conn.close()
        print("✓ Data load completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    print(f"\n=== Data Load Process Complete ===")


if __name__ == "__main__":
    main()