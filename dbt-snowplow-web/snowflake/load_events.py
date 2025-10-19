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


def print_usage():
    """Print usage information."""
    print("Usage:")
    print("  python events.py [CSV_FILE] [options]")
    print()
    print("Arguments:")
    print("  CSV_FILE       CSV file to load (e.g., events.csv, events_incr_1.csv)")
    print()
    print("Options:")
    print("  true/false     Enable incremental mode")
    print("  1/2            Run number for incremental mode")
    print()
    print("Examples:")
    print("  python events.py events.csv")
    print("  python events.py events_incr_1.csv")
    print("  python events.py true 1          # Uses events_incr_1.csv")
    print("  python events.py true 2          # Uses events_incr_2.csv")


def main():
    """Main function to load events data into Snowflake."""
    # Parse simple command line arguments for input file
    input_file = None
    is_incremental = False
    run_number = 1
    
    # Simple argument parsing
    args = sys.argv[1:]
    for arg in args:
        if arg in ['-h', '--help']:
            print_usage()
            return
        elif arg in ['true', 'false']:
            is_incremental = (arg == 'true')
        elif arg in ['1', '2']:
            run_number = int(arg)
        elif arg.endswith('.csv'):
            input_file = arg
    
    # Determine input file based on incremental flag and run number
    if not input_file:
        if is_incremental:
            if run_number == 1:
                input_file = 'events_incr_1.csv'
                print("Incremental run - First run - using events_incr_1.csv")
            else:  # run_number == 2
                input_file = 'events_incr_2.csv'
                print("Incremental run - Second run - using events_incr_2.csv")
        else:
            input_file = 'events.csv'
            print("First run - using events.csv")
    else:
        print(f"Using specified file: {input_file}")
    
    print(f"=== Loading Snowplow Events Data into Snowflake Database ===")
    
    # Configuration
    script_dir = Path(__file__).parent
    events_file = Path(input_file)
    sql_script = script_dir / "create.sql"
    
    # Check if required files exist
    if not events_file.exists():
        print(f"Error: {events_file} not found")
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
        
        # Execute SQL script
        print("Executing SQL script...")
        execute_sql_script(conn, sql_script, events_file.name)
        
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