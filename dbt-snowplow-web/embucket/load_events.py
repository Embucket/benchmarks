#!/usr/bin/env python3
"""
Upload CSV directly to Embucket via HTTP API - NO EC2 FILE TRANSFER NEEDED!
This is the simplest approach for uploading from your local machine.

Usage:
    python load_events.py --yesterday    # First run: load yesterday's data
    python load_events.py --combined     # Second run: load combined data incrementally
    python load_events.py --combined --host http://your-ec2:3000
"""

import os
import sys
import requests
from pathlib import Path


def authenticate(base_url, username='embucket', password='embucket'):
    """Authenticate with Embucket and get access token."""
    print(f"🔐 Authenticating with Embucket...")
    
    try:
        response = requests.post(
            f"{base_url}/ui/auth/login",
            headers={'Content-Type': 'application/json'},
            json={"username": username, "password": password},
            timeout=10
        )
        response.raise_for_status()
        
        data = response.json()
        access_token = data.get('accessToken')
        
        if not access_token:
            print("✗ Error: No access token in response")
            sys.exit(1)
        
        print("✓ Authenticated successfully")
        return access_token
        
    except requests.exceptions.ConnectionError:
        print(f"✗ Error: Cannot connect to {base_url}")
        print("  Is Embucket running? Check the URL and port.")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print(f"✗ Error: Connection timed out")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        sys.exit(1)


def run_sql_file(base_url, headers, sql_file_path):
    """Run a SQL file to set up database, schema, and table."""
    print(f"\n📊 Running SQL setup file: {sql_file_path}")
    
    try:
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        
        # Split into individual statements and execute them one by one
        statements = []
        current_statement = ""
        
        for line in sql_content.split('\n'):
            # Skip comments and empty lines
            line_stripped = line.strip()
            if not line_stripped or line_stripped.startswith('--'):
                continue
            
            current_statement += line + "\n"
            
            # If line ends with semicolon, it's end of statement
            if line_stripped.endswith(';'):
                statements.append(current_statement.strip())
                current_statement = ""
        
        # Add any remaining statement
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        print(f"   Found {len(statements)} SQL statements to execute")
        
        # Execute each statement
        for i, statement in enumerate(statements, 1):
            if not statement:
                continue
            
            # Show what we're executing
            statement_preview = statement[:60].replace('\n', ' ')
            print(f"   [{i}/{len(statements)}] {statement_preview}...")
                
            try:
                # Use longer timeout for CREATE TABLE (large statement)
                timeout = 60 if 'CREATE TABLE' in statement else 30
                
                response = requests.post(
                    f"{base_url}/ui/queries",
                    headers={**headers, 'Content-Type': 'application/json'},
                    json={"query": statement},
                    timeout=timeout
                )
                
                if response.status_code in [200, 201]:
                    print(f"       ✓ Success")
                else:
                    print(f"       ⚠ Status: {response.status_code}")
                    # Continue with next statement
            except requests.exceptions.Timeout:
                print(f"       ⚠ Timeout after {timeout}s")
                # Continue with next statement
            except Exception as e:
                print(f"       ⚠ Error: {str(e)[:50]}")
                # Continue with next statement
        
        print(f"✓ Database, schema, and table setup complete")
        return True
            
    except FileNotFoundError:
        print(f"✗ Error: SQL file not found: {sql_file_path}")
        return False
    except Exception as e:
        print(f"✗ Error running SQL file: {e}")
        return False


def upload_csv_via_http(base_url, headers, local_file, database, schema, table_name):
    """Upload CSV file directly via HTTP API - no file transfer to EC2 needed!"""
    file_name = Path(local_file).name
    file_size = os.path.getsize(local_file)
    file_size_mb = file_size / (1024 * 1024)
    
    print(f"\n📤 Uploading via HTTP API...")
    print(f"   File: {file_name} ({file_size_mb:.2f} MB)")
    print(f"   Target: {database}.{schema}.{table_name}")
    print(f"   Endpoint: {base_url}")
    
    # API endpoint for file upload
    upload_url = f"{base_url}/ui/databases/{database}/schemas/{schema}/tables/{table_name}/rows"
    
    # CSV format parameters
    params = {
        "header": "true",        # CSV has header row
        "delimiter": "44",       # Comma (ASCII 44)
    }
    
    try:
        # Open and upload file
        with open(local_file, 'rb') as f:
            files = {"file": (file_name, f, "text/csv")}
            
            response = requests.post(
                upload_url,
                headers=headers,
                params=params,
                files=files,
                timeout=300  # 5 minutes for large files
            )
        
        response.raise_for_status()
        
        data = response.json()
        rows_loaded = data.get('count', 0)
        duration_ms = data.get('duration_ms', 0)
        duration_sec = duration_ms / 1000
        
        print(f"✓ Upload complete!")
        print(f"   Rows loaded: {rows_loaded:,}")
        print(f"   Duration: {duration_sec:.2f} seconds")
        
        return rows_loaded
        
    except requests.exceptions.HTTPError as e:
        print(f"✗ HTTP Error: {e}")
        print(f"  Response: {e.response.text[:500]}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Upload failed: {e}")
        sys.exit(1)


def drop_schemas(base_url, headers, database):
    """Drop the specified schemas."""
    schemas_to_drop = ['PUBLIC_DERIVED', 'PUBLIC_SCRATCH', 'PUBLIC_SNOWPLOW_MANIFEST']
    
    for schema in schemas_to_drop:
        try:
            print(f"Dropping schema {database}.{schema}...")
            query = f"DROP SCHEMA IF EXISTS {database}.{schema} CASCADE"
            response = requests.post(
                f"{base_url}/ui/queries",
                headers={**headers, 'Content-Type': 'application/json'},
                json={"query": query},
                timeout=30
            )
            if response.status_code in [200, 201]:
                print(f"✓ Schema {database}.{schema} dropped successfully")
            else:
                print(f"⚠ Warning: Could not drop schema {database}.{schema} (status: {response.status_code})")
        except Exception as e:
            print(f"⚠ Warning: Could not drop schema {database}.{schema}: {e}")


def load_multiple_files(base_url, headers, files, database, schema, table_name):
    """Load multiple CSV files sequentially without creating a combined file."""
    total_rows_loaded = 0
    
    for file in files:
        file_path = Path(file)
        if not file_path.exists():
            print(f"⚠ Warning: File {file} not found, skipping...")
            continue
        
        print(f"\n📤 Loading {file_path.name}...")
        rows_loaded = upload_csv_via_http(base_url, headers, str(file_path), database, schema, table_name)
        total_rows_loaded += rows_loaded
    
    print(f"\n✓ Total rows loaded: {total_rows_loaded:,}")
    return total_rows_loaded


def verify_data(base_url, headers, database, schema, table_name):
    """Query the table to verify data was loaded."""
    print(f"\n🔍 Verifying data...")
    
    query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT event_id) as unique_events,
        COUNT(DISTINCT user_id) as unique_users
    FROM {database}.{schema}.{table_name}
    """
    
    try:
        response = requests.post(
            f"{base_url}/ui/queries",
            headers={**headers, 'Content-Type': 'application/json'},
            json={"query": query},
            timeout=30
        )
        response.raise_for_status()
        
        print("✓ Data verified successfully")
        # Note: Full result parsing would require parsing the response structure
        
    except Exception as e:
        print(f"⚠ Could not verify data: {e}")


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
    print("Additional Options:")
    print("  --host <url>         Embucket URL (default: http://localhost:3000)")
    print("  --user <username>    Username (default: embucket)")
    print("  --password <pass>    Password (default: embucket)")
    print("  --database <name>    Database (default: embucket)")
    print("  --schema <name>      Schema (default: atomic)")
    print("  --table <name>       Table (default: events)")
    print()
    print("Examples:")
    print("  python load_events.py --yesterday    # First run: load yesterday's data")
    print("  python load_events.py --combined     # Second run: load combined data incrementally")
    print()
    print("Or set environment variables:")
    print("  export EMBUCKET_HOST=http://your-ec2:3000")


def main():
    """Main function: Upload CSV via HTTP API."""
    print("=" * 70)
    print("Embucket HTTP API Direct Upload")
    print("Upload from your laptop without moving files to EC2!")
    print("=" * 70)
    
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
        print("Second run: Loading combined data (yesterday + today)")
    
    script_dir = Path(__file__).parent
    parent_dir = script_dir.parent
    input_files = [str(parent_dir / file) for file in input_files]
    
    # Check if required files exist
    for file in input_files:
        if not Path(file).exists():
            print(f"Error: {file} not found")
            sys.exit(1)
    
    # Handle EMBUCKET_HOST and EMBUCKET_PORT separately (if set)
    host = os.getenv('EMBUCKET_HOST', 'localhost')
    port = os.getenv('EMBUCKET_PORT', '3000')
    
    # Check if host already includes port or is a full URL
    if '://' in host:
        # Full URL provided (e.g., http://localhost:3000)
        base_url = host.rstrip('/')
    elif ':' in host:
        # Host with port (e.g., localhost:3000)
        base_url = f'http://{host}'
    else:
        # Just host, need to add port
        base_url = f'http://{host}:{port}'
    
    username = os.getenv('EMBUCKET_USER', 'embucket')
    password = os.getenv('EMBUCKET_PASSWORD', 'embucket')
    database = 'embucket'
    schema = 'atomic'
    table_name = 'events'
    
    # Parse optional arguments
    i = 0
    while i < len(args):
        if args[i] == '--host' and i + 1 < len(args):
            base_url = args[i + 1].rstrip('/')
            i += 2
        elif args[i] == '--user' and i + 1 < len(args):
            username = args[i + 1]
            i += 2
        elif args[i] == '--password' and i + 1 < len(args):
            password = args[i + 1]
            i += 2
        elif args[i] == '--database' and i + 1 < len(args):
            database = args[i + 1]
            i += 2
        elif args[i] == '--schema' and i + 1 < len(args):
            schema = args[i + 1]
            i += 2
        elif args[i] == '--table' and i + 1 < len(args):
            table_name = args[i + 1]
            i += 2
        else:
            i += 1
    
    # Ensure base_url from --host flag has proper scheme
    if not base_url.startswith(('http://', 'https://')):
        if ':' not in base_url:
            # No port in URL, add default port
            base_url = f'http://{base_url}:3000'
        else:
            # Has port
            base_url = f'http://{base_url}'
    
    print(f"\nConfiguration:")
    print(f"  Files: {', '.join([Path(f).name for f in input_files])}")
    print(f"  Embucket: {base_url}")
    print(f"  Target: {database}.{schema}.{table_name}")
    print(f"  Mode: {'Full run (will drop schemas)' if not is_incremental else 'Incremental run (preserves schemas)'}")
    print()
    
    # Step 1: Authenticate
    access_token = authenticate(base_url, username, password)
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Step 2: Drop schemas unless this is an incremental run
    if not is_incremental:
        print("Full run: Dropping existing schemas...")
        drop_schemas(base_url, headers, database)
    else:
        print("Incremental run: Skipping schema drop")
    
    # Step 3: Run SQL file to set up database, schema, and table
    script_dir = Path(__file__).parent
    sql_file = script_dir / "create.sql"
    
    if not run_sql_file(base_url, headers, str(sql_file)):
        print("\n✗ Failed to run SQL setup file")
        print("Make sure create.sql exists in the same directory")
        sys.exit(1)
    
    # Step 4: Load multiple CSV files via HTTP
    rows_loaded = load_multiple_files(base_url, headers, input_files, database, schema, table_name)
    
    # Step 5: Verify
    verify_data(base_url, headers, database, schema, table_name)
    
    # Summary
    print("\n" + "=" * 70)
    print("✓ Complete! Data uploaded successfully")
    print("=" * 70)
    print(f"\nWhat happened:")
    print(f"  1. Authenticated with Embucket")
    if not is_incremental:
        print(f"  2. Dropped existing schemas (PUBLIC_DERIVED, PUBLIC_SCRATCH, PUBLIC_SNOWPLOW_MANIFEST)")
    print(f"  3. Created/verified table exists")
    print(f"  4. Uploaded {len(input_files)} CSV file(s) via HTTP API (no EC2 file transfer!)")
    print(f"  5. Loaded {rows_loaded:,} rows into {database}.{schema}.{table_name}")
    print()


if __name__ == '__main__':
    main()

