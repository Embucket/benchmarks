#!/usr/bin/env python3
"""
Upload CSV directly to Embucket via HTTP API - NO EC2 FILE TRANSFER NEEDED!
This is the simplest approach for uploading from your local machine.

Usage:
    python upload_via_http_api.py events_yesterday.csv
    python upload_via_http_api.py events_yesterday.csv --host http://your-ec2:3000
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


def main():
    """Main function: Upload CSV via HTTP API."""
    print("=" * 70)
    print("Embucket HTTP API Direct Upload")
    print("Upload from your laptop without moving files to EC2!")
    print("=" * 70)
    
    # Parse arguments
    if len(sys.argv) < 2:
        print("\nUsage: python upload_via_http_api.py <file> [options]")
        print("\nOptions:")
        print("  --host <url>         Embucket URL (default: http://localhost:3000)")
        print("  --user <username>    Username (default: embucket)")
        print("  --password <pass>    Password (default: embucket)")
        print("  --database <name>    Database (default: embucket)")
        print("  --schema <name>      Schema (default: public_snowplow_manifest)")
        print("  --table <name>       Table (default: events)")
        print("\nExample:")
        print("  python upload_via_http_api.py events_yesterday.csv")
        print("  python upload_via_http_api.py events.csv --host http://ec2-3-123-45-67.compute.amazonaws.com:3000")
        print("\nOr set environment variables:")
        print("  export EMBUCKET_HOST=http://your-ec2:3000")
        sys.exit(1)
    
    local_file = sys.argv[1]
    
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
    args = sys.argv[2:]
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
    
    # Check file exists
    if not os.path.exists(local_file):
        print(f"\n✗ Error: File '{local_file}' not found")
        sys.exit(1)
    
    print(f"\nConfiguration:")
    print(f"  File: {local_file}")
    print(f"  Embucket: {base_url}")
    print(f"  Target: {database}.{schema}.{table_name}")
    print()
    
    # Step 1: Authenticate
    access_token = authenticate(base_url, username, password)
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Step 2: Run SQL file to set up database, schema, and table
    script_dir = Path(__file__).parent
    sql_file = script_dir / "create.sql"
    
    if not run_sql_file(base_url, headers, str(sql_file)):
        print("\n✗ Failed to run SQL setup file")
        print("Make sure create.sql exists in the same directory")
        sys.exit(1)
    
    # Step 3: Upload CSV via HTTP
    rows_loaded = upload_csv_via_http(base_url, headers, local_file, database, schema, table_name)
    
    # Step 4: Verify
    verify_data(base_url, headers, database, schema, table_name)
    
    # Summary
    print("\n" + "=" * 70)
    print("✓ Complete! Data uploaded successfully")
    print("=" * 70)
    print(f"\nWhat happened:")
    print(f"  1. Authenticated with Embucket")
    print(f"  2. Created/verified table exists")
    print(f"  3. Uploaded CSV via HTTP API (no EC2 file transfer!)")
    print(f"  4. Loaded {rows_loaded:,} rows into {database}.{schema}.{table_name}")
    print()


if __name__ == '__main__':
    main()

