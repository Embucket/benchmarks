import argparse
import duckdb
import json
import sys
import os
import urllib.request
import urllib.error

# --- Add path to bench_infra ---
current_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.abspath(os.path.join(current_dir, '../bench_infra'))
sys.path.append(lib_dir)

from bench_infra import common


def get_execution_time_breakdown(profile):
    """
    Parses DuckDB JSON profile to extract operator timing.
    """
    # Load if a path was passed
    if isinstance(profile, str):
        with open(profile, 'r') as f:
            profile_data = json.load(f)
    else:
        profile_data = profile

    # Root wall-clock latency (seconds) lives at the top level
    def find_latency(node):
        if isinstance(node, dict) and node.get('latency') is not None:
            return float(node['latency'])
        for ch in (node.get('children') or []):
            v = find_latency(ch)
            if v is not None:
                return v
        return None

    root_latency = float(find_latency(profile_data) or 0.0)

    breakdown = {
        "overall_time": root_latency,
        "processing": 0.0,
        "synchronization": 0.0,
        "operators": []
    }

    # Flatten operator tree for stats (Simplified version of original logic)
    def walk_collect(node):
        if not isinstance(node, dict): return
        op_name = node.get('operator_name') or node.get('operator_type')

        if op_name:
            op_timing = float(node.get('operator_timing') or 0.0)
            # In a full implementation, you would calculate CPU/Blocked time here
            breakdown["operators"].append({
                "name": op_name,
                "timing": op_timing
            })

        for ch in (node.get('children') or []):
            walk_collect(ch)

    walk_collect(profile_data)
    return breakdown


def setup_s3_credentials(conn):
    """Configures S3 access using Env Vars or EC2 Metadata (IMDSv2)."""
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("SET s3_region='us-east-2'")
    conn.execute("SET s3_use_ssl=true")

    # 1. Try Environment Variables
    if 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
        print(f"✓ Using AWS credentials from ENV")
        return

    # 2. Try EC2 Instance Metadata (IMDSv2)
    try:
        # IMDSv2 Token
        token_url = 'http://169.254.169.254/latest/api/token'
        req = urllib.request.Request(token_url, headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'}, method='PUT')
        with urllib.request.urlopen(req, timeout=2) as r:
            token = r.read().decode('utf-8')

        # Role Name
        role_url = 'http://169.254.169.254/latest/meta-data/iam/security-credentials/'
        req = urllib.request.Request(role_url, headers={'X-aws-ec2-metadata-token': token})
        with urllib.request.urlopen(req, timeout=2) as r:
            role_name = r.read().decode('utf-8').strip()

        # Credentials
        creds_url = f'http://169.254.169.254/latest/meta-data/iam/security-credentials/{role_name}'
        req = urllib.request.Request(creds_url, headers={'X-aws-ec2-metadata-token': token})
        with urllib.request.urlopen(req, timeout=2) as r:
            creds = json.loads(r.read().decode('utf-8'))

        conn.execute(f"SET s3_access_key_id='{creds['AccessKeyId']}'")
        conn.execute(f"SET s3_secret_access_key='{creds['SecretAccessKey']}'")
        conn.execute(f"SET s3_session_token='{creds['Token']}'")
        print(f"✓ Using AWS credentials from EC2 Role: {role_name}")
    except Exception as e:
        print(f"⚠ Warning: Could not fetch EC2 credentials: {e}")


def register_tables(conn, mode, data_dir):
    """Registers tables as Views depending on mode."""
    if mode == 'internal':
        print("✓ Using internal DB tables")
        return

    tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
    for table in tables:
        # Logic to find correct path (partitioned vs single file)
        path_candidates = [
            f"{data_dir}/{table}/*.parquet",  # Partitioned
            f"{data_dir}/{table}.parquet",  # Single
            os.path.join(data_dir, table, "*.parquet"),
            os.path.join(data_dir, f"{table}.parquet")
        ]

        registered = False
        for path in path_candidates:
            # Simple heuristic: if it looks like a glob or exists, try it
            if '*' in path or os.path.exists(path.replace("/*.parquet", "")):
                try:
                    conn.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
                    print(f"✓ Registered {table} -> {path}")
                    registered = True
                    break
                except:
                    continue

        if not registered:
            print(f"⚠ Warning: Could not register table {table}")


def main(args):
    # 1. Setup Connection
    if args.mode == 'internal':
        if not os.path.exists(args.db_file):
            print(f"Error: DB file not found: {args.db_file}")
            sys.exit(1)
        conn = duckdb.connect(args.db_file, read_only=True)
    else:
        conn = duckdb.connect(':memory:')
        if args.mode == 'parquet-s3':
            setup_s3_credentials(conn)

    # 2. Configuration
    conn.execute(f"SET temp_directory = '{args.temp_dir}'")
    print(f"✓ Temp dir: {args.temp_dir}")

    if args.memory_limit_mb:
        conn.execute(f"SET memory_limit = '{args.memory_limit_mb}MB'")
    if args.threads:
        conn.execute(f"SET threads = {args.threads}")

    # 3. Register Tables
    register_tables(conn, args.mode, args.data_dir)

    # 4. Profiling Init
    conn.execute("SET enable_profiling = 'json'")
    conn.execute("SET profiling_mode = 'detailed'")

    # 5. Prepare Results
    query_numbers = args.queries if args.queries else list(range(1, 23))

    # Unified result structure (key 'queries')
    results = {
        'engine': 'duckdb',
        'version': duckdb.__version__,
        'mode': args.mode,
        'iterations': args.iterations,
        'queries': {}
    }

    output_dir = os.path.dirname(args.output) if args.output else "."
    os.makedirs(output_dir, exist_ok=True)

    # 6. Run Loop
    for query_num in query_numbers:
        query_file = os.path.join(args.queries_dir, f"q{query_num:02d}.sql")
        if not os.path.exists(query_file):
            print(f"Skipping Q{query_num} (not found)")
            continue

        with open(query_file, 'r') as f:
            query_sql = f.read()

        print(f"=== Running Query {query_num} ===")

        # --- UNIFIED: Drop Cache ---
        common.drop_os_caches()
        # ---------------------------

        iteration_times = []

        for i in range(args.iterations):
            print(f"  Iteration {i + 1}/{args.iterations}...", end=' ', flush=True)
            try:
                # Generate unique profile path for this iteration
                profile_path = os.path.join(args.temp_dir, f"duck_profile_q{query_num:02d}_iter{i + 1}.json")
                os.makedirs(os.path.dirname(profile_path), exist_ok=True)

                # Reset profiling output to the new file
                conn.execute("SET profiling_output = ''")
                conn.execute(f"SET profiling_output = '{profile_path}'")

                # Execute query
                result = conn.execute(query_sql).fetchall()

                # Parse the profile file generated by DuckDB
                breakdown = get_execution_time_breakdown(profile_path)
                elapsed = breakdown.get('overall_time')

                iteration_times.append(elapsed)
                print(f"{elapsed:.2f}s ({len(result)} rows)")

                # Save breakdown for the first iteration
                if i == 0:
                    breakdown_file = os.path.join(output_dir, f"query_{query_num}_breakdown.json")
                    with open(breakdown_file, 'w') as fout:
                        json.dump({"EXECUTION_TIME_BREAKDOWN": breakdown}, fout, indent=2)

            except Exception as e:
                print(f"ERROR: {e}")
                # Try to reset profiling on error
                try:
                    conn.execute("SET profiling_output = ''")
                except:
                    pass
                break

        # Store stats
        if iteration_times:
            avg_time = sum(iteration_times) / len(iteration_times)
            print(f"  Query {query_num}: avg={avg_time:.2f}s")
            results['queries'][query_num] = iteration_times

    # 7. Save Results (Unified)
    common.save_results(results, args.output)
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', help='Path to parquet files or S3')
    parser.add_argument('--db-file', help='DuckDB file')
    parser.add_argument('--queries-dir', required=True)
    parser.add_argument('--temp-dir', required=True)
    parser.add_argument('--iterations', type=int, default=3)
    parser.add_argument('--output', required=True)
    parser.add_argument('--query', action='append', type=int, dest='queries')
    parser.add_argument('--memory-limit', type=int, dest='memory_limit_mb')
    parser.add_argument('--threads', type=int)
    parser.add_argument('--mode', choices=['parquet', 'parquet-s3', 'internal'], required=True)

    args = parser.parse_args()

    # Validation
    if args.mode.startswith('parquet') and not args.data_dir:
        parser.error("--data-dir required")
    if args.mode == 'internal' and not args.db_file:
        parser.error("--db-file required")

    main(args)