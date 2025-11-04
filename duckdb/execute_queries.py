import subprocess

import duckdb
import time
import json
import sys
import os
import glob

def get_execution_time_breakdown(profile):
    """
    profile: either a dict (already loaded JSON) or a path to a JSON file.
    Returns:
      {
        overall_time, processing, synchronization, operators: [...],
        processing_percentage, synchronization_percentage,
        operator_tree: {
          tree: {...},              # nested operator tree (QUERY root)
          nodes: [...], edges: [...]# flattened for graphing
        }
      }
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
        "overall_time": root_latency,           # wall-clock query time (s)
        "processing": 0.0,                      # sum of min(cpu_time, operator_timing)
        "synchronization": 0.0,                 # sum of blocked_thread_time
        "operators": []
    }

    # ---- Per-operator list --------------------------------
    def walk_collect(node):
        if not isinstance(node, dict):
            return
        op_name = node.get('operator_name')
        op_type = node.get('operator_type')

        if op_name or op_type:
            op_timing = float(node.get('operator_timing') or 0.0)       # seconds
            cpu_time = float(node.get('cpu_time') or 0.0)               # seconds
            blocked = float(node.get('blocked_thread_time') or 0.0)     # seconds

            entry = {
                "name": op_name or op_type or "UNKNOWN",
                "type": op_type or op_name or "UNKNOWN",
                "timing": op_timing,
                "cpu_time": cpu_time,
                "blocked_time": blocked,
                "rows_produced": node.get('operator_cardinality', 0),
                "rows_scanned": node.get('operator_rows_scanned', 0),
                "bytes_read": node.get('total_bytes_read', 0),
                "bytes_written": node.get('total_bytes_written', 0),
            }

            if root_latency > 0:
                entry["overall_percentage"] = 100.0 * (op_timing / root_latency)
                entry["processing_percentage"] = 100.0 * (min(cpu_time, op_timing) / root_latency)
                entry["synchronization_percentage"] = 100.0 * (blocked / root_latency)
            else:
                entry["overall_percentage"] = entry["processing_percentage"] = entry["synchronization_percentage"] = 0.0

            breakdown["operators"].append(entry)
            breakdown["processing"] += min(cpu_time, op_timing)
            breakdown["synchronization"] += blocked

        for ch in (node.get('children') or []):
            walk_collect(ch)

    walk_collect(profile_data)

    if root_latency > 0:
        breakdown["processing_percentage"] = 100.0 * (breakdown["processing"] / root_latency)
        breakdown["synchronization_percentage"] = 100.0 * (breakdown["synchronization"] / root_latency)
    else:
        breakdown["processing_percentage"] = 0.0
        breakdown["synchronization_percentage"] = 0.0

    breakdown["operators"].sort(key=lambda x: x.get("overall_percentage", 0.0), reverse=True)

    # ---- Build operator tree (nested) ----------------------
    def is_operator(n):
        return isinstance(n, dict) and (n.get('operator_name') or n.get('operator_type'))

    def op_entry(n):
        name = n.get('operator_name') or n.get('operator_type') or "UNKNOWN"
        typ  = n.get('operator_type') or n.get('operator_name') or "UNKNOWN"
        t    = float(n.get('operator_timing') or 0.0)
        c    = float(n.get('cpu_time') or 0.0)
        b    = float(n.get('blocked_thread_time') or 0.0)
        entry = {
            "name": name,
            "type": typ,
            "timing": t,
            "cpu_time": c,
            "blocked_time": b,
            "rows_produced": n.get('operator_cardinality', 0),
            "rows_scanned": n.get('operator_rows_scanned', 0),
            "bytes_read": n.get('total_bytes_read', 0),
            "bytes_written": n.get('total_bytes_written', 0),
            "children": []
        }
        if root_latency > 0:
            entry["overall_percentage"] = 100.0 * (t / root_latency)
            entry["processing_percentage"] = 100.0 * (min(c, t) / root_latency)
            entry["synchronization_percentage"] = 100.0 * (b / root_latency)
        else:
            entry["overall_percentage"] = entry["processing_percentage"] = entry["synchronization_percentage"] = 0.0
        return entry

    def build_operator_subtree(node):
        if not isinstance(node, dict):
            return []

        # Build children first
        child_ops = []
        for ch in (node.get('children') or []):
            sub = build_operator_subtree(ch)
            if isinstance(sub, list):
                child_ops.extend(sub)
            elif isinstance(sub, dict):
                child_ops.append(sub)

        # If this node is an operator, attach operator-children and return it
        if is_operator(node):
            e = op_entry(node)
            e["children"] = child_ops
            return e

        # Not an operator: bubble up the collected children
        return child_ops

    op_forest = build_operator_subtree(profile_data)
    op_children = [op_forest] if isinstance(op_forest, dict) else op_forest

    query_root = {
        "name": "QUERY",
        "type": "ROOT",
        "timing": root_latency,
        "overall_percentage": 100.0 if root_latency > 0 else 0.0,
        "children": op_children
    }

    # ---- Flat graph (nodes + edges) -----------------------
    nodes, edges = [], []
    counter = {"id": 0}
    def assign_ids(n, parent_id=None, depth=0):
        nid = counter["id"]
        counter["id"] += 1
        nodes.append({
            "id": nid,
            "parent_id": parent_id,
            "depth": depth,
            "name": n.get("name"),
            "type": n.get("type"),
            "timing": n.get("timing"),
            "overall_percentage": n.get("overall_percentage"),
            "processing_percentage": n.get("processing_percentage"),
            "synchronization_percentage": n.get("synchronization_percentage"),
            "rows_produced": n.get("rows_produced"),
            "rows_scanned": n.get("rows_scanned"),
            "bytes_read": n.get("bytes_read"),
            "bytes_written": n.get("bytes_written"),
        })
        for ch in n.get("children", []):
            cid = assign_ids(ch, parent_id=nid, depth=depth+1)
            edges.append({"parent": nid, "child": cid})
        return nid

    assign_ids(query_root)

    breakdown["operator_tree"] = {
        "tree": query_root,
        "nodes": nodes,
        "edges": edges
    }

    return breakdown


def main(data_dir, queries_dir, temp_dir, iterations, output_file, queries_to_run, memory_limit_mb, threads, mode, db_file, timestamp):
    # Create DuckDB connection based on mode
    if mode == 'internal':
        if not db_file or not os.path.exists(db_file):
            print(f"Error: Database file not found: {db_file}")
            print("Please run download-tpch-db.sh first to download the database file.")
            sys.exit(1)

        print(f"✓ Using internal database file: {db_file}")
        conn = duckdb.connect(db_file, read_only=True)
    elif mode == 'parquet':
        if not data_dir or not os.path.exists(data_dir):
            print(f"Error: Data directory not found: {data_dir}")
            print("Please run generate-tpch-data.sh first to generate the data.")
            sys.exit(1)

        print(f"✓ Using parquet files from: {data_dir}")
        conn = duckdb.connect(':memory:')
    else:  # parquet-s3 mode
        if not data_dir:
            print(f"Error: S3 path is required for parquet-s3 mode")
            sys.exit(1)

        print(f"✓ Using parquet files from S3: {data_dir}")
        conn = duckdb.connect(':memory:')

    # Configure temp directory
    conn.execute(f"SET temp_directory = '{temp_dir}'")
    print(f"✓ Set temp directory: {temp_dir}")

    # Set memory limit if specified
    if memory_limit_mb:
        conn.execute(f"SET memory_limit = '{memory_limit_mb}MB'")
        print(f"✓ Set memory limit: {memory_limit_mb} MB")

    # Set number of threads if specified
    if threads:
        conn.execute(f"SET threads = {threads}")
        print(f"✓ Set threads: {threads}")
    else:
        print(f"✓ Using default threads")

    # Configure S3 access for parquet-s3 mode
    if mode == 'parquet-s3':
        import urllib.request
        import urllib.error

        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        conn.execute("SET s3_region='us-east-2'")
        conn.execute("SET s3_use_ssl=true")

        if 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
            conn.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
            conn.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")
            if 'AWS_SESSION_TOKEN' in os.environ:
                conn.execute(f"SET s3_session_token='{os.environ['AWS_SESSION_TOKEN']}'")
            print(f"✓ Using AWS credentials from environment variables")
        else:
            try:
                token_url = 'http://169.254.169.254/latest/api/token'
                token_request = urllib.request.Request(
                    token_url,
                    headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'},
                    method='PUT'
                )
                with urllib.request.urlopen(token_request, timeout=2) as response:
                    token = response.read().decode('utf-8')

                role_url = 'http://169.254.169.254/latest/meta-data/iam/security-credentials/'
                role_request = urllib.request.Request(
                    role_url,
                    headers={'X-aws-ec2-metadata-token': token}
                )
                with urllib.request.urlopen(role_request, timeout=2) as response:
                    role_name = response.read().decode('utf-8').strip()

                creds_url = f'http://169.254.169.254/latest/meta-data/iam/security-credentials/{role_name}'
                creds_request = urllib.request.Request(
                    creds_url,
                    headers={'X-aws-ec2-metadata-token': token}
                )
                with urllib.request.urlopen(creds_request, timeout=2) as response:
                    creds = json.loads(response.read().decode('utf-8'))

                conn.execute(f"SET s3_access_key_id='{creds['AccessKeyId']}'")
                conn.execute(f"SET s3_secret_access_key='{creds['SecretAccessKey']}'")
                conn.execute(f"SET s3_session_token='{creds['Token']}'")
                print(f"✓ Using AWS credentials from EC2 instance profile ({role_name})")
            except Exception as e:
                print(f"⚠ Warning: Could not fetch EC2 instance credentials: {e}")
                print(f"  No IAM role attached to EC2 instance")
                print(f"  To fix this:")
                print(f"    1. Attach an IAM role with S3 read permissions to this EC2 instance, OR")
                print(f"    2. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables, OR")
                print(f"    3. Make the S3 bucket publicly accessible")
                print(f"  Attempting to proceed with anonymous access (will fail if bucket is not public)...")

        print(f"✓ Configured S3 access (region: us-east-2)")

    print()

    # Register Parquet files as tables (for parquet and parquet-s3 modes)
    if mode == 'parquet':
        tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
        for table in tables:
            single_file = os.path.join(data_dir, f"{table}.parquet")
            dir_pattern = os.path.join(data_dir, table, "*.parquet")

            if os.path.exists(single_file):
                table_path = single_file
            elif glob.glob(dir_pattern):
                table_path = dir_pattern
            else:
                print(f"⚠ No parquet files found for table: {table} "
                      f"(checked `{single_file}` and `{dir_pattern}`)")
                continue

            conn.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{table_path}')")
            print(f"✓ Registered table: {table} -> {table_path}")
        print()
    elif mode == 'parquet-s3':
        tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
        for table in tables:
            # Try both single file and directory pattern on S3
            candidate_paths = [f"{data_dir}/{table}.parquet", f"{data_dir}/{table}/*.parquet"]
            created = False
            for table_path in candidate_paths:
                try:
                    conn.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{table_path}')")
                    print(f"✓ Registered table: {table} -> {table_path}")
                    created = True
                    break
                except duckdb.IOException:
                    continue
            if not created:
                print(f"⚠ No parquet files found for table: {table} "
                      f"(tried `{candidate_paths[0]}` and `{candidate_paths[1]}`)")
        print()
    else:
        print("✓ Using tables from internal database")
        print()

    # Enable JSON profiling once per session
    conn.execute("SET enable_profiling = 'json'")
    conn.execute("SET profiling_mode = 'detailed'")

    # Determine which queries to run
    if queries_to_run:
        query_numbers = queries_to_run
    else:
        query_numbers = list(range(1, 23))  # All 22 TPC-H queries

    results = {
        'timestamp': timestamp,
        'engine': 'duckdb',
        'duckdb-version': duckdb.__version__,
        'mode': mode,
        'data_path': data_dir if mode in ['parquet', 'parquet-s3'] else db_file,
        'temp_dir': temp_dir,
        'iterations': iterations,
        'memory_limit_mb': memory_limit_mb,
        'threads': threads
    }

    output_dir = os.path.dirname(output_file) if output_file else "."

    for query_num in query_numbers:
        print(f"=== Running Query {query_num} ===")
        query_file = os.path.join(queries_dir, f"q{query_num:02d}.sql")

        if not os.path.exists(query_file):
            print(f"⚠ Query file not found: {query_file}")
            continue

        with open(query_file, 'r') as f:
            query = f.read()

        iteration_times = []
        # Flush disk buffers and drop OS caches to ensure cold-start conditions before query execution
        print('Flushing disk buffers and dropping OS caches for cold-start query execution...')
        subprocess.run(["sudo", "sync"], check=True)
        subprocess.run(
            ["sudo", "tee", "/proc/sys/vm/drop_caches"],
            input="3\n", text=True, check=True
        )
        # Give the system a short delay to complete cache drop operations
        print('Waiting 3 seconds for the system to finalize cache drop...')
        time.sleep(3)
        for i in range(iterations):
            print(f"  Iteration {i + 1}/{iterations}...", end=' ', flush=True)

            try:
                # Ensure profiling is disabled before configuring it for this run
                conn.execute("SET profiling_output = ''")

                # Profile only the first actual execution
                profile_path = os.path.join(temp_dir, f"duck_profile_q{query_num:02d}_iter{i + 1}.json")
                if i == 0:
                    os.makedirs(os.path.dirname(profile_path), exist_ok=True)
                    conn.execute(f"SET profiling_output = '{profile_path}'")

                # Execute the query
                result = conn.execute(query).fetchall()

                # Get execution time from breakdown for first iteration
                if i == 0 and os.path.exists(profile_path):
                    breakdown = get_execution_time_breakdown(profile_path)
                    elapsed = breakdown.get('overall_time')

                iteration_times.append(elapsed)
                print(f"{elapsed:.2f}s ({len(result)} rows)")

                # After first iteration, parse and save the execution breakdown
                if i == 0 and os.path.exists(profile_path):
                    try:
                        breakdown_file = os.path.join(output_dir, f"query_{query_num}_breakdown.json")
                        with open(breakdown_file, 'w') as fout:
                            json.dump({"EXECUTION_TIME_BREAKDOWN": breakdown}, fout, indent=2)
                        print(f"  ✓ Breakdown saved to: {breakdown_file}")
                    except Exception as pe:
                        print(f"  ⚠ Failed to parse breakdown: {pe}")
                elif i == 0:
                    print(f"  ⚠ Profile file not found: {profile_path}")

            except Exception as e:
                # Ensure profiling is disabled after an error
                try:
                    conn.execute("SET profiling_output = ''")
                except Exception:
                    pass
                print(f"ERROR: {e}")
                break

        if iteration_times:
            avg_time = sum(iteration_times) / len(iteration_times)
            min_time = min(iteration_times)
            max_time = max(iteration_times)
            print(f"  Query {query_num}: avg={avg_time:.2f}s, min={min_time:.2f}s, max={max_time:.2f}s")
            results[str(query_num)] = iteration_times

        print()

    # Save results
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"✓ Results saved to: {output_file}")
    conn.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', help='Directory containing parquet files (required for parquet mode) or S3 path (required for parquet-s3 mode)')
    parser.add_argument('--db-file', help='Path to DuckDB database file (required for internal mode)')
    parser.add_argument('--queries-dir', required=True)
    parser.add_argument('--temp-dir', required=True)
    parser.add_argument('--iterations', type=int, default=3)
    parser.add_argument('--output', required=True)
    parser.add_argument('--query', action='append', type=int, dest='queries')
    parser.add_argument('--memory-limit', type=int, dest='memory_limit_mb')
    parser.add_argument('--threads', type=int)
    parser.add_argument('--mode', choices=['parquet', 'parquet-s3', 'internal'], required=True,
                        help='Benchmark mode: parquet (use local parquet files), parquet-s3 (use S3 parquet files), or internal (use DuckDB database file)')
    parser.add_argument('--timestamp', required=True, help='Timestamp for the benchmark run')

    args = parser.parse_args()

    # Validate mode-specific requirements
    if args.mode == 'parquet' and not args.data_dir:
        parser.error("--data-dir is required when using parquet mode")
    if args.mode == 'parquet-s3' and not args.data_dir:
        parser.error("--data-dir (S3 path) is required when using parquet-s3 mode")
    if args.mode == 'internal' and not args.db_file:
        parser.error("--db-file is required when using internal mode")

    main(args.data_dir, args.queries_dir, args.temp_dir, args.iterations,
         args.output, args.queries, args.memory_limit_mb, args.threads, args.mode, args.db_file, args.timestamp)
