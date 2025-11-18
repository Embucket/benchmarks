#!/usr/bin/env python3
"""
DataFusion TPC-H Benchmark using datafusion-cli

This script uses the datafusion-cli command-line tool instead of the Python datafusion library
to avoid memory issues with certain queries (especially query 21).
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time

# --- Add path to bench_infra ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(root_dir)

from bench_infra import common


def get_datafusion_version():
    """Get the version of datafusion-cli."""
    try:
        result = subprocess.run(
            ['datafusion-cli', '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        # Output format: "DataFusion CLI x.y.z"
        version_line = result.stdout.strip()
        return version_line
    except Exception as e:
        print(f"Warning: Could not get datafusion-cli version: {e}")
        return "unknown"


def create_table_registration_script(data_dir, mode, table_names):
    """
    Create a SQL script to register all tables.

    Args:
        data_dir: Path to data directory (local or S3)
        mode: 'parquet' or 'parquet-s3'
        table_names: List of table names to register
    
    Returns:
        String containing SQL commands to register tables
    """
    sql_commands = []

    for table in table_names:
        if mode == 'parquet-s3':
            # For partitioned S3 data, pointing to the base directory is often sufficient
            # e.g., s3://bucket/path/lineitem/
            path = f"{data_dir}/{table}/"
            sql_commands.append(
                f"CREATE EXTERNAL TABLE IF NOT EXISTS {table} STORED AS PARQUET LOCATION '{path}';"
            )
        else:
            possible_paths = [
                f"{data_dir}/{table}.parquet",
                f"{data_dir}/{table}",
                f"{data_dir}/{table}/*.parquet",
            ]
            path = None
            for p in possible_paths:
                check_path = p.replace("/*.parquet", "")
                if os.path.exists(check_path):
                    path = p
                    break
            if path is None:
                raise FileNotFoundError(f"Could not find data for table {table} in {data_dir}")
            sql_commands.append(
                f"CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{path}';"
            )

    return "\n".join(sql_commands)


def create_config_script(prefer_hash_join=False):
    """
    Create a SQL script with DataFusion configuration settings.

    Args:
        prefer_hash_join: Whether to prefer hash joins over sort-merge joins

    Returns:
        String containing SQL SET commands
    """
    config_commands = [
        "SET datafusion.execution.target_partitions = '32';",
    ]
    return "\n".join(config_commands)


def execute_query_with_cli(query_sql, setup_sql, timeout=3600):
    """
    Execute a query using datafusion-cli.

    Args:
        query_sql: The SQL query to execute
        setup_sql: SQL commands to run before the query (table registration, config)
        timeout: Maximum execution time in seconds

    Returns:
        Tuple of (execution_time, success, error_message, explain_output)
    """
    # Create a single temporary file with both setup and query
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        # Write setup commands (table registration, config)
        f.write("-- Setup: Table registration and configuration\n")
        f.write(setup_sql)
        f.write("\n\n")

        # Write the query wrapped in EXPLAIN ANALYZE
        f.write("-- Query execution with EXPLAIN ANALYZE\n")

        # Split query by semicolons to handle multi-statement queries
        queries = [q.strip() for q in query_sql.split(';') if q.strip()]

        for sql in queries:
            # Remove leading comments to find the actual SQL statement
            sql_lines = sql.split('\n')
            first_sql_line = None
            for line in sql_lines:
                stripped = line.strip()
                if stripped and not stripped.startswith('--'):
                    first_sql_line = stripped.upper()
                    break

            # Wrap SELECT/WITH queries in EXPLAIN ANALYZE
            # WITH is used for CTEs (Common Table Expressions) and should also be wrapped
            if first_sql_line and (first_sql_line.startswith('SELECT') or first_sql_line.startswith('WITH')):
                f.write(f"EXPLAIN ANALYZE {sql}")
                if not sql.rstrip().endswith(';'):
                    f.write(';')
                f.write("\n")
            else:
                # For non-SELECT statements (CREATE VIEW, etc.), execute normally
                f.write(sql)
                if not sql.rstrip().endswith(';'):
                    f.write(';')
                f.write("\n")

        temp_file = f.name

    try:
        # Debug: print the temp file path so we can inspect it
        print(f"  Executing SQL file: {temp_file}")
        print(f"  You can inspect it with: cat {temp_file}")

        start_time = time.time()

        # Execute datafusion-cli with EXPLAIN ANALYZE
        result = subprocess.run(
            ['datafusion-cli', '--format', 'json', '-f', temp_file],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=os.environ.copy()
        )

        end_time = time.time()
        wall_clock_time = end_time - start_time

        print(f"  Wall clock time: {wall_clock_time:.2f}s")

        # Check if execution was successful
        if result.returncode != 0:
            error_msg = result.stderr if result.stderr else result.stdout
            return wall_clock_time, False, error_msg, None

        # Parse execution time from EXPLAIN ANALYZE output
        import re
        execution_time = None
        explain_output = result.stdout  # Capture the full EXPLAIN ANALYZE output

        if result.stdout:
            # Find all elapsed times in the output
            elapsed_matches = re.findall(r'Elapsed ([\d.]+) seconds\.', result.stdout)
            if elapsed_matches:
                # Pick the largest elapsed time
                execution_time = max(float(et) for et in elapsed_matches)
                print(f"  Parsed execution time from EXPLAIN ANALYZE: {execution_time:.2f}s")
            else:
                print("Could not find elapsed time in EXPLAIN ANALYZE output")
                execution_time = None

        return execution_time, True, None, explain_output

    except subprocess.TimeoutExpired:
        return timeout, False, f"Query timed out after {timeout} seconds", None
    except Exception as e:
        return 0, False, str(e), None
    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_file)
        except:
            pass


def run_benchmark(data_dir, queries_dir, iterations, output_file,
                  queries_to_run=None, prefer_hash_join=False, mode='parquet'):
    datafusion_version = get_datafusion_version()
    print(f"DataFusion CLI version: {datafusion_version}")
    print(f"Data Dir: {data_dir}")
    print(f"Mode: {mode}")
    print()

    num_queries = 22
    table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

    # Create setup SQL
    table_registration_sql = create_table_registration_script(data_dir, mode, table_names)
    config_sql = create_config_script(prefer_hash_join)
    setup_sql = config_sql + "\n\n" + table_registration_sql

    results = {
        'engine': 'datafusion-cli',
        'version': datafusion_version,
        'data_path': data_dir,
        'query_path': queries_dir,
        'iterations': iterations,
        'mode': mode,
        'queries': {}
    }

    if queries_to_run:
        queries_list = queries_to_run
    else:
        queries_list = list(range(1, num_queries + 1))

    # 1. Outer loop: Queries
    for query_num in queries_list:
        print(f"\n{'=' * 60}")
        print(f"Running Query {query_num}")
        print(f"{'=' * 60}")

        # 2. Drop Cache ONCE before the set of iterations for this query
        common.drop_os_caches()

        # Prepare result list
        results['queries'][query_num] = []

        # Logic for replacement queries (Q18, Q21)
        query_file = os.path.join(queries_dir, f"q{query_num}.sql")

        # Check for replacements
        if query_num == 21:
            rep_path = os.path.join(os.path.dirname(__file__), "21_query_replacement.sql")
            if os.path.exists(rep_path):
                query_file = rep_path
                print(f"ℹ️  Using replacement for Q21")
        elif query_num == 18:
            rep_path = os.path.join(os.path.dirname(__file__), "18_query_replacement.sql")
            if os.path.exists(rep_path):
                query_file = rep_path
                print(f"ℹ️  Using replacement for Q18")

        if not os.path.exists(query_file):
            print(f"⚠️  Warning: Query file not found: {query_file}")
            continue

        with open(query_file, 'r') as f:
            query_sql = f.read()

        # 3. Inner loop: Iterations
        for i in range(iterations):
            print(f"  Iteration {i + 1}/{iterations}...", end=' ', flush=True)

            execution_time, success, error_msg, explain_output = execute_query_with_cli(query_sql, setup_sql)

            if success:
                print(f"✓ {execution_time:.2f}s")
                results['queries'][query_num].append(execution_time)

                # Save plan only for first iteration
                if i == 0 and explain_output:
                    out_dir = os.path.dirname(output_file) if output_file else "."
                    with open(os.path.join(out_dir, f"plan_q{query_num}.txt"), 'w') as f:
                        f.write(explain_output)
            else:
                print(f"✗ Failed")
                results['queries'][query_num].append(None)

        # Print stats for this query
        timings = [t for t in results['queries'][query_num] if t is not None]
        if timings:
            avg = sum(timings) / len(timings)
            print(f"  -> Avg: {avg:.2f}s (Min: {min(timings):.2f}s, Max: {max(timings):.2f}s)")

    # Write results
    print(f"\nWriting results to {output_file}")
    common.save_results(results, output_file)
    print("Done!")


def main():
    parser = argparse.ArgumentParser(
        description="DataFusion TPC-H/TPC-DS benchmark using datafusion-cli"
    )
    parser.add_argument("--data-dir", required=True,
                       help="Path to data directory (local path or S3 path)")
    parser.add_argument("--queries-dir", required=True,
                       help="Path to directory containing query SQL files")
    parser.add_argument("--iterations", type=int, default=3,
                       help="Number of iterations to run (default: 3)")
    parser.add_argument("--output", required=True,
                       help="Output JSON file for results")
    parser.add_argument("--mode", choices=["parquet", "parquet-s3"], default="parquet",
                       help="Data source mode (default: parquet)")
    parser.add_argument("--query", type=int, action='append', dest='queries_to_run',
                       help="Specific query number to run (can be specified multiple times)")
    parser.add_argument("--prefer-hash-join", action='store_true',
                       help="Prefer hash join over sort-merge join")

    args = parser.parse_args()
    
    # Validate datafusion-cli is available
    try:
        subprocess.run(['datafusion-cli', '--version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: datafusion-cli is not installed or not in PATH")
        print("Please install it with: cargo install datafusion-cli")
        sys.exit(1)

    run_benchmark(
        data_dir=args.data_dir,
        queries_dir=args.queries_dir,
        iterations=args.iterations,
        output_file=args.output,
        queries_to_run=args.queries_to_run,
        prefer_hash_join=args.prefer_hash_join,
        mode=args.mode
    )


if __name__ == "__main__":
    main()

