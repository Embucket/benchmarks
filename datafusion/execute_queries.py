#!/usr/bin/env python3
"""
DataFusion TPC-H Benchmark using datafusion-cli

This script uses the datafusion-cli command-line tool instead of the Python datafusion library
to avoid memory issues with certain queries (especially query 21).
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path


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
            # For S3 mode, use direct S3 path (single file per table)
            path = f"{data_dir}/{table}.parquet"
        else:
            # For local mode, try different possible patterns
            possible_paths = [
                f"{data_dir}/{table}.parquet",
                f"{data_dir}/{table}",
                f"{data_dir}/{table}/*.parquet",
            ]
            
            # Find the first path that exists
            path = None
            for p in possible_paths:
                check_path = p.replace("/*.parquet", "")
                if os.path.exists(check_path):
                    path = p
                    break
            
            if path is None:
                raise FileNotFoundError(f"Could not find data for table {table} in {data_dir}")
        
        # Create external table using CREATE EXTERNAL TABLE
        sql_commands.append(f"CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{path}';")
    
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
            # Debug: print first 500 chars of output
            print(f"  Output preview (first 500 chars):")
            print(f"  {result.stdout[:500]}")

            # Look for execution time in EXPLAIN ANALYZE output
            # Pattern: "total_time=XXXms" or "Execution Time: XXX ms"
            time_patterns = [
                r'total_time=(\d+(?:\.\d+)?)ms',
                r'(?:Total )?Execution Time:\s+([\d.]+)\s*ms',
            ]

            # Find all elapsed times in the output
            elapsed_matches = re.findall(r'Elapsed ([\d.]+) seconds\.', result.stdout)

            # Use the last occurrence as the total query time
            execution_time = float(elapsed_matches[-1])
            print(f"  Parsed execution time from EXPLAIN ANALYZE: {execution_time:.2f}s")

            for pattern in time_patterns:
                time_match = re.search(pattern, result.stdout, re.IGNORECASE)
                if time_match:
                    execution_time = float(time_match.group(1)) / 1000.0  # Convert ms to seconds
                    print(f"  Parsed execution time from EXPLAIN ANALYZE: {execution_time:.2f}s")
                    break

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


def run_benchmark(benchmark, data_dir, queries_dir, iterations, output_file,
                  queries_to_run=None, prefer_hash_join=False, mode='parquet'):
    """
    Run the TPC-H benchmark using datafusion-cli.

    Args:
        benchmark: 'tpch' or 'tpcds'
        data_dir: Path to data directory
        queries_dir: Path to query files directory
        iterations: Number of iterations to run
        output_file: Path to output JSON file
        queries_to_run: List of specific query numbers to run (None = all)
        prefer_hash_join: Whether to prefer hash joins
        mode: 'parquet' or 'parquet-s3'
    """
    # Get DataFusion version
    datafusion_version = get_datafusion_version()
    print(f"DataFusion CLI version: {datafusion_version}")
    print()
    
    # Define table names based on benchmark
    if benchmark == "tpch":
        num_queries = 22
        table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    elif benchmark == "tpcds":
        num_queries = 99
        table_names = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
                      "customer_address", "customer_demographics", "date_dim", "time_dim", "household_demographics",
                      "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns",
                      "store_sales", "warehouse", "web_page", "web_returns", "web_sales", "web_site"]
    else:
        raise ValueError(f"Invalid benchmark: {benchmark}")

    # Create setup SQL (table registration + configuration)
    print("Creating table registration script...")
    table_registration_sql = create_table_registration_script(data_dir, mode, table_names)
    config_sql = create_config_script(prefer_hash_join)
    setup_sql = config_sql + "\n\n" + table_registration_sql

    print("Configuration:")
    print(f"  Mode: {mode}")
    print(f"  Data directory: {data_dir}")
    print(f"  Prefer hash join: {prefer_hash_join}")
    print()
    
    # Initialize results
    results = {
        'engine': 'datafusion-cli',
        'datafusion-version': datafusion_version,
        'benchmark': benchmark,
        'data_path': data_dir,
        'query_path': queries_dir,
        'iterations': iterations,
        'prefer_hash_join': prefer_hash_join,
        'mode': mode
    }
    
    # Determine which queries to run
    if queries_to_run:
        queries_list = queries_to_run
        print(f"Running specific queries: {queries_list}")
    else:
        queries_list = list(range(1, num_queries + 1))
        print(f"Running all {num_queries} queries")
    
    print()
    
    # Run multiple iterations
    for iteration in range(iterations):
        print(f"\n{'='*80}")
        print(f"Iteration {iteration + 1}/{iterations}")
        print(f"{'='*80}\n")

        for query_num in queries_list:
            print('Flushing disk buffers and dropping OS caches for cold-start query execution...')
            subprocess.run(["sudo", "sync"], check=True)
            subprocess.run(
                ["sudo", "tee", "/proc/sys/vm/drop_caches"],
                input="3\n", text=True, check=True
            )
            print('Waiting 3 seconds for the system to finalize cache drop...')
            time.sleep(3)
            # Check if this is query 21 and use replacement query if available
            if query_num == 21:
                replacement_path = os.path.join(os.path.dirname(__file__), "21_query_replacement.sql")
                if os.path.exists(replacement_path):
                    query_file = replacement_path
                    print(f"{'='*80}")
                    print(f"⚠️  USING OPTIMIZED REPLACEMENT QUERY FOR Q21")
                    print(f"   Original query uses too much memory")
                    print(f"   Using replacement query from: {replacement_path}")
                    print(f"{'='*80}")
                else:
                    query_file = os.path.join(queries_dir, f"q{query_num}.sql")
            # Check if this is query 18 and use replacement query if available
            elif query_num == 18:
                replacement_path = os.path.join(os.path.dirname(__file__), "18_query_replacement.sql")
                if os.path.exists(replacement_path):
                    query_file = replacement_path
                    print(f"{'='*80}")
                    print(f"⚠️  USING OPTIMIZED REPLACEMENT QUERY FOR Q18")
                    print(f"   Using replacement query from: {replacement_path}")
                    print(f"{'='*80}")
                else:
                    query_file = os.path.join(queries_dir, f"q{query_num}.sql")
            else:
                query_file = os.path.join(queries_dir, f"q{query_num}.sql")

            if not os.path.exists(query_file):
                print(f"⚠️  Warning: Query file not found: {query_file}")
                continue

            print(f"Running query {query_num}...")

            # Read query SQL
            with open(query_file, 'r') as f:
                query_sql = f.read()

            # Execute query
            execution_time, success, error_msg, explain_output = execute_query_with_cli(query_sql, setup_sql)

            if success:
                print(f"✓ Query {query_num} completed in {execution_time:.2f} seconds")

                # Save EXPLAIN ANALYZE output to file (only on first iteration)
                if iteration == 0 and explain_output:
                    output_dir = os.path.dirname(output_file) if output_file else "."
                    plan_file = os.path.join(output_dir, f"query_{query_num}_plan.txt")
                    with open(plan_file, 'w') as f:
                        f.write(f"DataFusion EXPLAIN ANALYZE - Query {query_num}\n")
                        f.write("=" * 80 + "\n\n")
                        f.write(explain_output)
                        f.write("\n" + "=" * 80 + "\n")
                    print(f"  ✓ Query plan saved to: {plan_file}")

                # Store timing
                if query_num not in results:
                    results[query_num] = []
                results[query_num].append(execution_time)
            else:
                print(f"✗ Query {query_num} failed: {error_msg}")
                # Store failure
                if query_num not in results:
                    results[query_num] = []
                results[query_num].append(None)

            print()
    
    # Calculate statistics
    print(f"\n{'='*80}")
    print("Summary")
    print(f"{'='*80}\n")
    
    for query_num in queries_list:
        if query_num in results and results[query_num]:
            timings = [t for t in results[query_num] if t is not None]
            if timings:
                avg_time = sum(timings) / len(timings)
                min_time = min(timings)
                max_time = max(timings)
                print(f"Query {query_num:2d}: avg={avg_time:6.2f}s, min={min_time:6.2f}s, max={max_time:6.2f}s")
            else:
                print(f"Query {query_num:2d}: FAILED")
    
    # Write results to file
    print(f"\nWriting results to {output_file}")
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=4)
    
    print("Done!")


def main():
    parser = argparse.ArgumentParser(
        description="DataFusion TPC-H/TPC-DS benchmark using datafusion-cli"
    )
    parser.add_argument("--benchmark", required=True, choices=["tpch", "tpcds"],
                       help="Benchmark to run")
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
        benchmark=args.benchmark,
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

