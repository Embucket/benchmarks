#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Modified version of tpcbench.py that configures DataFusion to use local SSD for spill

import argparse
import os
import glob
import shutil
import tempfile
import resource

import datafusion
from datafusion import SessionContext, RuntimeEnvBuilder
from datetime import datetime
import json
import time

def main(benchmark: str, data_path: str, query_path: str, iterations: int, output_file: str, temp_dir: str, queries_to_run: list[int] | None = None, memory_limit_mb: int | None = None, prefer_hash_join: bool = False, max_temp_dir_size_gb: int = 1000):

    # Increase file descriptor limit to handle large Parquet datasets
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        # Try to set to hard limit (or 65536 if hard limit is higher)
        new_limit = min(hard, 65536)
        resource.setrlimit(resource.RLIMIT_NOFILE, (new_limit, hard))
        print(f"✓ Increased file descriptor limit from {soft} to {new_limit}")
    except Exception as e:
        print(f"✗ Could not increase file descriptor limit: {e}")

    # Register the tables
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
        raise ValueError("invalid benchmark")

    # Create SessionContext with custom temp directory for spill
    print(f"Configuring DataFusion to use temp directory: {temp_dir}")

    # Set Python's temp directory to use the same location
    # This prevents Python/Arrow from filling up the root filesystem
    os.environ['TMPDIR'] = temp_dir
    os.environ['TEMP'] = temp_dir
    os.environ['TMP'] = temp_dir
    tempfile.tempdir = temp_dir
    print(f"✓ Set Python temp directory to: {temp_dir}")

    # Clean temp directory before starting
    if os.path.exists(temp_dir):
        print(f"Cleaning temp directory: {temp_dir}")
        try:
            # Remove all files and subdirectories
            for item in os.listdir(temp_dir):
                item_path = os.path.join(temp_dir, item)
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            print(f"✓ Temp directory cleaned")
        except Exception as e:
            print(f"✗ WARNING: Could not clean temp directory: {e}")

    # Create temp directory if it doesn't exist
    os.makedirs(temp_dir, exist_ok=True)

    # Verify temp directory is writable
    test_file = os.path.join(temp_dir, ".test_write")
    try:
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        print(f"✓ Temp directory is writable: {temp_dir}")
    except Exception as e:
        print(f"✗ WARNING: Temp directory is not writable: {e}")
        raise

    # Create RuntimeEnv with temp directory and optional memory limit
    # Use RuntimeEnvBuilder (new API, replaces deprecated RuntimeConfig)

    # Configure the temp directory for spilling
    runtime_env = RuntimeEnvBuilder().with_temp_file_path(temp_dir)

    # Set memory pool with limit if specified
    if memory_limit_mb:
        memory_limit_bytes = memory_limit_mb * 1024 * 1024
        print(f"Setting memory limit: {memory_limit_mb} MB ({memory_limit_bytes:,} bytes)")
        # Use fair spill pool - works best for queries with multiple spillable operators
        runtime_env = runtime_env.with_fair_spill_pool(memory_limit_bytes)
    else:
        print("Using unbounded memory pool (no memory limit)")

    # Create SessionContext with the runtime environment
    ctx = SessionContext(runtime=runtime_env)

    # Set batch size - smaller batches use less memory but may be slower
    # Default is 8192. Larger values (32768) use more memory per batch
    # With limited memory, use smaller batches to reduce memory pressure
    try:
        ctx.sql("SET datafusion.execution.batch_size = 8192")
        print(f"✓ Set batch_size = 8192")
    except Exception as e:
        print(f"✗ Could not set batch_size: {e}")

    # Set prefer_hash_join configuration
    try:
        ctx.sql(f"SET datafusion.optimizer.prefer_hash_join = {str(prefer_hash_join).lower()}")
        print(f"✓ Set prefer_hash_join = {prefer_hash_join}")
    except Exception as e:
        print(f"✗ Could not set prefer_hash_join: {e}")

    # Enable explain mode to show query plan after execution
    try:
        ctx.sql("SET datafusion.explain.logical_plan_only = false")
        ctx.sql("SET datafusion.explain.physical_plan_only = false")
        print(f"✓ Enabled query plan output")
    except Exception as e:
        print(f"✗ Could not enable explain mode: {e}")

    # Set target partitions to control parallelism and memory usage
    # Lower values = less memory, slower queries. Higher values = more memory, faster queries
    # Recommended: 4-8 for memory-constrained, 16-32 for balanced, 64+ for performance
    # Note: RepartitionExec does NOT support spilling, so lower values reduce memory pressure
    try:
        ctx.sql("SET datafusion.execution.target_partitions = 4")
        print(f"✓ Set target_partitions = 4")
    except Exception as e:
        print(f"✗ Could not set target_partitions: {e}")

    # Enable coalesce batches to reduce memory fragmentation
    try:
        ctx.sql("SET datafusion.execution.coalesce_batches = true")
        print(f"✓ Enabled coalesce_batches")
    except Exception as e:
        print(f"✗ Could not enable coalesce_batches: {e}")

    # Reduce sort spill reservation to allow more aggressive spilling
    try:
        ctx.sql("SET datafusion.execution.sort_spill_reservation_bytes = 1048576")  # 1MB instead of default 10MB
        print(f"✓ Set sort_spill_reservation_bytes = 1MB")
    except Exception as e:
        print(f"✗ Could not set sort_spill_reservation_bytes: {e}")

    # Set max temp directory size (default 1TB)
    # DataFusion expects a string with units like "1000G" or "1T"
    try:
        ctx.sql(f"SET datafusion.runtime.max_temp_directory_size = '{max_temp_dir_size_gb}G'")
        print(f"✓ Set max_temp_directory_size = {max_temp_dir_size_gb} GB")
    except Exception as e:
        print(f"✗ Could not set max_temp_directory_size: {e}")

    # Verify temp directory disk space
    total, used, free = shutil.disk_usage(temp_dir)
    print(f"✓ Temp directory configured at: {temp_dir}")
    print(f"  Disk space: {free / (1024**3):.1f} GB free / {total / (1024**3):.1f} GB total")
    print()

    for table in table_names:
        # Try different possible file patterns
        # tpchgen-cli might create files like table.parquet or table/*.parquet
        possible_paths = [
            f"{data_path}/{table}.parquet",
            f"{data_path}/{table}",
            f"{data_path}/{table}/*.parquet",
        ]

        registered = False
        for path in possible_paths:
            if os.path.exists(path.replace("/*.parquet", "")):
                try:
                    print(f"Registering table {table} using path {path}")
                    ctx.register_parquet(table, path)
                    registered = True
                    break
                except Exception as e:
                    print(f"  Failed to register {path}: {e}")
                    continue

        if not registered:
            print(f"ERROR: Could not find data for table {table}")
            print(f"  Tried paths: {possible_paths}")
            print(f"  Contents of {data_path}:")
            if os.path.exists(data_path):
                for item in os.listdir(data_path):
                    print(f"    {item}")
            raise FileNotFoundError(f"Could not find data for table {table}")

    results = {
        'engine': 'datafusion-python',
        'datafusion-version': datafusion.__version__,
        'benchmark': benchmark,
        'data_path': data_path,
        'query_path': query_path,
        'temp_dir': temp_dir,
        'iterations': iterations,
        'memory_limit_mb': memory_limit_mb,
        'prefer_hash_join': prefer_hash_join,
        'max_temp_dir_size_gb': max_temp_dir_size_gb
    }

    # Determine which queries to run
    if queries_to_run:
        queries_list = queries_to_run
        print(f"Running specific queries: {queries_list}")
    else:
        queries_list = list(range(1, num_queries + 1))
        print(f"Running all {num_queries} queries")

    # Run multiple iterations
    for iteration in range(iterations):
        print(f"\n=== Iteration {iteration + 1}/{iterations} ===\n")

        for query in queries_list:
            # read text file
            path = f"{query_path}/q{query}.sql"
            print(f"Reading query {query} using path {path}")
            with open(path, "r") as f:
                text = f.read()
                # each file can contain multiple queries
                queries = text.split(";")

                # Check temp directory before query
                temp_files_before = []
                for root, dirs, files in os.walk(temp_dir):
                    for f in files:
                        temp_files_before.append(os.path.join(root, f))

                print(f"Temp files before query: {len(temp_files_before)}")

                start_time = time.time()
                max_temp_size = 0

                for sql in queries:
                    sql = sql.strip()
                    if len(sql) > 0:
                        print(f"Executing: {sql[:100]}...")  # Print first 100 chars

                        # Check if query contains DDL statements (can't use EXPLAIN ANALYZE with them)
                        sql_upper = sql.upper()
                        use_explain_analyze = not any(stmt in sql_upper for stmt in ["CREATE VIEW", "DROP VIEW", "CREATE TABLE", "DROP TABLE"])

                        if use_explain_analyze:
                            # Use EXPLAIN ANALYZE to execute query and get metrics
                            explain_sql = f"EXPLAIN ANALYZE {sql}"
                            df = ctx.sql(explain_sql)
                        else:
                            # For queries with DDL statements, execute normally
                            df = ctx.sql(sql)

                        # Check temp directory during execution (before collect)
                        temp_size_during = sum(
                            os.path.getsize(os.path.join(root, f))
                            for root, dirs, files in os.walk(temp_dir)
                            for f in files
                            if os.path.exists(os.path.join(root, f))
                        )
                        max_temp_size = max(max_temp_size, temp_size_during)

                        rows = df.collect()

                        # Save query execution plan to file (only if EXPLAIN ANALYZE was used)
                        if use_explain_analyze:
                            # Convert to pandas to get full string values without truncation
                            import pyarrow as pa
                            import re

                            for batch in rows:
                                # Get the actual string values from the Arrow batch
                                plan_type_array = batch.column(0)
                                plan_array = batch.column(1)

                                for i in range(len(batch)):
                                    plan_type = plan_type_array[i].as_py()
                                    plan_text = plan_array[i].as_py()

                                    # Pretty print the plan with proper indentation
                                    # Add newlines after each operator and indent properly
                                    formatted_plan = plan_text
                                    # Add newline before each Exec operator
                                    formatted_plan = re.sub(r'([A-Z][a-zA-Z]+Exec:)', r'\n\1', formatted_plan)
                                    # Indent nested operators
                                    lines = formatted_plan.split('\n')
                                    indented_lines = []
                                    indent_level = 0
                                    for line in lines:
                                        if line.strip():
                                            # Count leading spaces to determine nesting
                                            if 'Exec:' in line:
                                                indented_lines.append('  ' * indent_level + line.strip())
                                                indent_level += 1
                                            else:
                                                indented_lines.append('  ' * max(0, indent_level - 1) + line.strip())

                                    formatted_plan = '\n'.join(indented_lines)

                                    # Save to file in same directory as output_file for full details
                                    output_dir = os.path.dirname(output_file) if output_file else "."
                                    plan_file = os.path.join(output_dir, f"query_{query}_plan.txt")
                                    with open(plan_file, 'w') as f:
                                        f.write(f"{plan_type}:\n")
                                        f.write("=" * 80 + "\n\n")
                                        f.write(formatted_plan)
                                        f.write("\n\n" + "=" * 80 + "\n")

                                    print(f"✓ Query plan saved to: {plan_file}")

                end_time = time.time()
                elapsed = end_time - start_time

                # Check temp directory after query
                temp_files_after = []
                total_size = 0
                for root, dirs, files in os.walk(temp_dir):
                    for f in files:
                        fp = os.path.join(root, f)
                        if os.path.exists(fp):
                            temp_files_after.append(fp)
                            total_size += os.path.getsize(fp)

                temp_files_created = len(temp_files_after) - len(temp_files_before)
                new_files = set(temp_files_after) - set(temp_files_before)

                size_mb = total_size / (1024 * 1024)
                max_size_mb = max_temp_size / (1024 * 1024)

                print(f"Query {query} took {elapsed:.2f} seconds")
                print(f"  Temp files created: {temp_files_created}, Total temp size: {size_mb:.2f} MB")
                print(f"  Max temp size during execution: {max_size_mb:.2f} MB")

                if new_files:
                    print(f"  New temp files:")
                    for f in list(new_files)[:5]:  # Show first 5
                        print(f"    - {f}")

                # Store timings for each iteration
                if query not in results:
                    results[query] = []
                results[query].append(elapsed)

    # Calculate statistics for each query
    print("\n=== Summary ===\n")
    for query in queries_list:
        if query in results:
            timings = results[query]
            avg_time = sum(timings) / len(timings)
            min_time = min(timings)
            max_time = max(timings)
            print(f"Query {query:2d}: avg={avg_time:6.2f}s, min={min_time:6.2f}s, max={max_time:6.2f}s")

    str_json = json.dumps(results, indent=4)
    print(f"\nWriting results to {output_file}")
    with open(output_file, "w") as f:
        f.write(str_json)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataFusion benchmark derived from TPC-H / TPC-DS")
    parser.add_argument("--benchmark", required=True, help="Benchmark to run (tpch or tpcds)")
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument("--queries", required=True, help="Path to query files")
    parser.add_argument("--iterations", type=int, default=3, help="Number of iterations to run (default: 3)")
    parser.add_argument("--output", default=None, help="Output JSON file (default: auto-generated)")
    parser.add_argument("--temp-dir", required=True, help="Temporary directory for DataFusion spill operations")
    parser.add_argument("--query", type=int, action='append', dest='queries_to_run',
                        help="Specific query number to run (can be specified multiple times, e.g., --query 1 --query 18)")
    parser.add_argument("--memory-limit", type=int, dest='memory_limit_mb',
                        help="Memory limit in MB (forces spilling when exceeded, e.g., --memory-limit 122880 for 120GB)")
    parser.add_argument("--prefer-hash-join", dest='prefer_hash_join',
                        action='store_true', default=False,
                        help="Prefer hash join over sort-merge join (default: False)")
    parser.add_argument("--max-temp-dir-size", type=int, dest='max_temp_dir_size_gb', default=1000,
                        help="Maximum temp directory size in GB (default: 1000GB = 1TB)")
    args = parser.parse_args()

    # Generate default output filename if not specified
    if args.output is None:
        current_time_millis = int(datetime.now().timestamp() * 1000)
        args.output = f"datafusion-python-{args.benchmark}-{current_time_millis}.json"

    main(args.benchmark, args.data, args.queries, args.iterations, args.output, args.temp_dir, args.queries_to_run, args.memory_limit_mb, args.prefer_hash_join, args.max_temp_dir_size_gb)

