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

# Modified version of tpcbench.py - spilling disabled (DataFusion spilling does not work properly)

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

def main(benchmark: str, data_path: str, query_path: str, iterations: int, output_file: str, queries_to_run: list[int] | None = None, prefer_hash_join: bool = True, mode: str = "parquet"):

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

    # Create SessionContext without spilling (spilling does not work properly in DataFusion)
    print(f"Configuring DataFusion without spilling (all queries must fit in RAM)")
    print(f"Using unbounded memory pool (no memory limit)")

    # Create SessionContext with default runtime environment
    ctx = SessionContext()

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
        ctx.sql("SET datafusion.execution.target_partitions = 32")
        print(f"✓ Set target_partitions = 32")
    except Exception as e:
        print(f"✗ Could not set target_partitions: {e}")

    # Enable coalesce batches to reduce memory fragmentation
    try:
        ctx.sql("SET datafusion.execution.coalesce_batches = true")
        print(f"✓ Enabled coalesce_batches")
    except Exception as e:
        print(f"✗ Could not enable coalesce_batches: {e}")

    print()

    # Configure S3 access for parquet-s3 mode
    if mode == 'parquet-s3':
        print(f"Configuring S3 access for parquet-s3 mode...")
        try:
            # Set S3 region
            ctx.sql("SET datafusion.execution.object_store.s3.region = 'us-east-2'")
            print(f"✓ Set S3 region to us-east-2")

            # Check if AWS credentials are available in environment
            if 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
                print(f"✓ Using AWS credentials from environment variables")
            else:
                # Fetch credentials from EC2 instance metadata
                import urllib.request
                import urllib.error
                try:
                    # Get IMDSv2 token
                    token_url = 'http://169.254.169.254/latest/api/token'
                    token_request = urllib.request.Request(
                        token_url,
                        headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'},
                        method='PUT'
                    )
                    with urllib.request.urlopen(token_request, timeout=2) as response:
                        token = response.read().decode('utf-8')

                    # Get IAM role name
                    role_url = 'http://169.254.169.254/latest/meta-data/iam/security-credentials/'
                    role_request = urllib.request.Request(
                        role_url,
                        headers={'X-aws-ec2-metadata-token': token}
                    )
                    with urllib.request.urlopen(role_request, timeout=2) as response:
                        role_name = response.read().decode('utf-8').strip()

                    # Get credentials
                    creds_url = f'http://169.254.169.254/latest/meta-data/iam/security-credentials/{role_name}'
                    creds_request = urllib.request.Request(
                        creds_url,
                        headers={'X-aws-ec2-metadata-token': token}
                    )
                    with urllib.request.urlopen(creds_request, timeout=2) as response:
                        creds = json.loads(response.read().decode('utf-8'))

                    # Set credentials as environment variables for DataFusion to use
                    os.environ['AWS_ACCESS_KEY_ID'] = creds['AccessKeyId']
                    os.environ['AWS_SECRET_ACCESS_KEY'] = creds['SecretAccessKey']
                    os.environ['AWS_SESSION_TOKEN'] = creds['Token']
                    print(f"✓ Using AWS credentials from EC2 instance profile ({role_name})")
                except Exception as e:
                    print(f"⚠ Warning: Could not fetch EC2 instance credentials: {e}")
                    print(f"  No IAM role attached to EC2 instance")
                    print(f"  To fix this:")
                    print(f"    1. Attach an IAM role with S3 read permissions to this EC2 instance, OR")
                    print(f"    2. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
                    print(f"  Attempting to proceed with anonymous access (will fail if bucket is not public)...")

            print(f"✓ Configured S3 access")
        except Exception as e:
            print(f"✗ Could not configure S3 access: {e}")
        print()

    for table in table_names:
        if mode == 'parquet-s3':
            # For S3 mode, use direct S3 path (single file per table)
            path = f"{data_path}/{table}.parquet"
            try:
                print(f"Registering table {table} using S3 path {path}")
                ctx.register_parquet(table, path)
                registered = True
            except Exception as e:
                print(f"  Failed to register {path}: {e}")
                registered = False
        else:
            # Try different possible file patterns for local files
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
        'iterations': iterations,
        'prefer_hash_join': prefer_hash_join
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
            # Check if this is query 21 and use replacement query if available
            if query == 21:
                replacement_path = f"{query_path}/21_query_replacement.sql"
                if os.path.exists(replacement_path):
                    path = replacement_path
                    print(f"{'='*80}")
                    print(f"⚠️  USING OPTIMIZED REPLACEMENT QUERY FOR Q21")
                    print(f"   Original query uses too much memory for DataFusion")
                    print(f"   Using replacement query from: {replacement_path}")
                    print(f"{'='*80}")
                else:
                    path = f"{query_path}/q{query}.sql"
                    print(f"Reading query {query} using path {path}")
            else:
                path = f"{query_path}/q{query}.sql"
                print(f"Reading query {query} using path {path}")

            with open(path, "r") as f:
                text = f.read()
                # each file can contain multiple queries
                queries = text.split(";")

                start_time = time.time()

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

                print(f"Query {query} took {elapsed:.2f} seconds")

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
    parser = argparse.ArgumentParser(description="DataFusion benchmark derived from TPC-H / TPC-DS (spilling disabled)")
    parser.add_argument("--benchmark", required=True, help="Benchmark to run (tpch or tpcds)")
    parser.add_argument("--data", required=True, help="Path to data files (local path or S3 path)")
    parser.add_argument("--queries", required=True, help="Path to query files")
    parser.add_argument("--iterations", type=int, default=3, help="Number of iterations to run (default: 3)")
    parser.add_argument("--output", default=None, help="Output JSON file (default: auto-generated)")
    parser.add_argument("--mode", type=str, default="parquet", choices=["parquet", "parquet-s3"],
                        help="Data source mode: 'parquet' for local files, 'parquet-s3' for S3 (default: parquet)")
    parser.add_argument("--query", type=int, action='append', dest='queries_to_run',
                        help="Specific query number to run (can be specified multiple times, e.g., --query 1 --query 18)")
    parser.add_argument("--prefer-hash-join", dest='prefer_hash_join',
                        action='store_true', default=False,
                        help="Prefer hash join over sort-merge join (default: False)")
    args = parser.parse_args()

    # Generate default output filename if not specified
    if args.output is None:
        current_time_millis = int(datetime.now().timestamp() * 1000)
        args.output = f"datafusion-python-{args.benchmark}-{current_time_millis}.json"

    main(args.benchmark, args.data, args.queries, args.iterations, args.output, args.queries_to_run, args.prefer_hash_join, args.mode)

