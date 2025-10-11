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

import datafusion
from datafusion import SessionContext, RuntimeConfig
from datetime import datetime
import json
import time

def main(benchmark: str, data_path: str, query_path: str, iterations: int, output_file: str, temp_dir: str):

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
    
    # Create temp directory if it doesn't exist
    os.makedirs(temp_dir, exist_ok=True)
    
    # Create RuntimeConfig with temp directory
    runtime_config = RuntimeConfig().with_temp_file_path(temp_dir)
    ctx = SessionContext(runtime_config)
    
    # Also set via SQL for additional configuration options
    ctx.sql(f"SET datafusion.execution.temp_file_path = '{temp_dir}'")

    for table in table_names:
        path = f"{data_path}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        ctx.register_parquet(table, path)

    results = {
        'engine': 'datafusion-python',
        'datafusion-version': datafusion.__version__,
        'benchmark': benchmark,
        'data_path': data_path,
        'query_path': query_path,
        'temp_dir': temp_dir,
        'iterations': iterations
    }

    # Run multiple iterations
    for iteration in range(iterations):
        print(f"\n=== Iteration {iteration + 1}/{iterations} ===\n")
        
        for query in range(1, num_queries + 1):
            # read text file
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
                        df = ctx.sql(sql)
                        rows = df.collect()

                        print(f"Query {query} returned {len(rows)} rows")
                end_time = time.time()
                elapsed = end_time - start_time
                print(f"Query {query} took {elapsed:.2f} seconds")

                # Store timings for each iteration
                if query not in results:
                    results[query] = []
                results[query].append(elapsed)

    # Calculate statistics for each query
    print("\n=== Summary ===\n")
    for query in range(1, num_queries + 1):
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
    args = parser.parse_args()

    # Generate default output filename if not specified
    if args.output is None:
        current_time_millis = int(datetime.now().timestamp() * 1000)
        args.output = f"datafusion-python-{args.benchmark}-{current_time_millis}.json"

    main(args.benchmark, args.data, args.queries, args.iterations, args.output, args.temp_dir)

