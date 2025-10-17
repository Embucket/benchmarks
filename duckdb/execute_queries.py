#!/usr/bin/env python3
import duckdb
import time
import json
import sys
import os
import glob

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

    print()

    # Register Parquet files as tables (for parquet and parquet-s3 modes)
    if mode == 'parquet':
        tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
        for table in tables:
            table_path = os.path.join(data_dir, table, '*.parquet')
            conn.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{table_path}')")
            print(f"✓ Registered table: {table}")
        print()
    elif mode == 'parquet-s3':
        tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
        for table in tables:
            # S3 path format: s3://bucket/path/table/*.parquet
            table_path = f"{data_dir}/{table}/*.parquet"
            conn.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{table_path}')")
            print(f"✓ Registered table: {table}")
        print()
    else:
        print("✓ Using tables from internal database")
        print()
    
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
    
    for query_num in query_numbers:
        print(f"=== Running Query {query_num} ===")
        query_file = os.path.join(queries_dir, f"q{query_num:02d}.sql")

        if not os.path.exists(query_file):
            print(f"⚠ Query file not found: {query_file}")
            continue

        with open(query_file, 'r') as f:
            query = f.read()

        iteration_times = []

        for i in range(iterations):
            print(f"  Iteration {i+1}/{iterations}...", end=' ', flush=True)

            start = time.time()
            try:
                # Check if query contains DDL statements (can't use EXPLAIN ANALYZE with them)
                query_upper = query.upper()
                use_explain_analyze = not any(stmt in query_upper for stmt in ["CREATE VIEW", "DROP VIEW", "CREATE TABLE", "DROP TABLE"])

                if use_explain_analyze:
                    # Use EXPLAIN ANALYZE to get execution metrics
                    explain_query = f"EXPLAIN ANALYZE {query}"
                    explain_result = conn.execute(explain_query).fetchall()

                    # Save EXPLAIN ANALYZE output to file (only on first iteration)
                    if i == 0:
                        output_dir = os.path.dirname(output_file) if output_file else "."
                        plan_file = os.path.join(output_dir, f"query_{query_num}_plan.txt")
                        with open(plan_file, 'w') as f:
                            f.write(f"DuckDB EXPLAIN ANALYZE - Query {query_num}\n")
                            f.write("=" * 80 + "\n\n")
                            for row in explain_result:
                                f.write(str(row[1]) + "\n")  # explain_value column
                            f.write("\n" + "=" * 80 + "\n")
                        print(f"\n  ✓ Query plan saved to: {plan_file}")

                    # Execute the actual query for timing
                    result = conn.execute(query).fetchall()
                else:
                    # For DDL queries, execute normally
                    result = conn.execute(query).fetchall()

                elapsed = time.time() - start
                iteration_times.append(elapsed)
                print(f"{elapsed:.2f}s ({len(result)} rows)")
            except Exception as e:
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

