#!/usr/bin/env python3
import duckdb
import time
import json
import sys
import os
import glob

def main(data_dir, queries_dir, temp_dir, iterations, output_file, queries_to_run, memory_limit_mb, threads):
    # Create DuckDB connection
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
    
    # Register Parquet files as tables
    tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
    for table in tables:
        table_path = os.path.join(data_dir, table, '*.parquet')
        conn.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{table_path}')")
        print(f"✓ Registered table: {table}")
    
    print()
    
    # Determine which queries to run
    if queries_to_run:
        query_numbers = queries_to_run
    else:
        query_numbers = list(range(1, 23))  # All 22 TPC-H queries
    
    results = {
        'engine': 'duckdb',
        'duckdb-version': duckdb.__version__,
        'data_path': data_dir,
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
    parser.add_argument('--data-dir', required=True)
    parser.add_argument('--queries-dir', required=True)
    parser.add_argument('--temp-dir', required=True)
    parser.add_argument('--iterations', type=int, default=3)
    parser.add_argument('--output', required=True)
    parser.add_argument('--query', action='append', type=int, dest='queries')
    parser.add_argument('--memory-limit', type=int, dest='memory_limit_mb')
    parser.add_argument('--threads', type=int)
    
    args = parser.parse_args()
    main(args.data_dir, args.queries_dir, args.temp_dir, args.iterations, 
         args.output, args.queries, args.memory_limit_mb, args.threads)

