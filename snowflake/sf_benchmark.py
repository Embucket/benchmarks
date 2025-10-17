import os
import sys
import time
import json
import re
import glob
import logging
import argparse
import snowflake.connector as sf

from dotenv import load_dotenv

load_dotenv()


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_tpch_queries(queries_dir):
    """Get TPC-H benchmark queries."""
    queries = []

    # Find all SQL files in the queries directory
    query_files = sorted(glob.glob(os.path.join(queries_dir, "q*.sql")))

    for query_file in query_files:
        query_name = os.path.basename(query_file).replace(".sql", "")
        # Extract query number (e.g., "q01" -> 1)
        query_num = int(re.search(r'q(\d+)', query_name).group(1))

        with open(query_file, 'r') as f:
            query_text = f.read()

        queries.append((query_num, query_name, query_text))

    return queries


def create_snowflake_connection(tpch_scale_factor):
    """Create connection to Snowflake."""
    # Read credentials from environment variables
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    database = os.environ.get("SNOWFLAKE_DATABASE")
    schema = os.environ.get("SNOWFLAKE_SCHEMA")

    if not all([user, password, account, warehouse, database]):
        print("Error: Missing Snowflake credentials in environment variables")
        print(
            "Required: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE")
        sys.exit(1)

    print(f"Connecting to Snowflake account: {account}")
    print(f"Using warehouse: {warehouse}")

    conn = sf.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema if schema else "PUBLIC"
    )

    cursor = conn.cursor()

    # Use Snowflake sample data
    tpch_db = f"SNOWFLAKE_SAMPLE_DATA"
    tpch_schema = f"TPCH_SF{tpch_scale_factor}"
    cursor.execute(f"USE DATABASE {tpch_db}")
    cursor.execute(f"USE SCHEMA {tpch_schema}")
    print(f"Using Snowflake sample data (scale factor: {tpch_scale_factor})")

    # Disable result caching
    cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    print("Disabled query result cache (USE_CACHED_RESULT = FALSE)")

    print()
    return conn, cursor


def main(queries_dir, temp_dir, iterations, output_file, queries_to_run, timestamp, tpch_scale_factor):
    # Get benchmark configuration from environment variables
    warehouse_size = os.environ["SNOWFLAKE_WAREHOUSE_SIZE"]

    # Create results dictionary
    results = {
        'timestamp': timestamp,
        'engine': 'snowflake',
        'snowflake-warehouse-size': warehouse_size,
        'scale_factor': tpch_scale_factor,
        'iterations': iterations,
        'mode': 'sample'
    }

    # Create connection with the scale factor
    sf_conn, sf_cursor = create_snowflake_connection(tpch_scale_factor)

    # Load queries
    all_queries = get_tpch_queries(queries_dir)

    # Filter queries if specified
    if queries_to_run:
        queries = [(num, name, query) for num, name, query in all_queries if num in queries_to_run]
    else:
        queries = all_queries

    # Execute each query
    for query_num, query_name, query in queries:
        print(f"=== Running Query {query_num} ===")

        iteration_times = []
        query_results = []

        # Run iterations for this query
        for i in range(iterations):
            print(f"  Iteration {i + 1}/{iterations}...", end=' ', flush=True)

            start_time = time.time()

            try:
                # Handle multi-statement queries (specifically for Q15)
                if query_num == 15:
                    # Extract statements from the query
                    statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]

                    # Get the original database and schema to return to later
                    sf_cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                    current_db, current_schema = sf_cursor.fetchone()
                    sample_db = "SNOWFLAKE_SAMPLE_DATA"
                    sample_schema = f"TPCH_SF{tpch_scale_factor}"

                    try:
                        # Get the user's database from connection parameters for view creation
                        user_db = os.environ.get("SNOWFLAKE_DATABASE")
                        user_schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")

                        # 1. Switch to user's database/schema to create the view
                        sf_cursor.execute(f"USE DATABASE {user_db}")
                        sf_cursor.execute(f"USE SCHEMA {user_schema}")

                        # 2. Create view with fully qualified table references
                        create_view_stmt = statements[0].replace(
                            "FROM\n        lineitem",
                            f"FROM\n        {sample_db}.{sample_schema}.lineitem"
                        )
                        sf_cursor.execute(create_view_stmt)

                        # 3. Switch back to sample database for main query execution
                        sf_cursor.execute(f"USE DATABASE {sample_db}")
                        sf_cursor.execute(f"USE SCHEMA {sample_schema}")

                        # 4. Execute main query with fully qualified view reference
                        main_query = statements[1].replace(
                            "supplier,\n    revenue0",
                            f"supplier,\n    {user_db}.{user_schema}.revenue0"
                        ).replace(
                            "FROM revenue0",
                            f"FROM {user_db}.{user_schema}.revenue0"
                        )
                        sf_cursor.execute(main_query)
                        result = sf_cursor.fetchall()

                        # Get query ID for performance data
                        sf_cursor.execute("SELECT LAST_QUERY_ID()")
                        query_id = sf_cursor.fetchone()[0]
                    finally:
                        # 5. Clean up - drop the view and restore context
                        try:
                            sf_cursor.execute(f"DROP VIEW IF EXISTS {user_db}.{user_schema}.revenue0")
                        except Exception as e:
                            print(f"Warning: Failed to drop view: {e}")

                        # Restore original context
                        sf_cursor.execute(f"USE DATABASE {current_db}")
                        sf_cursor.execute(f"USE SCHEMA {current_schema}")

                else:
                    # Normal case - single statement query
                    sf_cursor.execute(query)
                    result = sf_cursor.fetchall()
                    sf_cursor.execute("SELECT LAST_QUERY_ID()")
                    query_id = sf_cursor.fetchone()[0]

                # Get performance metrics from query history
                if query_id:
                    sf_cursor.execute(f"""
                        SELECT
                            QUERY_ID,
                            TOTAL_ELAPSED_TIME
                        FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY())
                        WHERE QUERY_ID = '{query_id}'
                    """)

                    history_record = sf_cursor.fetchone()
                    if history_record:
                        elapsed_ms = history_record[1]  # TOTAL_ELAPSED_TIME in ms
                        elapsed_sec = elapsed_ms / 1000.0

                        query_results.append({
                            'query_id': query_id,
                            'total_time_ms': elapsed_ms
                        })

                        iteration_times.append(elapsed_sec)
                        print(f"{elapsed_sec:.2f}s")
                else:
                    elapsed = time.time() - start_time
                    iteration_times.append(elapsed)
                    print(f"{elapsed:.2f}s (query ID not available)")

            except Exception as e:
                print(f"ERROR: {e}")
                break

        # Calculate statistics for this query
        if iteration_times:
            avg_time = sum(iteration_times) / len(iteration_times)
            min_time = min(iteration_times)
            max_time = max(iteration_times)
            print(f"  Query {query_num}: avg={avg_time:.2f}s, min={min_time:.2f}s, max={max_time:.2f}s")

            results[f"query_{query_num}"] = {
                'iteration_times': iteration_times,
                'query_results': query_results,
                'avg_time': avg_time,
                'min_time': min_time,
                'max_time': max_time
            }

        print()

    # Save results to JSON
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"Results saved to: {output_file}")

    sf_cursor.close()
    sf_conn.close()

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run TPC-H benchmark on Snowflake")
    parser.add_argument('--queries-dir', required=True, help='Directory containing SQL query files')
    parser.add_argument('--temp-dir', default='/tmp', help='Temporary directory')
    parser.add_argument('--iterations', type=int, default=3, help='Number of iterations per query')
    parser.add_argument('--output', required=True, help='Output JSON file path')
    parser.add_argument('--query', action='append', type=int, dest='queries', help='Specific query number to run')
    parser.add_argument('--scale-factor', type=int, help='TPC-H scale factor (e.g., 1, 10, 100)')
    parser.add_argument('--timestamp', help='Timestamp for the benchmark run',
                      default=time.strftime('%Y-%m-%d_%H:%M:%S'))

    args = parser.parse_args()

    main(args.queries_dir, args.temp_dir, args.iterations,
         args.output, args.queries, args.timestamp, args.scale_factor)
