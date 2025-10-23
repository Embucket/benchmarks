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
    query_files = glob.glob(os.path.join(queries_dir, "q*.sql"))

    # Custom sort to handle numeric ordering correctly
    query_files.sort(key=lambda x: int(re.search(r'q(\d+)', os.path.basename(x)).group(1)))

    for query_file in query_files:
        query_name = os.path.basename(query_file).replace(".sql", "")
        # Extract query number (e.g., "q01" -> 1)
        query_num = int(re.search(r'q(\d+)', query_name).group(1))

        with open(query_file, 'r') as f:
            query_text = f.read()

        queries.append((query_num, query_name, query_text))

    return queries


def save_query_plan(sf_cursor, query_num, query_text, output_dir, user_db, user_schema):
    """Save query plan using EXPLAIN_ANALYZE procedure."""
    plan_file = os.path.join(output_dir, f"query_{query_num}_plan.txt")

    try:
        # Call the procedure with fully qualified name
        sf_cursor.execute(f"CALL {user_db}.{user_schema}.EXPLAIN_ANALYZE($$\n{query_text}\n$$)")
        result_raw = sf_cursor.fetchone()[0]

        with open(plan_file, 'w') as f:
            f.write(f"Snowflake Query Plan - Query {query_num}\n")
            f.write("=" * 80 + "\n\n")

            # Handle result based on its type
            if isinstance(result_raw, dict):
                # It's already a dictionary
                result = result_raw

                # Write query ID
                f.write(f"Query ID: {result.get('query_id', 'N/A')}\n\n")

                # Write plan
                f.write("EXECUTION PLAN:\n")
                plan_data = result.get('plan', {})
                f.write(json.dumps(plan_data, indent=2))
                f.write("\n\n")

                # Write stats
                f.write("OPERATOR STATISTICS:\n")
                stats_data = result.get('stats', [])
                f.write(json.dumps(stats_data, indent=2))
                f.write("\n\n")

                # Write summary
                f.write("QUERY SUMMARY:\n")
                summary_data = result.get('summary', {})
                f.write(json.dumps(summary_data, indent=2))
                f.write("\n\n")
            else:
                # If it's not a dictionary, write the raw result
                f.write("RAW RESULT:\n")
                f.write(str(result_raw))
                f.write("\n\n")

            f.write("=" * 80 + "\n")

        logger.info(f"Query plan saved to: {plan_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to save query plan: {e}")
        return False


def create_snowflake_connection(tpch_scale_factor, warehouse_size=None):
    """Create connection to Snowflake and recreate warehouse if size is specified."""
    # Read credentials from environment variables
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    database = os.environ.get("SNOWFLAKE_DATABASE")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")

    if not all([user, password, account, warehouse, database]):
        print("Error: Missing Snowflake credentials in environment variables")
        print(
            "Required: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE")
        sys.exit(1)

    print(f"Connecting to Snowflake account: {account}")

    # Connect with user's database
    conn = sf.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        schema=schema
    )

    cursor = conn.cursor()

    # Create or replace the warehouse
    try:
        print(f"Creating or replacing warehouse '{warehouse}' with size '{warehouse_size}'")
        cursor.execute(f"""
            CREATE OR REPLACE WAREHOUSE {warehouse}
            WITH WAREHOUSE_SIZE = {warehouse_size}
        """)
        print(f"Successfully created warehouse '{warehouse}' with size '{warehouse_size}'")
    except Exception as e:
        print(f"Warning: Failed to create or replace warehouse: {warehouse} with size: {warehouse_size} : {e}")

    cursor.execute(f"USE WAREHOUSE {warehouse}")
    print(f"Using warehouse: {warehouse}")

    # Create procedure in user's database
    print(f"Creating procedure in {database}.{schema} to retrieve query plan")
    cursor.execute(
    """
    CREATE OR REPLACE PROCEDURE EXPLAIN_ANALYZE(sql_text STRING)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    $$
    DECLARE
      qid STRING;
    BEGIN
      EXECUTE IMMEDIATE :sql_text;
      qid := SQLID;

      RETURN OBJECT_CONSTRUCT(
        'query_id', qid,
        'plan_json', PARSE_JSON(SYSTEM$EXPLAIN_PLAN_JSON(:qid)),
        'plan_text', SYSTEM$EXPLAIN_JSON_TO_TEXT(SYSTEM$EXPLAIN_PLAN_JSON(:qid)),
        'stats', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(GET_QUERY_OPERATOR_STATS(:qid))),
        'summary', (SELECT OBJECT_CONSTRUCT(*)
                    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
                    WHERE query_id = :qid)
      );
    END;
    $$;
    """
    )

    # Now switch to sample data for queries
    tpch_db = "SNOWFLAKE_SAMPLE_DATA"
    tpch_schema = f"TPCH_SF{tpch_scale_factor}"
    cursor.execute(f"USE DATABASE {tpch_db}")
    cursor.execute(f"USE SCHEMA {tpch_schema}")
    print(f"Using Snowflake sample data (scale factor: {tpch_scale_factor})")

    # Disable result caching
    cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    print("Disabled query result cache (USE_CACHED_RESULT = FALSE)")

    print()
    return conn, cursor, database, schema


def main(queries_dir, iterations, output_file, queries_to_run, timestamp, tpch_scale_factor, warehouse_size_arg=None):
    warehouse_size = warehouse_size_arg if warehouse_size_arg else os.environ.get("SNOWFLAKE_WAREHOUSE_SIZE")
    if not warehouse_size:
        print(
            "Error: Missing Snowflake warehouse size. Provide it via --warehouse-size or SNOWFLAKE_WAREHOUSE_SIZE env var")
        sys.exit(1)

    # Create output directory for plans
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)

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
    sf_conn, sf_cursor, user_db, user_schema = create_snowflake_connection(tpch_scale_factor, warehouse_size)

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

        # Special handling for query plan extraction for Q15
        if query_num == 15:
            # Extract statements
            statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]

            if len(statements) >= 2:
                # Get the original database and schema to return to later
                sf_cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                current_db, current_schema = sf_cursor.fetchone()
                sample_db = "SNOWFLAKE_SAMPLE_DATA"
                sample_schema = f"TPCH_SF{tpch_scale_factor}"

                try:
                    # 1. Switch to user's database/schema to create the view
                    sf_cursor.execute(f"USE DATABASE {user_db}")
                    sf_cursor.execute(f"USE SCHEMA {user_schema}")

                    # 2. Create view with fully qualified table references
                    create_view_stmt = statements[0].replace(
                        "FROM\n        lineitem",
                        f"FROM\n        {sample_db}.{sample_schema}.lineitem"
                    )
                    sf_cursor.execute(create_view_stmt)

                    # 3. Now prepare the SELECT statement with fully qualified references
                    select_stmt = statements[1].replace(
                        "supplier,\n    revenue0",
                        f"{sample_db}.{sample_schema}.supplier,\n    revenue0"
                    )

                    # 4. Save plan for the fully qualified SELECT statement
                    save_query_plan(sf_cursor, query_num, select_stmt, output_dir, user_db, user_schema)
                finally:
                    # Clean up - drop the view and restore context
                    try:
                        sf_cursor.execute(f"DROP VIEW IF EXISTS revenue0")
                    except Exception as e:
                        logger.error(f"Failed to drop view: {e}")

                    # Restore original context
                    sf_cursor.execute(f"USE DATABASE {current_db}")
                    sf_cursor.execute(f"USE SCHEMA {current_schema}")
            else:
                logger.error(f"Could not extract statements from query {query_num}")
        else:
            # For other queries, save the full query plan
            save_query_plan(sf_cursor, query_num, query, output_dir, user_db, user_schema)

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
                        # 1. Switch to user's database/schema to create the view
                        sf_cursor.execute(f"USE DATABASE {user_db}")
                        sf_cursor.execute(f"USE SCHEMA {user_schema}")

                        # 2. Create view with fully qualified table references
                        create_view_stmt = statements[0].replace(
                            "FROM\n        lineitem",
                            f"FROM\n        {sample_db}.{sample_schema}.lineitem"
                        )
                        sf_cursor.execute(create_view_stmt)

                        # 3. Execute main query with qualified supplier reference
                        main_query = statements[1].replace(
                            "supplier,\n    revenue0",
                            f"{sample_db}.{sample_schema}.supplier,\n    revenue0"
                        )
                        sf_cursor.execute(main_query)
                        result = sf_cursor.fetchall()

                        # Get query ID for performance data
                        sf_cursor.execute("SELECT LAST_QUERY_ID()")
                        query_id = sf_cursor.fetchone()[0]
                    finally:
                        # Clean up - drop the view and restore context
                        try:
                            sf_cursor.execute(f"DROP VIEW IF EXISTS revenue0")
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
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"Results saved to: {output_file}")

    sf_cursor.close()
    sf_conn.close()

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run TPC-H benchmark on Snowflake")
    parser.add_argument('--queries-dir', default='tpch_queries', help='Directory containing SQL query files')
    parser.add_argument('--iterations', type=int, default=3, help='Number of iterations per query')
    parser.add_argument('--output', required=True, help='Base output directory path')
    parser.add_argument('--query', action='append', type=int, dest='queries', help='Specific query number to run')
    parser.add_argument('--scale-factor', type=int, required=True, help='TPC-H scale factor (e.g., 1, 10, 100)')
    parser.add_argument('--timestamp', help='Timestamp for the benchmark run',
                        default=time.strftime('%Y-%m-%d_%H:%M:%S'))
    parser.add_argument('--warehouse-size', help='Snowflake warehouse size (e.g., XSMALL, SMALL, MEDIUM, etc)')

    args = parser.parse_args()

    # Get warehouse size
    warehouse_size = args.warehouse_size if args.warehouse_size else os.environ.get("SNOWFLAKE_WAREHOUSE_SIZE")
    if not warehouse_size:
        print(
            "Error: Missing Snowflake warehouse size. Provide it via --warehouse-size or SNOWFLAKE_WAREHOUSE_SIZE env var")
        sys.exit(1)

    # Construct standard filename based on scale factor
    filename = f"tpch_sf{args.scale_factor}_results.json"

    # Create output directory with warehouse size as subdirectory
    output_dir = os.path.join(args.output, warehouse_size.upper())
    output_path = os.path.join(output_dir, filename)

    # Ensure directory exists
    os.makedirs(output_dir, exist_ok=True)

    print(f"Output will be saved to: {output_path}")

    main(args.queries_dir, args.iterations,
         output_path, args.queries, args.timestamp, args.scale_factor, args.warehouse_size)


