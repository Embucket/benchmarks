#!/usr/bin/env python3
"""
Enrich run_results.json with actual row counts from Embucket tables.

This script queries Embucket to get the actual number of rows in each table
and updates the run_results.json file with accurate row counts. This is needed
because rows_affected returns 1 for CREATE TABLE statements instead
of the actual row count.
"""

import json
import os
import sys
import argparse
import snowflake.connector


def get_embucket_connection():
    """Create an Embucket connection using environment variables."""
    return snowflake.connector.connect(
        host=os.environ.get('EMBUCKET_HOST', 'localhost'),
        port=int(os.environ.get('EMBUCKET_PORT', '3000')),
        protocol=os.environ.get('EMBUCKET_PROTOCOL', 'http'),
        account=os.environ.get('EMBUCKET_ACCOUNT', 'test'),
        user=os.environ['EMBUCKET_USER'],
        password=os.environ['EMBUCKET_PASSWORD'],
        database=os.environ['EMBUCKET_DATABASE'],
        warehouse=os.environ['EMBUCKET_WAREHOUSE'],
        role=os.environ['EMBUCKET_ROLE'],
        schema=os.environ['EMBUCKET_SCHEMA']
    )


def get_table_row_count(conn, database, schema, table_name):
    """Query Embucket to get the actual row count for a table."""
    cursor = conn.cursor()
    try:
        # Use COUNT(*) to get accurate row count
        # Don't quote identifiers - will handle case sensitivity
        query = f'SELECT COUNT(*) FROM {database}.{schema}.{table_name}'
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        print(f"Warning: Could not get row count for {schema}.{table_name}: {e}", file=sys.stderr)
        return None
    finally:
        cursor.close()


def enrich_run_results(manifest_path, run_results_path, output_path):
    """
    Enrich run_results.json with actual row counts from Embucket.
    
    Args:
        manifest_path: Path to manifest.json
        run_results_path: Path to run_results.json
        output_path: Path to write enriched run_results.json
    """
    # Load manifest and run_results
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    
    with open(run_results_path, 'r') as f:
        run_results = json.load(f)
    
    # Connect to Embucket
    print("Connecting to Embucket...")
    conn = get_embucket_connection()
    
    database = os.environ['EMBUCKET_DATABASE']
    
    # Process each result
    enriched_count = 0
    for result in run_results['results']:
        unique_id = result['unique_id']
        
        # Only process models
        if not unique_id.startswith('model.'):
            continue
        
        # Get model info from manifest
        if unique_id not in manifest['nodes']:
            continue
        
        model = manifest['nodes'][unique_id]
        schema = model['schema']
        table_name = model['alias'] if 'alias' in model else model['name']
        
        # Get actual row count from Embucket
        row_count = get_table_row_count(conn, database, schema, table_name)
        
        if row_count is not None:
            # Add actual_row_count field
            result['actual_row_count'] = row_count
            enriched_count += 1
            print(f"  {table_name}: {row_count:,} rows")
    
    conn.close()
    
    # Write enriched run_results
    with open(output_path, 'w') as f:
        json.dump(run_results, f, indent=2)
    
    print(f"\n✓ Enriched {enriched_count} models with actual row counts")
    print(f"✓ Wrote enriched results to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Enrich run_results.json with actual row counts from Embucket')
    parser.add_argument('--manifest', required=True, help='Path to manifest.json')
    parser.add_argument('--run-results', required=True, help='Path to run_results.json')
    parser.add_argument('--output', required=True, help='Path to write enriched run_results.json')
    
    args = parser.parse_args()
    
    enrich_run_results(args.manifest, args.run_results, args.output)


if __name__ == '__main__':
    main()

