#!/usr/bin/env python3
"""
Benchmark Visualization Script

This script gathers benchmark results and creates visualizations comparing
different systems. It finds result JSON files for a specified scale factor,
calculates averages of multiple runs, and generates comparison charts.
"""

import json
import os
import glob
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import numpy as np
import argparse


def find_result_files(benchmark: str, scale_factor: int, base_dir: str = ".") -> List[Tuple[str, str]]:
    """
    Find all result JSON files for a given benchmark and scale factor.

    Args:
        benchmark: Benchmark name (e.g., 'tpch')
        scale_factor: Scale factor (e.g., 1000 for sf1000)
        base_dir: Base directory to search from

    Returns:
        List of tuples (system_name, file_path)
    """
    # Define patterns to search for both naming conventions
    patterns = [
        f"{benchmark}_sf{scale_factor}_results.json",  # underscore format
        f"{benchmark}-sf{scale_factor}*-results.json"  # hyphen format
    ]
    result_files = []

    # Search in duckdb results directories
    for mode in ['internal', 'parquet', 'parquet-s3']:
        results_base = os.path.join(base_dir, 'duckdb', f'results-{mode}')
        if os.path.exists(results_base):
            # Search in all EC2 instance type subdirectories
            for ec2_type_dir in os.listdir(results_base):
                ec2_type_path = os.path.join(results_base, ec2_type_dir)
                if os.path.isdir(ec2_type_path):
                    for pattern in patterns:
                        search_path = os.path.join(ec2_type_path, pattern)
                        for file_path in glob.glob(search_path):
                            system_name = f"duckdb-{mode}-{ec2_type_dir}"
                            result_files.append((system_name, file_path))

    # Search in datafusion results directories with similar pattern handling
    for mode in ['parquet', 'parquet-s3']:
        datafusion_results_base = os.path.join(base_dir, 'datafusion', f'results-{mode}')
        if os.path.exists(datafusion_results_base):
            for ec2_type_dir in os.listdir(datafusion_results_base):
                ec2_type_path = os.path.join(datafusion_results_base, ec2_type_dir)
                if os.path.isdir(ec2_type_path):
                    for pattern in patterns:
                        search_path = os.path.join(ec2_type_path, pattern)
                        for file_path in glob.glob(search_path):
                            system_name = f"datafusion-{mode}-{ec2_type_dir}"
                            result_files.append((system_name, file_path))

    # Search in snowflake results directories
    snowflake_results_base = os.path.join(base_dir, 'snowflake', 'results')
    if os.path.exists(snowflake_results_base):
        for warehouse_dir in os.listdir(snowflake_results_base):
            warehouse_path = os.path.join(snowflake_results_base, warehouse_dir)
            if os.path.isdir(warehouse_path):
                for pattern in patterns:
                    search_path = os.path.join(warehouse_path, pattern)
                    for file_path in glob.glob(search_path):
                        system_name = f"snowflake-{warehouse_dir}"
                        result_files.append((system_name, file_path))

    return result_files




def load_and_process_results(file_path: str) -> Dict:
    """
    Load a result JSON file and calculate averages for each query.

    Args:
        file_path: Path to the result JSON file

    Returns:
        Dictionary with metadata and averaged query results
    """
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Extract metadata
    metadata = {
        'timestamp': data.get('timestamp'),
        'ec2_instance_type': data.get('ec2_instance_type',
                                      data.get('snowflake-warehouse-size', 'unknown')),
        'usd_per_hour': data.get('usd_per_hour', calculate_snowflake_cost(data)),
        'engine': data.get('engine'),
        'mode': data.get('mode'),
        'iterations': data.get('iterations', 3)
    }

    # Calculate averages for each query
    query_averages = {}

    for key, value in data.items():
        # Handle snowflake query format (query_X)
        if key.startswith('query_') and isinstance(value, dict) and 'avg_time' in value:
            query_num = int(key.split('_')[1])
            query_averages[query_num] = value['avg_time']
        # Handle standard numeric keys
        elif key.isdigit():
            query_num = int(key)
            if isinstance(value, list) and len(value) > 0:
                query_averages[query_num] = np.mean(value)

    return {
        'metadata': metadata,
        'query_averages': query_averages
    }


def calculate_snowflake_cost(data: Dict) -> float:
    """
    Calculate the per-hour cost for a Snowflake warehouse.

    Args:
        data: The loaded JSON data containing Snowflake metadata

    Returns:
        Estimated USD per hour for the Snowflake warehouse
    """
    # Get the warehouse size or use a default value
    warehouse_size = data.get('snowflake-warehouse-size', 'X-SMALL').upper()

    # Approximate hourly costs for different Snowflake warehouse sizes
    # Based on general pricing tiers (adjust as needed)
    costs = {
        'X-SMALL': 1.0,  # Base unit for comparison
        'SMALL': 2.0,  # 2x X-SMALL
        'MEDIUM': 4.0,  # 4x X-SMALL
        'LARGE': 8.0,  # 8x X-SMALL
    }

    # Return the cost for the given warehouse size or a default value
    base_cost = 2.0  # Approximate base cost per hour for X-SMALL warehouse
    return base_cost * costs.get(warehouse_size, 1.0)


def calculate_costs(duration_seconds: float, usd_per_hour: float) -> float:
    """
    Calculate cost for a given duration.
    
    Args:
        duration_seconds: Duration in seconds
        usd_per_hour: Cost per hour in USD
    
    Returns:
        Cost in USD
    """
    return (duration_seconds / 3600) * usd_per_hour


def create_bar_chart(data: Dict[str, Dict], title: str, ylabel: str, 
                     filename: str, is_cost: bool = False):
    """
    Create a bar chart comparing systems.
    
    Args:
        data: Dictionary mapping system names to their data
        title: Chart title
        ylabel: Y-axis label
        filename: Output filename
        is_cost: Whether this is a cost chart (affects formatting)
    """
    systems = list(data.keys())
    colors = plt.cm.Set3(np.linspace(0, 1, len(systems)))
    
    # Get all query numbers (sorted)
    all_queries = sorted(set(
        query_num 
        for system_data in data.values() 
        for query_num in system_data['query_averages'].keys()
    ))
    
    # Prepare data for plotting
    x = np.arange(len(all_queries))
    width = 0.8 / len(systems)  # Width of bars
    
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # Create bars for each system
    for i, (system, system_data) in enumerate(data.items()):
        query_averages = system_data['query_averages']
        ec2_type = system_data['metadata']['ec2_instance_type']
        
        # Get values for all queries (0 if missing)
        values = [query_averages.get(q, 0) for q in all_queries]
        
        # Convert to costs if needed
        if is_cost:
            usd_per_hour = system_data['metadata']['usd_per_hour']
            values = [calculate_costs(v, usd_per_hour) for v in values]
        
        # Plot bars
        offset = (i - len(systems)/2 + 0.5) * width
        bars = ax.bar(x + offset, values, width, label=f"{system} ({ec2_type})", 
                     color=colors[i], alpha=0.8)
    
    # Customize chart
    ax.set_xlabel('Query Number', fontsize=12, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels([f"Q{q}" for q in all_queries], rotation=0)
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Format y-axis
    if is_cost:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'${y:.4f}'))
    
    plt.tight_layout()
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved chart: {filename}")
    plt.close()


def create_total_bar_chart(data: Dict[str, Dict], title: str, ylabel: str,
                           filename: str, is_cost: bool = False):
    """
    Create a bar chart showing total values across systems.
    
    Args:
        data: Dictionary mapping system names to their data
        title: Chart title
        ylabel: Y-axis label
        filename: Output filename
        is_cost: Whether this is a cost chart (affects formatting)
    """
    systems = list(data.keys())
    colors = plt.cm.Set3(np.linspace(0, 1, len(systems)))
    
    totals = []
    labels = []
    
    for system, system_data in data.items():
        query_averages = system_data['query_averages']
        ec2_type = system_data['metadata']['ec2_instance_type']
        
        # Calculate total
        total = sum(query_averages.values())
        
        # Convert to cost if needed
        if is_cost:
            usd_per_hour = system_data['metadata']['usd_per_hour']
            total = calculate_costs(total, usd_per_hour)
        
        totals.append(total)
        labels.append(f"{system}\n({ec2_type})")
    
    # Create bar chart
    fig, ax = plt.subplots(figsize=(10, 8))
    x = np.arange(len(systems))
    bars = ax.bar(x, totals, color=colors, alpha=0.8, width=0.6)
    
    # Add value labels on top of bars
    for bar, total in zip(bars, totals):
        height = bar.get_height()
        if is_cost:
            label = f'${total:.4f}'
        else:
            label = f'{total:.2f}s'
        ax.text(bar.get_x() + bar.get_width()/2., height,
                label, ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    # Customize chart
    ax.set_xlabel('System', fontsize=12, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Format y-axis
    if is_cost:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'${y:.4f}'))
    
    plt.tight_layout()
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved chart: {filename}")
    plt.close()


def main(benchmark, scale_factor, output_dir, base_dir):
    """Main function to generate all visualizations."""
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Find and load result files
    print(f"Searching for {benchmark.upper()} SF{scale_factor} results in {base_dir}...")
    result_files = find_result_files(benchmark, scale_factor, base_dir)

    if not result_files:
        print(f"No result files found for {benchmark.upper()} SF{scale_factor}")
        return

    print(f"Found {len(result_files)} result file(s):")
    for system_name, file_path in result_files:
        print(f"  - {system_name}: {file_path}")

    # Load and process all results
    all_data = {}
    for system_name, file_path in result_files:
        print(f"\nProcessing {system_name}...")
        all_data[system_name] = load_and_process_results(file_path)

        # Print summary
        metadata = all_data[system_name]['metadata']
        query_count = len(all_data[system_name]['query_averages'])
        print(f"  EC2 Type: {metadata['ec2_instance_type']}")
        print(f"  USD/hour: ${metadata['usd_per_hour']}")
        print(f"  Queries: {query_count}")

    # Generate charts
    print("\nGenerating visualizations...")

    # Chart 1: Duration of each query across systems
    duration_chart_path = os.path.join(output_dir, f'{benchmark}_sf{scale_factor}_query_duration.png')
    create_bar_chart(
        all_data,
        title=f'{benchmark.upper()} SF{scale_factor} - Query Duration Comparison',
        ylabel='Duration (seconds)',
        filename=duration_chart_path,
        is_cost=False
    )

    # Chart 2: Total sum of duration across systems
    total_duration_path = os.path.join(output_dir, f'{benchmark}_sf{scale_factor}_total_duration.png')
    create_total_bar_chart(
        all_data,
        title=f'{benchmark.upper()} SF{scale_factor} - Total Duration Comparison',
        ylabel='Total Duration (seconds)',
        filename=total_duration_path,
        is_cost=False
    )

    # Chart 3: Cost of each query across systems
    query_cost_path = os.path.join(output_dir, f'{benchmark}_sf{scale_factor}_query_cost.png')
    create_bar_chart(
        all_data,
        title=f'{benchmark.upper()} SF{scale_factor} - Query Cost Comparison',
        ylabel='Cost (USD)',
        filename=query_cost_path,
        is_cost=True
    )

    # Chart 4: Total cost across systems
    total_cost_path = os.path.join(output_dir, f'{benchmark}_sf{scale_factor}_total_cost.png')
    create_total_bar_chart(
        all_data,
        title=f'{benchmark.upper()} SF{scale_factor} - Total Cost Comparison',
        ylabel='Total Cost (USD)',
        filename=total_cost_path,
        is_cost=True
    )

    print(f"\nAll visualizations saved to '{output_dir}/' directory")


if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate benchmark visualizations')
    parser.add_argument('--benchmark', default='tpch', help='Benchmark name (e.g., tpch)')
    parser.add_argument('--scale-factor', type=int, default=10, help='Scale factor (e.g., 10 for sf10)')
    parser.add_argument('--output-dir', default='visualizations', help='Output directory for charts')
    parser.add_argument('--base-dir', default='.',
                        help='Base directory where benchmark results are stored')

    args = parser.parse_args()

    main(
        args.benchmark,
        args.scale_factor,
        args.output_dir,
        args.base_dir
    )

