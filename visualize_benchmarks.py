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
from pathlib import Path
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import numpy as np


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
    pattern = f"{benchmark}-sf{scale_factor}-*-results.json"
    result_files = []

    # Search in duckdb results directories (now organized by EC2 instance type)
    for mode in ['internal', 'parquet']:
        results_base = os.path.join(base_dir, 'duckdb', f'results-{mode}')
        if os.path.exists(results_base):
            # Search in all EC2 instance type subdirectories
            for ec2_type_dir in os.listdir(results_base):
                ec2_type_path = os.path.join(results_base, ec2_type_dir)
                if os.path.isdir(ec2_type_path):
                    search_path = os.path.join(ec2_type_path, pattern)
                    for file_path in glob.glob(search_path):
                        system_name = f"duckdb-{mode}-{ec2_type_dir}"
                        result_files.append((system_name, file_path))

    # Search in datafusion results directories (organized by EC2 instance type)
    datafusion_results_base = os.path.join(base_dir, 'datafusion', 'results')
    if os.path.exists(datafusion_results_base):
        # Search in all EC2 instance type subdirectories
        for ec2_type_dir in os.listdir(datafusion_results_base):
            ec2_type_path = os.path.join(datafusion_results_base, ec2_type_dir)
            if os.path.isdir(ec2_type_path):
                search_path = os.path.join(ec2_type_path, pattern)
                for file_path in glob.glob(search_path):
                    system_name = f"datafusion-{ec2_type_dir}"
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
        'ec2_instance_type': data.get('ec2_instance_type'),
        'usd_per_hour': data.get('usd_per_hour'),
        'engine': data.get('engine'),
        'mode': data.get('mode'),
        'iterations': data.get('iterations', 3)
    }
    
    # Calculate averages for each query
    query_averages = {}
    for key, value in data.items():
        # Query results are stored with numeric keys
        if key.isdigit():
            query_num = int(key)
            if isinstance(value, list) and len(value) > 0:
                query_averages[query_num] = np.mean(value)
    
    return {
        'metadata': metadata,
        'query_averages': query_averages
    }


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


def main():
    """Main function to generate all visualizations."""
    # Configuration
    benchmark = 'tpch'
    scale_factor = 1000
    output_dir = 'visualizations'
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Find and load result files
    print(f"Searching for {benchmark.upper()} SF{scale_factor} results...")
    result_files = find_result_files(benchmark, scale_factor)
    
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
    create_bar_chart(
        all_data,
        title=f'{benchmark.upper()} SF{scale_factor} - Query Duration Comparison',
        ylabel='Duration (seconds)',
        filename=os.path.join(output_dir, f'{benchmark}_sf{scale_factor}_query_duration.png'),
        is_cost=False
    )
    
    # Chart 2: Total sum of duration across systems
    create_total_bar_chart(
        all_data,
        title=f'{benchmark.upper()} SF{scale_factor} - Total Duration Comparison',
        ylabel='Total Duration (seconds)',
        filename=os.path.join(output_dir, f'{benchmark}_sf{scale_factor}_total_duration.png'),
        is_cost=False
    )
    
    # Chart 3: Cost of each query across systems
    create_bar_chart(
        all_data,
        title=f'{benchmark.upper()} SF{scale_factor} - Query Cost Comparison',
        ylabel='Cost (USD)',
        filename=os.path.join(output_dir, f'{benchmark}_sf{scale_factor}_query_cost.png'),
        is_cost=True
    )
    
    # Chart 4: Total cost across systems
    create_total_bar_chart(
        all_data,
        title=f'{benchmark.upper()} SF{scale_factor} - Total Cost Comparison',
        ylabel='Total Cost (USD)',
        filename=os.path.join(output_dir, f'{benchmark}_sf{scale_factor}_total_cost.png'),
        is_cost=True
    )
    
    print(f"\nAll visualizations saved to '{output_dir}/' directory")


if __name__ == '__main__':
    main()

