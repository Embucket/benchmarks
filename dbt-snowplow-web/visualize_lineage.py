#!/usr/bin/env python3
"""
Script to visualize dbt-snowplow-web model lineage with execution durations.
Generates an interactive HTML report with Mermaid diagram.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Set, Tuple
from datetime import datetime


def parse_manifest(manifest_path: str) -> Tuple[Dict, Dict]:
    """Parse manifest.json to extract model metadata, dependencies, and sources."""
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    models = {}
    for unique_id, node in manifest['nodes'].items():
        if node['resource_type'] == 'model':
            # Extract schema from relation_name or schema field
            schema = node.get('schema', 'unknown')
            if 'public_' in schema:
                schema = schema.replace('public_', '')

            models[unique_id] = {
                'name': node['name'],
                'unique_id': unique_id,
                'schema': schema,
                'depends_on': node.get('depends_on', {}).get('nodes', []),
                'description': node.get('description', ''),
                'resource_type': 'model',
            }

    # Parse sources
    sources = {}
    for unique_id, node in manifest.get('sources', {}).items():
        schema = node.get('schema', 'atomic')
        sources[unique_id] = {
            'name': node['name'],
            'unique_id': unique_id,
            'schema': schema,
            'depends_on': [],  # Sources don't depend on anything
            'description': node.get('description', ''),
            'resource_type': 'source',
        }

    return models, sources


def parse_run_results(run_results_path: str) -> Dict:
    """Parse run_results.json to extract execution metrics."""
    with open(run_results_path, 'r') as f:
        run_results = json.load(f)
    
    metrics = {}
    for result in run_results['results']:
        unique_id = result['unique_id']
        
        # Only process models (skip operations, tests, etc.)
        if not unique_id.startswith('model.'):
            continue
        
        execution_time = result.get('execution_time', 0)
        # Use actual_row_count if available (from enrich_run_results.py), otherwise use rows_affected
        rows_affected = result.get('actual_row_count') or result.get('adapter_response', {}).get('rows_affected', 0)

        metrics[unique_id] = {
            'execution_time': execution_time,
            'rows_affected': rows_affected,
            'status': result.get('status', 'unknown'),
        }
    
    return metrics


def get_schema_color(schema: str) -> str:
    """Get color for a schema/layer."""
    color_map = {
        'derived': '#90EE90',      # Light green
        'scratch': '#FFB366',      # Light orange
        'snowplow_manifest': '#B19CD9',  # Light purple
        'manifest': '#B19CD9',     # Light purple
        'atomic': '#87CEEB',       # Light blue
    }
    return color_map.get(schema.lower(), '#D3D3D3')  # Light gray default


def shorten_model_name(name: str) -> str:
    """Shorten model name for display."""
    # Remove common prefixes
    name = name.replace('snowplow_web_', '')
    return name


def build_dependency_graph(models: Dict, sources: Dict, metrics: Dict) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Build nodes and edges for the dependency graph."""
    nodes = []
    edges = []

    # Combine models and sources for processing
    all_nodes = {**models, **sources}

    # Create a mapping of unique_id to short name
    id_to_short = {}
    for unique_id, node in all_nodes.items():
        short_name = shorten_model_name(node['name'])
        id_to_short[unique_id] = short_name

    # Build nodes with metrics (for models)
    for unique_id, model in models.items():
        if unique_id not in metrics:
            continue

        short_name = id_to_short[unique_id]
        metric = metrics[unique_id]

        exec_time = metric['execution_time']
        schema = model['schema']
        color = get_schema_color(schema)

        # Format node label (only show name and execution time, not row counts)
        label = f"{short_name}<br/>{exec_time:.1f}s"

        # Create node with styling
        node_id = short_name.replace('-', '_').replace(' ', '_')
        nodes.append({
            'id': node_id,
            'label': label,
            'color': color,
            'schema': schema,
            'unique_id': unique_id,
            'resource_type': 'model',
        })

    # Build source nodes (no metrics, just labels)
    # First, find sources referenced in depends_on
    sources_referenced = set()
    for unique_id, model in models.items():
        for dep_id in model['depends_on']:
            if dep_id.startswith('source.'):
                sources_referenced.add(dep_id)

    # Also check for 'events' table references in compiled SQL
    # This is a workaround for models that query tables directly without using source() macro
    for unique_id, model in models.items():
        # Check if this model likely queries the events table directly
        if 'base_sessions_lifecycle' in model['name'] or 'base_events' in model['name']:
            # Add the events source manually
            events_source_id = 'source.snowplow_web.atomic.events'
            if events_source_id not in sources:
                # Create a synthetic source entry
                sources[events_source_id] = {
                    'name': 'events',
                    'unique_id': events_source_id,
                    'schema': 'atomic',
                    'depends_on': [],
                    'description': 'Snowplow events table',
                    'resource_type': 'source',
                }
                id_to_short[events_source_id] = 'events'
            sources_referenced.add(events_source_id)

    for unique_id in sources_referenced:
        if unique_id in sources:
            source = sources[unique_id]
            short_name = id_to_short.get(unique_id, source['name'])
            schema = source['schema']
            color = get_schema_color(schema)

            # Format source label (no execution time)
            label = f"{short_name}<br/>(source)"

            node_id = short_name.replace('-', '_').replace(' ', '_')
            nodes.append({
                'id': node_id,
                'label': label,
                'color': color,
                'schema': schema,
                'unique_id': unique_id,
                'resource_type': 'source',
            })

    # Build edges
    for unique_id, model in models.items():
        if unique_id not in metrics:
            continue

        source_short = id_to_short[unique_id]
        source_id = source_short.replace('-', '_').replace(' ', '_')

        for dep_id in model['depends_on']:
            if dep_id in id_to_short:
                # Check if dependency is a model with metrics or a source
                if dep_id in metrics or dep_id.startswith('source.'):
                    target_short = id_to_short[dep_id]
                    target_id = target_short.replace('-', '_').replace(' ', '_')
                    edges.append((target_id, source_id))

        # Add manual edges from events source to models that query it directly
        if 'base_sessions_lifecycle' in model['name'] or 'base_events_this_run' in model['name']:
            events_source_id = 'source.snowplow_web.atomic.events'
            if events_source_id in id_to_short:
                target_id = 'events'
                edges.append((target_id, source_id))

    return nodes, edges


def generate_mermaid_diagram(nodes: List[Dict], edges: List[Tuple[str, str]]) -> str:
    """Generate Mermaid diagram syntax."""
    lines = ['graph TD']
    
    # Add nodes
    for node in nodes:
        node_id = node['id']
        label = node['label']
        lines.append(f'    {node_id}["{label}"]')
    
    # Add edges
    for source, target in edges:
        lines.append(f'    {source} --> {target}')
    
    # Add styling
    lines.append('')
    for i, node in enumerate(nodes):
        node_id = node['id']
        color = node['color']
        lines.append(f'    style {node_id} fill:{color},stroke:#333,stroke-width:2px')
    
    return '\n'.join(lines)


def calculate_summary_stats(metrics: Dict) -> Dict:
    """Calculate summary statistics."""
    if not metrics:
        return {}
    
    execution_times = [m['execution_time'] for m in metrics.values()]
    total_time = sum(execution_times)
    avg_time = total_time / len(execution_times) if execution_times else 0
    max_time = max(execution_times) if execution_times else 0
    
    # Find slowest running model
    slowest_model = max(metrics.items(), key=lambda x: x[1]['execution_time'])

    return {
        'total_models': len(metrics),
        'total_time': total_time,
        'avg_time': avg_time,
        'max_time': max_time,
        'slowest_model': slowest_model[0],
        'slowest_model_time': slowest_model[1]['execution_time'],
    }


def generate_html_report(title: str, mermaid_diagram: str, summary_stats: Dict,
                        nodes: List[Dict], metrics: Dict, models: Dict, row_label: str = "Rows Affected") -> str:
    """Generate HTML report with Mermaid diagram and details."""
    
    # Build model details table (only for models, not sources)
    table_rows = []
    model_nodes = [n for n in nodes if n['resource_type'] == 'model']
    for node in sorted(model_nodes, key=lambda x: metrics[x['unique_id']]['execution_time'], reverse=True):
        unique_id = node['unique_id']
        model = models[unique_id]
        metric = metrics[unique_id]

        table_rows.append(f'''
        <tr>
            <td>{model['name']}</td>
            <td>{model['schema']}</td>
            <td>{metric['execution_time']:.2f}s</td>
            <td>{metric['rows_affected']:,}</td>
            <td>{metric['status']}</td>
        </tr>
        ''')
    
    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 30px;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stat-label {{
            font-size: 14px;
            opacity: 0.9;
            margin-bottom: 5px;
        }}
        .stat-value {{
            font-size: 28px;
            font-weight: bold;
        }}
        .diagram-container {{
            background-color: #fafafa;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            overflow-x: auto;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            font-weight: 600;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .legend {{
            margin: 20px 0;
            padding: 15px;
            background-color: #f9f9f9;
            border-radius: 8px;
        }}
        .legend-item {{
            display: inline-block;
            margin-right: 20px;
            margin-bottom: 10px;
        }}
        .legend-color {{
            display: inline-block;
            width: 20px;
            height: 20px;
            margin-right: 5px;
            vertical-align: middle;
            border: 1px solid #333;
            border-radius: 3px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <h2>Summary Statistics</h2>
        <div class="summary">
            <div class="stat-card">
                <div class="stat-label">Total Models</div>
                <div class="stat-value">{summary_stats.get('total_models', 0)}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Execution Time</div>
                <div class="stat-value">{summary_stats.get('total_time', 0):.1f}s</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Average Time</div>
                <div class="stat-value">{summary_stats.get('avg_time', 0):.1f}s</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Slowest Model</div>
                <div class="stat-value">{summary_stats.get('slowest_model_time', 0):.1f}s</div>
            </div>
        </div>
        
        <h2>Model Lineage</h2>
        <div class="legend">
            <strong>Schema Legend:</strong><br/>
            <div class="legend-item">
                <span class="legend-color" style="background-color: #87CEEB;"></span>
                <span>Atomic (Source) - Raw events data</span>
            </div>
            <div class="legend-item">
                <span class="legend-color" style="background-color: #B19CD9;"></span>
                <span>Manifest (Metadata) - Tracking tables for incremental processing</span>
            </div>
            <div class="legend-item">
                <span class="legend-color" style="background-color: #FFB366;"></span>
                <span>Scratch (Intermediate) - Temporary transformations</span>
            </div>
            <div class="legend-item">
                <span class="legend-color" style="background-color: #90EE90;"></span>
                <span>Derived (Final Tables) - Final output tables</span>
            </div>
        </div>

        <div style="background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 12px; margin: 20px 0; border-radius: 4px;">
            <strong>Note:</strong> The diagram shows data flow from top to bottom. The <strong>events</strong> source table (blue) is the starting point for data transformations.
            Manifest tables (purple) like <em>incremental_manifest</em> and <em>base_quarantined_sessions</em> are metadata tracking tables used by the dbt-snowplow-web framework
            and may appear at the top due to having no data dependencies, but they support the incremental processing logic.
        </div>
        
        <div class="diagram-container">
            <div class="mermaid">
{mermaid_diagram}
            </div>
        </div>
        
        <h2>Model Execution Details</h2>
        <table>
            <thead>
                <tr>
                    <th>Model Name</th>
                    <th>Schema</th>
                    <th>Execution Time</th>
                    <th>{row_label}</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {''.join(table_rows)}
            </tbody>
        </table>
    </div>
    
    <script>
        mermaid.initialize({{ 
            startOnLoad: true,
            theme: 'default',
            flowchart: {{
                useMaxWidth: true,
                htmlLabels: true,
                curve: 'basis'
            }}
        }});
    </script>
</body>
</html>'''
    
    return html


def main():
    parser = argparse.ArgumentParser(description='Visualize dbt-snowplow-web model lineage')
    parser.add_argument('--manifest', required=True, help='Path to manifest.json')
    parser.add_argument('--run-results', required=True, help='Path to run_results.json')
    parser.add_argument('--output', required=True, help='Output HTML file path')
    parser.add_argument('--title', default='dbt-snowplow-web Lineage', help='Diagram title')
    parser.add_argument('--row-label', default='Rows Affected', help='Label for row count column (e.g., "Rows Created" or "Rows Affected")')

    args = parser.parse_args()

    print(f"Parsing manifest: {args.manifest}")
    models, sources = parse_manifest(args.manifest)
    print(f"Found {len(models)} models and {len(sources)} sources")

    print(f"Parsing run results: {args.run_results}")
    metrics = parse_run_results(args.run_results)
    print(f"Found metrics for {len(metrics)} models")

    print("Building dependency graph...")
    nodes, edges = build_dependency_graph(models, sources, metrics)
    print(f"Graph has {len(nodes)} nodes and {len(edges)} edges")

    print("Generating Mermaid diagram...")
    mermaid_diagram = generate_mermaid_diagram(nodes, edges)

    print("Calculating summary statistics...")
    summary_stats = calculate_summary_stats(metrics)

    print("Generating HTML report...")
    html = generate_html_report(args.title, mermaid_diagram, summary_stats, nodes, metrics, models, args.row_label)

    print(f"Writing output to: {args.output}")
    with open(args.output, 'w') as f:
        f.write(html)

    print("✓ Visualization complete!")
    print(f"\nSummary:")
    print(f"  Total models: {summary_stats['total_models']}")
    print(f"  Total execution time: {summary_stats['total_time']:.1f}s")
    print(f"  Average execution time: {summary_stats['avg_time']:.1f}s")
    print(f"  Slowest model: {summary_stats['slowest_model_time']:.1f}s")


if __name__ == '__main__':
    main()

