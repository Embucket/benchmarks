import json
import re
import tempfile
import matplotlib.pyplot as plt
from graphviz import Digraph
import matplotlib.image as mpimg
import argparse
import os
import glob


def extract_json_from_file(filepath):
    """
    Extracts the main JSON object from a text file.
    """
    try:
        with open(filepath, 'r') as f:
            content = f.read()

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        json_patterns = [
            r'({[\s\S]*})',
            r'RAW RESULT:\s*({[\s\S]*})\s*={10,}',
            r'({[^{]*?(?:{[^{]*?})*[^}]*?})'
        ]

        for pattern in json_patterns:
            match = re.search(pattern, content)
            if match:
                try:
                    return json.loads(match.group(1))
                except json.JSONDecodeError:
                    continue

        print(f"Error: Could not find valid JSON in {filepath}")
        print(f"File begins with: {content[:100]}...")
        print(f"File ends with: ...{content[-100:]}")
        return None

    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while reading the file: {e}")
        return None


def _render_duckdb_tree_image(exec_json) -> str:
    """
    Renders DuckDB operator tree to a temporary PNG and returns the path.
    Expects the DuckDB profile JSON shape with 'operator_tree' -> 'tree'.
    """
    tree_root = (
        exec_json.get('operator_tree', {}) or
        exec_json.get('EXECUTION_TIME_BREAKDOWN', {}).get('operator_tree', {})
    )
    tree_root = tree_root.get('tree')
    if not tree_root:
        raise ValueError("DuckDB: 'operator_tree.tree' is missing.")

    dot = Digraph(comment='DuckDB Plan', format='png')
    dot.attr(rankdir='BT', labelloc='t', label='Query Operator Tree')
    dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightgreen')
    dot.attr('edge', dir='back')

    counter = {'i': 0}

    def add_node(node):
        counter['i'] += 1
        node_id = f"n{counter['i']}"

        name = node.get('name', 'UNKNOWN')
        typ = node.get('type', 'UNKNOWN')
        timing = node.get('timing', None)
        rows = node.get('rows_produced', None)

        label_parts = [f"<b>{name}</b>"]
        if typ:
            label_parts.append(f"<i>{typ}</i>")
        if timing is not None:
            label_parts.append(f"{timing:.6f}s")
        if rows is not None:
            label_parts.append(f"rows: {rows}")

        label = "<br/>".join(label_parts)
        dot.node(node_id, f"<{label}>")

        for child in node.get('children', []) or []:
            child_id = add_node(child)
            dot.edge(child_id, node_id)

        return node_id

    add_node(tree_root)

    with tempfile.NamedTemporaryFile(suffix='.gv', delete=False) as tmp:
        temp_tree_path = tmp.name
    tree_image_path = dot.render(temp_tree_path, cleanup=True)
    return tree_image_path


def _plot_duckdb_execution_breakdown(ax, exec_json):
    """
    Plot DuckDB execution breakdown in seconds.
    - Uses per-operator `timing`/`cpu_time` for processing, `blocked_time` for synchronization.
    - Falls back to percentages using `overall_time` as total if needed.
    """
    import numpy as np
    import matplotlib.pyplot as plt

    def _coerce_float(v):
        try:
            return float(v)
        except Exception:
            return None

    def _first_number(d, keys):
        for k in keys:
            if k in d:
                val = _coerce_float(d[k])
                if val is not None:
                    return val
        return None

    etb = exec_json.get('EXECUTION_TIME_BREAKDOWN', {}) or {}
    ops = etb.get('operators', []) or []
    if not ops:
        ax.set_title('No operator timing data')
        return

    # Accept `overall_time` as total seconds too.
    total_seconds = _first_number(etb, [
        "overall_time",
        "total_time_seconds",
        "total_seconds",
        "total_time_s",
        "query_time_seconds",
        "query_time_s",
        "total_time"
    ])
    if total_seconds is None:
        total_ms = _first_number(etb, ["total_time_ms", "query_time_ms"])
        if total_ms is not None:
            total_seconds = total_ms / 1000.0

    labels = []
    processing_s = []
    synchronization_s = []

    for op in ops:
        name = (op.get('name') or op.get('type') or 'OP').strip()
        typ = op.get('type') or ''
        labels.append(f"{name} ({typ})" if typ else name)

        # Prefer explicit seconds from known keys in export
        blocked_s = _first_number(op, ["blocked_time", "synchronization_time_seconds",
                                       "synchronization_seconds", "synchronization_time_s",
                                       "synchronization_s", "synchronization_time"])

        # `timing` is the operator wall time; `cpu_time` is CPU time
        timing_s = _first_number(op, ["timing"])
        cpu_s = _first_number(op, ["cpu_time"])

        # Derive processing: timing - blocked if both known; otherwise use cpu_time; otherwise 0/fallback
        proc_s = None
        if timing_s is not None and blocked_s is not None:
            proc_s = max(timing_s - blocked_s, 0.0)
        elif cpu_s is not None:
            proc_s = cpu_s
        elif timing_s is not None:
            proc_s = timing_s  # if no blocked time reported

        # If still None and we know total, derive from percentages
        if proc_s is None and total_seconds is not None:
            proc_pct = _coerce_float(op.get('processing_percentage'))
            if proc_pct is not None:
                proc_s = (proc_pct / 100.0) * total_seconds

        # Synchronization from blocked time; fallback to percentage if needed
        sync_s = blocked_s
        if (sync_s is None or np.isnan(sync_s)) and total_seconds is not None:
            sync_pct = _coerce_float(op.get('synchronization_percentage'))
            if sync_pct is not None:
                sync_s = (sync_pct / 100.0) * total_seconds

        processing_s.append(proc_s or 0.0)
        synchronization_s.append(sync_s or 0.0)

    bottom = np.zeros(len(labels))
    for values, label in [(np.array(processing_s), "Processing (s)"),
                          (np.array(synchronization_s), "Synchronization (s)")]:
        ax.bar(labels, values, label=label, bottom=bottom)
        bottom += values

    ax.set_ylabel('Execution Time (s)')
    ax.set_title('DuckDB Execution Time by Operator (seconds)')
    ax.legend(loc='upper right')
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right')


def generate_combined_visualization_duckdb(exec_json, output_filename="query_analysis.png"):
    """
    Combined DuckDB visualization: stacked execution chart + operator tree.
    """
    plt.figure(figsize=(15, 12))

    tree_image_path = _render_duckdb_tree_image(exec_json)

    ax1 = plt.subplot(2, 1, 1)
    _plot_duckdb_execution_breakdown(ax1, exec_json)

    ax2 = plt.subplot(2, 1, 2)
    tree_img = mpimg.imread(tree_image_path)
    ax2.imshow(tree_img)
    ax2.axis('off')
    ax2.set_title('Query Operator Tree')

    plt.tight_layout()
    plt.savefig(output_filename, dpi=150)
    print(f"Successfully generated combined query analysis: {output_filename}")

    if os.path.exists(tree_image_path):
        os.remove(tree_image_path)


def process_all_duckdb_plans(base_dir, output_subdir, overwrite=False, dpi=150):
    for instance in os.listdir(base_dir):
        instance_dir = os.path.join(base_dir, instance)
        if not os.path.isdir(instance_dir):
            continue
        output_dir = os.path.join(instance_dir, output_subdir)
        os.makedirs(output_dir, exist_ok=True)
        for plan_path in glob.glob(os.path.join(instance_dir, "query_*_breakdown.json")):
            output_filename = os.path.join(
                output_dir,
                os.path.basename(plan_path).replace("_breakdown.json", "_analysis.png")
            )
            if not overwrite and os.path.exists(output_filename):
                print(f"Skipping existing: {output_filename}")
                continue
            data = extract_json_from_file(plan_path)
            if not data:
                print(f"Failed to load: {plan_path}")
                continue
            generate_combined_visualization_duckdb(data, output_filename=output_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch-generate DuckDB visualizations for all 'query_*_breakdown.json' files."
    )
    parser.add_argument("--base-dir", default="/Users/yevheniiniestierov/Desktop/benchmarks/duckdb/results-parquet",
                        help="Base folder with instance subfolders (default: 'results-parquet').")
    parser.add_argument("--output-subdir", default="/Users/yevheniiniestierov/Desktop/benchmarks/duckdb/duckdb/results-parquet/visualizations",
                        help="Subfolder name to store images under each instance (default: 'visualizations').")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing output files.")
    parser.add_argument("--dpi", type=int, default=150,
                        help="Matplotlib output DPI for combined image (default: 150).")
    args = parser.parse_args()
    process_all_duckdb_plans(
        base_dir=args.base_dir,
        output_subdir=args.output_subdir,
        overwrite=args.overwrite,
        dpi=args.dpi,
    )