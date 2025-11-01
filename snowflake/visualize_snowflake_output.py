import json
import re
import os
import tempfile
import numpy as np
import matplotlib.pyplot as plt
from graphviz import Digraph
import matplotlib.image as mpimg
from pathlib import Path
from typing import List, Tuple
import shutil
import argparse


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


def _extract_summary(obj: dict | None) -> dict | None:
    """
    Best-effort to find Snowflake summary dict in a loaded JSON.
    Looks for common casings at the top level.
    """
    if not isinstance(obj, dict):
        return None
    for k in ("summary", "Summary", "SUMMARY"):
        if isinstance(obj.get(k), dict):
            return obj[k]
    return None


def _render_snowflake_tree_image(plan_json, graphviz_dpi: int | None = None) -> str:
    """
    Renders Snowflake operator tree to a temporary PNG and returns the path.
    """
    if 'Operations' not in plan_json or not plan_json['Operations']:
        raise ValueError("Snowflake: 'Operations' key missing or empty in plan_json.")

    dot = Digraph(comment='Query Plan', format='png')
    dot.attr(rankdir='BT', labelloc='t', label='Query Operator Tree')
    dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue')
    dot.attr('edge', dir='back')
    if graphviz_dpi:
        dot.graph_attr.update({'dpi': str(graphviz_dpi)})

    operators_list = plan_json['Operations'][0]
    op_map = {op['id']: op for op in operators_list}

    for op in operators_list:
        op_id = op['id']
        op_type = op['operation']

        label = f"<b>O{op_id}: {op_type}</b>"
        if op_type == 'TableScan':
            table_name = op.get('objects', ['Unknown Table'])[0].split('.')[-1]
            partitions = op.get('partitionsAssigned', '?')
            partitions_total = op.get('partitionsTotal', '?')
            label += f"<br/>{table_name}<br/>Partitions: {partitions} / {partitions_total}"
        elif op_type == 'Filter':
            condition = op.get('expressions', ['?'])[0]
            condition = condition.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            label += f"<br/>{condition}"
        elif op_type == 'Aggregate':
            group_keys = [k for k in op.get('expressions', []) if 'groupKeys' in k]
            if group_keys:
                keys = group_keys[0].split(': [')[-1].replace(']', '')
                keys = keys.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                label += f"<br/>GROUP BY {keys}"
        elif op_type == 'Sort':
            sort_keys = op.get('expressions', ['?'])[0]
            sort_keys = sort_keys.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            label += f"<br/>ORDER BY {sort_keys}"

        dot.node(str(op_id), f"<{label}>")

        if 'parentOperators' in op:
            for parent_id in op['parentOperators']:
                if parent_id in op_map:
                    dot.edge(str(op_id), str(parent_id))

    with tempfile.NamedTemporaryFile(suffix='.gv', delete=False) as tmp:
        temp_tree_path = tmp.name
    tree_image_path = dot.render(temp_tree_path, cleanup=True)
    return tree_image_path


def _get_elapsed_seconds(op_stat: dict) -> float | None:
    """
    Try to infer per-operator total elapsed seconds from any *_MS/US/NS or second-looking fields.
    """
    if not isinstance(op_stat, dict):
        return None

    candidates = []
    for k, v in op_stat.items():
        if not isinstance(v, (int, float)):
            continue
        name = str(k).lower()
        if any(t in name for t in ("elapsed", "duration", "time")):
            candidates.append((name, float(v)))

    for name, val in candidates:
        if name.endswith("_ms") or "millis" in name or name.endswith("milliseconds"):
            return val / 1000.0
        if name.endswith("_us") or "micros" in name or name.endswith("microseconds"):
            return val / 1_000_000.0
        if name.endswith("_ns") or "nanos" in name or name.endswith("nanoseconds"):
            return val / 1_000_000_000.0

    for _, val in candidates:
        if val > 10_000:
            return val / 1000.0
        return val

    return None


def _get_total_query_seconds(summary: dict | None) -> float | None:
    """
    Returns total query time in seconds from Snowflake summary.
    """
    if not isinstance(summary, dict):
        return None

    for key in ("TOTAL_ELAPSED_TIME", "total_elapsed_time"):
        v = summary.get(key)
        if isinstance(v, (int, float)):
            return float(v) / 1000.0


def _plot_snowflake_time_breakdown(ax, stats: list[dict], summary: dict | None = None, normalize: bool = False):
    """
    Plot Snowflake time breakdown.
    - If breakdown values are fractions/percentages, multiply by TOTAL_ELAPSED_TIME seconds.
    - Else treat values as times (normalize big ms-looking values to seconds).
    - If normalize=True, convert each operator's stack to percentages (0..100).
    """
    total_query_seconds = _get_total_query_seconds(summary)

    labels: list[str] = []
    breakdown_data: dict[str, list[float]] = {}
    all_components: set[str] = set()

    for op_stat in sorted(stats, key=lambda x: x.get("OPERATOR_ID", 0)):
        op_id = op_stat.get("OPERATOR_ID")
        op_type = op_stat.get("OPERATOR_TYPE", "Operator")
        labels.append(f"O{op_id}: {op_type}")

        breakdown = dict(op_stat.get("EXECUTION_TIME_BREAKDOWN") or {})
        breakdown.pop("overall_percentage", None)

        if not breakdown:
            for c in all_components:
                breakdown_data.setdefault(c, []).append(0.0)
            continue

        values = list(breakdown.values())
        sm = sum(values) if values else 0.0
        mx = max(values) if values else 0.0

        is_fraction = mx <= 1.05 or sm <= 1.05
        is_percent = 95.0 <= sm <= 105.0
        is_ratio = is_fraction or is_percent

        ratio_base_seconds = total_query_seconds
        if ratio_base_seconds is None:
            ratio_base_seconds = _get_elapsed_seconds(op_stat)

        for component, value in breakdown.items():
            if is_ratio:
                frac = (value / 100.0) if is_percent else float(value)
                sec = (frac * ratio_base_seconds) if ratio_base_seconds is not None else 0.0
            else:
                v = float(value)
                sec = (v / 1000.0) if v > 10_000 else v

            all_components.add(component)
            breakdown_data.setdefault(component, []).append(sec)

        for c in all_components:
            if c not in breakdown:
                breakdown_data.setdefault(c, []).append(0.0)

    for component in list(all_components):
        arr = breakdown_data.get(component, [])
        if len(arr) < len(labels):
            arr.extend([0.0] * (len(labels) - len(arr)))
        breakdown_data[component] = arr

    sorted_components = sorted(all_components)

    # Normalize to percentages per operator if requested
    if normalize:
        totals = np.zeros(len(labels), dtype=float)
        for component in sorted_components:
            totals += np.array(breakdown_data[component], dtype=float)
        totals[totals == 0.0] = 1.0  # avoid div-by-zero

    bottom = np.zeros(len(labels))
    for component in sorted_components:
        values = np.array(breakdown_data[component], dtype=float)
        if normalize:
            values = (values / totals) * 100.0
        ax.bar(labels, values, label=component, bottom=bottom)
        bottom += values

    if normalize:
        ax.set_ylabel("Share of operator time (\%)")
        ax.set_title("Query Time Breakdown by Operator (normalized \%)")
        ax.set_ylim(0, 100)
    else:
        ax.set_ylabel("Execution Time (s)")
        ax.set_title("Query Time Breakdown by Operator (seconds, total elapsed)")

    ax.legend(loc="upper right", ncol=1, fontsize="small")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")


def generate_combined_visualization_snowflake(
    plan_json,
    stats,
    summary=None,
    output_filename="query_analysis.png",
    tree_output_path: str | os.PathLike | None = None,
    dpi: int = 150,
    width_per_op: float = 1.0,
    min_width: float = 15.0,
    height: float = 12.0,
    graphviz_dpi: int | None = None,
    normalize: bool = False,
    also_tree: bool = False,
):
    """
    Combined Snowflake visualization: stacked execution chart + operator tree.
    If also_tree=True and tree_output_path provided, also persist the rendered operator tree PNG there.
    Figure width scales with number of operators.
    """
    try:
        num_ops = len(plan_json.get("Operations", [[]])[0])
    except Exception:
        num_ops = 10
    fig_width = max(min_width, max(1, num_ops) * width_per_op)

    plt.figure(figsize=(fig_width, height))

    # Render the operator tree once and reuse it for the combined figure
    eff_graphviz_dpi = graphviz_dpi if graphviz_dpi is not None else dpi
    tree_image_path = _render_snowflake_tree_image(plan_json, graphviz_dpi=eff_graphviz_dpi)

    ax1 = plt.subplot(2, 1, 1)
    _plot_snowflake_time_breakdown(ax1, stats, summary=summary, normalize=normalize)

    ax2 = plt.subplot(2, 1, 2)
    tree_img = mpimg.imread(tree_image_path)
    ax2.imshow(tree_img)
    ax2.axis('off')

    plt.tight_layout()
    plt.savefig(output_filename, dpi=dpi)
    print(f"Successfully generated combined query analysis: {output_filename}")

    # Optionally persist the standalone tree image
    try:
        if also_tree and tree_output_path:
            shutil.copyfile(tree_image_path, tree_output_path)
            print(f"Saved operator tree: {tree_output_path}")
    finally:
        if os.path.exists(tree_image_path):
            os.remove(tree_image_path)


def process_snowflake_plan_dir(
    base_dir: str | Path,
    output_subdir: str = "viz",
    overwrite: bool = False,
    normalize: bool = False,
    dpi: int = 300,
    width_per_op: float = 1.0,
    min_width: float = 15.0,
    height: float = 12.0,
    graphviz_dpi: int | None = None,
    also_tree: bool = False,
) -> None:
    """
    Walk base_dir and process all Snowflake plan files matching 'query_*_plan.txt'.
    Produces:
      - Combined image: '<viz>/<query>_analysis.png'
      - Tree image: '<viz>/<query>_tree.png' (only if also_tree=True)
    """
    base = Path(base_dir).expanduser()
    if not base.exists():
        raise FileNotFoundError(f"Base directory not found: {base}")

    plan_files = sorted(p for p in base.rglob("query_*_plan.txt") if p.is_file())
    if not plan_files:
        print(f"No plan files found under: {base}")
        return

    processed = 0
    skipped = 0
    failures: List[Tuple[Path, str]] = []

    for plan_path in plan_files:
        try:
            instance_dir = plan_path.parent
            viz_dir = instance_dir / output_subdir
            viz_dir.mkdir(parents=True, exist_ok=True)

            stem = plan_path.stem  # e.g., 'query_1_plan'
            combined_out = viz_dir / (stem.replace("_plan", "_analysis") + ".png")
            tree_out = viz_dir / (stem.replace("_plan", "_tree") + ".png")
            overwrite = True
            if combined_out.exists() and not overwrite:
                print(f"Skip (exists): {combined_out}")
                skipped += 1
                continue

            data = extract_json_from_file(str(plan_path))
            if not data:
                failures.append((plan_path, "Could not parse JSON"))
                print(f"Failed: {plan_path} (no JSON)")
                continue

            summary = _extract_summary(data)

            # Accept either { plan_json, stats } or legacy { Operations, Stats }
            if "plan_json" in data and "stats" in data:
                plan_json = data["plan_json"]
                stats = data["stats"]
            elif "Operations" in data and "Stats" in data:
                plan_json = {"Operations": data["Operations"]}
                stats = data["Stats"]
            else:
                failures.append((plan_path, "Missing required keys: plan_json+stats or Operations+Stats"))
                print(f"Failed: {plan_path} (missing required keys)")
                continue

            generate_combined_visualization_snowflake(
                plan_json,
                stats,
                summary=summary,
                output_filename=str(combined_out),
                tree_output_path=str(tree_out) if also_tree else None,
                dpi=dpi,
                width_per_op=width_per_op,
                min_width=min_width,
                height=height,
                graphviz_dpi=graphviz_dpi,
                normalize=normalize,
                also_tree=also_tree,
            )
            processed += 1

        except Exception as e:
            failures.append((plan_path, str(e)))
            print(f"Failed: {plan_path} ({e})")

    print(f"Done. Processed: {processed}, Skipped: {skipped}, Failures: {len(failures)}")
    if failures:
        print("Failures:")
        for p, msg in failures:
            print(f" - {p}: {msg}")


def process_snowflake_plan_file(
    file_path: str | Path,
    output_subdir: str = "viz",
    overwrite: bool = False,
    normalize: bool = False,
    dpi: int = 300,
    width_per_op: float = 1.0,
    min_width: float = 15.0,
    height: float = 12.0,
    graphviz_dpi: int | None = None,
    also_tree: bool = False,
) -> None:
    """
    Process a single Snowflake plan file.
    """
    target = Path(file_path).expanduser()
    if not target.exists() or not target.is_file():
        print(f"Path not found or not a file: {target}")
        return

    instance_dir = target.parent
    viz_dir = instance_dir / output_subdir
    viz_dir.mkdir(parents=True, exist_ok=True)

    stem = target.stem
    combined_out = viz_dir / (stem.replace("_plan", "_analysis") + ".png")
    tree_out = viz_dir / (stem.replace("_plan", "_tree") + ".png")

    if combined_out.exists() and not overwrite:
        print(f"Skip (exists): {combined_out}")
        return

    data = extract_json_from_file(str(target))
    if not data:
        print(f"Could not load data from: {target}")
        return

    summary = _extract_summary(data)
    if "plan_json" in data and "stats" in data:
        plan_json = data["plan_json"]
        stats = data["stats"]
    elif "Operations" in data and "Stats" in data:
        plan_json = {"Operations": data["Operations"]}
        stats = data["Stats"]
    else:
        print("Snowflake input missing required keys: expected 'plan_json'+'stats' or 'Operations'+'Stats'.")
        return

    generate_combined_visualization_snowflake(
        plan_json,
        stats,
        summary=summary,
        output_filename=str(combined_out),
        tree_output_path=str(tree_out) if also_tree else None,
        dpi=dpi,
        width_per_op=width_per_op,
        min_width=min_width,
        height=height,
        graphviz_dpi=graphviz_dpi,
        normalize=normalize,
        also_tree=also_tree,
    )


def main():
    """
    CLI:
      --base-dir: base folder containing instance subfolders OR a single plan file path.
    """
    parser = argparse.ArgumentParser(
        description="Batch-generate Snowflake visualizations for all 'query_*_plan.txt' files."
    )
    parser.add_argument("--base-dir",
                        default="results",
                        help="Base folder with instance subfolders or a single plan file (default: 'results').")
    parser.add_argument("--output-subdir", default="visualizations",
                        help="Subfolder name to store images under each instance (default: 'visualizations').")
    parser.add_argument("--normalize", action="store_true",
                        help="Plot normalized percentage stacks instead of absolute seconds.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing output files.")
    parser.add_argument("--also-tree", action="store_true",
                        help="Also write a standalone operator tree PNG per plan.")

    # Quality controls
    parser.add_argument("--dpi", type=int, default=300,
                        help="Matplotlib output DPI for combined image (default: 300).")
    parser.add_argument("--graphviz-dpi", type=int, default=None,
                        help="Graphviz PNG DPI for the operator tree (default: same as --dpi).")
    parser.add_argument("--width-per-op", type=float, default=1.0,
                        help="Figure width in inches per operator (default: 1.0).")
    parser.add_argument("--min-width", type=float, default=15.0,
                        help="Minimum figure width in inches (default: 15).")
    parser.add_argument("--height", type=float, default=12.0,
                        help="Figure height in inches (default: 12).")

    args = parser.parse_args()

    path = Path(args.base_dir).expanduser()
    if path.is_file():
        process_snowflake_plan_file(
            file_path=path,
            output_subdir=args.output_subdir,
            overwrite=args.overwrite,
            normalize=args.normalize,
            dpi=args.dpi,
            width_per_op=args.width_per_op,
            min_width=args.min_width,
            height=args.height,
            graphviz_dpi=args.graphviz_dpi,
            also_tree=args.also_tree,
        )
    else:
        process_snowflake_plan_dir(
            base_dir=path,
            output_subdir=args.output_subdir,
            overwrite=args.overwrite,
            normalize=args.normalize,
            dpi=args.dpi,
            width_per_op=args.width_per_op,
            min_width=args.min_width,
            height=args.height,
            graphviz_dpi=args.graphviz_dpi,
            also_tree=args.also_tree,
        )


if __name__ == "__main__":
    main()
