from __future__ import annotations

import os
import html
import tempfile
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from graphviz import Digraph

from pathlib import Path
import argparse
import shutil


def _pick(m: Dict[str, Any], key: str, default=None):
    # Our parser may provide both "foo" and "foo_s". Prefer explicit "_s", fall back to base.
    if key in m:
        return m[key]
    if key.endswith("_s"):
        base = key[:-2]
        if base in m and isinstance(m[base], (int, float)):
            return m[base]
    return default


def _assign_node_ids(roots: List[Dict[str, Any]]) -> Dict[int, int]:
    """Stable 0..N-1 ids in DFS order."""
    ids: Dict[int, int] = {}
    counter = 0
    stack = list(roots)[::-1]
    while stack:
        n = stack.pop()
        nid = id(n)
        if nid not in ids:
            ids[nid] = counter
            counter += 1
        for c in n.get("children", []):
            stack.append(c)
    return ids


def _collect_nodes(roots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    stack = list(roots)[::-1]
    while stack:
        n = stack.pop()
        out.append(n)
        for c in n.get("children", []):
            stack.append(c)
    return out


def _escape(s: str) -> str:
    return html.escape(s, quote=False)


def _render_datafusion_tree_image(parsed: Dict[str, Any], graphviz_dpi: int = 192) -> str:
    """
    Renders DataFusion operator tree(s) to a temporary PNG and returns the path.
    Expects `parsed` from parse_datafusion_explain_text().
    """
    roots = parsed.get("plan_roots") or []
    if not roots:
        raise ValueError("DataFusion: 'plan_roots' missing or empty in parsed plan.")

    title = parsed.get("query_title") or "Query Operator Tree"
    ver = parsed.get("cli_version")
    if ver:
        title = f"{title} (CLI v{ver})"

    dot = Digraph(comment='DataFusion Plan', format='png')
    # Higher resolution PNG from Graphviz
    dot.attr('graph', dpi=str(graphviz_dpi), ranksep='0.5', nodesep='0.25')
    dot.attr(rankdir='BT', labelloc='t', label=title)
    dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue')
    dot.attr('edge', dir='back')

    ids = _assign_node_ids(roots)
    all_nodes = _collect_nodes(roots)

    for n in all_nodes:
        nid = ids[id(n)]
        t = n.get("type", "?")
        detail = (n.get("detail") or "").strip()
        if len(detail) > 200:
            detail = detail[:200] + "…"

        label = f"<b>O{nid}: {_escape(t)}</b>"
        if detail:
            label += f"<br/>{_escape(detail)}"

        m = n.get("metrics", {}) or {}
        metrics_bits = []
        rows = _pick(m, "output_rows")
        if rows is not None:
            metrics_bits.append(f"rows={int(rows):,}")
        elapsed = _pick(m, "elapsed_compute_s")
        if elapsed is None:
            elapsed = _pick(m, "time_elapsed_processing_s")
        if elapsed is not None:
            if elapsed >= 1:
                metrics_bits.append(f"elapsed={elapsed:.3f}s")
            else:
                ms = elapsed * 1_000
                if ms >= 1:
                    metrics_bits.append(f"elapsed={ms:.3f}ms")
                else:
                    metrics_bits.append(f"elapsed={ms*1000:.1f}µs")
        if metrics_bits:
            label += f"<br/><font point-size='10' color='#333333'>{' • '.join(metrics_bits)}</font>"

        dot.node(str(nid), f"<{label}>")

    for parent in all_nodes:
        pid = ids[id(parent)]
        for child in parent.get("children", []):
            cid = ids[id(child)]
            dot.edge(str(cid), str(pid))

    with tempfile.NamedTemporaryFile(suffix='.gv', delete=False) as tmp:
        temp_tree_path = tmp.name
    tree_image_path = dot.render(temp_tree_path, cleanup=True)
    return tree_image_path


_COMPONENT_MAP: List[Tuple[str, str]] = [
    ("compute", "elapsed_compute_s"),
    ("fetch", "fetch_time_s"),
    ("repartition", "repartition_time_s"),
    ("send", "send_time_s"),
    ("scan_total", "time_elapsed_scanning_total_s"),
    ("processing", "time_elapsed_processing_s"),
    ("open", "time_elapsed_opening_s"),
    ("metadata", "metadata_load_time_s"),
    ("bloom_eval", "bloom_filter_eval_time_s"),
    ("page_index_eval", "page_index_eval_time_s"),
    ("row_pushdown_eval", "row_pushdown_eval_time_s"),
    ("stats_eval", "statistics_eval_time_s"),
]

def _extract_breakdown_rows(parsed: Dict[str, Any]) -> Tuple[List[str], Dict[str, List[float]], List[float]]:
    """
    Build labels, a dict of component -> list of absolute seconds per node, and totals per node.
    """
    roots = parsed.get("plan_roots") or []
    if not roots:
        return [], {}, []

    ids = _assign_node_ids(roots)
    nodes = _collect_nodes(roots)

    labels: List[str] = []
    comp_values_abs: Dict[str, List[float]] = {c: [] for c, _ in _COMPONENT_MAP}
    totals: List[float] = []

    for n in nodes:
        nid = ids[id(n)]
        t = n.get("type", "?")
        labels.append(f"O{nid}: {t}")

        m = (n.get("metrics") or {})
        # Absolute seconds for each component (or 0 if missing)
        vals = {c: float(_pick(m, key, 0.0) or 0.0) for c, key in _COMPONENT_MAP}
        total = sum(vals.values())
        totals.append(total)

        for c, _ in _COMPONENT_MAP:
            comp_values_abs[c].append(vals[c])

    return labels, comp_values_abs, totals


def _plot_datafusion_execution_breakdown(ax, parsed: Dict[str, Any], normalize: bool = False):
    """
    Plots execution breakdown as stacked bars per operator.
    - normalize=False: stack absolute seconds (varied column heights)
    - normalize=True:  stack percentages (all columns height=100)
    """
    labels, comp_abs, totals = _extract_breakdown_rows(parsed)
    if not labels:
        ax.text(0.5, 0.5, "No execution timing metrics found", ha='center', va='center')
        ax.axis('off')
        return

    components_present = [
        c for c in (c for c, _ in _COMPONENT_MAP)
        if any(v > 0 for v in comp_abs.get(c, []))
    ]

    # Ensure consistent length
    n = len(labels)
    for c in components_present:
        if len(comp_abs[c]) < n:
            comp_abs[c].extend([0.0] * (n - len(comp_abs[c])))

    bottom = np.zeros(n, dtype=float)
    x = np.arange(n)

    if normalize:
        # Convert to percentages, keeping zeros safe
        comp_vals = {
            c: np.array([(v / t * 100.0) if t > 0 else 0.0 for v, t in zip(comp_abs[c], totals)], dtype=float)
            for c in components_present
        }
        y_label = "Execution Time (%)"
        title = "Query Execution Time Breakdown by Operator (normalized)"
    else:
        comp_vals = {c: np.array(comp_abs[c], dtype=float) for c in components_present}
        y_label = "Execution Time (s)"
        title = "Query Execution Time Breakdown by Operator"

    for c in components_present:
        vals = comp_vals[c]
        ax.bar(x, vals, bottom=bottom, label=c)
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_ylabel(y_label)
    ax.set_title(title)
    ax.legend(loc='upper right')


def process_all_datafusion_plans(base_dir: str,
                                 output_subdir: str = "visualizations",
                                 normalize: bool = False,
                                 overwrite: bool = False,
                                 also_tree: bool = False,
                                 dpi: int = 200,
                                 width_per_op: float = 0.7,
                                 min_width: float = 15.0,
                                 height: float = 12.0,
                                 graphviz_dpi: Optional[int] = None) -> None:
    """
    Walk `base_dir` recursively, find files named `query_*_plan.txt`, and render outputs
    into `<instance>/<output_subdir>/` as:
      - `query_*_analysis.png` (combined figure)
      - optionally `query_*_tree.png` (standalone operator tree)
    """
    from parse_datafusion_output import parse_datafusion_explain_text

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

            stem = plan_path.stem
            combined_out = viz_dir / (stem.replace("_plan", "_analysis") + ".png")
            tree_out = viz_dir / (stem.replace("_plan", "_tree") + ".png")

            if combined_out.exists() and not overwrite:
                print(f"Skip (exists): {combined_out}")
                skipped += 1
                continue

            txt = plan_path.read_text(encoding="utf-8")
            parsed = parse_datafusion_explain_text(txt)

            # Combined figure
            generate_combined_visualization_datafusion(
                parsed,
                output_filename=str(combined_out),
                normalize=normalize,
                dpi=dpi,
                width_per_op=width_per_op,
                min_width=min_width,
                height=height,
                graphviz_dpi=graphviz_dpi
            )

            # Optional standalone tree
            if also_tree:
                tmp_tree_png = _render_datafusion_tree_image_public(parsed, graphviz_dpi=(graphviz_dpi or dpi))
                try:
                    shutil.move(tmp_tree_png, str(tree_out))
                finally:
                    if os.path.exists(tmp_tree_png):
                        os.remove(tmp_tree_png)

            processed += 1
        except Exception as e:
            failures.append((plan_path, str(e)))

    print(f"Done. Processed={processed}, Skipped={skipped}, Failures={len(failures)}")
    if failures:
        for p, err in failures[:20]:
            print(f"  Fail: {p} -> {err}")
        if len(failures) > 20:
            print(f"  ... and {len(failures) - 20} more")


def generate_combined_visualization_datafusion(parsed: Dict[str, Any],
                                              output_filename: str = "query_analysis_datafusion.png",
                                              normalize: bool = False,
                                              dpi: int = 200,
                                              width_per_op: float = 0.7,
                                              min_width: float = 15.0,
                                              height: float = 12.0,
                                              graphviz_dpi: Optional[int] = None):
    """
    Combined DataFusion visualization: stacked execution chart + operator tree.
    `parsed` must come from parse_datafusion_explain_text().
    - dpi: Matplotlib output DPI.
    - width_per_op: inches per operator for dynamic width scaling.
    - min_width: minimum figure width in inches.
    - height: figure height in inches.
    - graphviz_dpi: PNG DPI for Graphviz-rendered tree (defaults to `dpi` if not set).
    """
    roots = parsed.get("plan_roots") or []
    n_ops = len(_collect_nodes(roots)) if roots else 0
    fig_width = max(min_width, max(1, n_ops) * width_per_op)

    tree_image_path = _render_datafusion_tree_image(parsed, graphviz_dpi=(graphviz_dpi or dpi))

    fig = plt.figure(figsize=(fig_width, height))

    # Top chart
    ax1 = plt.subplot(2, 1, 1)
    _plot_datafusion_execution_breakdown(ax1, parsed, normalize=normalize)

    # Bottom tree
    ax2 = plt.subplot(2, 1, 2)
    tree_img = mpimg.imread(tree_image_path)
    ax2.imshow(tree_img)
    ax2.axis('off')
    ax2.set_title('Query Operator Tree')

    plt.tight_layout()
    plt.savefig(output_filename, dpi=dpi)
    print(f"Successfully generated combined query analysis: {output_filename}")
    plt.close(fig)

    if os.path.exists(tree_image_path):
        os.remove(tree_image_path)


# Public alias mirroring your Snowflake function name pattern
def _render_datafusion_tree_image_public(parsed: Dict[str, Any], graphviz_dpi: int = 192) -> str:
    return _render_datafusion_tree_image(parsed, graphviz_dpi=graphviz_dpi)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch-generate DataFusion visualizations for all 'query_*_plan.txt' files."
    )
    parser.add_argument("--base-dir",
                        default="results-parquet",
                        help="Base folder with instance subfolders (default: 'results-parquet').")
    parser.add_argument("--output-subdir", default="visualizations",
                        help="Subfolder name to store images under each instance (default: 'visualizations').")
    parser.add_argument("--normalize", action="store_true",
                        help="Plot normalized percentage stacks instead of absolute seconds.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing output files.")
    parser.add_argument("--also-tree", action="store_true",
                        help="Also write a standalone operator tree PNG per plan.")

    # New quality controls
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
    process_all_datafusion_plans(
        base_dir=args.base_dir,
        output_subdir=args.output_subdir,
        normalize=args.normalize,
        overwrite=args.overwrite,
        also_tree=args.also_tree,
        dpi=args.dpi,
        width_per_op=args.width_per_op,
        min_width=args.min_width,
        height=args.height,
        graphviz_dpi=args.graphviz_dpi,
    )