from __future__ import annotations
import json
import math
import re
import sys
from typing import Any, Dict, List, Optional, Tuple


_PLAN_HEADER_RE = re.compile(r"\|\s*plan_type\s*\|\s*plan\s*\|", re.IGNORECASE)
_VERSION_RE = re.compile(r"DataFusion CLI v([^\s]+)")
_QUERY_TITLE_RE = re.compile(r"^\s*DataFusion\s+EXPLAIN\s+ANALYZE\s*-\s*(.*)\s*$", re.IGNORECASE)
_ELAPSED_RE = re.compile(r"Elapsed\s+([0-9]*\.?[0-9]+)\s+seconds\.", re.IGNORECASE)
_METRICS_RE = re.compile(r"metrics=\[(.*?)\]\s*$")
_KEYVAL_RE = re.compile(r"([A-Za-z0-9_]+)\s*=\s*([^,]+)")
_BAR_LINE_RE = re.compile(r"^\+[-+]+\+$")


def _to_seconds(value: str) -> Optional[float]:
    """Convert a duration string to seconds. Handles s, ms, µs/us, ns."""
    s = value.strip()
    # Common formats: "0.024 seconds" (handled elsewhere), "975.594µs", "32ns", "5.16ms", "3.301003949s"
    # Normalize micro symbol variants
    s = s.replace("μs", "µs")
    try:
        if s.endswith("s") and not s.endswith(("ms", "µs", "us", "ns")):
            return float(s[:-1])
        if s.endswith("ms"):
            return float(s[:-2]) / 1_000.0
        if s.endswith("µs") or s.endswith("us"):
            return float(s[:-2]) / 1_000_000.0
        if s.endswith("ns"):
            return float(s[:-2]) / 1_000_000_000.0
        # Plain number? assume seconds
        return float(s)
    except ValueError:
        return None


def _to_bytes(value: str) -> Optional[int]:
    """
    Convert a byte string to int bytes. Accepts plain ints or "<num> B".
    If unit-less, try int(). If float with B, round down.
    """
    s = value.strip()
    if s.endswith(" B"):
        num = s[:-2].strip()
        try:
            return int(float(num))
        except ValueError:
            return None
    # Could be a plain integer (e.g., bytes_scanned=54073125055)
    try:
        return int(s)
    except ValueError:
        return None


def _parse_metric_value(v: str) -> Any:
    """
    Parse a single metrics value into a sensible Python type:
    - durations → dict with seconds and raw string
    - bytes → int
    - ints/floats → number
    - else → raw string
    """
    raw = v.strip()

    # Try bytes
    b = _to_bytes(raw)
    if b is not None:
        return b

    # Try durations (with units)
    sec = _to_seconds(raw)
    if sec is not None and (raw.endswith(("s", "ms", "µs", "us", "ns"))):
        # keep both numeric seconds and the original string
        return {"_value": sec, "_unit": "s", "_raw": raw}

    # Try integer
    if re.fullmatch(r"[+-]?\d+", raw):
        try:
            return int(raw)
        except ValueError:
            pass

    # Try float
    if re.fullmatch(r"[+-]?\d+\.\d+", raw):
        try:
            return float(raw)
        except ValueError:
            pass

    # Fallback string
    return raw


def _normalize_metrics(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten duration dicts into *_s seconds; keep the base key as the original string.
    Example: {"elapsed_compute": {"_value": 0.001, "_unit": "s", "_raw": "1ms"}} →
             {"elapsed_compute": "1ms", "elapsed_compute_s": 0.001}
    """
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict) and v.get("_unit") == "s":
            out[f"{k}_s"] = v["_value"]
            out[k] = v.get("_raw", v["_value"])
        else:
            out[k] = v
    return out


def _split_plan_columns(line: str) -> Optional[Tuple[str, str]]:
    """Given a table line starting with '|', return (plan_type_text, plan_text)."""
    if not line.startswith("|"):
        return None
    # Find bar indices
    bar_idx = [i for i, ch in enumerate(line) if ch == "|"]
    if len(bar_idx) < 3:
        return None
    left = line[bar_idx[0] + 1 : bar_idx[1]].strip()
    # Keep plan text with leading spaces (indentation encodes the tree)
    plan = line[bar_idx[1] + 1 : bar_idx[-1]].rstrip("\n")
    # Strip a single leading space if table provides it; we still keep the internal plan indentation intact
    if plan.startswith(" "):
        plan = plan[1:]
    return left, plan


def _build_tree(plan_lines: List[Tuple[str, str]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Turn indented plan lines into a tree.
    plan_lines: list of (plan_type, plan_text) preserving order.
    """
    roots: List[Dict[str, Any]] = []
    stack: List[Dict[str, Any]] = []
    flat: List[Dict[str, Any]] = []

    current_plan_type: Optional[str] = None

    for col_left, plan_text in plan_lines:
        if col_left:
            current_plan_type = col_left

        text = plan_text.rstrip()
        if not text:
            continue

        # Determine depth by leading spaces in plan_text
        leading_spaces = len(text) - len(text.lstrip(" "))
        depth = leading_spaces // 2  # DataFusion uses 2-space indents

        node_line = text.lstrip(" ")
        # Extract metrics
        m = _METRICS_RE.search(node_line)
        metrics_raw = {}
        line_wo_metrics = node_line
        if m:
            metrics_blob = m.group(1)
            for kv in _KEYVAL_RE.finditer(metrics_blob):
                key, val = kv.group(1), kv.group(2)
                metrics_raw[key] = _parse_metric_value(val)
            line_wo_metrics = node_line[: m.start()].rstrip()

        # Node type and detail
        if ":" in line_wo_metrics:
            node_type, detail = line_wo_metrics.split(":", 1)
            node_type = node_type.strip()
            detail = detail.strip()
        else:
            node_type = line_wo_metrics.strip()
            detail = ""

        node: Dict[str, Any] = {
            "type": node_type,
            "detail": detail,
            "plan_type": current_plan_type,
            "metrics": _normalize_metrics(metrics_raw),
            "raw": node_line,
            "depth": depth,
            "children": [],
        }

        # Stack-based parenting
        if depth == 0:
            roots.append(node)
            stack = [node]
        else:
            # Ensure stack has parent at depth-1
            if depth > len(stack):
                # If indentation jumps, pad with last known
                parent = stack[-1]
            else:
                parent = stack[depth - 1]
                stack = stack[:depth]
            parent["children"].append(node)
            stack.append(node)

        flat.append(node)

    return roots, flat


def parse_datafusion_explain_text(text: str) -> Dict[str, Any]:
    """Parse the full DataFusion EXPLAIN ANALYZE text into structured data."""
    lines = text.splitlines()

    # Metadata
    query_title = None
    version = None
    for ln in lines[:10]:
        mt = _QUERY_TITLE_RE.search(ln)
        if mt and not query_title:
            query_title = mt.group(1).strip()
        mv = _VERSION_RE.search(ln)
        if mv and not version:
            version = mv.group(1).strip()

    # Elapsed pings (outside the plan table)
    elapsed_pings: List[float] = []
    for ln in lines:
        me = _ELAPSED_RE.search(ln)
        if me:
            try:
                elapsed_pings.append(float(me.group(1)))
            except ValueError:
                pass

    # Locate plan table
    plan_start_idx = None
    for i, ln in enumerate(lines):
        if _PLAN_HEADER_RE.search(ln):
            # The useful rows start after the next border line
            plan_start_idx = i + 1
            break

    plan_lines: List[Tuple[str, str]] = []
    if plan_start_idx is not None:
        # Skip the header separator line (should be +----+----+)
        i = plan_start_idx
        # Advance to the line after the border
        while i < len(lines) and not lines[i].startswith("|"):
            i += 1
        # Collect until we hit the closing border line
        while i < len(lines):
            ln = lines[i]
            if _BAR_LINE_RE.match(ln):
                break
            if ln.startswith("|"):
                cols = _split_plan_columns(ln)
                if cols:
                    plan_lines.append(cols)
            i += 1

    roots, flat = _build_tree(plan_lines)

    return {
        "query_title": query_title,
        "cli_version": version,
        "elapsed_pings_s": elapsed_pings,
        "plan_roots": roots,
        "nodes_flat": flat,
    }
