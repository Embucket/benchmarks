"""
Utility script for comparing Snowflake-compatible tables (Snowflake vs. Embucket).

The script supports:
 1. Total row count comparison.
 2. Row-by-row data comparison (in-memory pandas).
 3. Hash-based comparison with optional CSV/Parquet exports.

Example (row-level compare in-memory):
    python data_diff.py \
        --snowflake-table SNOW_DB.PUBLIC.SOURCE_TABLE \
        --embucket-table EMBUCKET_DB.PUBLIC.TARGET_TABLE \
        --key-columns ID,EVENT_TIMESTAMP

Example (hash export + comparison):
    python data_diff.py \
        --snowflake-table SNOW_DB.PUBLIC.SOURCE_TABLE \
        --embucket-table EMBUCKET_DB.PUBLIC.TARGET_TABLE \
        --key-columns ID \
        --hash-compare \
        --export-format parquet \
        --export-dir ./exports \
        --hash-columns ID,EVENT_TIMESTAMP,COL_A,COL_B

"""
from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import pandas as pd
import snowflake.connector
import sys
import warnings

# Silence noisy pandas DBAPI warning when using Snowflake connector directly
warnings.filterwarnings(
    "ignore",
    message=r"^pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)


# Predefined Snowplow tables to compare (schema.table; database inferred from env)
DEFAULT_SNOWPLOW_TABLES: List[str] = [
    # manifest
    "public_snowplow_manifest.snowplow_web_base_quarantined_sessions",
    "public_snowplow_manifest.snowplow_web_incremental_manifest",
    "public_snowplow_manifest.snowplow_web_base_sessions_lifecycle_manifest",
    # scratch
    "public_scratch.snowplow_web_base_new_event_limits",
    "public_scratch.snowplow_web_base_sessions_this_run",
    "public_scratch.snowplow_web_base_events_this_run",
    "public_scratch.snowplow_web_consent_events_this_run",
    "public_scratch.snowplow_web_pv_engaged_time",
    "public_scratch.snowplow_web_pv_scroll_depth",
    "public_scratch.snowplow_web_sessions_this_run",
    "public_scratch.snowplow_web_vital_events_this_run",
    "public_scratch.snowplow_web_page_views_this_run",
    "public_scratch.snowplow_web_vitals_this_run",
    "public_scratch.snowplow_web_users_sessions_this_run",
    "public_scratch.snowplow_web_users_aggs",
    "public_scratch.snowplow_web_users_lasts",
    "public_scratch.snowplow_web_users_this_run",
    # derived
    "public_derived.snowplow_web_user_mapping",
    "public_derived.snowplow_web_consent_log",
    "public_derived.snowplow_web_consent_cmp_stats",
    "public_derived.snowplow_web_consent_versions",
    "public_derived.snowplow_web_sessions",
    "public_derived.snowplow_web_page_views",
    "public_derived.snowplow_web_consent_users",
    "public_derived.snowplow_web_vitals",
    "public_derived.snowplow_web_consent_scope_status",
    "public_derived.snowplow_web_consent_totals",
    "public_derived.snowplow_web_vital_measurements",
    "public_derived.snowplow_web_users",
]

@dataclass
class ConnectionConfig:
    """Holds Snowflake-compatible connection parameters."""

    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    protocol: Optional[str] = None

    def validate(self, label: str) -> None:
        missing = [
            field_name
            for field_name, value in self.__dict__.items()
            if field_name not in ("role", "host", "port", "protocol") and (value is None or value == "")
        ]
        if missing:
            raise ValueError(
                f"Missing {label} connection parameters: {', '.join(sorted(missing))}"
            )


HARDCODED_CREDENTIALS = {
    "snowflake": {
        # All values intentionally None to force reading from .env / environment / CLI.
        "user": None,
        "password": None,
        "account": None,
        "warehouse": None,
        "database": None,
        "schema": None,
        "role": None,
        "host": None,
        "port": None,
        "protocol": None,
    },
    "embucket": {
        "user": None,
        "password": None,
        "account": None,
        "warehouse": None,
        "database": None,
        "schema": None,
        "role": None,
        "host": None,
        "port": None,
        "protocol": None,
    },
}


def create_connection(config: ConnectionConfig):
    """Create a new Snowflake-compatible connection from config."""
    connect_kwargs = {
        "user": config.user,
        "password": config.password,
        "account": config.account,
        "warehouse": config.warehouse,
        "database": config.database,
        "schema": config.schema,
        "role": config.role,
    }
    if config.host:
        connect_kwargs["host"] = config.host
    if config.port:
        connect_kwargs["port"] = int(config.port)
    if config.protocol:
        connect_kwargs["protocol"] = config.protocol

    conn = snowflake.connector.connect(**connect_kwargs)
    return conn


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def split_table_identifier(table_identifier: str) -> Tuple[str, str, str]:
    parts = [part.strip() for part in table_identifier.split(".")]
    if len(parts) != 3:
        raise ValueError(
            f"Expected fully-qualified identifier DATABASE.SCHEMA.TABLE, got: {table_identifier}"
        )
    database, schema, table = (part.strip('"') for part in parts)
    return database, schema, table


def qualify_table_name(
    table_identifier: str, default_database: Optional[str], default_schema: Optional[str]
) -> str:
    """
    Allow users to pass TABLE, SCHEMA.TABLE, or DATABASE.SCHEMA.TABLE.
    Fill missing pieces from provided defaults (usually env-derived).
    """
    parts = [p.strip() for p in table_identifier.split(".") if p.strip()]
    if len(parts) == 3:
        return table_identifier
    if len(parts) == 2:
        if not default_database:
            raise ValueError(
                f"Cannot qualify '{table_identifier}' without a default database. "
                "Set SNOWFLAKE_DATABASE/EMBUCKET_DATABASE or pass a fully-qualified name."
            )
        return f"{default_database}.{parts[0]}.{parts[1]}"
    if len(parts) == 1:
        if not default_database or not default_schema:
            raise ValueError(
                f"Cannot qualify '{table_identifier}' without default database and schema. "
                "Set *_DATABASE and *_SCHEMA env vars or pass a fully-qualified name."
            )
        return f"{default_database}.{default_schema}.{parts[0]}"
    raise ValueError(f"Invalid table identifier: {table_identifier}")


def get_row_count(connection, table_identifier: str) -> int:
    """Return the total number of rows in the given table."""
    query = f"SELECT COUNT(*) AS ROW_COUNT FROM {table_identifier}"
    with connection.cursor() as cursor:
        cursor.execute(query)
        (count,) = cursor.fetchone()
    return int(count)


def fetch_table(connection, table_identifier: str) -> pd.DataFrame:
    """Fetch the full table into a pandas DataFrame."""
    query = f"SELECT * FROM {table_identifier}"
    df = pd.read_sql(query, connection)
    return df


ENV_LINE_PATTERN = re.compile(r"^\s*(?:export\s+)?([^=\s]+)\s*=\s*(.*)\s*$")
ENV_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::-[^}]*)?\}")
ENV_DEFAULT_PATTERN = re.compile(r"\$\{([^}:]+)(?::-([^}]*))\}")


def expand_env_value(value: str, env_lookup: Dict[str, str]) -> str:
    """
    Expand ${VAR} and ${VAR:-default} constructs within the value using env_lookup/os.environ.
    """

    def replace_default(match: re.Match) -> str:
        var_name = match.group(1)
        default_value = match.group(2)
        return env_lookup.get(var_name) or os.environ.get(var_name) or (default_value or "")

    def replace_simple(match: re.Match) -> str:
        var_name = match.group(1)
        return env_lookup.get(var_name) or os.environ.get(var_name) or ""

    value = ENV_DEFAULT_PATTERN.sub(replace_default, value)
    value = ENV_VAR_PATTERN.sub(replace_simple, value)
    return value


def load_env_file(path: Optional[Path]) -> Dict[str, str]:
    env_values: Dict[str, str] = {}
    if path is None:
        return env_values

    def read_contents(target: Path) -> Optional[str]:
        try:
            return target.read_text()
        except FileNotFoundError:
            return None

    contents = read_contents(path)
    if contents is None and path.name == ".env":
        template_path = path.with_name("env.template")
        contents = read_contents(template_path)
    if contents is None:
        return env_values

    for line in contents.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = ENV_LINE_PATTERN.match(line)
        if not match:
            continue
        key, raw_value = match.groups()
        value = raw_value.strip().strip('"').strip("'")
        value = expand_env_value(value, {**env_values, **os.environ})
        env_values[key] = value

    return env_values


def merge_env_sources(primary: Optional[Path], fallback: Optional[Path]) -> Dict[str, str]:
    combined: Dict[str, str] = {}
    for candidate in [primary, fallback]:
        if candidate is None:
            continue
        combined.update(load_env_file(candidate))
    return combined


def get_table_columns(connection, table_identifier: str) -> List[str]:
    """
    Retrieve column names for a table via information_schema ordered by ordinal position.
    """
    database, schema, table = split_table_identifier(table_identifier)
    database_quoted = quote_identifier(database)
    query = f"""
        SELECT column_name
        FROM {database_quoted}.information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema.upper(), table.upper()))
        rows = cursor.fetchall()
    return [row[0] for row in rows]


def get_table_columns_via_select(connection, table_identifier: str) -> List[str]:
    """
    Retrieve column names by selecting zero rows (works for systems without information_schema).
    """
    probe_sql = f"SELECT * FROM {table_identifier} LIMIT 0"
    df = pd.read_sql(probe_sql, connection)
    return list(df.columns)


def iterate_table_chunks(
    connection,
    table_identifier: str,
    selected_columns: Optional[Sequence[str]],
    chunksize: Optional[int],
) -> Iterator[pd.DataFrame]:
    if selected_columns:
        column_list = ", ".join(selected_columns)
    else:
        column_list = "*"
    query = f"SELECT {column_list} FROM {table_identifier}"
    if chunksize:
        for chunk in pd.read_sql(query, connection, chunksize=chunksize):
            yield chunk
    else:
        yield pd.read_sql(query, connection)


def normalize_columns(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ensure both dataframes share the same column ordering.

    Missing columns are added with NaN values so that comparisons can still run.
    """
    all_columns = sorted(set(df1.columns).union(df2.columns))
    df1_aligned = df1.reindex(columns=all_columns)
    df2_aligned = df2.reindex(columns=all_columns)
    return df1_aligned, df2_aligned


def normalize_key_series(series: pd.Series) -> pd.Series:
    """Normalize key values for robust joins: string-cast, strip whitespace, lowercase."""
    return series.astype(str).str.strip().str.lower()


def normalize_key_index(index: pd.Index) -> pd.Index:
    """Normalize Index or MultiIndex by applying normalize_key_series to each level."""
    if isinstance(index, pd.MultiIndex):
        normalized_levels = [
            normalize_key_series(index.get_level_values(i)) for i in range(index.nlevels)
        ]
        return pd.MultiIndex.from_arrays(normalized_levels, names=index.names)
    # Regular Index
    return normalize_key_series(pd.Index(index))


def compare_tables(
    snowflake_conn,
    embucket_conn,
    snowflake_table: str,
    embucket_table: str,
    key_columns: Sequence[str],
) -> None:
    """Perform row count and row-by-row comparisons and print summaries."""
    print("=== Row Count Comparison ===")
    snowflake_count = get_row_count(snowflake_conn, snowflake_table)
    embucket_count = get_row_count(embucket_conn, embucket_table)
    print(f"{snowflake_table}: {snowflake_count:,} rows")
    print(f"{embucket_table}: {embucket_count:,} rows")
    if snowflake_count == embucket_count:
        print("Row counts match.")
    else:
        print(f"Row counts differ by {snowflake_count - embucket_count:,} rows.")

    print("\n=== Row-Level Comparison ===")
    print("Fetching data... (ensure the tables are reasonably sized)")
    snowflake_df = fetch_table(snowflake_conn, snowflake_table)
    embucket_df = fetch_table(embucket_conn, embucket_table)

    snowflake_df, embucket_df = normalize_columns(snowflake_df, embucket_df)

    if not key_columns:
        raise ValueError(
            "At least one key column is required for row-level comparison. "
            "Use --key-columns to specify them."
        )

    resolved_key_columns = resolve_columns_case_insensitive(
        key_columns,
        snowflake_df.columns,
        f"Snowflake table {snowflake_table}",
        "Key columns",
    )
    resolve_columns_case_insensitive(
        key_columns,
        embucket_df.columns,
        f"Embucket table {embucket_table}",
        "Key columns",
    )

    # Normalize key columns on both sides before indexing
    for key_col in resolved_key_columns:
        snowflake_df[key_col] = normalize_key_series(snowflake_df[key_col])
        embucket_df[key_col] = normalize_key_series(embucket_df[key_col])

    snowflake_df.set_index(list(resolved_key_columns), inplace=True)
    embucket_df.set_index(list(resolved_key_columns), inplace=True)

    # Normalize resulting indexes too (covers any upstream oddities)
    snowflake_df.index = normalize_key_index(snowflake_df.index)
    embucket_df.index = normalize_key_index(embucket_df.index)

    snowflake_df.sort_index(inplace=True)
    embucket_df.sort_index(inplace=True)

    non_key_columns = [
        column for column in snowflake_df.columns if column not in resolved_key_columns
    ]

    # Align indexes to capture missing rows on either side.
    unified_index = snowflake_df.index.union(embucket_df.index)
    # Use reindex to allow labels missing on either side without raising KeyError
    snowflake_aligned = snowflake_df.reindex(unified_index)[non_key_columns]
    embucket_aligned = embucket_df.reindex(unified_index)[non_key_columns]

    differences = snowflake_aligned.compare(embucket_aligned, align_axis=0)

    missing_in_embucket = unified_index.difference(embucket_df.index)
    missing_in_snowflake = unified_index.difference(snowflake_df.index)

    print(f"Rows present in Snowflake but missing in Embucket: {len(missing_in_embucket):,}")
    print(f"Rows present in Embucket but missing in Snowflake: {len(missing_in_snowflake):,}")
    print(f"Rows with differing values: {differences.index.nunique():,}")

    if not differences.empty:
        print("\nSample differences (up to 10 rows):")
        sample = differences.groupby(level=0).head(10)
        print(sample)
    else:
        print("No row-level value differences detected.")


def compute_row_hashes(
    df: pd.DataFrame, columns: Sequence[str], algorithm: str, null_placeholder: str = "<NULL>"
) -> pd.Series:
    """
    Compute deterministic row-level hashes for the specified columns.

    The resulting series aligns with `df` and contains hex digests.
    """
    if not columns:
        raise ValueError("No columns provided to compute_row_hashes.")

    normalized = df.loc[:, list(columns)].copy()
    normalized = normalized.fillna(null_placeholder)
    normalized = normalized.astype(str)
    concatenated = normalized.apply(lambda row: "\u001f".join(row.values), axis=1)

    def digest(value: str) -> str:
        hasher = hashlib.new(algorithm)
        hasher.update(value.encode("utf-8"))
        return hasher.hexdigest()

    return concatenated.map(digest)


def count_row_hashes_for_table(
    connection,
    table_identifier: str,
    hash_columns: Optional[Sequence[str]],
    algorithm: str,
    chunksize: Optional[int],
    null_placeholder: str = "<NULL>",
) -> Tuple[Dict[str, int], int, Sequence[str]]:
    """
    Stream a table and return:
      - hash -> count mapping for rows (multiset counts)
      - total rows processed
      - resolved hash columns used
    """
    # If columns not specified, fetch all via a zero-row select
    if not hash_columns:
        try:
            all_cols = get_table_columns(connection, table_identifier)
        except Exception:
            all_cols = get_table_columns_via_select(connection, table_identifier)
        resolved_hash_columns = list(all_cols)
    else:
        try:
            all_cols = get_table_columns(connection, table_identifier)
        except Exception:
            all_cols = get_table_columns_via_select(connection, table_identifier)
        resolved_hash_columns = resolve_columns_case_insensitive(
            hash_columns, all_cols, f"table {table_identifier}", "Hash columns"
        )

    selected_columns_order = list(resolved_hash_columns)
    counts: Dict[str, int] = {}
    total_rows = 0

    for chunk in iterate_table_chunks(connection, table_identifier, selected_columns_order, chunksize):
        chunk = chunk.reset_index(drop=True)
        chunk.columns = [str(c) for c in chunk.columns]
        # Ensure all required columns exist
        _ = resolve_columns_case_insensitive(
            resolved_hash_columns, chunk.columns, f"table {table_identifier}", "Hash columns"
        )
        row_hash = compute_row_hashes(chunk, resolved_hash_columns, algorithm, null_placeholder)
        vc = row_hash.value_counts()
        for h, c in vc.items():
            counts[h] = counts.get(h, 0) + int(c)
        total_rows += int(len(chunk))

    return counts, total_rows, resolved_hash_columns


def _value_counts_for_column(
    connection,
    table_identifier: str,
    column: str,
    chunksize: Optional[int],
    null_placeholder: str = "<NULL>",
) -> Dict[str, int]:
    """Stream a single column and accumulate value counts as strings (trimmed).

    Resolves the requested column name case-insensitively against the table's columns.
    """
    counts: Dict[str, int] = {}
    # Resolve actual column name using a zero-row select to get columns list
    try:
        available_columns = get_table_columns_via_select(connection, table_identifier)
    except Exception:
        available_columns = get_table_columns(connection, table_identifier)
    resolved = resolve_columns_case_insensitive([column], available_columns, f"table {table_identifier}", "Column")[0]

    for chunk in iterate_table_chunks(connection, table_identifier, [resolved], chunksize):
        series = chunk[resolved].fillna(null_placeholder).astype(str).str.strip()
        vc = series.value_counts()
        for v, c in vc.items():
            counts[str(v)] = counts.get(str(v), 0) + int(c)
    return counts


def no_key_hash_compare(
    snowflake_conn,
    embucket_conn,
    snowflake_table: str,
    embucket_table: str,
    hash_columns: Optional[Sequence[str]],
    algorithm: str,
    chunksize: Optional[int],
    export_format: Optional[str],
    export_dir: Optional[Path],
    include_full_row: bool,
    null_placeholder: str,
    summary_format: str,
    only_summary: bool,
    show_examples: bool = False,
    examples_limit: int = 3,
    return_row: bool = False,
) -> Optional[Tuple[List[str], List[str], List[Tuple[str, List[Tuple[str, int, int]]]]]]:
    # Column sets for summary
    snowflake_columns = get_table_columns(snowflake_conn, snowflake_table)
    try:
        embucket_columns = get_table_columns_via_select(embucket_conn, embucket_table)
    except Exception:
        embucket_columns = []

    if not only_summary:
        print("\n=== Hash-Based Comparison (no key) ===")
        print(f"Hash algorithm: {algorithm.upper()}")
        if hash_columns:
            print(f"Hash columns (requested): {', '.join(hash_columns)}")
        else:
            print("Hash columns: ALL columns")

    sf_counts, sf_total, resolved_hash_cols = count_row_hashes_for_table(
        snowflake_conn,
        snowflake_table,
        hash_columns,
        algorithm,
        chunksize,
        null_placeholder,
    )
    eb_counts, eb_total, _ = count_row_hashes_for_table(
        embucket_conn,
        embucket_table,
        hash_columns if hash_columns else resolved_hash_cols,
        algorithm,
        chunksize,
        null_placeholder,
    )

    all_hashes = set(sf_counts.keys()).union(eb_counts.keys())
    extra_sf = 0
    extra_eb = 0
    for h in all_hashes:
        a = sf_counts.get(h, 0)
        b = eb_counts.get(h, 0)
        if a > b:
            extra_sf += (a - b)
        elif b > a:
            extra_eb += (b - a)

    # Per-column mismatch count (data-level, order-agnostic)
    columns_to_check = resolved_hash_cols
    col_mismatch = 0
    mismatching_columns: List[str] = []
    column_examples: List[Tuple[str, List[Tuple[str, int, int]]]] = []
    for col in columns_to_check:
        try:
            sf_vc = _value_counts_for_column(snowflake_conn, snowflake_table, col, chunksize, null_placeholder)
            eb_vc = _value_counts_for_column(embucket_conn, embucket_table, col, chunksize, null_placeholder)
            if sf_vc != eb_vc:
                col_mismatch += 1
                if show_examples and len(mismatching_columns) < max(1, examples_limit):
                    mismatching_columns.append(col)
                    # build sample deltas
                    sample: List[Tuple[str, int, int]] = []
                    keys = set(sf_vc.keys()) | set(eb_vc.keys())
                    for v in keys:
                        a = sf_vc.get(v, 0)
                        b = eb_vc.get(v, 0)
                        if a != b:
                            sample.append((str(v), int(a), int(b)))
                            if len(sample) >= examples_limit:
                                break
                    column_examples.append((col, sample))
        except Exception:
            # If either side fails to read column, count as mismatch
            col_mismatch += 1
            if show_examples and len(mismatching_columns) < max(1, examples_limit):
                mismatching_columns.append(col)

    # Summary metrics
    try:
        total_columns = len(snowflake_columns)
        snow_cols_lower = {c.lower() for c in snowflake_columns}
        emb_cols_lower = {c.lower() for c in embucket_columns}
        total_columns_match = len(snow_cols_lower.intersection(emb_cols_lower))
        total_columns_not_match = max(total_columns - total_columns_match, 0)
    except Exception:
        total_columns = 0
        total_columns_match = 0
        total_columns_not_match = 0

    # total_rows as max for visibility
    total_rows_union = max(sf_total, eb_total)

    # Render summary table
    headers = [
        "table_name",
        "total_rows",
        "rows_missmatch",
        "total_column",
        "col_mismatch",
        "extra_in_sf",
        "extra_in_em",
    ]
    try:
        _, _, table_only = split_table_identifier(snowflake_table)
    except Exception:
        table_only = snowflake_table
    # rows_missmatch defined as absolute difference in total rows
    rows_missmatch = abs(int(sf_total) - int(eb_total))
    row = [
        table_only,
        str(total_rows_union),
        str(rows_missmatch),
        str(total_columns),
        str(col_mismatch),
        str(extra_sf),
        str(extra_eb),
    ]

    if return_row:
        return headers, row, column_examples
    else:
        fmt = summary_format or "markdown"
        if not only_summary:
            print("\nSummary:")
        if fmt == "csv":
            print(",".join(headers))
            print(",".join(row))
        elif fmt == "tsv":
            print("\t".join(headers))
            print("\t".join(row))
        elif fmt == "box":
            widths = [max(len(h), len(r)) for h, r in zip(headers, row)]
            def line(sep_left="+", sep_mid="+", sep_right="+", fill="-"):
                return sep_left + sep_mid.join(fill * (w + 2) for w in widths) + sep_right
            def render(values, is_header=False):
                cells = []
                for i, (v, w) in enumerate(zip(values, widths)):
                    cell = " " + (v.ljust(w) if i == 0 or is_header else v.rjust(w)) + " "
                    cells.append(cell)
                return "|" + "|".join(cells) + "|"
            print(line())
            print(render(headers, is_header=True))
            print(line("+", "+", "+", "-"))
            print(render(row))
            print(line())
        elif fmt == "plain":
            widths = [max(len(h), len(r)) for h, r in zip(headers, row)]
            def pad(values):
                out = []
                for i, (v, w) in enumerate(zip(values, widths)):
                    out.append(v.ljust(w) if i == 0 else v.rjust(w))
                return "  ".join(out)
            print(pad(headers))
            print(pad(row))
        else:
            print(" | ".join(headers))
            print(" | ".join(["---"] + ["---:" for _ in headers[1:]]))
            print(" | ".join(row))

    # Optional: print example mismatches
    if show_examples and col_mismatch > 0:
        print("\nColumn mismatches (examples):")
        for col, samples in column_examples[:examples_limit]:
            print(f"- {col}:")
            for v, a, b in samples[:examples_limit]:
                print(f"    value={v!r}  snowflake={a}  embucket={b}")
    return None


def write_chunk_csv(path: Path, df: pd.DataFrame, header_written: bool) -> bool:
    df.to_csv(path, mode="a" if header_written else "w", header=not header_written, index=False)
    return True


def write_chunk_parquet(path: Path, df: pd.DataFrame, parquet_writer_holder: dict) -> bool:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pyarrow is required for Parquet export. Install via `pip install pyarrow`."
        ) from exc

    table = pa.Table.from_pandas(df, preserve_index=False)
    if parquet_writer_holder["writer"] is None:
        parquet_writer_holder["writer"] = pq.ParquetWriter(path, table.schema)
    parquet_writer_holder["writer"].write_table(table)
    return True


def finalize_parquet_writer(parquet_writer_holder: dict) -> None:
    writer = parquet_writer_holder.get("writer")
    if writer is not None:
        writer.close()
        parquet_writer_holder["writer"] = None


def export_hash_dataset(
    connection,
    table_identifier: str,
    key_columns: Sequence[str],
    hash_columns: Optional[Sequence[str]],
    algorithm: str,
    chunksize: Optional[int],
    export_path: Optional[Path],
    export_format: Optional[str],
    include_full_row: bool,
    null_placeholder: str = "<NULL>",
) -> Tuple[pd.DataFrame, Sequence[str]]:
    """
    Stream table data, compute row hashes, optionally export to disk, and return hashes.

    Returns a DataFrame indexed by key columns with a `ROW_HASH` column and the
    list of columns that were hashed.
    """
    resolved_hash_columns: Optional[List[str]] = list(hash_columns) if hash_columns else None
    selected_columns: Optional[List[str]] = None
    if resolved_hash_columns:
        selected_columns = list(dict.fromkeys(list(key_columns) + resolved_hash_columns))

    hash_frames: List[pd.DataFrame] = []
    header_written = False
    parquet_writer_holder = {"writer": None}

    for chunk in iterate_table_chunks(connection, table_identifier, selected_columns, chunksize):
        chunk = chunk.reset_index(drop=True)
        chunk.columns = [str(col) for col in chunk.columns]

        chunk_key_columns = resolve_columns_case_insensitive(
            key_columns,
            chunk.columns,
            f"table {table_identifier}",
            "Key columns",
        )

        # Normalize key columns in the chunk for robust alignment
        for key_col in chunk_key_columns:
            if key_col in chunk.columns:
                chunk[key_col] = normalize_key_series(chunk[key_col])

        if resolved_hash_columns is None:
            resolved_hash_columns = [
                col for col in chunk.columns if col not in chunk_key_columns
            ]
            if not resolved_hash_columns:
                resolved_hash_columns = list(chunk.columns)

        chunk_hash_columns = resolve_columns_case_insensitive(
            resolved_hash_columns,
            chunk.columns,
            f"table {table_identifier}",
            "Hash columns",
        )

        chunk["ROW_HASH"] = compute_row_hashes(chunk, chunk_hash_columns, algorithm, null_placeholder)

        if export_path and export_format:
            if include_full_row:
                to_export = chunk
            else:
                to_export = chunk[list(chunk_key_columns) + ["ROW_HASH"]]

            if export_format == "csv":
                header_written = write_chunk_csv(export_path, to_export, header_written)
            elif export_format == "parquet":
                write_chunk_parquet(export_path, to_export, parquet_writer_holder)
                header_written = True
            else:
                raise ValueError(f"Unsupported export format: {export_format}")

        hash_frame = chunk[list(chunk_key_columns) + ["ROW_HASH"]].copy()
        hash_frame.set_index(list(chunk_key_columns), inplace=True)
        # Normalize index to ensure consistent key representation
        hash_frame.index = normalize_key_index(hash_frame.index)
        hash_frames.append(hash_frame)

    if export_format == "parquet":
        finalize_parquet_writer(parquet_writer_holder)

    if not hash_frames:
        empty = pd.DataFrame(columns=["ROW_HASH"])
        empty.set_index(list(key_columns), inplace=True)
        return empty, resolved_hash_columns or []

    hash_df = pd.concat(hash_frames)
    return hash_df, resolved_hash_columns or []


def hash_based_compare(
    snowflake_conn,
    embucket_conn,
    snowflake_table: str,
    embucket_table: str,
    key_columns: Sequence[str],
    hash_columns: Optional[Sequence[str]],
    algorithm: str,
    chunksize: Optional[int],
    export_format: Optional[str],
    export_dir: Optional[Path],
    include_full_row: bool,
    null_placeholder: str = "<NULL>",
    summary_format: str = "markdown",
    only_summary: bool = False,
) -> None:
    if not key_columns:
        raise ValueError("Key columns are required for hash-based comparison.")

    snowflake_columns = get_table_columns(snowflake_conn, snowflake_table)
    try:
        embucket_columns = get_table_columns_via_select(embucket_conn, embucket_table)
    except Exception:
        embucket_columns = []

    resolved_key_columns = resolve_columns_case_insensitive(
        key_columns,
        snowflake_columns,
        f"Snowflake table {snowflake_table}",
        "Key columns",
    )

    if hash_columns:
        resolved_hash_columns = resolve_columns_case_insensitive(
            hash_columns,
            snowflake_columns,
            f"Snowflake table {snowflake_table}",
            "Hash columns",
        )
    else:
        resolved_hash_columns = [
            column for column in snowflake_columns if column not in resolved_key_columns
        ]
        if not resolved_hash_columns:
            raise ValueError(
                "No non-key columns available for hashing; specify --hash-columns explicitly."
            )

    export_dir_path: Optional[Path] = None
    if export_format:
        export_dir_path = export_dir or Path(".")
        export_dir_path.mkdir(parents=True, exist_ok=True)

    def build_export_path(label: str) -> Optional[Path]:
        if not export_dir_path:
            return None
        suffix = "parquet" if export_format == "parquet" else "csv"
        filename = f"{label.replace('.', '_').replace(' ', '_')}_hash.{suffix}"
        return export_dir_path / filename

    if not only_summary:
        print("\n=== Hash-Based Comparison ===")
        print(f"Hash algorithm: {algorithm.upper()}")
        print("Hash columns derived from Snowflake information_schema.")

    snowflake_hashes, resolved_hash_columns = export_hash_dataset(
        snowflake_conn,
        snowflake_table,
        resolved_key_columns,
        resolved_hash_columns,
        algorithm,
        chunksize,
        build_export_path("snowflake"),
        export_format,
        include_full_row,
        null_placeholder,
    )

    embucket_hashes, embucket_hash_columns = export_hash_dataset(
        embucket_conn,
        embucket_table,
        resolved_key_columns,
        resolved_hash_columns,
        algorithm,
        chunksize,
        build_export_path("embucket"),
        export_format,
        include_full_row,
        null_placeholder,
    )

    if resolved_hash_columns != embucket_hash_columns:
        raise ValueError(
            "Hash column resolution mismatch between Snowflake and Embucket results. "
            f"Snowflake columns: {resolved_hash_columns}, Embucket columns: {embucket_hash_columns}"
        )

    # Extra safety: normalize indexes (covers any external calls passing frames)
    snowflake_hashes.index = normalize_key_index(snowflake_hashes.index)
    embucket_hashes.index = normalize_key_index(embucket_hashes.index)

    if resolved_hash_columns:
        print(f"Hash columns: {', '.join(resolved_hash_columns)}")
    else:
        print("Hash columns: (none)")

    if export_format and export_dir_path and not only_summary:
        print(f"Exported hashed data to {export_dir_path} as {export_format.upper()}")
        if include_full_row:
            print("Exports include full row data alongside hashes.")
        else:
            print("Exports include key columns and row hashes only.")

    missing_in_embucket = snowflake_hashes.index.difference(embucket_hashes.index)
    missing_in_snowflake = embucket_hashes.index.difference(snowflake_hashes.index)

    common_index = snowflake_hashes.index.intersection(embucket_hashes.index)
    snowflake_common = snowflake_hashes.loc[common_index]
    embucket_common = embucket_hashes.loc[common_index]
    mismatched = snowflake_common["ROW_HASH"] != embucket_common["ROW_HASH"]

    if not only_summary:
        print(f"Rows missing in Embucket: {len(missing_in_embucket):,}")
        print(f"Rows missing in Snowflake: {len(missing_in_snowflake):,}")
        print(f"Rows with differing hashes: {int(mismatched.sum()):,}")

    if len(missing_in_embucket) > 0 and not only_summary:
        print("Sample keys missing in Embucket:")
        for key in list(missing_in_embucket)[:10]:
            print(f"  {key}")
    if len(missing_in_snowflake) > 0 and not only_summary:
        print("Sample keys missing in Snowflake:")
        for key in list(missing_in_snowflake)[:10]:
            print(f"  {key}")
    if mismatched.any() and not only_summary:
        sample_keys = list(common_index[mismatched])[:10]
        print("Sample keys with differing hashes:")
        for key in sample_keys:
            print(f"  {key}")
        print("Use the exported files or rerun without --hash-compare to inspect full row differences.")
    else:
        if missing_in_embucket.empty and missing_in_snowflake.empty and not only_summary:
            print("All shared rows have matching hashes.")

    # Summary table line
    try:
        sf_count = get_row_count(snowflake_conn, snowflake_table)
    except Exception:
        sf_count = None
    try:
        em_count = get_row_count(embucket_conn, embucket_table)
    except Exception:
        em_count = None

    union_keys = snowflake_hashes.index.union(embucket_hashes.index)
    total_rows_union = union_keys.size

    # Column set comparison (case-insensitive)
    snow_cols_lower = {c.lower() for c in snowflake_columns}
    emb_cols_lower = {c.lower() for c in embucket_columns}
    total_columns = len(snowflake_columns)
    total_columns_match = len(snow_cols_lower.intersection(emb_cols_lower))
    total_columns_not_match = max(total_columns - total_columns_match, 0)

    # Summary line(s) with multiple format options
    headers = [
        "table_name",
        "total_rows",
        "rows_missmatch",
        "total_column",
    ]
    # Display just the table name (no database/schema) in the summary
    try:
        _, _, table_only = split_table_identifier(snowflake_table)
    except Exception:
        table_only = snowflake_table
    # rows_missmatch defined as absolute difference in total rows
    if sf_count is not None and em_count is not None:
        rows_missmatch = abs(int(sf_count) - int(em_count))
    else:
        rows_missmatch = "-"
    row = [
        table_only,
        str(total_rows_union),
        str(rows_missmatch),
        str(total_columns),
    ]
    fmt = summary_format or "markdown"
    if not only_summary:
        print("\nSummary:")
    if fmt == "csv":
        print(",".join(headers))
        print(",".join(row))
    elif fmt == "tsv":
        print("\t".join(headers))
        print("\t".join(row))
    elif fmt == "box":
        widths = [max(len(h), len(r)) for h, r in zip(headers, row)]
        def line(sep_left="+", sep_mid="+", sep_right="+", fill="-"):
            return sep_left + sep_mid.join(fill * (w + 2) for w in widths) + sep_right
        def render(values, is_header=False):
            cells = []
            for i, (v, w) in enumerate(zip(values, widths)):
                if i == 0 or is_header:
                    cell = " " + v.ljust(w) + " "
                else:
                    cell = " " + v.rjust(w) + " "
                cells.append(cell)
            return "|" + "|".join(cells) + "|"
        print(line())
        print(render(headers, is_header=True))
        print(line("+", "+", "+", "-"))
        print(render(row))
        print(line())
    elif fmt == "plain":
        widths = [max(len(h), len(r)) for h, r in zip(headers, row)]
        def pad(values):
            out = []
            for i, (v, w) in enumerate(zip(values, widths)):
                out.append(v.ljust(w) if i == 0 else v.rjust(w))
            return "  ".join(out)
        print(pad(headers))
        print(pad(row))
    else:  # markdown
        print(" | ".join(headers))
        print(" | ".join(["---"] + ["---:" for _ in headers[1:]]))
        print(" | ".join(row))


def parse_key_columns(raw: Optional[str]) -> List[str]:
    if raw is None:
        return []
    return [col.strip() for col in raw.split(",") if col.strip()]


def parse_hash_columns(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None:
        return None
    columns = [col.strip() for col in raw.split(",") if col.strip()]
    if not columns:
        raise ValueError("Hash columns list cannot be empty.")
    return columns


def resolve_columns_case_insensitive(
    requested: Sequence[str],
    available: Sequence[str],
    label: str,
    column_type: str,
) -> List[str]:
    available_map = {str(column).lower(): str(column) for column in available}
    resolved: List[str] = []
    missing: List[str] = []

    for column in requested:
        actual = available_map.get(column.lower())
        if actual is None:
            missing.append(column)
        else:
            resolved.append(actual)

    if missing:
        raise ValueError(
            f"{column_type} {missing} not found in {label}. Available columns: {list(available_map.values())}"
        )

    return resolved


def build_connection_config(
    args: argparse.Namespace, prefix: str, env_values: Dict[str, str]
) -> ConnectionConfig:
    hardcoded_defaults = HARDCODED_CREDENTIALS.get(prefix, {})

    def resolve(attr: str) -> Optional[str]:
        cli_value = getattr(args, f"{prefix}_{attr}")
        if cli_value:
            return cli_value
        env_name = f"{prefix.upper()}_{attr.upper()}"
        # Prefer process environment (like load_events.py), then CLI/env files fallback
        env_value = os.environ.get(env_name)
        if env_value:
            return env_value
        if env_name in env_values:
            env_file_value = env_values.get(env_name)
            if env_file_value:
                return env_file_value
        return hardcoded_defaults.get(attr)

    return ConnectionConfig(
        user=resolve("user"),
        password=resolve("password"),
        account=resolve("account"),
        warehouse=resolve("warehouse"),
        database=(resolve("database") or None) and resolve("database").upper(),
        schema=(resolve("schema") or None) and resolve("schema").upper(),
        role=resolve("role"),
        host=resolve("host"),
        port=_coerce_int(resolve("port")),
        protocol=resolve("protocol"),
    )


def _coerce_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Expected integer value, got {value!r}")


def add_connection_arguments(parser: argparse.ArgumentParser, prefix: str) -> None:
    friendly = prefix.capitalize()
    parser.add_argument(f"--{prefix}-user", dest=f"{prefix}_user", help=f"{friendly} username")
    parser.add_argument(
        f"--{prefix}-password", dest=f"{prefix}_password", help=f"{friendly} password"
    )
    parser.add_argument(
        f"--{prefix}-account",
        dest=f"{prefix}_account",
        help=f"{friendly} account identifier (e.g. xy12345.us-east-1)",
    )
    parser.add_argument(
        f"--{prefix}-warehouse", dest=f"{prefix}_warehouse", help=f"{friendly} warehouse"
    )
    parser.add_argument(
        f"--{prefix}-database", dest=f"{prefix}_database", help=f"{friendly} database"
    )
    parser.add_argument(f"--{prefix}-schema", dest=f"{prefix}_schema", help=f"{friendly} schema")
    parser.add_argument(f"--{prefix}-role", dest=f"{prefix}_role", help=f"{friendly} role (optional)")
    parser.add_argument(f"--{prefix}-host", dest=f"{prefix}_host", help=f"{friendly} host (optional)")
    parser.add_argument(f"--{prefix}-port", dest=f"{prefix}_port", type=int, help=f"{friendly} port (optional)")
    parser.add_argument(f"--{prefix}-protocol", dest=f"{prefix}_protocol", help=f"{friendly} protocol (optional)")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare tables between Snowflake and Embucket using Snowflake-compatible connections.",
    )

    add_connection_arguments(parser, "snowflake")
    add_connection_arguments(parser, "embucket")

    parser.add_argument(
        "--snowflake-table",
        required=False,
        help="Fully-qualified Snowflake table name (e.g. DATABASE.SCHEMA.TABLE).",
    )
    parser.add_argument(
        "--embucket-table",
        required=False,
        help="Fully-qualified Embucket table name (e.g. DATABASE.SCHEMA.TABLE).",
    )
    parser.add_argument(
        "--compare-default-tables",
        action="store_true",
        help="Compare a built-in list of Snowplow tables in batch (database inferred from env).",
    )
    parser.add_argument(
        "--batch-summary-format",
        choices=["csv", "tsv", "markdown", "plain", "box"],
        default="csv",
        help="Output format for --compare-default-tables (default: csv).",
    )
    parser.add_argument(
        "--batch-print-examples",
        action="store_true",
        help="After the consolidated table, print mismatch examples for each table (no-key mode).",
    )
    parser.add_argument(
        "--exclude-tables",
        help="Comma-separated list of schema.table to exclude in --compare-default-tables mode.",
    )
    parser.add_argument(
        "--key-columns",
        required=False,
        help="Comma-separated list of columns that uniquely identify rows (e.g. id,date).",
    )
    parser.add_argument(
        "--hash-compare",
        action="store_true",
        help="Enable hash-based comparison (generates row hashes and compares them).",
    )
    parser.add_argument(
        "--no-key-hash-compare",
        action="store_true",
        help="Compare row-hash multisets without aligning on keys (order-agnostic, no primary key required).",
    )
    parser.add_argument(
        "--hash-columns",
        help="Comma-separated list of columns to include in the hash (default: all non-key columns).",
    )
    parser.add_argument(
        "--hash-algorithm",
        default="md5",
        help="Hash algorithm to use (any hashlib-supported algorithm, default: md5).",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100000,
        help="Chunk size for streaming queries (set to 0 or omit to load entire table).",
    )
    parser.add_argument(
        "--export-format",
        choices=["csv", "parquet"],
        help="Optional export format for hashed data.",
    )
    parser.add_argument(
        "--export-dir",
        help="Directory to write exported hash files (default: current working directory).",
    )
    parser.add_argument(
        "--export-include-full-row",
        action="store_true",
        help="Include the full row data in exports instead of only key columns and hashes.",
    )
    parser.add_argument(
        "--skip-row-compare",
        action="store_true",
        help="Skip the in-memory full row comparison (useful when only hash comparison is needed).",
    )
    parser.add_argument(
        "--null-placeholder",
        default="<NULL>",
        help="Placeholder string used when hashing NULL values (default: <NULL>).",
    )
    parser.add_argument(
        "--summary-format",
        choices=["markdown", "plain", "csv", "tsv", "box"],
        default="markdown",
        help="Summary output format (default: markdown).",
    )
    parser.add_argument(
        "--only-summary",
        action="store_true",
        help="Print only the summary table (suppresses other logs).",
    )
    parser.add_argument(
        "--show-col-mismatch-examples",
        action="store_true",
        help="In no-key compare, print first mismatching column names and sample value deltas.",
    )
    parser.add_argument(
        "--col-mismatch-examples-limit",
        type=int,
        default=3,
        help="Max number of mismatching columns and example values to print (default: 3).",
    )
    parser.add_argument(
        "--snowflake-env-file",
        default=str(Path(__file__).parent / "snowflake" / ".env"),
        help="Path to Snowflake .env file (default: ./snowflake/.env relative to this script).",
    )
    parser.add_argument(
        "--embucket-env-file",
        default=str(Path(__file__).parent / "embucket" / ".env"),
        help="Path to Embucket .env file (default: ./embucket/.env relative to this script).",
    )

    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_arguments()

    # Align with load_events.py behavior: rely on process environment first.
    # We still parse .env files if provided, but they do NOT override process env.
    snowflake_env_values: Dict[str, str] = load_env_file(Path(args.snowflake_env_file)) if args.snowflake_env_file else {}
    embucket_env_values: Dict[str, str] = load_env_file(Path(args.embucket_env_file)) if args.embucket_env_file else {}

    snowflake_config = build_connection_config(args, "snowflake", snowflake_env_values)
    embucket_config = build_connection_config(args, "embucket", embucket_env_values)

    snowflake_config.validate("Snowflake")
    embucket_config.validate("Embucket")

    key_columns = parse_key_columns(args.key_columns)
    hash_columns = parse_hash_columns(args.hash_columns)
    chunksize = args.chunksize if args.chunksize and args.chunksize > 0 else None

    if not args.only_summary and not args.compare_default_tables:
        print("Snowflake connection parameters:")
        print(
            {
                "account": snowflake_config.account,
                "user": snowflake_config.user,
                "warehouse": snowflake_config.warehouse,
                "database": snowflake_config.database,
                "schema": snowflake_config.schema,
                "role": snowflake_config.role,
            }
        )

    with create_connection(snowflake_config) as sf_conn, create_connection(
        embucket_config
    ) as emb_conn:
        # Batch mode: compare predefined Snowplow tables
        if args.compare_default_tables:
            fmt = args.batch_summary_format or "csv"
            # Collect rows for a single consolidated output
            collected_rows: List[List[str]] = []
            headers: Optional[List[str]] = None
            examples_by_table: List[Tuple[str, List[Tuple[str, List[Tuple[str, int, int]]]]]] = []
            # Build exclusion set (normalized to lowercase)
            exclude_set: set = set()
            if getattr(args, "exclude_tables", None):
                exclude_set = {t.strip().lower() for t in args.exclude_tables.split(",") if t.strip()}
            for rel in DEFAULT_SNOWPLOW_TABLES:
                if rel.lower() in exclude_set:
                    continue
                # Qualify using env database + provided schema.table
                snowflake_table_qualified = qualify_table_name(
                    rel, snowflake_config.database, None
                )
                embucket_table_qualified = qualify_table_name(
                    rel, embucket_config.database, None
                )
                # Progress indicator to stderr so it won't break consolidated stdout table
                try:
                    sys.stderr.write(f"Processing {snowflake_table_qualified} ...\n")
                    sys.stderr.flush()
                except Exception:
                    pass
                if args.no_key_hash_compare or not args.key_columns:
                    result = no_key_hash_compare(
                        sf_conn,
                        emb_conn,
                        snowflake_table_qualified,
                        embucket_table_qualified,
                        parse_hash_columns(args.hash_columns),
                        args.hash_algorithm,
                        args.chunksize if args.chunksize and args.chunksize > 0 else None,
                        None,
                        None,
                        args.export_include_full_row,
                        args.null_placeholder,
                        fmt,
                        True,
                        args.batch_print_examples,  # compute examples only if we plan to print later
                        args.col_mismatch_examples_limit if hasattr(args, "col_mismatch_examples_limit") else 3,
                        True,  # return_row
                    )
                    if result:
                        headers, row, examples = result
                        collected_rows.append(row)
                        if args.batch_print_examples and examples:
                            # Use the table_name from the row (first column)
                            examples_by_table.append((row[0], examples))
                else:
                    # Row-hash mode requires keys
                    # Fallback to printing individually (not consolidated) for key mode
                    hash_based_compare(
                        sf_conn,
                        emb_conn,
                        snowflake_table_qualified,
                        embucket_table_qualified,
                        parse_key_columns(args.key_columns),
                        parse_hash_columns(args.hash_columns),
                        args.hash_algorithm,
                        args.chunksize if args.chunksize and args.chunksize > 0 else None,
                        None,
                        None,
                        args.export_include_full_row,
                        args.null_placeholder,
                        "box" if fmt == "box" else fmt,
                        True,
                    )
            # Render one consolidated table
            if headers is None:
                return
            if fmt == "csv":
                print(",".join(headers))
                for row in collected_rows:
                    print(",".join(row))
            elif fmt == "tsv":
                print("\t".join(headers))
                for row in collected_rows:
                    print("\t".join(row))
            elif fmt == "box":
                # Compute widths across all rows
                widths = [len(h) for h in headers]
                for row in collected_rows:
                    widths = [max(w, len(v)) for w, v in zip(widths, row)]
                def line(sep_left="+", sep_mid="+", sep_right="+", fill="-"):
                    return sep_left + sep_mid.join(fill * (w + 2) for w in widths) + sep_right
                def render(values, is_header=False):
                    cells = []
                    for i, (v, w) in enumerate(zip(values, widths)):
                        cells.append(" " + (v.ljust(w) if i == 0 or is_header else v.rjust(w)) + " ")
                    return "|" + "|".join(cells) + "|"
                print(line())
                print(render(headers, is_header=True))
                print(line("+", "+", "+", "-"))
                for row in collected_rows:
                    print(render(row))
                print(line())
            elif fmt == "plain":
                widths = [len(h) for h in headers]
                for row in collected_rows:
                    widths = [max(w, len(v)) for w, v in zip(widths, row)]
                def pad(values):
                    out = []
                    for i, (v, w) in enumerate(zip(values, widths)):
                        out.append(v.ljust(w) if i == 0 else v.rjust(w))
                    return "  ".join(out)
                print(pad(headers))
                for row in collected_rows:
                    print(pad(row))
            else:  # markdown
                print(" | ".join(headers))
                print(" | ".join(["---"] + ["---:" for _ in headers[1:]]))
                for row in collected_rows:
                    print(" | ".join(row))
            # Print examples after consolidated table, if requested
            if args.batch_print_examples and examples_by_table:
                print("\nExamples:")
                for table_name, examples in examples_by_table:
                    print(f"- {table_name}:")
                    for col, samples in examples[: (args.col_mismatch_examples_limit or 3)]:
                        print(f"  {col}:")
                        for v, a, b in samples[: (args.col_mismatch_examples_limit or 3)]:
                            print(f"    value={v!r}  snowflake={a}  embucket={b}")
            return
        # Auto-qualify table identifiers using env-derived defaults if needed
        snowflake_table_qualified = qualify_table_name(
            args.snowflake_table, snowflake_config.database, snowflake_config.schema
        )
        embucket_table_qualified = qualify_table_name(
            args.embucket_table, embucket_config.database, embucket_config.schema
        )

        if not args.skip_row_compare and not args.no_key_hash_compare:
            compare_tables(
                sf_conn,
                emb_conn,
                snowflake_table_qualified,
                embucket_table_qualified,
                key_columns,
            )

        if args.no_key_hash_compare:
            no_key_hash_compare(
                sf_conn,
                emb_conn,
                snowflake_table_qualified,
                embucket_table_qualified,
                hash_columns,
                args.hash_algorithm,
                chunksize,
                args.export_format,
                Path(args.export_dir) if args.export_dir else None,
                args.export_include_full_row,
                args.null_placeholder,
                args.summary_format,
                args.only_summary,
                args.show_col_mismatch_examples,
                args.col_mismatch_examples_limit,
            )
        elif args.hash_compare:
            hash_based_compare(
                sf_conn,
                emb_conn,
                snowflake_table_qualified,
                embucket_table_qualified,
                key_columns,
                hash_columns,
                args.hash_algorithm,
                chunksize,
                args.export_format,
                Path(args.export_dir) if args.export_dir else None,
                args.export_include_full_row,
                args.null_placeholder,
                args.summary_format,
                args.only_summary,
            )


if __name__ == "__main__":
    main()