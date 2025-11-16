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

Connection credentials can be passed as CLI arguments or via environment
variables. For example, to use environment variables:
    export SNOWFLAKE_USER=...
    export SNOWFLAKE_PASSWORD=...
    export SNOWFLAKE_ACCOUNT=...
    export SNOWFLAKE_WAREHOUSE=...
    export SNOWFLAKE_DATABASE=...
    export SNOWFLAKE_SCHEMA=...
    export SNOWFLAKE_ROLE=...          # optional

    export EMBUCKET_USER=...
    export EMBUCKET_PASSWORD=...
    export EMBUCKET_ACCOUNT=...
    export EMBUCKET_WAREHOUSE=...
    export EMBUCKET_DATABASE=...
    export EMBUCKET_SCHEMA=...
    export EMBUCKET_ROLE=...           # optional
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

    snowflake_df.set_index(list(resolved_key_columns), inplace=True)
    embucket_df.set_index(list(resolved_key_columns), inplace=True)

    snowflake_df.sort_index(inplace=True)
    embucket_df.sort_index(inplace=True)

    non_key_columns = [
        column for column in snowflake_df.columns if column not in resolved_key_columns
    ]

    # Align indexes to capture missing rows on either side.
    unified_index = snowflake_df.index.union(embucket_df.index)
    snowflake_aligned = snowflake_df.loc[unified_index, non_key_columns]
    embucket_aligned = embucket_df.loc[unified_index, non_key_columns]

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
) -> None:
    if not key_columns:
        raise ValueError("Key columns are required for hash-based comparison.")

    snowflake_columns = get_table_columns(snowflake_conn, snowflake_table)

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

    if resolved_hash_columns:
        print(f"Hash columns: {', '.join(resolved_hash_columns)}")
    else:
        print("Hash columns: (none)")

    if export_format and export_dir_path:
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

    print(f"Rows missing in Embucket: {len(missing_in_embucket):,}")
    print(f"Rows missing in Snowflake: {len(missing_in_snowflake):,}")
    print(f"Rows with differing hashes: {int(mismatched.sum()):,}")

    if len(missing_in_embucket) > 0:
        print("Sample keys missing in Embucket:")
        for key in list(missing_in_embucket)[:10]:
            print(f"  {key}")
    if len(missing_in_snowflake) > 0:
        print("Sample keys missing in Snowflake:")
        for key in list(missing_in_snowflake)[:10]:
            print(f"  {key}")
    if mismatched.any():
        sample_keys = list(common_index[mismatched])[:10]
        print("Sample keys with differing hashes:")
        for key in sample_keys:
            print(f"  {key}")
        print("Use the exported files or rerun without --hash-compare to inspect full row differences.")
    else:
        if missing_in_embucket.empty and missing_in_snowflake.empty:
            print("All shared rows have matching hashes.")


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
        required=True,
        help="Fully-qualified Snowflake table name (e.g. DATABASE.SCHEMA.TABLE).",
    )
    parser.add_argument(
        "--embucket-table",
        required=True,
        help="Fully-qualified Embucket table name (e.g. DATABASE.SCHEMA.TABLE).",
    )
    parser.add_argument(
        "--key-columns",
        required=True,
        help="Comma-separated list of columns that uniquely identify rows (e.g. id,date).",
    )
    parser.add_argument(
        "--hash-compare",
        action="store_true",
        help="Enable hash-based comparison (generates row hashes and compares them).",
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
    snowflake_env_values: Dict[str, str] = {}
    embucket_env_values: Dict[str, str] = {}

    snowflake_config = build_connection_config(args, "snowflake", snowflake_env_values)
    embucket_config = build_connection_config(args, "embucket", embucket_env_values)

    snowflake_config.validate("Snowflake")
    embucket_config.validate("Embucket")

    key_columns = parse_key_columns(args.key_columns)
    hash_columns = parse_hash_columns(args.hash_columns)
    chunksize = args.chunksize if args.chunksize and args.chunksize > 0 else None

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
        # Auto-qualify table identifiers using env-derived defaults if needed
        snowflake_table_qualified = qualify_table_name(
            args.snowflake_table, snowflake_config.database, snowflake_config.schema
        )
        embucket_table_qualified = qualify_table_name(
            args.embucket_table, embucket_config.database, embucket_config.schema
        )

        if not args.skip_row_compare:
            compare_tables(
                sf_conn,
                emb_conn,
                snowflake_table_qualified,
                embucket_table_qualified,
                key_columns,
            )

        if args.hash_compare:
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
            )


if __name__ == "__main__":
    main()