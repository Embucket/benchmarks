# dbt-snowplow-web Benchmark and Data Comparison Tools

This directory contains tools for benchmarking dbt-snowplow-web transformations and comparing data between Snowflake and Embucket databases.

## Prerequisites

- Python 3.9+ (Python 3.10+ recommended)
- Access to Snowflake and Embucket databases
- Environment files configured (see below)

## Setup

### 1. Environment Configuration

Create `.env` files in the respective directories:

- `snowflake/.env` - Snowflake connection credentials
- `embucket/.env` - Embucket connection credentials

Example `.env` file format:
```bash
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=your-user
export SNOWFLAKE_PASSWORD=your-password
export SNOWFLAKE_WAREHOUSE=your-warehouse
export SNOWFLAKE_DATABASE=your-database
export SNOWFLAKE_SCHEMA=your-schema
export SNOWFLAKE_ROLE=your-role
```

### 2. Install Dependencies

Dependencies are automatically installed when running `benchmark.sh`. The script installs:
- dbt and dbt-snowplow-web packages
- playwright (for dashboard screenshots)
- pandas, snowflake-connector-python, pyarrow (for data_diff.py)

## Running Benchmarks

### Snowflake Benchmark

```bash
cd snowflake/
./benchmark.sh [SCALE_FACTOR]
```

**Parameters:**
- `SCALE_FACTOR` (optional): Data scale factor in GB (default: 0.2)

**What it does:**
1. Sets up Python virtual environment
2. Installs dependencies
3. Generates test event data
4. Loads data into Snowflake
5. Runs dbt models (first run with full refresh)
6. Generates incremental data
7. Runs dbt models again (incremental run)
8. Captures screenshots and run results

**Output:**
- Run results: `run_results_first_run.json`, `run_results_incremental_run.json`
- Screenshots: `screenshots/` directory
- Enriched run results: `run_results_*_enriched.json`

### Embucket Benchmark

```bash
cd embucket/
./benchmark.sh [SCALE_FACTOR]
```

Same workflow as Snowflake benchmark, but targets Embucket database.

## Data Comparison Tool (`data_diff.py`)

The `data_diff.py` script compares tables between Snowflake and Embucket databases using hash-based comparison methods.

### Basic Usage

#### Single Table Comparison (No Key - Column-level)

Compare all columns without requiring a primary key:

```bash
python data_diff.py \
  --snowflake-table PUBLIC_SCRATCH.SNOWPLOW_WEB_VITALS_THIS_RUN \
  --embucket-table PUBLIC_SCRATCH.SNOWPLOW_WEB_VITALS_THIS_RUN \
  --no-key-hash-compare \
  --summary-format box --only-summary \
  --show-col-mismatch-examples --col-mismatch-examples-limit 10 \
  --snowflake-env-file ./snowflake/.env \
  --embucket-env-file ./embucket/.env
```

#### Single Table Comparison (With Key - Row-level)

Compare tables using a primary key for row alignment:

```bash
python data_diff.py \
  --snowflake-table PUBLIC_SCRATCH.SNOWPLOW_WEB_VITALS_THIS_RUN \
  --embucket-table PUBLIC_SCRATCH.SNOWPLOW_WEB_VITALS_THIS_RUN \
  --hash-compare \
  --key-columns event_id \
  --summary-format box --only-summary \
  --snowflake-env-file ./snowflake/.env \
  --embucket-env-file ./embucket/.env
```

#### Batch Comparison (All Predefined Tables)

Compare all predefined Snowplow tables at once:

```bash
python data_diff.py \
  --compare-default-tables \
  --no-key-hash-compare \
  --batch-summary-format box --only-summary \
  --batch-print-examples --col-mismatch-examples-limit 5 \
  --snowflake-env-file ./snowflake/.env \
  --embucket-env-file ./embucket/.env
```

#### Batch Comparison with Keys

Use key-based comparison for tables that have keys defined in the script:

```bash
python data_diff.py \
  --compare-default-tables \
  --hash-compare \
  --no-key-hash-compare \
  --batch-summary-format box --only-summary \
  --snowflake-env-file ./snowflake/.env \
  --embucket-env-file ./embucket/.env
```

This will:
- Use key-based comparison for tables with keys in `DEFAULT_KEY_MAP`
- Use no-key comparison for tables without keys
- Output a single consolidated table

#### Exclude Specific Tables

Skip certain tables in batch mode:

```bash
python data_diff.py \
  --compare-default-tables \
  --no-key-hash-compare \
  --exclude-tables public_scratch.snowplow_web_vitals_this_run,public_derived.snowplow_web_users \
  --batch-summary-format box --only-summary \
  --snowflake-env-file ./snowflake/.env \
  --embucket-env-file ./embucket/.env
```

### Command-Line Options

#### Connection Options
- `--snowflake-table TABLE` - Snowflake table (schema.table or fully qualified)
- `--embucket-table TABLE` - Embucket table (schema.table or fully qualified)
- `--snowflake-env-file PATH` - Path to Snowflake .env file (default: ./snowflake/.env)
- `--embucket-env-file PATH` - Path to Embucket .env file (default: ./embucket/.env)

#### Comparison Mode
- `--no-key-hash-compare` - Compare without primary keys (order-agnostic, column-level)
- `--hash-compare` - Compare with primary keys (row-level alignment)
- `--key-columns COL1,COL2` - Specify key columns for row-level comparison

#### Batch Mode
- `--compare-default-tables` - Compare all predefined Snowplow tables
- `--batch-summary-format FORMAT` - Output format: csv, tsv, markdown, plain, box (default: csv)
- `--batch-print-examples` - Print mismatch examples after consolidated table
- `--exclude-tables TABLE1,TABLE2` - Exclude tables from batch comparison

#### Output Format
- `--summary-format FORMAT` - Format: markdown, plain, csv, tsv, box (default: markdown)
- `--only-summary` - Print only the summary table (suppress other logs)

#### Examples and Debugging
- `--show-col-mismatch-examples` - Show example value differences for mismatching columns
- `--col-mismatch-examples-limit N` - Max number of examples to show (default: 3)

#### Advanced Options
- `--hash-columns COL1,COL2` - Specific columns to hash (default: all columns)
- `--hash-algorithm ALG` - Hash algorithm: md5, sha256, etc. (default: md5)
- `--chunksize N` - Chunk size for streaming (default: 100000)
- `--export-format FORMAT` - Export hashes: csv, parquet
- `--export-dir DIR` - Directory for exports

### Summary Table Columns

The output table includes:

- **table_name** - Table name (without database/schema)
- **total_rows** - Maximum row count between Snowflake and Embucket
- **rows_missmatch** - Absolute difference in row counts
- **total_column** - Total number of columns in Snowflake table
- **col_mismatch** - Number of columns with value differences (no-key mode only)
- **extra_in_sf** - Extra row hashes in Snowflake (no-key mode only)
- **extra_in_em** - Extra row hashes in Embucket (no-key mode only)

### Comparison Modes Explained

#### No-Key Mode (`--no-key-hash-compare`)
- **Best for**: Tables without reliable primary keys, order-agnostic comparison
- **How it works**: 
  - Computes row hashes for all selected columns
  - Compares multisets of hashes (order doesn't matter)
  - Compares value frequency distributions per column
- **Output**: Shows column-level mismatches and row hash differences

#### Key-Based Mode (`--hash-compare --key-columns`)
- **Best for**: Tables with reliable primary keys, precise row-to-row comparison
- **How it works**:
  - Aligns rows by primary key
  - Compares hash of non-key columns for each aligned row
  - Identifies missing rows and hash mismatches
- **Output**: Shows missing rows and rows with differing hashes

### Predefined Tables

The script includes a predefined list of Snowplow tables in `DEFAULT_SNOWPLOW_TABLES`:

**Manifest tables:**
- `public_snowplow_manifest.snowplow_web_base_quarantined_sessions`
- `public_snowplow_manifest.snowplow_web_incremental_manifest`
- `public_snowplow_manifest.snowplow_web_base_sessions_lifecycle_manifest`

**Scratch tables:**
- `public_scratch.snowplow_web_base_new_event_limits`
- `public_scratch.snowplow_web_base_sessions_this_run`
- `public_scratch.snowplow_web_base_events_this_run`
- `public_scratch.snowplow_web_consent_events_this_run`
- `public_scratch.snowplow_web_pv_engaged_time`
- `public_scratch.snowplow_web_pv_scroll_depth`
- `public_scratch.snowplow_web_sessions_this_run`
- `public_scratch.snowplow_web_vital_events_this_run`
- `public_scratch.snowplow_web_page_views_this_run`
- `public_scratch.snowplow_web_vitals_this_run`
- `public_scratch.snowplow_web_users_sessions_this_run`
- `public_scratch.snowplow_web_users_aggs`
- `public_scratch.snowplow_web_users_lasts`
- `public_scratch.snowplow_web_users_this_run`

**Derived tables:**
- `public_derived.snowplow_web_user_mapping`
- `public_derived.snowplow_web_consent_log`
- `public_derived.snowplow_web_consent_cmp_stats`
- `public_derived.snowplow_web_consent_versions`
- `public_derived.snowplow_web_sessions`
- `public_derived.snowplow_web_page_views`
- `public_derived.snowplow_web_consent_users`
- `public_derived.snowplow_web_vitals`
- `public_derived.snowplow_web_consent_scope_status`
- `public_derived.snowplow_web_consent_totals`
- `public_derived.snowplow_web_vital_measurements`
- `public_derived.snowplow_web_users`

### Default Keys

Some tables have default keys defined in the script for key-based comparison:

- `public_scratch.snowplow_web_base_events_this_run`: `event_id`
- `public_scratch.snowplow_web_vital_events_this_run`: `event_id`
- `public_scratch.snowplow_web_page_views_this_run`: `page_view_id`
- `public_scratch.snowplow_web_sessions_this_run`: `domain_sessionid`
- `public_scratch.snowplow_web_vitals_this_run`: `event_id`
- `public_scratch.snowplow_web_users_this_run`: `user_id`
- `public_derived.snowplow_web_page_views`: `page_view_id`
- `public_derived.snowplow_web_sessions`: `domain_sessionid`
- `public_derived.snowplow_web_users`: `user_id`

### Examples

#### Quick Check: Single Table
```bash
python data_diff.py \
  --snowflake-table PUBLIC_SCRATCH.SNOWPLOW_WEB_VITALS_THIS_RUN \
  --embucket-table PUBLIC_SCRATCH.SNOWPLOW_WEB_VITALS_THIS_RUN \
  --no-key-hash-compare \
  --summary-format box --only-summary
```

#### Full Comparison: All Tables with Examples
```bash
python data_diff.py \
  --compare-default-tables \
  --no-key-hash-compare \
  --batch-summary-format box --only-summary \
  --batch-print-examples --col-mismatch-examples-limit 10
```

#### Export to CSV
```bash
python data_diff.py \
  --compare-default-tables \
  --no-key-hash-compare \
  --batch-summary-format csv --only-summary > comparison_results.csv
```

## Troubleshooting

### Connection Issues
- Verify `.env` files are correctly configured
- Check that database and schema names are uppercase (Snowflake requirement)
- Ensure network connectivity to both databases

### Missing Dependencies
- Run `pip install pandas snowflake-connector-python pyarrow` manually
- Or re-run `benchmark.sh` to install all dependencies

### No Examples Showing
- Ensure `--show-col-mismatch-examples` or `--batch-print-examples` is included
- Increase `--col-mismatch-examples-limit` if needed
- Check that there are actual mismatches (col_mismatch > 0)

### Table Not Found
- Verify table names match exactly (case-sensitive for Snowflake)
- Check that database and schema are correctly set in `.env` files
- Use fully qualified names: `DATABASE.SCHEMA.TABLE`

## Notes

- The script automatically uppercases database and schema names from environment variables
- Column names are resolved case-insensitively
- Key values are normalized (trimmed, lowercased) for robust comparison
- Progress messages are written to stderr, so they won't interfere with table output
- Warnings (pandas DBAPI, boto3 deprecation) are suppressed for cleaner output

