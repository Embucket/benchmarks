# Snowflake TPC-H Benchmark
TPC-H queries were taken from [Apache DataFusion](https://github.com/apache/datafusion/tree/main/benchmarks/queries) repository with minor syntax changes for queries 4, 15, 20, and 22 to run on Snowflake. Data is read from Snowflake's sample database: SNOWFLAKE_SAMPLE_DATA.TPCH_SF{N}.

Scripts to run and measure TPC-H query performance on **Snowflake** with control over warehouse size, iteration count, and query selection.

---

## Files

- `sf_benchmark.py` — Python script that executes TPC-H queries against Snowflake  
- `run_tpch_benchmark.sh` — Shell wrapper to run the benchmark with various options

## Requirements

- Python **3.6+**
- **Snowflake** account and valid credentials
- **snowflake-connector-python** package (the script will install it automatically if missing)

## Setup

Create a `.env` file in the `snowflake` directory with your credentials:

```dotenv
# Snowflake credentials
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_DATABASE=TPCH_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=BENCHMARK_WH

# Warehouse configuration
SNOWFLAKE_WAREHOUSE_SIZE=MEDIUM  # Options: XSMALL, SMALL, MEDIUM, LARGE, XLARGE, etc.
```

## Quick Start

Run all TPC-H queries on **SF10 (10 GB)** with **3 iterations** (defaults):

```bash
./run_tpch_benchmark.sh 10
```

## Examples

```bash
# 1) Run with 5 iterations on SF100
./run_tpch_benchmark.sh 100 --iterations 5

# 2) Run only queries 1 and 6 with an XL warehouse
./run_tpch_benchmark.sh 10 --query 1 --query 6 --warehouse-size XLARGE

# 3) Specify a custom output file
./run_tpch_benchmark.sh 1 --output my-results.json

## Output

The output includes:
- Benchmark metadata** (timestamp, engine, warehouse size)
- Test configuration** (scale factor, iterations, mode)
- Per-query metrics**, including:
  - Array of execution times per iteration (in seconds)
  - Complete query execution details with Snowflake query IDs
  - Summary statistics (average, minimum, maximum)

## Notes

- Query result caching is **disabled** to ensure accurate measurements.
- Data is read from Snowflake's sample database: `SNOWFLAKE_SAMPLE_DATA.TPCH_SF{N}`.
