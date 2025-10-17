#!/usr/bin/env bash
set -euo pipefail

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load environment variables from .env
ENV_FILE="${SCRIPT_DIR}/.env"
if [[ -f "${ENV_FILE}" ]]; then
  echo ">>> Loading environment from ${ENV_FILE}"
  set -o allexport
  source "${ENV_FILE}"
  set +o allexport
else
  echo "Error: Environment file not found: ${ENV_FILE}"
  exit 1
fi

# Usage function
usage() {
  cat <<EOF
Usage: $0 <scale_factor> [options]

Run TPC-H benchmark using Snowflake.

Arguments:
  scale_factor    The TPC-H scale factor (e.g., 1, 10, 100, 1000)

Optional Arguments:
  --iterations N           Number of iterations to run (default: 3)
  --output FILE            Output JSON file name (will be saved in results/ directory)
  --query N                Run only specific query number (can be specified multiple times)
  --warehouse-size SIZE    Snowflake warehouse size (default: from env.sh)
EOF
  exit 1
}

# Check if scale factor argument is provided
if [[ $# -lt 1 ]]; then
  echo "Error: Scale factor argument is required"
  echo
  usage
fi

SCALE_FACTOR="$1"
shift

# Validate scale factor is a positive number
if ! [[ "${SCALE_FACTOR}" =~ ^[0-9]+$ ]] || [[ "${SCALE_FACTOR}" -le 0 ]]; then
  echo "Error: Scale factor must be a positive integer"
  echo
  usage
fi

# Parse optional arguments
ITERATIONS=3
OUTPUT_FILE=""
QUERY_ARGS=()
WAREHOUSE_SIZE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --iterations)
      if [[ $# -lt 2 ]] || [[ ! "$2" =~ ^[0-9]+$ ]]; then
        echo "Error: --iterations requires a positive integer"
        exit 1
      fi
      ITERATIONS="$2"
      shift 2
      ;;
    --output)
      if [[ $# -lt 2 ]]; then
        echo "Error: --output requires a file path"
        exit 1
      fi
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --query)
      if [[ $# -lt 2 ]] || [[ ! "$2" =~ ^[0-9]+$ ]]; then
        echo "Error: --query requires a query number"
        exit 1
      fi
      QUERY_ARGS+=("$2")
      shift 2
      ;;
    --warehouse-size)
      if [[ $# -lt 2 ]]; then
        echo "Error: --warehouse-size requires a size (e.g., SMALL, MEDIUM, LARGE)"
        exit 1
      fi
      WAREHOUSE_SIZE="$2"
      shift 2
      ;;
    *)
      echo "Error: Unknown option: $1"
      echo
      usage
      ;;
  esac
done

# Create results directory
RESULTS_DIR="$(pwd)/results"
mkdir -p "${RESULTS_DIR}"

# Generate timestamp for results
TIMESTAMP=$(date +"%Y-%m-%d_%H:%M:%S")

# Set default output file if not specified
if [[ -z "${OUTPUT_FILE}" ]]; then
  OUTPUT_FILE="${RESULTS_DIR}/tpch-sf${SCALE_FACTOR}-snowflake-results.json"
else
  # If user specified output file, move it to results directory but keep the filename
  OUTPUT_BASENAME=$(basename "${OUTPUT_FILE}")
  OUTPUT_FILE="${RESULTS_DIR}/${OUTPUT_BASENAME}"
fi

# Check for required environment variables
for VAR in SNOWFLAKE_USER SNOWFLAKE_PASSWORD SNOWFLAKE_ACCOUNT SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE; do
  if [[ -z "${!VAR:-}" ]]; then
    echo "Error: Required environment variable ${VAR} is not set"
    exit 1
  fi
done

# Set warehouse size if provided via command line
if [[ -n "${WAREHOUSE_SIZE}" ]]; then
  export SNOWFLAKE_WAREHOUSE_SIZE="${WAREHOUSE_SIZE}"
elif [[ -z "${SNOWFLAKE_WAREHOUSE_SIZE:-}" ]]; then
  echo "Error: SNOWFLAKE_WAREHOUSE_SIZE not set in environment or via --warehouse-size"
  exit 1
fi

echo "=== Snowflake TPC-H Benchmark ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Warehouse: ${SNOWFLAKE_WAREHOUSE}"
echo "Warehouse Size: ${SNOWFLAKE_WAREHOUSE_SIZE}"
echo "Iterations: ${ITERATIONS}"
echo "Results Directory: ${RESULTS_DIR}"
echo "Output File: ${OUTPUT_FILE}"
echo "Mode: Snowflake sample data"
echo

# Set temp directory
TEMP_DIR="/tmp/snowflake-benchmark"
mkdir -p "${TEMP_DIR}"

# Hardcoded queries directory - replace with the actual path to your queries
QUERIES_DIR="tpch_queries"
echo ">>> Using TPC-H queries from: ${QUERIES_DIR}"

# Verify queries exist in directory
QUERY_COUNT=$(ls ${QUERIES_DIR}/q*.sql 2>/dev/null | wc -l)
if [[ ${QUERY_COUNT} -eq 0 ]]; then
  echo "Error: No query files (q*.sql) found in ${QUERIES_DIR}"
  exit 1
fi
echo ">>> Found ${QUERY_COUNT} TPC-H query files"

# Check if Python Snowflake connector is installed
echo ">>> Checking for Python Snowflake connector..."
if ! python3 -c "import snowflake.connector" &> /dev/null; then
  echo ">>> Python Snowflake connector not found. Installing..."
  pip3 install snowflake-connector-python --quiet
  echo ">>> Python Snowflake connector installed successfully"
else
  echo ">>> Python Snowflake connector already installed"
fi

echo

# Build Python command
BENCHMARK_SCRIPT="${SCRIPT_DIR}/sf_benchmark.py"

if [[ ! -f "${BENCHMARK_SCRIPT}" ]]; then
  echo "Error: Benchmark script not found: ${BENCHMARK_SCRIPT}"
  exit 1
fi

echo ">>> Running benchmark..."
echo

# Build Python command
PYTHON_CMD="python3 ${BENCHMARK_SCRIPT} --queries-dir ${QUERIES_DIR} --temp-dir ${TEMP_DIR} --iterations ${ITERATIONS} --output ${OUTPUT_FILE} --timestamp \"${TIMESTAMP}\" --scale-factor ${SCALE_FACTOR}"

# Add query arguments if specified
if [[ ${#QUERY_ARGS[@]:-0} -gt 0 ]]; then
  for query in "${QUERY_ARGS[@]}"; do
    PYTHON_CMD="${PYTHON_CMD} --query ${query}"
  done
fi

# Run the benchmark
eval "${PYTHON_CMD}"

echo
echo ">>> Benchmark complete!"
echo ">>> Results directory: ${RESULTS_DIR}"
echo ">>> Results file: ${OUTPUT_FILE}"
