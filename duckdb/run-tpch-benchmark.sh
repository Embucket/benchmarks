#!/usr/bin/env bash
set -euo pipefail

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/../env.sh" ]]; then
  source "${SCRIPT_DIR}/../env.sh"
else
  MOUNT_POINT="${MOUNT_POINT:-/mnt/data}"
fi

# --- DETECT INSTANCE TYPE (Using bench_infra) ---
ROOT_DIR="$(dirname "${SCRIPT_DIR}")"
INSTANCE_TYPE=$(python3 -c "import sys; sys.path.append('${ROOT_DIR}'); from bench_infra import common; print(common.get_ec2_metadata())" 2>/dev/null || echo "unknown")

# Usage function
usage() {
  cat <<EOF
Usage: $0 <scale_factor> --mode <MODE> [options]

Run TPC-H benchmark using DuckDB.

Arguments:
  scale_factor    The TPC-H scale factor (e.g., 1, 10, 100, 1000)

Required Options:
  --mode MODE              Benchmark mode: 'parquet', 'parquet-s3', or 'internal'

Optional Arguments:
  --iterations N           Number of iterations to run (default: 3)
  --output FILE            Output JSON file name (default: auto-generated in ../results/duckdb/)
  --query N                Run only specific query number (can be specified multiple times)
  --memory-limit MB        Memory limit in MB
  --threads N              Number of threads to use

Examples:
  $0 1 --mode parquet
  $0 10 --mode parquet --iterations 5
  $0 100 --mode parquet --query 1 --query 6
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

# Validate scale factor
if ! [[ "${SCALE_FACTOR}" =~ ^[0-9]+$ ]] || [[ "${SCALE_FACTOR}" -le 0 ]]; then
  echo "Error: Scale factor must be a positive integer"
  echo
  usage
fi

# Parse optional arguments
MODE=""
ITERATIONS=3
OUTPUT_FILE=""
QUERY_ARGS=()
MEMORY_LIMIT=""
THREADS=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="$(realpath "$2")"
      shift 2
      ;;
    --query)
      QUERY_ARGS+=("$2")
      shift 2
      ;;
    --memory-limit)
      MEMORY_LIMIT="$2"
      shift 2
      ;;
    --threads)
      THREADS="$2"
      shift 2
      ;;
    *)
      echo "Error: Unknown option $1"
      usage
      ;;
  esac
done

# Validate mode
if [[ -z "${MODE}" ]]; then
  echo "Error: --mode argument is required"
  echo
  usage
fi
if [[ "${MODE}" != "parquet" && "${MODE}" != "parquet-s3" && "${MODE}" != "internal" ]]; then
  echo "Error: Invalid mode '${MODE}'. Must be 'parquet', 'parquet-s3', or 'internal'"
  usage
fi

# --- PATH LOGIC ---
if [[ -z "${OUTPUT_FILE}" ]]; then
  DEFAULT_RESULTS_DIR="${SCRIPT_DIR}/results-${MODE}"
  mkdir -p "${DEFAULT_RESULTS_DIR}"
  OUTPUT_FILE="${DEFAULT_RESULTS_DIR}/${INSTANCE_TYPE}/tpch-sf${SCALE_FACTOR}_${MODE}-results.json"
fi

RESULTS_DIR="$(dirname "${OUTPUT_FILE}")"
mkdir -p "${RESULTS_DIR}"

echo "=== DuckDB TPC-H Benchmark ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Mode: ${MODE}"
echo "Output File: ${OUTPUT_FILE}"
echo

# Set paths based on mode
TEMP_DIR="${MOUNT_POINT}/duckdb/temp"
mkdir -p "${TEMP_DIR}"

if [[ "${MODE}" == "parquet" ]]; then
  DATA_DIR="${MOUNT_POINT}/tpch-data/sf${SCALE_FACTOR}"
  DB_FILE=""
  if [[ ! -d "${DATA_DIR}" ]]; then
    echo "Error: Data directory not found: ${DATA_DIR}"
    echo "Please run generate-tpch-data.sh first."
    exit 1
  fi
  echo ">>> Data directory: ${DATA_DIR}"

elif [[ "${MODE}" == "parquet-s3" ]]; then
  DATA_DIR="s3://embucket-testdata/tpch/${SCALE_FACTOR}_partitioned"
  DB_FILE=""
  echo ">>> S3 data path: ${DATA_DIR}"

else  # internal mode
  DATA_DIR=""
  DB_FILE="${MOUNT_POINT}/duckdb/tpch-sf${SCALE_FACTOR}.db"
  if [[ ! -f "${DB_FILE}" ]]; then
    echo "Error: Database file not found: ${DB_FILE}"
    echo "Please run download-tpch-db.sh first."
    exit 1
  fi
  echo ">>> Database file: ${DB_FILE}"
fi

echo ">>> Temp directory (for spill): ${TEMP_DIR}"
echo

# --- INSTALLATION CHECK (DUCKDB CLI) ---
echo ">>> Checking for DuckDB CLI..."
if ! command -v duckdb &> /dev/null; then
  echo ">>> DuckDB CLI not found. Installing..."
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    if ! command -v unzip &> /dev/null; then
      sudo apt-get update -qq && sudo apt-get install -y unzip
    fi
    ARCH=$(uname -m)
    if [[ "$ARCH" == "x86_64" ]]; then DUCKDB_ARCH="amd64";
    elif [[ "$ARCH" == "aarch64" ]]; then DUCKDB_ARCH="arm64";
    else echo "Error: Unsupported architecture: $ARCH"; exit 1; fi

    DUCKDB_URL="https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-${DUCKDB_ARCH}.zip"
    wget --show-progress -q "$DUCKDB_URL" -O /tmp/duckdb.zip
    unzip -q /tmp/duckdb.zip -d /tmp/
    sudo mv /tmp/duckdb /usr/local/bin/
    sudo chmod +x /usr/local/bin/duckdb
    rm /tmp/duckdb.zip
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    brew install duckdb
  fi
else
  echo ">>> DuckDB CLI already installed."
fi

# --- INSTALLATION CHECK (PYTHON LIB) ---
echo ">>> Checking for Python DuckDB package..."
# Temporarily disable exit on error
set +e
(cd /tmp && python3 -c "import duckdb" 2>/dev/null)
CHECK_RESULT=$?
set -e

if [[ ${CHECK_RESULT} -ne 0 ]]; then
  echo ">>> Python DuckDB package not found. Installing..."
  if ! command -v pip3 &> /dev/null; then
    sudo apt-get update -qq && sudo apt-get install -y python3-pip
  fi
  pip3 install duckdb --break-system-packages
fi

# --- SYSTEM LIMITS ---
echo ">>> Adjusting file limits..."
ulimit -n 65536 2>/dev/null || echo "Warning: Could not set ulimit -n 65536"

# --- FETCH QUERIES ---
QUERIES_DIR="${MOUNT_POINT}/duckdb/tpch-queries"
if [[ ! -d "${QUERIES_DIR}" ]]; then
  echo ">>> Cloning TPC-H queries..."
  mkdir -p "$(dirname "${QUERIES_DIR}")"
  git clone --depth 1 https://github.com/duckdb/duckdb.git /tmp/duckdb-repo
  mkdir -p "${QUERIES_DIR}"
  cp -r /tmp/duckdb-repo/extension/tpch/dbgen/queries/* "${QUERIES_DIR}/"
  rm -rf /tmp/duckdb-repo
fi

# --- EXECUTION ---
if [[ -f "${SCRIPT_DIR}/run_benchmark.py" ]]; then
  BENCHMARK_SCRIPT="${SCRIPT_DIR}/run_benchmark.py"
elif [[ -f "${SCRIPT_DIR}/execute_queries.py" ]]; then
  BENCHMARK_SCRIPT="${SCRIPT_DIR}/execute_queries.py"
else
  echo "Error: Could not find Python benchmark script in ${SCRIPT_DIR}"
  exit 1
fi

echo ">>> Running Python benchmark script: $(basename "${BENCHMARK_SCRIPT}")"

# Build command
PYTHON_CMD="python3 ${BENCHMARK_SCRIPT} \
  --queries-dir ${QUERIES_DIR} \
  --temp-dir ${TEMP_DIR} \
  --iterations ${ITERATIONS} \
  --output ${OUTPUT_FILE} \
  --mode ${MODE}"

if [[ "${MODE}" == "parquet" || "${MODE}" == "parquet-s3" ]]; then
  PYTHON_CMD="${PYTHON_CMD} --data-dir ${DATA_DIR}"
else
  PYTHON_CMD="${PYTHON_CMD} --db-file ${DB_FILE}"
fi

for query in "${QUERY_ARGS[@]}"; do
  PYTHON_CMD="${PYTHON_CMD} --query ${query}"
done

if [[ -n "${MEMORY_LIMIT}" ]]; then
  PYTHON_CMD="${PYTHON_CMD} --memory-limit ${MEMORY_LIMIT}"
fi
if [[ -n "${THREADS}" ]]; then
  PYTHON_CMD="${PYTHON_CMD} --threads ${THREADS}"
fi

(cd /tmp && eval "${PYTHON_CMD}")

echo
echo ">>> Benchmark complete!"
echo ">>> Results saved to: ${OUTPUT_FILE}"