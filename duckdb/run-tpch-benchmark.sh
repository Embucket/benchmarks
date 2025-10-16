#!/usr/bin/env bash
set -euo pipefail

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../env.sh"

# Usage function
usage() {
  cat <<EOF
Usage: $0 <scale_factor> [options]

Run TPC-H benchmark using DuckDB.

Arguments:
  scale_factor    The TPC-H scale factor (e.g., 1, 10, 100, 1000)

Options:
  --iterations N           Number of iterations to run (default: 3)
  --output FILE            Output JSON file for results (default: tpch-sf<scale_factor>-results.json)
  --query N                Run only specific query number (can be specified multiple times)
  --memory-limit MB        Memory limit in MB (e.g., --memory-limit 10240 for 10GB)
  --threads N              Number of threads to use (default: all available cores)

Examples:
  $0 1                                    # Run all queries on SF1 data
  $0 10 --iterations 5                    # Run 5 iterations on SF10 data
  $0 100 --query 1 --query 6              # Run only queries 1 and 6 on SF100 data
  $0 1000 --memory-limit 190000           # Run on SF1000 with 190GB memory limit

The benchmark will use data from: ${MOUNT_POINT}/tpch-data/sf<scale_factor>/
Temp files will be written to: ${MOUNT_POINT}/duckdb/temp/
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
OUTPUT_FILE=""  # Will be set to absolute path later
QUERY_ARGS=()  # Array to store query numbers
MEMORY_LIMIT=""  # Memory limit in MB
THREADS=""  # Number of threads

while [[ $# -gt 0 ]]; do
  case $1 in
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="$(realpath "$2")"  # Convert to absolute path
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

# Set default output file if not specified
if [[ -z "${OUTPUT_FILE}" ]]; then
  OUTPUT_FILE="$(pwd)/tpch-sf${SCALE_FACTOR}-duckdb-results.json"
fi

echo "=== DuckDB TPC-H Benchmark ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Iterations: ${ITERATIONS}"
echo "Output File: ${OUTPUT_FILE}"
echo

# Set paths
DATA_DIR="${MOUNT_POINT}/tpch-data/sf${SCALE_FACTOR}"
TEMP_DIR="${MOUNT_POINT}/duckdb/temp"

# Check if data directory exists
if [[ ! -d "${DATA_DIR}" ]]; then
  echo "Error: Data directory not found: ${DATA_DIR}"
  echo "Please run generate-tpch-data.sh first to generate the data."
  exit 1
fi

echo ">>> Data directory: ${DATA_DIR}"
echo ">>> Temp directory (for spill): ${TEMP_DIR}"
echo

# Create temp directory if it doesn't exist
mkdir -p "${TEMP_DIR}"

# Install DuckDB if needed
echo ">>> Checking for DuckDB installation..."
if ! command -v duckdb &> /dev/null; then
  echo ">>> DuckDB not found. Installing..."

  # Detect OS
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    # Check for unzip utility
    if ! command -v unzip &> /dev/null; then
      echo ">>> unzip not found. Installing unzip..."
      sudo apt-get update -qq
      sudo apt-get install -y unzip
    fi

    # Detect architecture
    ARCH=$(uname -m)
    if [[ "$ARCH" == "x86_64" ]]; then
      DUCKDB_ARCH="amd64"
    elif [[ "$ARCH" == "aarch64" ]] || [[ "$ARCH" == "arm64" ]]; then
      DUCKDB_ARCH="arm64"
    else
      echo "Error: Unsupported architecture: $ARCH"
      exit 1
    fi

    echo ">>> Downloading DuckDB for Linux ($ARCH)..."
    DUCKDB_URL="https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-${DUCKDB_ARCH}.zip"
    if ! wget --show-progress -q "$DUCKDB_URL" -O /tmp/duckdb.zip; then
      echo "Error: Failed to download DuckDB from $DUCKDB_URL"
      exit 1
    fi

    if ! unzip -q /tmp/duckdb.zip -d /tmp/; then
      echo "Error: Failed to unzip DuckDB"
      rm /tmp/duckdb.zip
      exit 1
    fi

    sudo mv /tmp/duckdb /usr/local/bin/
    sudo chmod +x /usr/local/bin/duckdb
    rm /tmp/duckdb.zip
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    echo ">>> Installing DuckDB via Homebrew..."
    brew install duckdb
  else
    echo "Error: Unsupported OS: $OSTYPE"
    exit 1
  fi

  echo ">>> DuckDB installed successfully"
else
  echo ">>> DuckDB already installed: $(duckdb --version)"
fi

echo

# Check for Python DuckDB package
echo ">>> Checking for Python DuckDB package..."
if ! python3 -c "import duckdb" &> /dev/null; then
  echo ">>> Python DuckDB package not found. Installing..."
  pip3 install duckdb --break-system-packages --quiet
  echo ">>> Python DuckDB package installed successfully"
else
  echo ">>> Python DuckDB package already installed"
fi

echo

# Increase file descriptor limit for this session
CURRENT_LIMIT=$(ulimit -n)
if [[ ${CURRENT_LIMIT} -lt 65536 ]]; then
  echo ">>> Increasing file descriptor limit from ${CURRENT_LIMIT} to 65536..."
  ulimit -n 65536 2>/dev/null || {
    echo "⚠ Warning: Could not increase file descriptor limit to 65536"
    echo "  Current limit: $(ulimit -n)"
    echo "  You may encounter 'Too many open files' errors with large datasets"
    echo ""
  }
  echo "✓ File descriptor limit: $(ulimit -n)"
else
  echo "✓ File descriptor limit already sufficient: ${CURRENT_LIMIT}"
fi
echo

# Clone or update TPC-H queries repository
QUERIES_DIR="${MOUNT_POINT}/duckdb/tpch-queries"
if [[ ! -d "${QUERIES_DIR}" ]]; then
  echo ">>> Cloning TPC-H queries..."
  mkdir -p "$(dirname "${QUERIES_DIR}")"
  git clone https://github.com/duckdb/duckdb.git /tmp/duckdb-repo
  mkdir -p "${QUERIES_DIR}"
  cp -r /tmp/duckdb-repo/extension/tpch/dbgen/queries/* "${QUERIES_DIR}/"
  rm -rf /tmp/duckdb-repo
else
  echo ">>> TPC-H queries already exist at: ${QUERIES_DIR}"
fi

echo

# Use the Python benchmark script from the duckdb directory
BENCHMARK_SCRIPT="${SCRIPT_DIR}/run_benchmark.py"

if [[ ! -f "${BENCHMARK_SCRIPT}" ]]; then
  echo "Error: Benchmark script not found: ${BENCHMARK_SCRIPT}"
  exit 1
fi

echo ">>> Running benchmark..."
echo

# Build Python command
PYTHON_CMD="python3 ${BENCHMARK_SCRIPT} --data-dir ${DATA_DIR} --queries-dir ${QUERIES_DIR} --temp-dir ${TEMP_DIR} --iterations ${ITERATIONS} --output ${OUTPUT_FILE}"

# Add query arguments if specified
for query in "${QUERY_ARGS[@]}"; do
  PYTHON_CMD="${PYTHON_CMD} --query ${query}"
done

# Add memory limit if specified
if [[ -n "${MEMORY_LIMIT}" ]]; then
  PYTHON_CMD="${PYTHON_CMD} --memory-limit ${MEMORY_LIMIT}"
fi

# Add threads if specified
if [[ -n "${THREADS}" ]]; then
  PYTHON_CMD="${PYTHON_CMD} --threads ${THREADS}"
fi

# Run the benchmark
eval "${PYTHON_CMD}"

echo
echo ">>> Benchmark complete!"
echo ">>> Results saved to: ${OUTPUT_FILE}"

