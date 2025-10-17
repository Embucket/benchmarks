#!/usr/bin/env bash
set -euo pipefail

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../env.sh"

# Usage function
usage() {
  cat <<EOF
Usage: $0 <scale_factor> --mode <MODE> [options]

Run TPC-H benchmark using DuckDB.

Arguments:
  scale_factor    The TPC-H scale factor (e.g., 1, 10, 100, 1000)

Required Options:
  --mode MODE              Benchmark mode: 'parquet', 'parquet-s3', or 'internal'
                           - parquet: Use parquet files from ${MOUNT_POINT}/tpch-data/sf<scale_factor>/
                           - parquet-s3: Use parquet files from S3 (s3://embucket-testdata/tpch/<scale_factor>)
                           - internal: Use DuckDB database file from ${MOUNT_POINT}/duckdb/tpch-sf<scale_factor>.db

Optional Arguments:
  --iterations N           Number of iterations to run (default: 3)
  --output FILE            Output JSON file name (will be saved in results-<mode>/ directory)
  --query N                Run only specific query number (can be specified multiple times)
  --memory-limit MB        Memory limit in MB (e.g., --memory-limit 10240 for 10GB)
  --threads N              Number of threads to use (default: all available cores)

Results:
  All results are saved to: results-<mode>/
  - Existing files in the directory are deleted before each run
  - Results JSON includes timestamp of the benchmark run
  - Query execution plans are saved alongside results

Examples:
  $0 1 --mode parquet                     # Run all queries on SF1 parquet data
  $0 10 --mode parquet --iterations 5     # Run 5 iterations on SF10 parquet data
  $0 100 --mode parquet --query 1 --query 6  # Run only queries 1 and 6 on SF100 parquet data
  $0 1000 --mode internal                 # Run on SF1000 using internal database file
  $0 1000 --mode parquet --memory-limit 190000  # Run on SF1000 parquet with 190GB memory limit
  $0 1 --mode parquet-s3                  # Run all queries on SF1 parquet data from S3

Parquet mode uses data from: ${MOUNT_POINT}/tpch-data/sf<scale_factor>/
Parquet-S3 mode uses data from: s3://embucket-testdata/tpch/<scale_factor>
Internal mode uses database from: ${MOUNT_POINT}/duckdb/tpch-sf<scale_factor>.db
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
MODE=""  # Mode is now required
ITERATIONS=3
OUTPUT_FILE=""  # Will be set to absolute path later
QUERY_ARGS=()  # Array to store query numbers
MEMORY_LIMIT=""  # Memory limit in MB
THREADS=""  # Number of threads

while [[ $# -gt 0 ]]; do
  case $1 in
    --mode)
      MODE="$2"
      if [[ "${MODE}" != "parquet" && "${MODE}" != "parquet-s3" && "${MODE}" != "internal" ]]; then
        echo "Error: Invalid mode '${MODE}'. Must be 'parquet', 'parquet-s3', or 'internal'"
        usage
      fi
      shift 2
      ;;
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

# Validate that mode is specified
if [[ -z "${MODE}" ]]; then
  echo "Error: --mode argument is required"
  echo
  usage
fi

# Fetch EC2 instance type for directory organization
echo ">>> Detecting EC2 instance type..."
EC2_INSTANCE_TYPE=$(python3 -c "
import urllib.request
import urllib.error

try:
    # IMDSv2 requires a token
    token_url = 'http://169.254.169.254/latest/api/token'
    token_request = urllib.request.Request(
        token_url,
        headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'},
        method='PUT'
    )

    with urllib.request.urlopen(token_request, timeout=2) as response:
        token = response.read().decode('utf-8')

    # Use token to get instance type
    metadata_url = 'http://169.254.169.254/latest/meta-data/instance-type'
    metadata_request = urllib.request.Request(
        metadata_url,
        headers={'X-aws-ec2-metadata-token': token}
    )

    with urllib.request.urlopen(metadata_request, timeout=2) as response:
        instance_type = response.read().decode('utf-8').strip()

    print(instance_type)
except:
    print('unknown')
" 2>/dev/null || echo "unknown")

if [[ "${EC2_INSTANCE_TYPE}" == "unknown" ]]; then
  echo "⚠ Warning: Could not detect EC2 instance type. Using 'unknown' as directory name."
  echo "  Results will be saved to: results-${MODE}/unknown/"
else
  echo "✓ Detected EC2 instance type: ${EC2_INSTANCE_TYPE}"
fi

# Create results directory with mode and EC2 instance type
RESULTS_DIR="$(pwd)/results-${MODE}/${EC2_INSTANCE_TYPE}"

# If results directory exists and has files, delete them
if [[ -d "${RESULTS_DIR}" ]]; then
  FILE_COUNT=$(find "${RESULTS_DIR}" -type f | wc -l)
  if [[ ${FILE_COUNT} -gt 0 ]]; then
    echo ">>> Cleaning existing results directory: ${RESULTS_DIR}"
    echo ">>> Removing ${FILE_COUNT} existing file(s)..."
    rm -f "${RESULTS_DIR}"/*
  fi
fi

# Create results directory
mkdir -p "${RESULTS_DIR}"

# Generate timestamp for results
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

# Set default output file if not specified
if [[ -z "${OUTPUT_FILE}" ]]; then
  OUTPUT_FILE="${RESULTS_DIR}/tpch-sf${SCALE_FACTOR}-${MODE}-results.json"
else
  # If user specified output file, move it to results directory but keep the filename
  OUTPUT_BASENAME=$(basename "${OUTPUT_FILE}")
  OUTPUT_FILE="${RESULTS_DIR}/${OUTPUT_BASENAME}"
fi

echo "=== DuckDB TPC-H Benchmark ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Mode: ${MODE}"
echo "Iterations: ${ITERATIONS}"
echo "Results Directory: ${RESULTS_DIR}"
echo "Output File: ${OUTPUT_FILE}"
echo

# Set paths based on mode
TEMP_DIR="${MOUNT_POINT}/duckdb/temp"

if [[ "${MODE}" == "parquet" ]]; then
  DATA_DIR="${MOUNT_POINT}/tpch-data/sf${SCALE_FACTOR}"
  DB_FILE=""

  # Check if data directory exists
  if [[ ! -d "${DATA_DIR}" ]]; then
    echo "Error: Data directory not found: ${DATA_DIR}"
    echo "Please run generate-tpch-data.sh first to generate the data."
    exit 1
  fi

  echo ">>> Data directory: ${DATA_DIR}"
  echo ">>> Temp directory (for spill): ${TEMP_DIR}"
  echo
elif [[ "${MODE}" == "parquet-s3" ]]; then
  DATA_DIR="s3://embucket-testdata/tpch/${SCALE_FACTOR}"
  DB_FILE=""

  echo ">>> S3 data path: ${DATA_DIR}"
  echo ">>> Temp directory (for spill): ${TEMP_DIR}"
  echo
else  # internal mode
  DATA_DIR=""
  DB_FILE="${MOUNT_POINT}/duckdb/tpch-sf${SCALE_FACTOR}.db"

  # Check if database file exists
  if [[ ! -f "${DB_FILE}" ]]; then
    echo "Error: Database file not found: ${DB_FILE}"
    echo "Please run download-tpch-db.sh first to download the database file."
    exit 1
  fi

  echo ">>> Database file: ${DB_FILE}"
  echo ">>> Database size: $(du -h "${DB_FILE}" | cut -f1)"
  echo ">>> Temp directory (for spill): ${TEMP_DIR}"
  echo
fi

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
BENCHMARK_SCRIPT="${SCRIPT_DIR}/execute_queries.py"

if [[ ! -f "${BENCHMARK_SCRIPT}" ]]; then
  echo "Error: Benchmark script not found: ${BENCHMARK_SCRIPT}"
  exit 1
fi

echo ">>> Running benchmark..."
echo

# Build Python command based on mode
PYTHON_CMD="python3 ${BENCHMARK_SCRIPT} --queries-dir ${QUERIES_DIR} --temp-dir ${TEMP_DIR} --iterations ${ITERATIONS} --output ${OUTPUT_FILE} --mode ${MODE} --timestamp \"${TIMESTAMP}\""

if [[ "${MODE}" == "parquet" || "${MODE}" == "parquet-s3" ]]; then
  PYTHON_CMD="${PYTHON_CMD} --data-dir ${DATA_DIR}"
else
  PYTHON_CMD="${PYTHON_CMD} --db-file ${DB_FILE}"
fi

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
echo ">>> Results directory: ${RESULTS_DIR}"
echo ">>> Results file: ${OUTPUT_FILE}"

# Add EC2 metadata to results
echo
echo ">>> Adding EC2 metadata to results..."
EC2_METADATA_SCRIPT="${SCRIPT_DIR}/../add_ec2_metadata.py"

if [[ -f "${EC2_METADATA_SCRIPT}" ]]; then
  python3 "${EC2_METADATA_SCRIPT}" "${OUTPUT_FILE}"
else
  echo "⚠ Warning: EC2 metadata script not found: ${EC2_METADATA_SCRIPT}"
  echo "  Skipping EC2 metadata collection"
  echo "  Please add 'ec2_instance_type' and 'usd_per_hour' manually to the result file"
fi

