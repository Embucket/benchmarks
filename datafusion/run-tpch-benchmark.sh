#!/usr/bin/env bash
set -euo pipefail

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../env.sh"

# Ensure cargo bin directory is in PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Usage function
usage() {
  cat <<EOF
Usage: $0 <scale_factor> --mode <MODE> [options]

Run DataFusion TPC-H benchmark on generated data using datafusion-cli.

Note: This script uses datafusion-cli instead of the Python datafusion library
      to avoid memory issues with certain queries (especially query 21).

Arguments:
  scale_factor    The TPC-H scale factor to benchmark (must match generated data)

Required Options:
  --mode MODE              Data source mode: 'parquet' (local files) or 'parquet-s3' (S3)

Optional Arguments:
  --iterations N           Number of iterations to run (default: 3)
  --output FILE            Output JSON file for results (default: tpch-sf<scale_factor>-<mode>-results.json)
  --query N                Run only specific query number (can be specified multiple times)

Examples:
  $0 1 --mode parquet                         # Run all queries on SF1 local data
  $0 1 --mode parquet-s3                      # Run all queries on SF1 data from S3
  $0 100 --mode parquet --iterations 5        # Run all queries on SF100 data with 5 iterations
  $0 10 --mode parquet --output my-results.json      # Run all queries and save to custom file
  $0 1 --mode parquet --query 18              # Run only query 18 on SF1 data
  $0 1 --mode parquet --query 1 --query 18    # Run only queries 1 and 18 on SF1 data

For local mode (parquet):
  The script expects data to be at: ${MOUNT_POINT}/tpch-data/sf<scale_factor>/

For S3 mode (parquet-s3):
  The script uses data from: s3://embucket-testdata/tpch/<scale_factor>/
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
MODE=""  # Required - no default
ITERATIONS=3
OUTPUT_FILE=""  # Will be set to absolute path later
QUERY_ARGS=()  # Array to store --query arguments

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
      OUTPUT_FILE="$(realpath "$2")"  # Convert to absolute path
      shift 2
      ;;
    --query)
      QUERY_ARGS+=("--query" "$2")
      shift 2
      ;;
    *)
      echo "Error: Unknown option $1"
      usage
      ;;
  esac
done

# Validate mode is provided
if [[ -z "${MODE}" ]]; then
  echo "Error: --mode is required"
  usage
fi

# Validate mode value
if [[ "${MODE}" != "parquet" && "${MODE}" != "parquet-s3" ]]; then
  echo "Error: Invalid mode '${MODE}'. Must be 'parquet' or 'parquet-s3'"
  usage
fi

# Set default output file to results directory if not specified
if [[ -z "${OUTPUT_FILE}" ]]; then
  OUTPUT_FILE="${RESULTS_DIR}/tpch-sf${SCALE_FACTOR}-${MODE}-results.json"
else
  # If user specified output file, move it to results directory but keep the filename
  OUTPUT_BASENAME=$(basename "${OUTPUT_FILE}")
  OUTPUT_FILE="${RESULTS_DIR}/${OUTPUT_BASENAME}"
fi

echo "=== DataFusion TPC-H Benchmark ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Mode: ${MODE}"
echo "Iterations: ${ITERATIONS}"
echo "Results Directory: ${RESULTS_DIR}"
echo "Output File: ${OUTPUT_FILE}"
echo

# Set paths based on mode
BENCHMARK_REPO_DIR="${MOUNT_POINT}/datafusion/datafusion-benchmarks"

if [[ "${MODE}" == "parquet-s3" ]]; then
  DATA_DIR="s3://embucket-testdata/tpch/${SCALE_FACTOR}_partitioned"
  echo ">>> S3 data path: ${DATA_DIR}"
else
  DATA_DIR="${MOUNT_POINT}/tpch-data/sf${SCALE_FACTOR}"

  # Check if data directory exists for local mode
  if [[ ! -d "${DATA_DIR}" ]]; then
    echo "Error: Data directory not found: ${DATA_DIR}"
    echo "Please run generate-tpch-data.sh first to generate the data."
    exit 1
  fi

  echo ">>> Data directory: ${DATA_DIR}"
  echo ">>> Benchmark repository: ${BENCHMARK_REPO_DIR}"
fi

echo

# Check if datafusion-cli is installed
if ! command -v datafusion-cli &> /dev/null; then
  echo "Error: datafusion-cli is not installed"
  echo "Please install it with: cargo install datafusion-cli"
  exit 1
fi

echo ">>> Checking datafusion-cli version..."
datafusion-cli --version

echo

# Clone or update DataFusion benchmarks repository (for query files)
if [[ -d "${BENCHMARK_REPO_DIR}" ]]; then
  echo ">>> DataFusion benchmarks repository already exists"
  echo ">>> Updating repository..."
  cd "${BENCHMARK_REPO_DIR}"
  git pull
else
  echo ">>> Cloning DataFusion benchmarks repository..."
  mkdir -p "$(dirname "${BENCHMARK_REPO_DIR}")"
  git clone https://github.com/apache/datafusion-benchmarks.git "${BENCHMARK_REPO_DIR}"
  cd "${BENCHMARK_REPO_DIR}"
fi

echo

# Run the benchmark
echo ">>> Running TPC-H benchmark..."
echo ">>> This may take a while depending on the scale factor and number of iterations..."
echo

# Build the command with optional parameters
CMD_ARGS=(
  --data-dir "${DATA_DIR}"
  --queries-dir "${BENCHMARK_REPO_DIR}/tpch/queries"
  --iterations "${ITERATIONS}"
  --output "${OUTPUT_FILE}"
)

# Add query arguments if specified
if [[ ${#QUERY_ARGS[@]} -gt 0 ]]; then
  CMD_ARGS+=("${QUERY_ARGS[@]}")
fi

# Add mode parameter
CMD_ARGS+=(--mode "${MODE}")

# Run the benchmark with our execute_queries.py script
python3 "${SCRIPT_DIR}/execute_queries.py" "${CMD_ARGS[@]}"

echo
echo ">>> Benchmark complete!"
echo ">>> Results saved to: ${OUTPUT_FILE}"
echo ">>> Full path: $(realpath "${OUTPUT_FILE}" 2>/dev/null || echo "${OUTPUT_FILE}")"
echo

# Display summary if jq is available
if command -v jq &> /dev/null; then
  echo ">>> Summary of results:"
  jq '.' "${OUTPUT_FILE}" || cat "${OUTPUT_FILE}"
else
  echo ">>> Install 'jq' to see formatted results"
  echo ">>> Raw results:"
  cat "${OUTPUT_FILE}"
fi

echo ">>> Done! Results saved to ${OUTPUT_FILE}"
