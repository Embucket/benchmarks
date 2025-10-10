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
Usage: $0 <scale_factor> [options]

Run DataFusion TPC-H benchmark on generated data.

Arguments:
  scale_factor    The TPC-H scale factor to benchmark (must match generated data)

Options:
  --iterations N  Number of iterations to run (default: 3)
  --output FILE   Output JSON file for results (default: tpch-sf<scale_factor>-results.json)

Examples:
  $0 1                           # Run benchmark on SF1 data
  $0 100 --iterations 5          # Run benchmark on SF100 data with 5 iterations
  $0 10 --output my-results.json # Run benchmark and save to custom file

The script expects data to be at: ${MOUNT_POINT}/datafusion/tpch-sf<scale_factor>/
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
OUTPUT_FILE="tpch-sf${SCALE_FACTOR}-results.json"

while [[ $# -gt 0 ]]; do
  case $1 in
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    *)
      echo "Error: Unknown option $1"
      usage
      ;;
  esac
done

echo "=== DataFusion TPC-H Benchmark ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Iterations: ${ITERATIONS}"
echo "Output File: ${OUTPUT_FILE}"
echo

# Set paths
DATA_DIR="${MOUNT_POINT}/datafusion/tpch-sf${SCALE_FACTOR}"
BENCHMARK_REPO_DIR="${MOUNT_POINT}/datafusion/datafusion-benchmarks"

# Check if data directory exists
if [[ ! -d "${DATA_DIR}" ]]; then
  echo "Error: Data directory not found: ${DATA_DIR}"
  echo "Please run generate-tpch-data.sh first to generate the data."
  exit 1
fi

echo ">>> Data directory: ${DATA_DIR}"
echo ">>> Benchmark repository: ${BENCHMARK_REPO_DIR}"
echo

# Clone or update DataFusion benchmarks repository
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

# Install Python dependencies
echo ">>> Installing Python dependencies..."
if ! command -v python3 &> /dev/null; then
  echo "Error: python3 is not installed"
  exit 1
fi

# Check if pip is available
if ! command -v pip3 &> /dev/null; then
  echo ">>> Installing pip..."
  sudo apt-get update
  sudo apt-get install -y python3-pip
fi

# Install requirements
if [[ -f requirements.txt ]]; then
  pip3 install -r requirements.txt --user
else
  echo "Warning: requirements.txt not found, installing datafusion manually"
  pip3 install datafusion --user
fi

echo

# Run the benchmark
echo ">>> Running TPC-H benchmark..."
echo ">>> This may take a while depending on the scale factor and number of iterations..."
echo

cd "${BENCHMARK_REPO_DIR}/runners/datafusion-python"

# Run the benchmark with specified parameters
python3 tpcbench.py \
  --benchmark tpch \
  --data "${DATA_DIR}" \
  --queries "${BENCHMARK_REPO_DIR}/tpch/queries/" \
  --iterations "${ITERATIONS}" \
  --output "${OUTPUT_FILE}"

echo
echo ">>> Benchmark complete!"
echo ">>> Results saved to: ${OUTPUT_FILE}"
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

echo
echo "Done."

