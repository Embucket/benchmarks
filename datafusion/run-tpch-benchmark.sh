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
  --iterations N           Number of iterations to run (default: 3)
  --output FILE            Output JSON file for results (default: tpch-sf<scale_factor>-results.json)
  --query N                Run only specific query number (can be specified multiple times)
  --memory-limit MB        Memory limit in MB (forces spilling, e.g., --memory-limit 1024 for 1GB)
  --hash-join              Enable hash join preference (default: sort-merge join)
  --max-temp-dir-size GB   Maximum temp directory size in GB (default: 1000GB = 1TB)

Examples:
  $0 1                           # Run all queries on SF1 data
  $0 100 --iterations 5          # Run all queries on SF100 data with 5 iterations
  $0 10 --output my-results.json # Run all queries and save to custom file
  $0 1 --query 18                       # Run only query 18 on SF1 data (sort-merge join)
  $0 1 --query 1 --query 18             # Run only queries 1 and 18 on SF1 data
  $0 1 --query 18 --memory-limit 1024   # Run query 18 with 1GB memory limit (forces spilling)
  $0 1000 --hash-join                   # Run all queries using hash join instead of sort-merge join

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
OUTPUT_FILE=""  # Will be set to absolute path later
QUERY_ARGS=()  # Array to store --query arguments
MEMORY_LIMIT=""  # Memory limit in MB
PREFER_HASH_JOIN="false"  # Default to sort-merge join (better spilling support)
MAX_TEMP_DIR_SIZE="1000"  # Max temp directory size in GB (default 1TB)

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
      QUERY_ARGS+=("--query" "$2")
      shift 2
      ;;
    --memory-limit)
      MEMORY_LIMIT="$2"
      shift 2
      ;;
    --hash-join)
      PREFER_HASH_JOIN="true"
      shift 1
      ;;
    --max-temp-dir-size)
      MAX_TEMP_DIR_SIZE="$2"
      shift 2
      ;;
    *)
      echo "Error: Unknown option $1"
      usage
      ;;
  esac
done

# Set default output file to current directory if not specified
if [[ -z "${OUTPUT_FILE}" ]]; then
  OUTPUT_FILE="$(pwd)/tpch-sf${SCALE_FACTOR}-results.json"
fi

echo "=== DataFusion TPC-H Benchmark ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Iterations: ${ITERATIONS}"
echo "Output File: ${OUTPUT_FILE}"
echo

# Set paths
DATA_DIR="${MOUNT_POINT}/datafusion/tpch-sf${SCALE_FACTOR}"
BENCHMARK_REPO_DIR="${MOUNT_POINT}/datafusion/datafusion-benchmarks"
TEMP_DIR="${MOUNT_POINT}/datafusion/temp"

# Check if data directory exists
if [[ ! -d "${DATA_DIR}" ]]; then
  echo "Error: Data directory not found: ${DATA_DIR}"
  echo "Please run generate-tpch-data.sh first to generate the data."
  exit 1
fi

echo ">>> Data directory: ${DATA_DIR}"
echo ">>> Benchmark repository: ${BENCHMARK_REPO_DIR}"
echo ">>> Temp directory (for spill): ${TEMP_DIR}"
echo

# Create temp directory for DataFusion spill operations
echo ">>> Creating temp directory for DataFusion spill operations..."
mkdir -p "${TEMP_DIR}"

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

# Get Python version for the venv package
PYTHON_VERSION=$(python3 --version | awk '{print $2}' | cut -d. -f1,2)

# Install python3-venv if not available
echo ">>> Ensuring python3-venv is installed..."
sudo apt-get update
sudo apt-get install -y python3-venv python${PYTHON_VERSION}-venv

# Create virtual environment if it doesn't exist
VENV_DIR="${BENCHMARK_REPO_DIR}/venv"
if [[ ! -d "${VENV_DIR}" ]] || [[ ! -f "${VENV_DIR}/bin/python" ]]; then
  echo ">>> Creating Python virtual environment..."
  # Remove broken venv if it exists
  rm -rf "${VENV_DIR}"
  python3 -m venv "${VENV_DIR}"

  # Verify creation
  if [[ ! -f "${VENV_DIR}/bin/activate" ]]; then
    echo "Error: Failed to create virtual environment"
    exit 1
  fi
  echo ">>> Virtual environment created successfully"
else
  echo ">>> Virtual environment already exists"
fi

# Activate virtual environment
echo ">>> Activating virtual environment..."
source "${VENV_DIR}/bin/activate"

# Install requirements
if [[ -f requirements.txt ]]; then
  echo ">>> Installing requirements from requirements.txt..."
  pip install -r requirements.txt
else
  echo "Warning: requirements.txt not found, installing datafusion manually"
  pip install datafusion
fi

echo

# Copy our patched benchmark script
echo ">>> Copying patched benchmark script..."
cp "${SCRIPT_DIR}/tpcbench-patched.py" "${BENCHMARK_REPO_DIR}/runners/datafusion-python/"

# Run the benchmark
echo ">>> Running TPC-H benchmark..."
echo ">>> This may take a while depending on the scale factor and number of iterations..."
echo

cd "${BENCHMARK_REPO_DIR}/runners/datafusion-python"

# Build the command with optional parameters
CMD_ARGS=(
  --benchmark tpch
  --data "${DATA_DIR}"
  --queries "${BENCHMARK_REPO_DIR}/tpch/queries"
  --iterations "${ITERATIONS}"
  --output "${OUTPUT_FILE}"
  --temp-dir "${TEMP_DIR}"
)

# Add query arguments if specified
if [[ ${#QUERY_ARGS[@]} -gt 0 ]]; then
  CMD_ARGS+=("${QUERY_ARGS[@]}")
fi

# Add memory limit if specified
if [[ -n "${MEMORY_LIMIT}" ]]; then
  CMD_ARGS+=(--memory-limit "${MEMORY_LIMIT}")
  echo ">>> Memory limit: ${MEMORY_LIMIT} MB"
fi

# Add prefer-hash-join setting
if [[ "${PREFER_HASH_JOIN}" == "true" ]]; then
  CMD_ARGS+=(--prefer-hash-join)
  echo ">>> Join strategy: Hash join"
else
  echo ">>> Join strategy: Sort-merge join (default)"
fi

# Add max temp directory size (always set, default 1TB)
CMD_ARGS+=(--max-temp-dir-size "${MAX_TEMP_DIR_SIZE}")
echo ">>> Max temp directory size: ${MAX_TEMP_DIR_SIZE} GB"

# Run the benchmark with specified parameters (using python from venv and our patched script)
"${VENV_DIR}/bin/python" tpcbench-patched.py "${CMD_ARGS[@]}"

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

echo
echo "Done."

