#!/usr/bin/env bash
set -euo pipefail

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Check for env.sh existence
if [[ -f "${SCRIPT_DIR}/../env.sh" ]]; then
  source "${SCRIPT_DIR}/../env.sh"
else
  MOUNT_POINT="${MOUNT_POINT:-/mnt/data}"
fi

# Ensure cargo bin directory is in PATH
export PATH="$HOME/.cargo/bin:$PATH"

# --- DETECT INSTANCE TYPE (Using bench_infra) ---
ROOT_DIR="$(dirname "${SCRIPT_DIR}")"
export PYTHONPATH="${SCRIPT_DIR}/.."
INSTANCE_TYPE=$(python3 -c "import sys sys.path.append('${ROOT_DIR}') try: from bench_infra import common; print(common.get_ec2_instance_type()); except: print('unknown')" 2>/dev/null || echo "unknown")

# Usage function
usage() {
  cat <<EOF
Usage: $0 <scale_factor> --mode <MODE> [options]

Run DataFusion TPC-H benchmark using datafusion-cli.

Arguments:
  scale_factor    The TPC-H scale factor to benchmark

Required Options:
  --mode MODE     Data source mode: 'parquet' or 'parquet-s3'

Optional Arguments:
  --iterations N  Number of iterations (default: 3)
  --output FILE   Output JSON file
  --query N       Run specific query

Examples:
  $0 1 --mode parquet
  $0 100 --mode parquet-s3
EOF
  exit 1
}

# Check arguments
if [[ $# -lt 1 ]]; then
  echo "Error: Scale factor argument is required"
  usage
fi

SCALE_FACTOR="$1"
shift

if ! [[ "${SCALE_FACTOR}" =~ ^[0-9]+$ ]] || [[ "${SCALE_FACTOR}" -le 0 ]]; then
  echo "Error: Scale factor must be a positive integer"
  usage
fi

MODE=""
ITERATIONS=3
OUTPUT_FILE=""
QUERY_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --mode) MODE="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --output) OUTPUT_FILE="$(realpath "$2")"; shift 2 ;;
    --query) QUERY_ARGS+=("--query" "$2"); shift 2 ;;
    *) echo "Error: Unknown option $1"; usage ;;
  esac
done

if [[ -z "${MODE}" ]]; then
  echo "Error: --mode is required"
  usage
fi

if [[ "${MODE}" != "parquet" && "${MODE}" != "parquet-s3" ]]; then
  echo "Error: Invalid mode '${MODE}'"
  usage
fi

# --- PATH GENERATION (UNIFIED) ---
if [[ -z "${OUTPUT_FILE}" ]]; then
  # Matches DuckDB structure: results/datafusion/
  DEFAULT_OUTPUT_DIR="${SCRIPT_DIR}/results-${MODE}"
  mkdir -p "${DEFAULT_OUTPUT_DIR}"
  OUTPUT_FILE="${DEFAULT_OUTPUT_DIR}/${INSTANCE_TYPE}/tpch-sf${SCALE_FACTOR}_${MODE}-results.json"
fi

RESULTS_DIR="$(dirname "${OUTPUT_FILE}")"
mkdir -p "${RESULTS_DIR}"

echo "=== DataFusion TPC-H Benchmark ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Mode: ${MODE}"
echo "Output File: ${OUTPUT_FILE}"
echo

# Set paths based on mode
BENCHMARK_REPO_DIR="${MOUNT_POINT}/datafusion/datafusion-benchmarks"

if [[ "${MODE}" == "parquet-s3" ]]; then
  DATA_DIR="s3://embucket-testdata/tpch/${SCALE_FACTOR}_partitioned"
  echo ">>> S3 data path: ${DATA_DIR}"
else
  DATA_DIR="${MOUNT_POINT}/tpch-data/sf${SCALE_FACTOR}"
  if [[ ! -d "${DATA_DIR}" ]]; then
    echo "Error: Data directory not found: ${DATA_DIR}"
    echo "Please run generate-tpch-data.sh first."
    exit 1
  fi
  echo ">>> Data directory: ${DATA_DIR}"
fi

echo

# --- AUTO-INSTALL LOGIC ---
echo ">>> Checking for datafusion-cli..."

if ! command -v datafusion-cli &> /dev/null; then
  echo ">>> datafusion-cli not found. Checking prerequisites..."

  # Check for Cargo/Rust
  if ! command -v cargo &> /dev/null; then
    echo ">>> Cargo (Rust) not found. Installing Rust..."
    # Install Rust non-interactively
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
    export PATH="$HOME/.cargo/bin:$PATH"
  fi

  echo ">>> Installing datafusion-cli via cargo (this may take a few minutes)..."
  # Install datafusion-cli
  cargo install datafusion-cli
else
  echo ">>> datafusion-cli is installed: $(datafusion-cli --version)"
fi

echo

# Clone DataFusion benchmarks repo
if [[ -d "${BENCHMARK_REPO_DIR}" ]]; then
  echo ">>> Updating DataFusion benchmarks repo..."
  cd "${BENCHMARK_REPO_DIR}"
  git pull
else
  echo ">>> Cloning DataFusion benchmarks repo..."
  mkdir -p "$(dirname "${BENCHMARK_REPO_DIR}")"
  git clone https://github.com/apache/datafusion-benchmarks.git "${BENCHMARK_REPO_DIR}"
  cd "${BENCHMARK_REPO_DIR}"
fi

echo ">>> Running Benchmark..."

# Define the python runner path (handles rename)
if [[ -f "${SCRIPT_DIR}/run_benchmark.py" ]]; then
  RUNNER="${SCRIPT_DIR}/run_benchmark.py"
else
  RUNNER="${SCRIPT_DIR}/execute_queries.py"
fi

CMD_ARGS=(
  --data-dir "${DATA_DIR}"
  --queries-dir "${BENCHMARK_REPO_DIR}/tpch/queries"
  --iterations "${ITERATIONS}"
  --output "${OUTPUT_FILE}"
  --mode "${MODE}"
)

if [[ ${#QUERY_ARGS[@]} -gt 0 ]]; then
  CMD_ARGS+=("${QUERY_ARGS[@]}")
fi

# Run Python
python3 "${RUNNER}" "${CMD_ARGS[@]}"

echo
echo ">>> Benchmark complete!"
echo ">>> Results saved to: ${OUTPUT_FILE}"