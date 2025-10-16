#!/usr/bin/env bash
set -euo pipefail

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../env.sh"

# Usage function
usage() {
  cat <<EOF
Usage: $0 <scale_factor>

Download TPC-H DuckDB database file for benchmarking.

Arguments:
  scale_factor    The TPC-H scale factor (currently only 1000 is available)

Examples:
  $0 1000         # Download SF1000 database file

The database will be downloaded to: ${MOUNT_POINT}/duckdb/tpch-sf<scale_factor>.db
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

# Validate scale factor is a positive number
if ! [[ "${SCALE_FACTOR}" =~ ^[0-9]+$ ]] || [[ "${SCALE_FACTOR}" -le 0 ]]; then
  echo "Error: Scale factor must be a positive integer"
  echo
  usage
fi

# Currently only SF1000 is available from DuckDB
if [[ "${SCALE_FACTOR}" != "1000" ]]; then
  echo "Error: Currently only scale factor 1000 is available for download"
  echo "Available: http://blobs.duckdb.org/data/tpch-sf1000.db"
  exit 1
fi

echo "=== DuckDB TPC-H Database Download ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo

# Set paths
DB_DIR="${MOUNT_POINT}/duckdb"
DB_FILE="${DB_DIR}/tpch-sf${SCALE_FACTOR}.db"

# Create directory if it doesn't exist
mkdir -p "${DB_DIR}"

# Check if file already exists
if [[ -f "${DB_FILE}" ]]; then
  echo ">>> Database file already exists: ${DB_FILE}"
  echo ">>> File size: $(du -h "${DB_FILE}" | cut -f1)"
  read -p "Do you want to re-download? (y/N): " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo ">>> Skipping download"
    exit 0
  fi
  echo ">>> Removing existing file..."
  rm -f "${DB_FILE}"
fi

# Download the database file
DB_URL="http://blobs.duckdb.org/data/tpch-sf${SCALE_FACTOR}.db"
echo ">>> Downloading from: ${DB_URL}"
echo ">>> Destination: ${DB_FILE}"
echo

# Use wget or curl depending on what's available
if command -v wget &> /dev/null; then
  echo ">>> Using wget to download..."
  if ! wget --show-progress -O "${DB_FILE}" "${DB_URL}"; then
    echo "Error: Failed to download database file"
    rm -f "${DB_FILE}"
    exit 1
  fi
elif command -v curl &> /dev/null; then
  echo ">>> Using curl to download..."
  if ! curl -L --progress-bar -o "${DB_FILE}" "${DB_URL}"; then
    echo "Error: Failed to download database file"
    rm -f "${DB_FILE}"
    exit 1
  fi
else
  echo "Error: Neither wget nor curl is available"
  echo "Please install wget or curl to download the database file"
  exit 1
fi

echo
echo ">>> Download complete!"
echo ">>> Database file: ${DB_FILE}"
echo ">>> File size: $(du -h "${DB_FILE}" | cut -f1)"
echo
echo "You can now run benchmarks using the 'internal' mode:"
echo "  ./run-tpch-benchmark.sh ${SCALE_FACTOR} --mode internal"

