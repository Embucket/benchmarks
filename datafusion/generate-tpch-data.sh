#!/usr/bin/env bash
set -euo pipefail

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../env.sh"

# Usage function
usage() {
  cat <<EOF
Usage: $0 <scale_factor>

Generate TPC-H benchmark data at the specified scale factor.

Arguments:
  scale_factor    The TPC-H scale factor (e.g., 1, 10, 100, 1000)
                  Scale factor N generates approximately N GB of data

Examples:
  $0 1      # Generate ~1GB of TPC-H data
  $0 10     # Generate ~10GB of TPC-H data
  $0 100    # Generate ~100GB of TPC-H data

The data will be generated under: ${MOUNT_POINT}/datafusion/tpch-sf<scale_factor>/
EOF
  exit 1
}

# Check if scale factor argument is provided
if [[ $# -ne 1 ]]; then
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

echo "=== TPC-H Data Generation ==="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Mount Point: ${MOUNT_POINT}"
echo

# Check if tpchgen-cli is installed
install_tpchgen() {
  echo ">>> Checking for tpchgen-cli installation..."
  
  if command -v tpchgen &> /dev/null; then
    echo ">>> tpchgen-cli is already installed"
    tpchgen --version || true
    return 0
  fi
  
  echo ">>> tpchgen-cli not found. Installing..."
  
  # Check if cargo is installed
  if ! command -v cargo &> /dev/null; then
    echo ">>> Cargo (Rust) is not installed. Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
  fi
  
  echo ">>> Installing tpchgen-cli via cargo..."
  cargo install tpchgen-cli
  
  # Verify installation
  if command -v tpchgen &> /dev/null; then
    echo ">>> tpchgen-cli successfully installed"
    tpchgen --version || true
  else
    echo "Error: Failed to install tpchgen-cli"
    exit 1
  fi
}

# Install tpchgen-cli if needed
install_tpchgen

# Create output directory under datafusion subdirectory
OUTPUT_DIR="${MOUNT_POINT}/datafusion/tpch-sf${SCALE_FACTOR}"

echo
echo ">>> Output directory: ${OUTPUT_DIR}"

# Check if directory already exists and has data
if [[ -d "${OUTPUT_DIR}" ]] && [[ -n "$(ls -A "${OUTPUT_DIR}" 2>/dev/null)" ]]; then
  echo ">>> Warning: Directory ${OUTPUT_DIR} already exists and contains files"
  read -p "Do you want to overwrite existing data? (y/N): " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo ">>> Aborted by user"
    exit 0
  fi
  echo ">>> Removing existing data..."
  rm -rf "${OUTPUT_DIR}"
fi

# Create the directory
mkdir -p "${OUTPUT_DIR}"

echo
echo ">>> Generating TPC-H data at scale factor ${SCALE_FACTOR}..."
echo ">>> This may take a while depending on the scale factor..."
echo

# Generate TPC-H data
# The tpchgen command generates data in the current directory, so we need to cd there
cd "${OUTPUT_DIR}"

# Run tpchgen with the specified scale factor
# Using all available CPU cores for parallel generation
CORES=$(nproc)
echo ">>> Using ${CORES} CPU cores for parallel generation"

tpchgen --scale-factor "${SCALE_FACTOR}" --num-chunks "${CORES}"

echo
echo ">>> TPC-H data generation complete!"
echo
echo ">>> Generated files:"
ls -lh "${OUTPUT_DIR}"
echo
echo ">>> Disk usage:"
du -sh "${OUTPUT_DIR}"
echo
echo ">>> Data location: ${OUTPUT_DIR}"
echo "Done."

