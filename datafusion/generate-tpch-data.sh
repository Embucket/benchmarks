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

  if command -v tpchgen-cli &> /dev/null; then
    echo ">>> tpchgen-cli is already installed"
    tpchgen-cli --version || true
    return 0
  fi

  echo ">>> tpchgen-cli not found. Installing..."

  # Install build essentials (C compiler and other build tools)
  echo ">>> Installing build dependencies..."
  if command -v apt-get &> /dev/null; then
    # Debian/Ubuntu
    sudo apt-get update
    sudo apt-get install -y build-essential pkg-config libssl-dev
  elif command -v yum &> /dev/null; then
    # Amazon Linux/RHEL/CentOS
    sudo yum groupinstall -y "Development Tools"
    sudo yum install -y openssl-devel pkg-config
  elif command -v dnf &> /dev/null; then
    # Fedora
    sudo dnf groupinstall -y "Development Tools"
    sudo dnf install -y openssl-devel pkg-config
  else
    echo "Warning: Could not detect package manager. Please install build-essential/gcc manually."
  fi

  # Check if cargo is installed
  if ! command -v cargo &> /dev/null; then
    echo ">>> Cargo (Rust) is not installed. Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # Source cargo environment to make it available in this script
    source "$HOME/.cargo/env"
    export PATH="$HOME/.cargo/bin:$PATH"
  fi

  echo ">>> Installing tpchgen-cli via cargo..."
  cargo install tpchgen-cli

  # Verify installation
  if command -v tpchgen-cli &> /dev/null; then
    echo ">>> tpchgen-cli successfully installed"
    tpchgen-cli --version || true
  else
    echo "Error: Failed to install tpchgen-cli"
    echo "Note: You may need to add ~/.cargo/bin to your PATH"
    echo "Run: export PATH=\"\$HOME/.cargo/bin:\$PATH\""
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

# Run tpchgen-cli with the specified scale factor
# Using all available CPU cores for parallel generation
CORES=$(nproc)
echo ">>> Using ${CORES} CPU cores for parallel generation"
echo ">>> Generating Parquet files..."

tpchgen-cli --scale-factor "${SCALE_FACTOR}" --num-threads "${CORES}" --format parquet

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

