#!/usr/bin/env bash
# Monitor temporary directory usage during benchmark execution

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

# Usage function
usage() {
  cat <<EOF
Usage: $0 <system>

Monitor temp directory usage for a specific benchmark system.

Arguments:
  system    The benchmark system to monitor (datafusion, duckdb, etc.)

Examples:
  $0 datafusion    # Monitor DataFusion temp directory
  $0 duckdb        # Monitor DuckDB temp directory

The script will monitor: ${MOUNT_POINT}/<system>/temp/
EOF
  exit 1
}

# Check if system argument is provided
if [[ $# -ne 1 ]]; then
  echo "Error: System argument is required"
  echo
  usage
fi

SYSTEM="$1"

# Set temp directory based on system
TEMP_DIR="${MOUNT_POINT}/${SYSTEM}/temp"

# Check if temp directory exists
if [[ ! -d "${TEMP_DIR}" ]]; then
  echo "Warning: Temp directory does not exist: ${TEMP_DIR}"
  echo "Creating directory..."
  mkdir -p "${TEMP_DIR}"
fi

echo "Monitoring temp directory for ${SYSTEM}: ${TEMP_DIR}"
echo "Press Ctrl+C to stop"
echo ""
echo "Timestamp                | Files | Total Size | Details"
echo "-------------------------|-------|------------|------------------"

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    
    if [[ -d "${TEMP_DIR}" ]]; then
        FILE_COUNT=$(find "${TEMP_DIR}" -type f 2>/dev/null | wc -l)
        TOTAL_SIZE=$(du -sh "${TEMP_DIR}" 2>/dev/null | awk '{print $1}')
        
        # Get details of largest files
        LARGEST=$(find "${TEMP_DIR}" -type f -exec ls -lh {} \; 2>/dev/null | sort -k5 -hr | head -3 | awk '{print $5}' | tr '\n' ' ')
        
        printf "%s | %5d | %10s | %s\n" "$TIMESTAMP" "$FILE_COUNT" "$TOTAL_SIZE" "$LARGEST"
    else
        printf "%s | %5s | %10s | Directory not found\n" "$TIMESTAMP" "N/A" "N/A"
    fi
    
    sleep 1
done

