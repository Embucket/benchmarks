#!/usr/bin/env bash
# Monitor temporary directory usage during benchmark execution

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../env.sh"

TEMP_DIR="${MOUNT_POINT}/datafusion/temp"

echo "Monitoring temp directory: ${TEMP_DIR}"
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

