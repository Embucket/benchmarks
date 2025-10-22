#!/bin/bash

set -e  # Exit on error

echo "=========================================="
echo "dbt-snowplow-web Incremental Benchmark"
echo "=========================================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found"
    echo "Please create .env file from env.template and configure Snowflake credentials"
    exit 1
fi

# Load environment variables
source .env

# Default scale factor
SCALE_FACTOR=${1:-0.2}

echo "Configuration:"
echo "  Scale Factor: ${SCALE_FACTOR} GB"
echo "  Database: ${SNOWFLAKE_DATABASE}"
echo "  Warehouse: ${SNOWFLAKE_WAREHOUSE}"
echo ""

# Step 1: Setup Python environment and install dependencies
echo "=========================================="
echo "Step 1: Setting up Python environment"
echo "=========================================="
python3 -m venv env
source env/bin/activate

./dbt.sh

source env/bin/activate

# Step 2: Generate test data
echo ""
echo "=========================================="
echo "Step 2: Generating test data"
echo "=========================================="

# Check if data files already exist
if [ -f "events_yesterday.csv" ] && [ -f "events_today.csv" ]; then
    echo "Data files already exist:"
    echo "  - events_yesterday.csv ($(du -h events_yesterday.csv | cut -f1))"
    echo "  - events_today.csv ($(du -h events_today.csv | cut -f1))"
    echo ""
    read -p "Do you want to regenerate the data? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Regenerating data..."
        python3 gen_events.py --gb ${SCALE_FACTOR}
    else
        echo "Skipping data generation, using existing files"
    fi
else
    echo "Generating new data files..."
    python3 gen_events.py --gb ${SCALE_FACTOR}
fi

# Step 3: Setup dbt-snowplow-web
echo ""
echo "=========================================="
echo "Step 3: Setting up dbt-snowplow-web"
echo "=========================================="
./snowplow_web.sh

# Step 4: First run - Load yesterday's data
echo ""
echo "=========================================="
echo "Step 4: First Run - Yesterday's Data"
echo "=========================================="
echo "Loading events_yesterday.csv..."
python3 load_events.py --yesterday

cd dbt-snowplow-web/

echo "Running dbt (first run)..."
dbt debug
dbt clean
dbt deps
dbt seed
dbt run --vars '{snowplow__enable_consent: true, snowplow__enable_cwv: true, snowplow__enable_iab: true, snowplow__enable_ua: true, snowplow__enable_yauaa: true, snowplow__start_date: '2025-10-01', snowplow__backfill_limit_days: 50, snowplow__cwv_days_to_measure: 999}'

echo ""
echo "✓ First run complete"

# Step 5: Second run - Load combined data (incremental)
echo ""
echo "=========================================="
echo "Step 5: Second Run - Combined Data (Incremental)"
echo "=========================================="
cd ..

echo "Loading combined data (yesterday + today)..."
python3 load_events.py --combined

cd dbt-snowplow-web/

echo "Running dbt (incremental run)..."
dbt debug
dbt clean
dbt deps
dbt seed
dbt run --vars '{snowplow__enable_consent: true, snowplow__enable_cwv: true, snowplow__enable_iab: true, snowplow__enable_ua: true, snowplow__enable_yauaa: true, snowplow__start_date: '2025-10-01', snowplow__backfill_limit_days: 50, snowplow__cwv_days_to_measure: 999}'

echo ""
echo "✓ Second run complete"

# Summary
echo ""
echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
echo "Results can be found in dbt-snowplow-web/target/"
echo ""
