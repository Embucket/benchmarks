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

../dbt.sh

source env/bin/activate

pip install -U pip setuptools wheel
pip install playwright
python -m playwright install chromium

# Step 2: Generate test data
echo ""
echo "=========================================="
echo "Step 2: Generating test data"
echo "=========================================="

# Check if data files already exist (in parent directory)
if [ -f "../events_yesterday.csv" ] && [ -f "../events_today.csv" ]; then
    echo "Data files already exist:"
    echo "  - ../events_yesterday.csv ($(du -h ../events_yesterday.csv | cut -f1))"
    echo "  - ../events_today.csv ($(du -h ../events_today.csv | cut -f1))"
    echo ""
    read -p "Do you want to regenerate the data? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Regenerating data..."
        (cd .. && python3 gen_events.py --gb ${SCALE_FACTOR})
    else
        echo "Skipping data generation, using existing files"
    fi
else
    echo "Generating new data files..."
    (cd .. && python3 gen_events.py --gb ${SCALE_FACTOR})
fi

# Step 3: Setup dbt-snowplow-web
echo ""
echo "=========================================="
echo "Step 3: Setting up dbt-snowplow-web"
echo "=========================================="
../snowplow_web.sh

# Step 4: First run - Load yesterday's data
echo ""
echo "=========================================="
echo "Step 4: First Run - Yesterday's Data"
echo "=========================================="
echo "Loading events_yesterday.csv..."
python3 load_events.py --yesterday

cd dbt-snowplow-web/

echo "Preparing dbt environment..."
dbt debug --target snowflake
dbt clean --target snowflake
dbt deps --target snowflake
echo "Loading seed files (reference data)..."
dbt seed --full-refresh --target snowflake

echo ""
echo "=========================================="
echo "PAUSED - Ready for First dbt Run"
echo "=========================================="
echo "Setup complete. Seed files loaded, yesterday's data loaded."
echo "You can now review the dashboard state before dbt models are built."
echo ""
echo "Press ENTER to run dbt and build the models..."
read -r

echo "Running dbt to build models (first run)..."
dbt run --full-refresh --target snowflake --vars '{snowplow__enable_consent: true, snowplow__enable_cwv: true, snowplow__enable_iab: true, snowplow__enable_ua: true, snowplow__enable_yauaa: true, snowplow__start_date: '2025-09-01', snowplow__backfill_limit_days: 50, snowplow__cwv_days_to_measure: 999}'

# Save run results for first run (outside target/ to survive dbt clean)
echo "Saving first run results..."
cp target/run_results.json ../run_results_first_run.json

# Enrich run results with actual row counts from Snowflake
echo "Querying Snowflake for actual row counts..."
cd ..
# Source environment variables for Snowflake connection
set -a
source .env
set +a
# Use virtual environment's Python which has snowflake-connector-python
env/bin/python3 enrich_run_results.py \
  --manifest dbt-snowplow-web/target/manifest.json \
  --run-results run_results_first_run.json \
  --output run_results_first_run_enriched.json

# Generate lineage visualization for first run
echo "Generating lineage visualization for first run..."
python3 ../visualize_lineage.py \
  --manifest dbt-snowplow-web/target/manifest.json \
  --run-results run_results_first_run_enriched.json \
  --output lineage_first_run.html \
  --title "dbt-snowplow-web First Run - Snowflake" \
  --row-label "Rows Created"
cd dbt-snowplow-web

echo ""
echo "✓ First run complete"
echo "✓ Lineage visualization saved to: lineage_first_run.html"
echo ""
echo "=========================================="
echo "PAUSED - Review First Run Results"
echo "=========================================="
echo "You can now review the first run results:"
echo "  - Lineage visualization: lineage_first_run.html"
echo "  - Query Snowflake to check table contents"
echo ""
echo "Press ENTER to continue with the incremental run..."
read -r

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
dbt debug --target snowflake
#dbt clean --target snowflake
dbt deps --target snowflake
dbt seed --target snowflake
dbt run --target snowflake --vars '{snowplow__enable_consent: true, snowplow__enable_cwv: true, snowplow__enable_iab: true, snowplow__enable_ua: true, snowplow__enable_yauaa: true, snowplow__start_date: '2025-09-01', snowplow__backfill_limit_days: 50, snowplow__cwv_days_to_measure: 999}'

# Save run results for incremental run (outside target/ for consistency)
echo "Saving incremental run results..."
cp target/run_results.json ../run_results_incremental_run.json

# Generate lineage visualization for incremental run
echo "Generating lineage visualization for incremental run..."
cd ..
python3 ../visualize_lineage.py \
  --manifest dbt-snowplow-web/target/manifest.json \
  --run-results run_results_incremental_run.json \
  --output lineage_incremental_run.html \
  --title "dbt-snowplow-web Incremental Run - Snowflake" \
  --row-label "Rows Affected"
cd dbt-snowplow-web

echo ""
echo "✓ Second run complete"
echo "✓ Lineage visualization saved to: lineage_incremental_run.html"

# Cleanup temporary files
cd ..
rm -f run_results_first_run.json run_results_first_run_enriched.json run_results_incremental_run.json
cd dbt-snowplow-web

# Generate screenshots for README
echo ""
echo "=========================================="
echo "Generating Screenshots for README"
echo "=========================================="
cd ..
env/bin/python3 generate_screenshots.py
cd dbt-snowplow-web

# Summary
echo ""
echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
echo "Results can be found in dbt-snowplow-web/target/"
echo ""
echo "Lineage visualizations:"
echo "  - First run:       lineage_first_run.html"
echo "  - Incremental run: lineage_incremental_run.html"
echo ""
echo "Screenshots for README:"
echo "  - ../../visualizations/dbt_snowplow_web_first_run_sf.png"
echo "  - ../../visualizations/dbt_snowplow_web_incremental_run_sf.png"
echo ""
