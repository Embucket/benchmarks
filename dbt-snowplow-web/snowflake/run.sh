
#!/bin/bash

python3 -m venv env
source env/bin/activate

./dbt.sh

source env/bin/activate

if [ -f .env ]; then
    source .env
fi

python3 gen_events.py --gb 0.1

python3 load_events.py events.csv

./snowplow_web.sh

cd dbt-snowplow-web/


dbt debug
 dbt clean
 dbt deps
 dbt seed
dbt run --vars '{snowplow__enable_consent: true, snowplow__enable_cwv: true, snowplow__enable_iab: true, snowplow__enable_ua: true, snowplow__enable_yauaa: true}'  