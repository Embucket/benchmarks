#!/bin/bash


git clone https://github.com/snowplow/dbt-snowplow-web.git

cd dbt-snowplow-web/

export DBT_PROFILES_DIR="$(pwd)/dbt-snowplow-web"

cat > profiles.yml <<EOF
default:
  target: snowflake
  outputs:
    snowflake:
      type: snowflake
      threads: 4
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA') }}"
      client_session_keep_alive: True
EOF
 