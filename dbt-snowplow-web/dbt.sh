#!/bin/bash


python3 -m venv env
source env/bin/activate
python3 -m pip install --upgrade pip >/dev/null 2>&1
python3 -m pip install dbt-core==1.9.8 dbt-snowflake==1.9.1
python3 -m pip install snowflake-connector-python>=3.0.0


