#!/bin/bash
set -e

# Install dependencies
if [ -e "/opt/airflow/requirements.txt" ]; then
  pip install --user -r /opt/airflow/requirements.txt
fi

# Initialize the Airflow DB
airflow db upgrade

# Create an admin user if not exists
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Start the webserver
exec airflow webserver