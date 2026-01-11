#!/bin/bash

set -e

echo "=== Setting up all components ==="

# Step 1: Setup shared models first
cd shared/grepx-shared-models
./setup.sh
cd ../..

# Step 2: Setup task generator (creates database)
cd servers/grepx-task-generator-server
./setup.sh
cd ../..

# Step 3: Setup Celery
cd servers/grepx-celery-server
./setup.sh
cd ../..

# Step 4: Setup Dagster
cd servers/grepx-dagster-server
./setup.sh
cd ../..

# Step 5: Setup Airflow
cd servers/grepx-apache-airflow-server
./setup.sh
cd ../..

echo "=== Setup complete ==="
