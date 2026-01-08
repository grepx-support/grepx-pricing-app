#!/bin/bash

set -e

echo "=== Setting up all components ==="

cd shared/grepx-shared-models
./setup.sh
cd ../..

cd servers/grepx-celery-server
./setup.sh
cd ../..

cd servers/grepx-dagster-server
./setup.sh
cd ../..

cd servers/grepx-task-generator-server
./setup.sh
cd ../..

echo "=== Setup complete ==="
