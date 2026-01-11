#!/bin/bash

COMMAND=${1:-start}

echo "=== Running all servers: $COMMAND ==="

cd servers/grepx-task-generator-server
./run.sh $COMMAND
cd ../..

cd servers/grepx-apache-airflow-server
./run.sh $COMMAND
cd ../..

cd servers/grepx-celery-server
./run.sh $COMMAND
cd ../..

cd servers/grepx-dagster-server
./run.sh $COMMAND
cd ../..

echo "=== All servers $COMMAND complete ==="
