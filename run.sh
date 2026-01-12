#!/bin/bash

COMMAND=${1:-start}

echo "=== Running all servers: $COMMAND ==="

# Detect and set PROJECT_ROOT
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Convert Git Bash path to Windows path for compatibility
# /c/Users/... -> C:/Users/...
if [[ "$PROJECT_ROOT" == /[a-z]/* ]]; then
    drive_letter="${PROJECT_ROOT:1:1}"
    rest_of_path="${PROJECT_ROOT:2}"
    PROJECT_ROOT="${drive_letter^^}:${rest_of_path}"
fi

export PROJECT_ROOT

# Load environment variables
if [ -f env.common ]; then
    echo "Loading environment variables from env.common..."
    source env.common
fi

# Expand PROJECT_ROOT in GREPX_MASTER_DB_URL
if [[ "$GREPX_MASTER_DB_URL" == *'${PROJECT_ROOT}'* ]]; then
    export GREPX_MASTER_DB_URL="${GREPX_MASTER_DB_URL//\$\{PROJECT_ROOT\}/$PROJECT_ROOT}"
fi

# Start/stop database server orchestrator first
if [ -d servers/grepx-database-server-orchastrator/orchestrator ]; then
    echo "Managing database server orchestrator: $COMMAND"
    cd servers/grepx-database-server-orchastrator/orchestrator
    ./run.sh $COMMAND
    cd ../../..
    
    if [ "$COMMAND" = "start" ]; then
        echo "Waiting for database server to be ready..."
        sleep 3
    fi
fi

# Start/stop other servers
echo "Managing celery server: $COMMAND"
cd servers/grepx-celery-server
./run.sh $COMMAND
cd ../..

echo "Managing dagster server: $COMMAND"
cd servers/grepx-dagster-server
./run.sh $COMMAND
cd ../..

echo "Managing prefect server: $COMMAND"
cd servers/grepx-prefect-server
./run.sh $COMMAND
cd ../..

if [ "$COMMAND" = "start" ]; then
    echo ""
    echo "=== All servers started ==="
    echo "Database Server: http://localhost:${DB_SERVER_PORT:-8000}"
    echo "Dagster UI: http://localhost:${DAGSTER_PORT:-3000}"
    echo "Prefect UI: http://localhost:4200"
    echo "Flower UI: http://localhost:${FLOWER_PORT:-5555}"
    echo "Master Database: ${GREPX_MASTER_DB_URL:-./data/grepx-master.db}"
elif [ "$COMMAND" = "stop" ]; then
    echo "=== All servers stopped ==="
else
    echo "=== All servers $COMMAND complete ==="
fi
