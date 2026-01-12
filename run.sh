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

# Start Prefect server
if [ -d servers/grepx-prefect-server ]; then
    cd servers/grepx-prefect-server
    # For 'start' and 'restart' commands, run 'all' to start both server and agent
    if [ "$COMMAND" = "start" ] || [ "$COMMAND" = "restart" ]; then
        ./run.sh all
    elif [ "$COMMAND" = "deploy" ] || [ "$COMMAND" = "prefect-deploy" ]; then
        ./run.sh run-deployments
    else
        ./run.sh $COMMAND
    fi
    cd ../..

    # Deploy Prefect flows
    prefect_deploy() {
        echo "Running Prefect deployments..."
        cd servers/grepx-task-generator-server || exit 1
        # Activate the environment and run the deployer
        if [ -f "venv/bin/activate" ]; then
            source venv/bin/activate
        elif [ -f "venv/Scripts/activate" ]; then
            source venv/Scripts/activate
        fi
        python src/main/task_generator/prefect_deployer.py
        cd ../..  # Return to main directory
    }

    # Only run deployments if explicit deploy command is given
    if [ "$COMMAND" = "deploy" ] || [ "$COMMAND" = "prefect-deploy" ]; then
        sleep 5  # Wait a bit before running deployments
        prefect_deploy
    fi
fi

if [ "$COMMAND" = "start" ]; then
    echo ""
    echo "=== All servers started ==="
    echo "Database Server: http://localhost:${DB_SERVER_PORT:-8000}"
    echo "Dagster UI: http://localhost:${DAGSTER_PORT:-3000}"
    echo "Flower UI: http://localhost:${FLOWER_PORT:-5555}"
    echo "Master Database: ${GREPX_MASTER_DB_URL:-./data/grepx-master.db}"
elif [ "$COMMAND" = "stop" ]; then
    echo "=== All servers stopped ==="
else
    echo "=== All servers $COMMAND complete ==="
fi