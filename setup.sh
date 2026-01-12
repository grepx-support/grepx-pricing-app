#!/bin/bash

set -e

# Handle clean option
if [ "${1:-}" = "clean" ]; then
    echo "=== Cleaning all components ==="
    
    echo "Cleaning shared models..."
    cd shared/grepx-shared-models
    ./setup.sh clean
    cd ../..
    
    echo "Cleaning database server orchestrator..."
    if [ -d servers/grepx-database-server-orchastrator/orchestrator ]; then
        cd servers/grepx-database-server-orchastrator/orchestrator
        ./setup.sh clean
        cd ../../..
    fi
    
    echo "Cleaning celery server..."
    cd servers/grepx-celery-server
    ./setup.sh clean
    cd ../..
    
    echo "Cleaning dagster server..."
    cd servers/grepx-dagster-server
    ./setup.sh clean
    cd ../..

    echo "Cleaning prefect server..."
    cd servers/grepx-prefect-server
    ./setup.sh clean
    cd ../..

    echo "Cleaning task generator server..."
    cd servers/grepx-task-generator-server
    ./setup.sh clean
    cd ../..

    echo ""
    echo "=== Clean complete ==="
    echo "Run ./setup.sh to reinstall all components"
    exit 0
fi

echo "=== Setting up all components ==="

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

# Expand PROJECT_ROOT in GREPX_MASTER_DB_URL if it contains variable
if [[ "$GREPX_MASTER_DB_URL" == *'${PROJECT_ROOT}'* ]]; then
    export GREPX_MASTER_DB_URL="${GREPX_MASTER_DB_URL//\$\{PROJECT_ROOT\}/$PROJECT_ROOT}"
fi

# Display configuration
echo "Project Root: $PROJECT_ROOT"
echo "Python version: ${PYTHON_VERSION:-python3.12}"
echo "Master DB: ${GREPX_MASTER_DB_URL}"
echo ""

# Setup shared models first (required by all servers)
echo "Setting up shared models..."
cd shared/grepx-shared-models
./setup.sh
cd ../..

# Setup database server orchestrator
echo "Setting up database server orchestrator..."
if [ -d servers/grepx-database-server-orchastrator/orchestrator ]; then
    cd servers/grepx-database-server-orchastrator/orchestrator
    ./setup.sh
    cd ../../..
fi

# Initialize master database
echo "Initializing master database..."
mkdir -p data
mkdir -p logs
echo "Database directory created at ./data"

# Setup other servers
echo "Setting up celery server..."
cd servers/grepx-celery-server
./setup.sh
cd ../..

echo "Setting up dagster server..."
cd servers/grepx-dagster-server
./setup.sh
cd ../..

echo "Setting up prefect server..."
cd servers/grepx-prefect-server
./setup.sh
cd ../..

echo "Setting up task generator server..."
cd servers/grepx-task-generator-server
./setup.sh
cd ../..

echo ""
echo "=== Setup complete ==="
echo "Master database will be created at: ${GREPX_MASTER_DB_URL:-./data/grepx-master.db}"
echo "To start all servers: ./run.sh start"
echo "To stop all servers: ./run.sh stop"
echo "To clean all venvs: ./setup.sh clean"
