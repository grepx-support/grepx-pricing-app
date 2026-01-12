#!/bin/bash

# Grepx Prefect Server Start Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Set PROJECT_ROOT
export PROJECT_ROOT="$(cd ../../ && pwd)"

# Load and expand environment variables from env.common
if [ -f "../../env.common" ]; then
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        # Expand ${PROJECT_ROOT} in the value
        value="${value//\$\{PROJECT_ROOT\}/$PROJECT_ROOT}"
        export "$key"="$value"
    done < ../../env.common
fi

# Set Prefect home directory
export PREFECT_HOME="${SCRIPT_DIR}/.prefect"
mkdir -p "$PREFECT_HOME"

echo "========================================="
echo "Starting Grepx Prefect Server"
echo "========================================="
echo "PREFECT_HOME: $PREFECT_HOME"
echo ""

# Activate virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "Error: Virtual environment not found. Run ./setup.sh first"
    exit 1
fi

# Check if Prefect server is already running
PREFECT_PID_FILE="$PREFECT_HOME/prefect-server.pid"

if [ -f "$PREFECT_PID_FILE" ]; then
    PID=$(cat "$PREFECT_PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        echo "Prefect server is already running (PID: $PID)"
        echo "To stop it, run: ./stop.sh"
        exit 1
    else
        echo "Removing stale PID file"
        rm -f "$PREFECT_PID_FILE"
    fi
fi

# Start Prefect server in the background
echo "Starting Prefect API server..."
nohup prefect server start --host 0.0.0.0 --port 4200 > "$PREFECT_HOME/prefect-server.log" 2>&1 &
PREFECT_SERVER_PID=$!
echo $PREFECT_SERVER_PID > "$PREFECT_PID_FILE"
echo "Prefect API server started (PID: $PREFECT_SERVER_PID)"

# Wait for Prefect server to be ready
echo "Waiting for Prefect server to be ready..."
sleep 5

# Set Prefect API URL
export PREFECT_API_URL="http://127.0.0.1:4200/api"

# Add src/main to PYTHONPATH
export PYTHONPATH="${SCRIPT_DIR}/src/main:${PYTHONPATH}"

# Run the orchestrator to register flows and deployments
echo ""
echo "Registering flows and deployments..."
cd "${SCRIPT_DIR}/src/main"
python -m prefect_app.main
cd "${SCRIPT_DIR}"

echo ""
echo "========================================="
echo "Prefect Server Started Successfully!"
echo "========================================="
echo "Prefect UI: http://127.0.0.1:4200"
echo "Prefect API: http://127.0.0.1:4200/api"
echo ""
echo " [MANDATORY] TO RUN THE PREFECT FLOWS AND TASKS RUN BELOW COMMANDS MANDATORY "
echo "To start a worker:"
echo "  source venv/bin/activate"
echo "  export PREFECT_API_URL=http://127.0.0.1:4200/api"
echo "  export GREPX_MASTER_DB_URL=\"sqlite:////Users/mahesh/Desktop/grepx-pricing-app/data/grepx-master.db\""
echo "  prefect worker start --pool <pool-name>"
echo ""
echo "To stop the server:"
echo "  ./stop.sh"
echo ""
