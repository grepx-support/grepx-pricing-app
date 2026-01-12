#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

load_env() {
    if [ -f "$PROJECT_ROOT/env.common" ]; then
        set -a
        source "$PROJECT_ROOT/env.common"
        set +a
    fi
    if [ -f "$SCRIPT_DIR/env.generator" ]; then
        set -a
        source "$SCRIPT_DIR/env.generator"
        set +a
    fi
}

load_env

LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/logs}"
DATE=$(date +%Y-%m-%d 2>/dev/null || date +%F)
SERVER_NAME="task-generator"

mkdir -p "$LOG_DIR"

activate_venv() {
    if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
        source "$SCRIPT_DIR/venv/bin/activate"
    elif [ -f "$SCRIPT_DIR/venv/Scripts/activate" ]; then
        source "$SCRIPT_DIR/venv/Scripts/activate"
    fi
}

run_generator() {
    echo "Running $SERVER_NAME..."
    activate_venv
    cd "$SCRIPT_DIR"
    python src/main/task_generator/main.py | tee "$LOG_DIR/${SERVER_NAME}_${DATE}.log"
}

###########################################################
# Prefect helpers (analogous to Dagster side)
###########################################################

run_prefect_server() {
  echo "Starting Prefect server..."
  activate_venv
  cd "$PROJECT_ROOT"          # usually Prefect runs from project root
  prefect server start
}

run_prefect_worker() {
  # WORK_POOL_NAME can come from env.common (e.g. PRICE_POOL_NAME)
  local POOL="${1:-price-pool}"
  echo "Starting Prefect worker for pool: $POOL"
  activate_venv
  cd "$PROJECT_ROOT"
  prefect worker start --pool "$POOL"
}

run_prefect_deploy() {
  echo "Running Prefect deployments (reading from database)..."
  activate_venv
  cd "$SCRIPT_DIR"
  python src/main/task_generator/prefect_deployer.py
}

case "${1:-run}" in
    start|run)
        run_generator
        ;;
    prefect-server)
        run_prefect_server
        ;;
    prefect-worker)
        # optional: ./run.sh prefect-worker price-pool
        run_prefect_worker "${2:-price-pool}"
        ;;
    prefect-deploy)
        run_prefect_deploy
        ;;
    stop)
        echo "$SERVER_NAME is not a service"
        ;;
    restart)
        run_generator
        ;;
    status)
        echo "$SERVER_NAME is not a service"
        ;;
    *)
        echo "Usage: $0 {start|run|prefect-server|prefect-worker|prefect-deploy|stop|restart|status}"
        exit 1
        ;;
esac
