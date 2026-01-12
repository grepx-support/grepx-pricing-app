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
    if [ -f "$SCRIPT_DIR/env.prefect" ]; then
        set -a
        source "$SCRIPT_DIR/env.prefect"
        set +a
    fi
}

load_env

LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/logs}"
DATE=$(date +%Y-%m-%d 2>/dev/null || date +%F)
SERVER_NAME="prefect"

mkdir -p "$LOG_DIR"

activate_venv() {
    if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
        source "$SCRIPT_DIR/venv/bin/activate"
    elif [ -f "$SCRIPT_DIR/venv/Scripts/activate" ]; then
        source "$SCRIPT_DIR/venv/Scripts/activate"
    fi
}

start_server() {
    echo "Starting $SERVER_NAME server (initialization)..."
    activate_venv
    cd "$SCRIPT_DIR"
    python src/main/prefect_app/main.py start | tee "$LOG_DIR/${SERVER_NAME}_${DATE}.log"
}

start_full_server() {
    echo "Starting full $SERVER_NAME server with API..."
    activate_venv
    cd "$PROJECT_ROOT"
    nohup prefect server start --host 127.0.0.1 --port 4200 > "$LOG_DIR/${SERVER_NAME}_api_${DATE}.log" 2>&1 &
    echo $! > "$LOG_DIR/${SERVER_NAME}_api.pid"
    echo "Prefect server started with PID $(cat "$LOG_DIR/${SERVER_NAME}_api.pid")"
}

start_agent() {
    echo "Starting $SERVER_NAME worker..."
    activate_venv
    cd "$PROJECT_ROOT"
    nohup prefect worker start --pool price-pool > "$LOG_DIR/${SERVER_NAME}_worker_${DATE}.log" 2>&1 &
    echo $! > "$LOG_DIR/${SERVER_NAME}_worker.pid"
    echo "Prefect worker started with PID $(cat "$LOG_DIR/${SERVER_NAME}_worker.pid")"
}

start_all() {
    start_full_server
    sleep 10  # Give server time to start
    start_agent
}

register_flows() {
    echo "Registering flows from database..."
    activate_venv
    cd "$SCRIPT_DIR"
    python src/main/prefect_app/main.py register-flows | tee "$LOG_DIR/${SERVER_NAME}_register_flows_${DATE}.log"
}

start_ui() {
    echo "Starting Prefect UI..."
    activate_venv
    cd "$PROJECT_ROOT"
    prefect ui
}

run_deployments() {
    echo "Running Prefect deployments..."
    activate_venv
    cd "$SCRIPT_DIR"
    python ../grepx-task-generator-server/src/main/task_generator/prefect_deployer.py
}

deploy_flows() {
    echo "Deploying Prefect flows from database..."
    activate_venv
    cd "$SCRIPT_DIR"
    python src/main/prefect_app/main.py register-flows
}



case "${1:-start}" in
    start)
        start_server
        ;;
    server)
        start_full_server
        ;;
    agent|worker)
        start_agent
        ;;
    all)
        start_all
        ;;
    register|register-flows)
        register_flows
        ;;
    ui)
        start_ui
        ;;
    deploy|deploy-flows)
        deploy_flows
        ;;
    run-deployments)
        run_deployments
        ;;
    stop)
        echo "$SERVER_NAME server is not a service"
        ;;
    restart)
        start_server
        ;;
    status)
        echo "$SERVER_NAME server is not a service"
        ;;
    *)
        echo "Usage: $0 {start|server|worker|all|register|deploy|run-deployments|ui|stop|restart|status}"
        exit 1
        ;;
esac