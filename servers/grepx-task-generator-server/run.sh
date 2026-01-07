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

case "${1:-run}" in
    start|run) run_generator ;;
    stop) echo "$SERVER_NAME is not a service" ;;
    restart) run_generator ;;
    status) echo "$SERVER_NAME is not a service" ;;
    *) echo "Usage: $0 {start|stop|restart|status|run}"; exit 1 ;;
esac
