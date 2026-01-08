#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

load_env() {
    if [ -f "$PROJECT_ROOT/env.common" ]; then
        set -a
        source "$PROJECT_ROOT/env.common"
        set +a
    fi
    if [ -f "$SCRIPT_DIR/env.celery" ]; then
        set -a
        source "$SCRIPT_DIR/env.celery"
        set +a
    fi
}

load_env

# Set LOG_DIR to base project logs directory (absolute path)
LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/logs}"
# Convert to absolute path if relative
if [[ "$LOG_DIR" != /* ]] && [[ "$LOG_DIR" != [A-Za-z]:* ]]; then
    LOG_DIR="$PROJECT_ROOT/$LOG_DIR"
fi
# Resolve to absolute path
LOG_DIR="$(mkdir -p "$LOG_DIR" && cd "$LOG_DIR" && pwd)"
DATE=$(date +%Y-%m-%d 2>/dev/null || date +%F)
SERVER_NAME="celery"
PID_FILE="$LOG_DIR/$SERVER_NAME.pid"

mkdir -p "$LOG_DIR"

activate_venv() {
    if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
        source "$SCRIPT_DIR/venv/bin/activate"
    elif [ -f "$SCRIPT_DIR/venv/Scripts/activate" ]; then
        source "$SCRIPT_DIR/venv/Scripts/activate"
    fi
}

start() {
    if [ -f "$PID_FILE" ]; then
        echo "$SERVER_NAME already running"
        return
    fi
    
    echo "Starting $SERVER_NAME server..."
    activate_venv
    cd "$SCRIPT_DIR"
    
    python -m celery -A src.main.celery_app worker --loglevel=info --pool=solo > "$LOG_DIR/${SERVER_NAME}_${DATE}.log" 2>&1 &
    echo $! > "$PID_FILE"

    python -m celery -A src.main.celery_app flower --address=${FLOWER_ADDRESS:-localhost} --port=${FLOWER_PORT:-5555} > "$LOG_DIR/${SERVER_NAME}_flower_${DATE}.log" 2>&1 &
    
    echo "$SERVER_NAME started (PID: $(cat $PID_FILE))"
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "$SERVER_NAME not running"
        return
    fi
    
    PID=$(cat "$PID_FILE")
    echo "Stopping $SERVER_NAME (PID: $PID)..."
    kill $PID 2>/dev/null || true
    pkill -f "celery.*worker" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "$SERVER_NAME stopped"
}

restart() {
    stop
    sleep 2
    start
}

status() {
    echo "Server: $SERVER_NAME"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 $PID 2>/dev/null; then
            echo "Status: RUNNING"
            echo "PID: $PID"
        else
            echo "Status: NOT RUNNING (stale PID)"
            rm -f "$PID_FILE"
        fi
    else
        echo "Status: NOT RUNNING"
    fi
    echo "Broker: ${CELERY_BROKER_URL:-redis://localhost:6379/0}"
    echo "Backend: ${CELERY_RESULT_BACKEND:-redis://localhost:6379/1}"
    if [ -n "${FLOWER_PORT:-}" ]; then
        echo "Flower UI: http://${FLOWER_ADDRESS:-localhost}:${FLOWER_PORT}"
    fi
    echo ""
}

case "${1:-start}" in
    start) start ;;
    stop) stop ;;
    restart) restart ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}"; exit 1 ;;
esac
