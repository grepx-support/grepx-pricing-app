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
    if [ -f "$SCRIPT_DIR/env.dagster" ]; then
        set -a
        source "$SCRIPT_DIR/env.dagster"
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
SERVER_NAME="dagster"
PID_FILE="$LOG_DIR/$SERVER_NAME.pid"

# Set DAGSTER_HOME to absolute path (required by Dagster)
DAGSTER_HOME="${DAGSTER_HOME:-$SCRIPT_DIR/.dagster_home}"
# Convert to absolute path if relative
if [[ "$DAGSTER_HOME" != /* ]] && [[ "$DAGSTER_HOME" != [A-Za-z]:* ]]; then
    # If it starts with ./, resolve relative to SCRIPT_DIR
    if [[ "$DAGSTER_HOME" == ./* ]]; then
        DAGSTER_HOME="$SCRIPT_DIR/${DAGSTER_HOME#./}"
    else
        DAGSTER_HOME="$SCRIPT_DIR/$DAGSTER_HOME"
    fi
fi
# Resolve to absolute path (create directory if needed)
DAGSTER_HOME="$(mkdir -p "$DAGSTER_HOME" && cd "$DAGSTER_HOME" && pwd)"

mkdir -p "$LOG_DIR"
export DAGSTER_HOME="$DAGSTER_HOME"

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
    
    python -m dagster dev > "$LOG_DIR/${SERVER_NAME}_${DATE}.log" 2>&1 &
    echo $! > "$PID_FILE"
    
    echo "$SERVER_NAME started (PID: $(cat $PID_FILE))"
    echo "UI: http://localhost:${DAGSTER_PORT:-3000}"
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "$SERVER_NAME not running"
        return
    fi
    
    PID=$(cat "$PID_FILE")
    echo "Stopping $SERVER_NAME (PID: $PID)..."
    kill $PID 2>/dev/null || true
    pkill -f "dagster dev" 2>/dev/null || true
    pkill -f "dagster-webserver" 2>/dev/null || true
    pkill -f "dagster-daemon" 2>/dev/null || true
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
    echo "Host: ${DAGSTER_HOST:-0.0.0.0}"
    echo "Port: ${DAGSTER_PORT:-3000}"
    echo "UI: http://localhost:${DAGSTER_PORT:-3000}"
    echo "Home: ${DAGSTER_HOME:-$SCRIPT_DIR/.dagster_home}"
    echo ""
}

case "${1:-start}" in
    start) start ;;
    stop) stop ;;
    restart) restart ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}"; exit 1 ;;
esac
