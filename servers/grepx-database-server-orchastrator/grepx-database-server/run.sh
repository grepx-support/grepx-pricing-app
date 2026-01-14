#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Go up 3 levels: grepx-database-server -> orchestrator -> grepx-database-server-orchastrator -> project root
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Convert Git Bash path to Windows path for compatibility
# /c/Users/... -> C:/Users/...
if [[ "$PROJECT_ROOT" == /[a-z]/* ]]; then
    drive_letter="${PROJECT_ROOT:1:1}"
    rest_of_path="${PROJECT_ROOT:2}"
    PROJECT_ROOT="${drive_letter^^}:${rest_of_path}"
fi

export PROJECT_ROOT

load_env() {
    if [ -f "$PROJECT_ROOT/env.common" ]; then
        echo "Loading common environment from $PROJECT_ROOT/env.common"
        set -a
        source "$PROJECT_ROOT/env.common"
        set +a
    fi
    if [ -f "$SCRIPT_DIR/env.database-server" ]; then
        echo "Loading database-server environment from $SCRIPT_DIR/env.database-server"
        set -a
        source "$SCRIPT_DIR/env.database-server"
        set +a
    fi
    
    # Expand PROJECT_ROOT in GREPX_MASTER_DB_URL
    if [[ "$GREPX_MASTER_DB_URL" == *'${PROJECT_ROOT}'* ]]; then
        export GREPX_MASTER_DB_URL="${GREPX_MASTER_DB_URL//\$\{PROJECT_ROOT\}/$PROJECT_ROOT}"
    fi
}

load_env

LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/logs}"
DATE=$(date +%Y-%m-%d 2>/dev/null || date +%F)
SERVER_NAME="grepx-database-server"
PID_FILE="$SCRIPT_DIR/${SERVER_NAME}.pid"

mkdir -p "$LOG_DIR"

activate_venv() {
    if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
        source "$SCRIPT_DIR/venv/bin/activate"
    elif [ -f "$SCRIPT_DIR/venv/Scripts/activate" ]; then
        source "$SCRIPT_DIR/venv/Scripts/activate"
    fi
}

start_server() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "$SERVER_NAME is already running (PID: $PID)"
            return 1
        else
            rm -f "$PID_FILE"
        fi
    fi
    
    echo "Starting $SERVER_NAME..."
    echo "  Host: ${SERVER_HOST:-0.0.0.0}"
    echo "  Port: ${SERVER_PORT:-8000}"
    activate_venv
    cd "$SCRIPT_DIR"
    
    nohup python src/main/main.py > "$LOG_DIR/${SERVER_NAME}_${DATE}.log" 2>&1 &
    echo $! > "$PID_FILE"
    
    sleep 2
    
    if ps -p $(cat "$PID_FILE") > /dev/null 2>&1; then
        echo "$SERVER_NAME started successfully (PID: $(cat "$PID_FILE"))"
        echo "Logs: $LOG_DIR/${SERVER_NAME}_${DATE}.log"
    else
        echo "Failed to start $SERVER_NAME"
        rm -f "$PID_FILE"
        return 1
    fi
}

stop_server() {
    if [ ! -f "$PID_FILE" ]; then
        echo "$SERVER_NAME is not running"
        return 1
    fi
    
    PID=$(cat "$PID_FILE")
    
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "$SERVER_NAME is not running"
        rm -f "$PID_FILE"
        return 1
    fi
    
    echo "Stopping $SERVER_NAME (PID: $PID)..."
    kill "$PID"
    
    for i in {1..10}; do
        if ! ps -p "$PID" > /dev/null 2>&1; then
            rm -f "$PID_FILE"
            echo "$SERVER_NAME stopped successfully"
            return 0
        fi
        sleep 1
    done
    
    echo "Force stopping $SERVER_NAME..."
    kill -9 "$PID" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "$SERVER_NAME stopped"
}

restart_server() {
    echo "Restarting $SERVER_NAME..."
    stop_server || true
    sleep 2
    start_server
}

status_server() {
    if [ ! -f "$PID_FILE" ]; then
        echo "$SERVER_NAME is not running"
        return 1
    fi
    
    PID=$(cat "$PID_FILE")
    
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "$SERVER_NAME is running (PID: $PID)"
        ps -p "$PID" -o pid,etime,cmd | tail -n +2
        return 0
    else
        echo "$SERVER_NAME is not running (stale PID file)"
        rm -f "$PID_FILE"
        return 1
    fi
}

run_server() {
    echo "Running $SERVER_NAME in foreground..."
    echo "  Host: ${SERVER_HOST:-0.0.0.0}"
    echo "  Port: ${SERVER_PORT:-8000}"
    activate_venv
    cd "$SCRIPT_DIR"
    python src/main/main.py | tee "$LOG_DIR/${SERVER_NAME}_${DATE}.log"
}

case "${1:-run}" in
    start)
        start_server
        ;;
    stop)
        stop_server
        ;;
    restart)
        restart_server
        ;;
    status)
        status_server
        ;;
    run)
        run_server
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|run}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the server in background"
        echo "  stop    - Stop the running server"
        echo "  restart - Restart the server"
        echo "  status  - Check server status"
        echo "  run     - Run the server in foreground"
        exit 1
        ;;
esac
