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
LOG_FILE="$LOG_DIR/task-generator.log"
PID_FILE="$LOG_DIR/task-generator.pid"
DATE=$(date +%Y-%m-%d)
SERVER_NAME="task-generator"

mkdir -p "$LOG_DIR"

# Helper functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

is_running() {
    local pid=$1
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

log_error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

activate_venv() {
    if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
        source "$SCRIPT_DIR/venv/bin/activate"
    elif [ -f "$SCRIPT_DIR/venv/Scripts/activate" ]; then
        source "$SCRIPT_DIR/venv/Scripts/activate"
    else
        log_error "Virtual environment not found at $SCRIPT_DIR/venv"
        exit 1
    fi
}

run_generator() {
    if [ -f "$PID_FILE" ]; then
        local old_pid=$(cat "$PID_FILE")
        if is_running "$old_pid"; then
            log "$SERVER_NAME is already running (PID: $old_pid)"
            return 0
        fi
    fi
    
    log "Starting $SERVER_NAME..."
    activate_venv
    cd "$SCRIPT_DIR"
    
    python src/main/task_generator/main.py >> "$LOG_FILE" 2>&1 &
    local pid=$!
    sleep 1
    
    if ! is_running "$pid"; then
        log_error "$SERVER_NAME failed to start. Check $LOG_FILE"
        return 1
    fi
    
    echo "$pid" > "$PID_FILE"
    log "$SERVER_NAME started (PID: $pid)"
}

stop_generator() {
    if [ ! -f "$PID_FILE" ]; then
        log "$SERVER_NAME is not running"
        return 0
    fi
    
    local pid=$(cat "$PID_FILE")
    if is_running "$pid"; then
        log "Stopping $SERVER_NAME (PID: $pid)..."
        kill "$pid" 2>/dev/null || true
        sleep 2
        if is_running "$pid"; then
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$PID_FILE"
        log "$SERVER_NAME stopped"
    else
        rm -f "$PID_FILE"
    fi
}

case "${1:-run}" in
    start|run) run_generator ;;
    stop) stop_generator ;;
    restart) stop_generator; sleep 1; run_generator ;;
    status) 
        if [ -f "$PID_FILE" ] && is_running "$(cat $PID_FILE)"; then
            log "$SERVER_NAME: RUNNING (PID: $(cat $PID_FILE))"
        else
            log "$SERVER_NAME: STOPPED"
        fi
        ;;
    logs) tail -f "$LOG_FILE" ;;
    *) echo "Usage: $0 {start|run|stop|restart|status|logs}"; exit 1 ;;
esac
