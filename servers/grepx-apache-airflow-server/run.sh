#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/apache_airflow_app/venv_airflow"
AIRFLOW_HOME="$SCRIPT_DIR"
PID_FILE="$AIRFLOW_HOME/airflow.pid"
LOG_DIR="$AIRFLOW_HOME/logs"
LOG_FILE="$LOG_DIR/airflow.log"

# Helper functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

log_error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

is_running() {
    local pid=$1
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

kill_port() {
    local port=$1
    local pids=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "$pids" | xargs kill -9 2>/dev/null || true
        log "Killed processes on port $port"
    fi
}

# Activate venv
activate() {
    if [ -f "$VENV_DIR/bin/activate" ]; then
        source "$VENV_DIR/bin/activate"
    else
        log_error "Virtual environment not found at $VENV_DIR"
        exit 1
    fi
}

# Start Airflow
start() {
    if [ -f "$PID_FILE" ] && is_running "$(cat $PID_FILE)" 2>/dev/null; then
        log "Airflow is already running (PID: $(cat $PID_FILE))"
        return 0
    fi
    
    mkdir -p "$LOG_DIR"
    activate
    export AIRFLOW_HOME="$AIRFLOW_HOME"
    
    log "Starting Airflow..."
    nohup airflow standalone >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    sleep 2
    log "Airflow started (PID: $(cat $PID_FILE))"
    log "Web UI: http://localhost:8080"
}

# Stop Airflow
stop() {
    if [ ! -f "$PID_FILE" ]; then
        log "Airflow is not running"
        return 0
    fi
    
    PID=$(cat "$PID_FILE")
    if is_running "$PID"; then
        log "Stopping Airflow (PID: $PID)..."
        kill "$PID" 2>/dev/null || true
        sleep 2
        if is_running "$PID"; then
            kill -9 "$PID" 2>/dev/null || true
        fi
        pkill -f "airflow.*standalone" 2>/dev/null || true
        rm -f "$PID_FILE"
        log "Airflow stopped"
    else
        rm -f "$PID_FILE"
    fi
    
    # Fallback: kill by port
    kill_port 8080
}

# Restart
restart() {
    log "Restarting Airflow..."
    stop
    sleep 1
    start
}

# Status
status() {
    if [ -f "$PID_FILE" ] && is_running "$(cat $PID_FILE)" 2>/dev/null; then
        log "✓ Airflow: RUNNING (PID: $(cat $PID_FILE))"
        log "  Web UI: http://localhost:8080"
    else
        log "✗ Airflow: STOPPED"
    fi
}

case "${1:-start}" in
    start) start ;;
    stop) stop ;;
    restart) restart ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}"; exit 1 ;;
esac
