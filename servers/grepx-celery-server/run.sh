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
        PID=$(cat "$PID_FILE")
        if kill -0 $PID 2>/dev/null; then
            echo "$SERVER_NAME already running (PID: $PID)"
            return
        else
            rm -f "$PID_FILE"
        fi
    fi
    
    echo "Starting $SERVER_NAME server..."
    activate_venv
    cd "$SCRIPT_DIR"
    
    python -m celery -A src.main.celery_app worker --loglevel=info --pool=solo > "$LOG_DIR/${SERVER_NAME}_${DATE}.log" 2>&1 &
    WORKER_PID=$!
    echo $WORKER_PID > "$PID_FILE"

    python -m celery -A src.main.celery_app flower --address=${FLOWER_ADDRESS:-0.0.0.0} --port=${FLOWER_PORT:-5555} > "$LOG_DIR/${SERVER_NAME}_flower_${DATE}.log" 2>&1 &
    FLOWER_PID=$!
    echo $FLOWER_PID > "${PID_FILE}.flower"
    
    echo "$SERVER_NAME started"
    echo "  Worker PID: $WORKER_PID"
    echo "  Flower PID: $FLOWER_PID"
    echo "  Flower UI: http://${FLOWER_ADDRESS:-localhost}:${FLOWER_PORT:-5555}"
}

stop() {
    echo "Stopping $SERVER_NAME..."
    
    # Kill Celery worker
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        kill $PID 2>/dev/null || true
        rm -f "$PID_FILE"
    fi
    
    # Kill Flower
    if [ -f "${PID_FILE}.flower" ]; then
        FLOWER_PID=$(cat "${PID_FILE}.flower")
        kill $FLOWER_PID 2>/dev/null || true
        rm -f "${PID_FILE}.flower"
    fi
    
    # Kill any remaining Celery worker processes
    pkill -f "celery.*worker" 2>/dev/null || true
    
    # Kill Flower processes
    pkill -f "celery.*flower" 2>/dev/null || true
    pkill -f "flower.*--port" 2>/dev/null || true
    
    # On Windows/Git Bash, kill processes on port 5555
    if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]] || [[ -n "$WINDIR" ]]; then
        echo "Killing processes on port ${FLOWER_PORT:-5555}..."
        # Find and kill process on port 5555
        netstat -ano | grep ":${FLOWER_PORT:-5555}" | grep LISTENING | awk '{print $5}' | while read pid; do
            if [ -n "$pid" ] && [ "$pid" != "0" ]; then
                echo "Killing PID $pid on port ${FLOWER_PORT:-5555}"
                taskkill //F //PID $pid 2>/dev/null || true
            fi
        done
        
        # Alternative method using PowerShell
        powershell.exe -Command "Get-NetTCPConnection -LocalPort ${FLOWER_PORT:-5555} -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id \$_.OwningProcess -Force -ErrorAction SilentlyContinue }" 2>/dev/null || true
        
        # Kill by process name pattern
        taskkill //F //FI "IMAGENAME eq python.exe" //FI "WINDOWTITLE eq *celery*" 2>/dev/null || true
        taskkill //F //FI "IMAGENAME eq python.exe" //FI "WINDOWTITLE eq *flower*" 2>/dev/null || true
        
        # Kill by command line pattern
        for pid in $(ps -W 2>/dev/null | grep -iE "celery|flower" | awk '{print $1}'); do
            if [ -n "$pid" ] && [ "$pid" != "PID" ]; then
                taskkill //F //PID $pid 2>/dev/null || true
            fi
        done
    else
        # Unix-like systems - use lsof
        if command -v lsof >/dev/null 2>&1; then
            lsof -ti:${FLOWER_PORT:-5555} | xargs kill -9 2>/dev/null || true
        fi
    fi
    
    # Wait for processes to terminate
    sleep 2
    
    # Verify port is free
    if command -v netstat >/dev/null 2>&1; then
        if netstat -ano 2>/dev/null | grep ":${FLOWER_PORT:-5555}" | grep LISTENING >/dev/null; then
            echo "Warning: Port ${FLOWER_PORT:-5555} still in use"
        else
            echo "Port ${FLOWER_PORT:-5555} is now free"
        fi
    fi
    
    echo "$SERVER_NAME stopped"
}

restart() {
    stop
    sleep 2
    start
}

status() {
    echo "Server: $SERVER_NAME"
    
    # Check worker status
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 $PID 2>/dev/null; then
            echo "Worker Status: RUNNING (PID: $PID)"
        else
            echo "Worker Status: NOT RUNNING (stale PID)"
            rm -f "$PID_FILE"
        fi
    else
        echo "Worker Status: NOT RUNNING"
    fi
    
    # Check flower status
    if [ -f "${PID_FILE}.flower" ]; then
        FLOWER_PID=$(cat "${PID_FILE}.flower")
        if kill -0 $FLOWER_PID 2>/dev/null; then
            echo "Flower Status: RUNNING (PID: $FLOWER_PID)"
        else
            echo "Flower Status: NOT RUNNING (stale PID)"
            rm -f "${PID_FILE}.flower"
        fi
    else
        echo "Flower Status: NOT RUNNING"
    fi
    
    echo "Broker: ${CELERY_BROKER_URL:-redis://localhost:6379/0}"
    echo "Backend: ${CELERY_RESULT_BACKEND:-redis://localhost:6379/1}"
    echo "Flower UI: http://${FLOWER_ADDRESS:-0.0.0.0}:${FLOWER_PORT:-5555}"
    echo ""
}

case "${1:-start}" in
    start) start ;;
    stop) stop ;;
    restart) restart ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}"; exit 1 ;;
esac
