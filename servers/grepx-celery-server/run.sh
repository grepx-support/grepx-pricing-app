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
    
    # Create unified log file for this session (overwrite old one)
    CELERY_LOG="$LOG_DIR/${SERVER_NAME}_${DATE}.log"
    
    # Start with fresh log (truncate if exists)
    > "$CELERY_LOG"
    
    echo "=== Celery Server Started at $(date) ===" >> "$CELERY_LOG"
    echo "" >> "$CELERY_LOG"
    
    # Start worker
    echo "--- Starting Celery Worker ---" >> "$CELERY_LOG"
    python -m celery -A src.main.celery_app worker --loglevel=info --pool=solo >> "$CELERY_LOG" 2>&1 &
    WORKER_PID=$!
    echo $WORKER_PID > "$PID_FILE"

    # Start flower
    echo "--- Starting Flower UI ---" >> "$CELERY_LOG"
    python -m celery -A src.main.celery_app flower --address=${FLOWER_ADDRESS:-0.0.0.0} --port=${FLOWER_PORT:-5555} >> "$CELERY_LOG" 2>&1 &
    FLOWER_PID=$!
    echo $FLOWER_PID > "${PID_FILE}.flower"
    
    echo "$SERVER_NAME started"
    echo "  Worker PID: $WORKER_PID"
    echo "  Flower PID: $FLOWER_PID"
    echo "  Log File: $CELERY_LOG"
    echo "  Flower UI: http://${FLOWER_ADDRESS:-localhost}:${FLOWER_PORT:-5555}"
    echo ""
    echo "To view logs:"
    echo "  tail -f $CELERY_LOG"
}

stop() {
    echo "Stopping $SERVER_NAME..."
    local worker_killed=false
    local flower_killed=false
    
    # Kill Celery worker
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 $PID 2>/dev/null; then
            echo "  Stopping worker (PID: $PID)..."
            kill $PID 2>/dev/null || true
            worker_killed=true
            
            # Wait for worker to stop (max 10 seconds)
            for i in {1..10}; do
                if ! kill -0 $PID 2>/dev/null; then
                    echo "  Worker stopped"
                    break
                fi
                sleep 1
            done
            
            # Force kill if still running
            if kill -0 $PID 2>/dev/null; then
                echo "  Force killing worker..."
                kill -9 $PID 2>/dev/null || true
                sleep 1
            fi
        fi
        rm -f "$PID_FILE"
    fi
    
    # Kill Flower
    if [ -f "${PID_FILE}.flower" ]; then
        FLOWER_PID=$(cat "${PID_FILE}.flower")
        if kill -0 $FLOWER_PID 2>/dev/null; then
            echo "  Stopping Flower (PID: $FLOWER_PID)..."
            kill $FLOWER_PID 2>/dev/null || true
            flower_killed=true
            
            # Wait for Flower to stop (max 10 seconds)
            for i in {1..10}; do
                if ! kill -0 $FLOWER_PID 2>/dev/null; then
                    echo "  Flower stopped"
                    break
                fi
                sleep 1
            done
            
            # Force kill if still running
            if kill -0 $FLOWER_PID 2>/dev/null; then
                echo "  Force killing Flower..."
                kill -9 $FLOWER_PID 2>/dev/null || true
                sleep 1
            fi
        fi
        rm -f "${PID_FILE}.flower"
    fi
    
    # Kill any remaining Celery worker processes
    echo "  Cleaning up any remaining processes..."
    pkill -f "celery.*worker" 2>/dev/null || true
    
    # Kill Flower processes
    pkill -f "celery.*flower" 2>/dev/null || true
    pkill -f "flower.*--port" 2>/dev/null || true
    
    # On Windows/Git Bash, kill processes on port 5555
    if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]] || [[ -n "$WINDIR" ]]; then
        echo "  Killing processes on port ${FLOWER_PORT:-5555}..."
        # Find and kill process on port 5555
        netstat -ano 2>/dev/null | grep ":${FLOWER_PORT:-5555}" | grep LISTENING | awk '{print $5}' | while read pid; do
            if [ -n "$pid" ] && [ "$pid" != "0" ]; then
                taskkill //F //PID $pid 2>/dev/null || true
            fi
        done
        
        # Alternative method using PowerShell
        powershell.exe -Command "Get-NetTCPConnection -LocalPort ${FLOWER_PORT:-5555} -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id \$_.OwningProcess -Force -ErrorAction SilentlyContinue }" 2>/dev/null || true
        
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
    
    # Wait for processes to fully terminate
    echo "  Waiting for cleanup..."
    sleep 2
    
    # Verify port is free
    local port_status="free"
    if command -v netstat >/dev/null 2>&1; then
        if netstat -ano 2>/dev/null | grep ":${FLOWER_PORT:-5555}" | grep LISTENING >/dev/null; then
            port_status="still in use"
            echo "  Warning: Port ${FLOWER_PORT:-5555} still in use"
        fi
    fi
    
    if [ "$port_status" = "free" ]; then
        echo "$SERVER_NAME stopped successfully"
    else
        echo "$SERVER_NAME stopped (with warnings)"
    fi
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
