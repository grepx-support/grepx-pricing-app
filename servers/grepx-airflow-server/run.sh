#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Convert Git Bash path to Windows path for compatibility
# /c/Users/... -> C:/Users/...
if [[ "$PROJECT_ROOT" == /[a-z]/* ]]; then
    drive_letter="${PROJECT_ROOT:1:1}"
    rest_of_path="${PROJECT_ROOT:2}"
    PROJECT_ROOT="${drive_letter^^}:${rest_of_path}"
fi

export PROJECT_ROOT

load_env() {
    # Set PROJECT_ROOT if not already set
    if [ -z "${PROJECT_ROOT:-}" ]; then
        PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
        # Convert Git Bash path to Windows path for compatibility
        if [[ "$PROJECT_ROOT" == /[a-z]/* ]]; then
            drive_letter="${PROJECT_ROOT:1:1}"
            rest_of_path="${PROJECT_ROOT:2}"
            PROJECT_ROOT="${drive_letter^^}:${rest_of_path}"
        fi
        export PROJECT_ROOT
    fi
    
    if [ -f "$PROJECT_ROOT/env.common" ]; then
        set -a
        source "$PROJECT_ROOT/env.common"
        set +a
    fi
    if [ -f "$SCRIPT_DIR/env.airflow" ]; then
        set -a
        source "$SCRIPT_DIR/env.airflow"
        set +a
    fi
    
    # Expand PROJECT_ROOT in GREPX_MASTER_DB_URL
    if [[ "$GREPX_MASTER_DB_URL" == *'${PROJECT_ROOT}'* ]]; then
        export GREPX_MASTER_DB_URL="${GREPX_MASTER_DB_URL//\$\{PROJECT_ROOT\}/$PROJECT_ROOT}"
    fi
    
    # Export all variables for child processes
    export PROJECT_ROOT
    export GREPX_MASTER_DB_URL
    export AIRFLOW_HOME
    export LOG_DIR
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
SERVER_NAME="airflow"
PID_FILE="$LOG_DIR/$SERVER_NAME.pid"

# Set AIRFLOW_HOME to absolute path
AIRFLOW_HOME="${AIRFLOW_HOME:-$SCRIPT_DIR/src/main/airflow_app}"
# Convert to absolute path if relative
if [[ "$AIRFLOW_HOME" != /* ]] && [[ "$AIRFLOW_HOME" != [A-Za-z]:* ]]; then
    if [[ "$AIRFLOW_HOME" == ./* ]]; then
        AIRFLOW_HOME="$SCRIPT_DIR/${AIRFLOW_HOME#./}"
    else
        AIRFLOW_HOME="$SCRIPT_DIR/$AIRFLOW_HOME"
    fi
fi
# Resolve to absolute path (create directory if needed)
AIRFLOW_HOME="$(mkdir -p "$AIRFLOW_HOME" && cd "$AIRFLOW_HOME" && pwd)"

# Convert Git Bash path to Windows path for AIRFLOW_HOME
if [[ "$AIRFLOW_HOME" == /[a-z]/* ]]; then
    drive_letter="${AIRFLOW_HOME:1:1}"
    rest_of_path="${AIRFLOW_HOME:2}"
    AIRFLOW_HOME="${drive_letter^^}:${rest_of_path}"
fi

mkdir -p "$LOG_DIR"
export AIRFLOW_HOME="$AIRFLOW_HOME"

activate_venv() {
    if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
        source "$SCRIPT_DIR/venv/bin/activate"
    elif [ -f "$SCRIPT_DIR/venv/Scripts/activate" ]; then
        source "$SCRIPT_DIR/venv/Scripts/activate"
    fi
}

initialize_airflow() {
    echo "Initializing Apache Airflow..."
    activate_venv
    cd "$SCRIPT_DIR"
    
    # Create necessary directories
    mkdir -p "$AIRFLOW_HOME/dags"
    
    # Initialize Airflow database
    # Airflow will automatically create its tables when db migrate is run
    echo "Initializing Airflow database..."
    # Activate venv and run with explicit environment variables
    source "$SCRIPT_DIR/venv/bin/activate"
    
    # Set environment variables for the database migration
    export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$LOG_DIR/airflow"
    export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="${AIRFLOW_PORT:-8080}"
    
    airflow db migrate >> "$LOG_DIR/${SERVER_NAME}_${DATE}.log" 2>&1
    
    echo "Airflow database initialized"
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
    
    # Initialize if needed
    initialize_airflow
    
    activate_venv
    cd "$SCRIPT_DIR"
    
    # Start Airflow standalone with explicit environment variables
    export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$LOG_DIR/airflow"
    export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="${AIRFLOW_PORT:-8080}"
    export AIRFLOW__WEBSERVER__SECRET_KEY="grepx-airflow-secret-key"
    export PROJECT_ROOT="$PROJECT_ROOT"
    
    airflow standalone >> "$LOG_DIR/${SERVER_NAME}_${DATE}.log" 2>&1 &
    echo $! > "$PID_FILE"
    
    sleep 3
    
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 $PID 2>/dev/null; then
            echo "$SERVER_NAME started (PID: $PID)"
            echo "UI: http://localhost:${AIRFLOW_PORT:-8080}"
            echo "Login: admin / admin123"
        else
            echo "$SERVER_NAME failed to start. Check $LOG_DIR/${SERVER_NAME}_${DATE}.log"
            rm -f "$PID_FILE"
        fi
    fi
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "$SERVER_NAME not running"
    else
        PID=$(cat "$PID_FILE")
        echo "Stopping $SERVER_NAME (PID: $PID)..."
        kill $PID 2>/dev/null || true
        sleep 2
        kill -9 $PID 2>/dev/null || true
        rm -f "$PID_FILE"
        echo "$SERVER_NAME stopped"
    fi
    
    # Kill any remaining airflow processes
    pkill -f "airflow.*standalone" 2>/dev/null || true
    pkill -f "airflow.*webserver" 2>/dev/null || true
    pkill -f "airflow.*scheduler" 2>/dev/null || true
    
    # Kill processes on port 8080
    if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]] || [[ -n "${WINDIR:-}" ]]; then
        netstat -ano 2>/dev/null | grep ":${AIRFLOW_PORT:-8080}" | grep LISTENING | awk '{print $5}' | while read pid; do
            if [ -n "$pid" ] && [ "$pid" != "0" ]; then
                taskkill //F //PID $pid 2>/dev/null || true
            fi
        done
    else
        if command -v lsof >/dev/null 2>&1; then
            lsof -ti:${AIRFLOW_PORT:-8080} | xargs kill -9 2>/dev/null || true
        fi
    fi
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
    echo "Port: ${AIRFLOW_PORT:-8080}"
    echo "UI: http://localhost:${AIRFLOW_PORT:-8080}"
    echo "Home: ${AIRFLOW_HOME}"
    echo "Login: admin / admin123"
    echo ""
}

case "${1:-start}" in
    start) start ;;
    stop) stop ;;
    restart) restart ;;
    status) status ;;
    init) initialize_airflow ;;
    *) echo "Usage: $0 {start|stop|restart|status|init}"; exit 1 ;;
esac
