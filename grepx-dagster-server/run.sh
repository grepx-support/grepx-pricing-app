#!/bin/bash
# =============================================================================
# run.sh - Dagster Service Manager
# Supports: start | stop | restart | status
# Modes: dev | prod
# =============================================================================

# -------------------------------
# CONFIGURATION
# -------------------------------
APP_NAME="dagster_server"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAGSTER_HOME="$SCRIPT_DIR/.dagster_home"
PID_FILE="$DAGSTER_HOME/$APP_NAME.pid"
DAEMON_PID_FILE="$DAGSTER_HOME/${APP_NAME}_daemon.pid"
WEBSERVER_PID_FILE="$DAGSTER_HOME/${APP_NAME}_webserver.pid"
LOG_FILE="$DAGSTER_HOME/$APP_NAME.log"
MODE="${2:-dev}"  # default mode: dev

# Ensure we're in the project root
cd "$SCRIPT_DIR"

# Create necessary directories
mkdir -p "$DAGSTER_HOME"

# Check if workspace.yaml exists
if [ ! -f "workspace.yaml" ]; then
    echo "ERROR: workspace.yaml not found in $SCRIPT_DIR"
    echo "Please ensure workspace.yaml exists in the project root"
    exit 1
fi

# Find and activate virtual environment
VENV_DIR="$SCRIPT_DIR/venv"
if [ -f "$VENV_DIR/bin/activate" ]; then
    ACTIVATE_PATH="$VENV_DIR/bin/activate"
elif [ -f "$VENV_DIR/Scripts/activate" ]; then
    ACTIVATE_PATH="$VENV_DIR/Scripts/activate"
else
    echo "ERROR: Virtual environment not found at $VENV_DIR"
    echo "Please run: python -m venv venv && pip install -r requirements.txt"
    exit 1
fi

# Get Python and Dagster paths from venv
if [ -f "$VENV_DIR/bin/python" ]; then
    PYTHON_EXE="$VENV_DIR/bin/python"
    DAGSTER_EXE="$VENV_DIR/bin/dagster"
    DAGSTER_DAEMON_EXE="$VENV_DIR/bin/dagster-daemon"
    DAGSTER_WEBSERVER_EXE="$VENV_DIR/bin/dagster-webserver"
elif [ -f "$VENV_DIR/Scripts/python.exe" ]; then
    PYTHON_EXE="$VENV_DIR/Scripts/python.exe"
    DAGSTER_EXE="$VENV_DIR/Scripts/dagster.exe"
    DAGSTER_DAEMON_EXE="$VENV_DIR/Scripts/dagster-daemon.exe"
    DAGSTER_WEBSERVER_EXE="$VENV_DIR/Scripts/dagster-webserver.exe"
else
    echo "ERROR: Could not find Python executable in virtual environment"
    exit 1
fi

# Verify dagster is installed
if [ ! -f "$DAGSTER_EXE" ]; then
    echo "ERROR: dagster not found in virtual environment"
    echo "Please install dependencies: pip install -r requirements.txt"
    exit 1
fi

# -------------------------------
# FUNCTIONS
# -------------------------------
start() {
    # Check if already running
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "$APP_NAME is already running (PID: $(cat $PID_FILE))"
        return
    fi

    echo "Starting $APP_NAME in $MODE mode..."
    echo "Working directory: $SCRIPT_DIR"
    echo "DAGSTER_HOME: $DAGSTER_HOME"
    echo "Log file: $LOG_FILE"
    
    # Export DAGSTER_HOME for dagster commands
    export DAGSTER_HOME="$DAGSTER_HOME"
    
    if [ "$MODE" = "dev" ]; then
        # Dev mode: dagster dev runs both webserver and daemon
        # It will automatically find workspace.yaml in current directory
        echo "Starting Dagster dev server..."
        echo "Using: $DAGSTER_EXE"
        echo "Working directory: $SCRIPT_DIR"
        
        # Change to script directory
        cd "$SCRIPT_DIR"
        
        # Use dagster executable (remove nohup for Windows/Git Bash compatibility)
        if [ -f "$DAGSTER_EXE" ]; then
            "$DAGSTER_EXE" dev > "$LOG_FILE" 2>&1 &
            MAIN_PID=$!
        else
            # Fallback to python -m dagster
            "$PYTHON_EXE" -m dagster dev > "$LOG_FILE" 2>&1 &
            MAIN_PID=$!
        fi
        
        echo $MAIN_PID > "$PID_FILE"
        sleep 2  # Give it a moment to start
        if kill -0 "$MAIN_PID" 2>/dev/null; then
            echo "$APP_NAME started in dev mode (PID: $MAIN_PID)"
            echo "Access UI at: http://localhost:3000"
            echo "Logs: tail -f $LOG_FILE"
        else
            echo "ERROR: Failed to start Dagster. Check logs: $LOG_FILE"
            echo "Last 20 lines of log:"
            tail -20 "$LOG_FILE" 2>/dev/null || echo "Log file not readable"
            rm -f "$PID_FILE"
            exit 1
        fi
        
    elif [ "$MODE" = "prod" ]; then
        # Prod mode: run daemon and webserver separately
        cd "$SCRIPT_DIR"
        echo "Starting Dagster daemon..."
        echo "Using: $DAGSTER_DAEMON_EXE"
        if [ -f "$DAGSTER_DAEMON_EXE" ]; then
            "$DAGSTER_DAEMON_EXE" run > "$LOG_FILE.daemon" 2>&1 &
        else
            "$PYTHON_EXE" -m dagster.daemon run > "$LOG_FILE.daemon" 2>&1 &
        fi
        DAEMON_PID=$!
        echo $DAEMON_PID > "$DAEMON_PID_FILE"
        
        sleep 2  # Give daemon time to start
        
        echo "Starting Dagster webserver..."
        echo "Using: $DAGSTER_WEBSERVER_EXE"
        # Webserver will automatically find workspace.yaml in current directory
        cd "$SCRIPT_DIR"
        if [ -f "$DAGSTER_WEBSERVER_EXE" ]; then
            "$DAGSTER_WEBSERVER_EXE" -h 0.0.0.0 -p 3000 > "$LOG_FILE.webserver" 2>&1 &
        else
            "$PYTHON_EXE" -m dagster.webserver -h 0.0.0.0 -p 3000 > "$LOG_FILE.webserver" 2>&1 &
        fi
        WEBSERVER_PID=$!
        echo $WEBSERVER_PID > "$WEBSERVER_PID_FILE"
        
        # Save main PID as daemon PID for stop function
        echo $DAEMON_PID > "$PID_FILE"
        
        echo "$APP_NAME started in prod mode"
        echo "  - Daemon PID: $DAEMON_PID"
        echo "  - Webserver PID: $WEBSERVER_PID"
        echo "Access UI at: http://localhost:3000"
        echo "Logs: tail -f $LOG_FILE.daemon $LOG_FILE.webserver"
    else
        echo "Invalid mode: $MODE. Use 'dev' or 'prod'."
        exit 1
    fi
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "$APP_NAME is not running."
        return
    fi

    MAIN_PID=$(cat "$PID_FILE")
    echo "Stopping $APP_NAME..."
    
    if [ "$MODE" = "prod" ]; then
        # Stop both daemon and webserver
        if [ -f "$DAEMON_PID_FILE" ]; then
            DAEMON_PID=$(cat "$DAEMON_PID_FILE")
            echo "Stopping daemon (PID: $DAEMON_PID)..."
            kill "$DAEMON_PID" 2>/dev/null || true
            rm -f "$DAEMON_PID_FILE"
        fi
        
        if [ -f "$WEBSERVER_PID_FILE" ]; then
            WEBSERVER_PID=$(cat "$WEBSERVER_PID_FILE")
            echo "Stopping webserver (PID: $WEBSERVER_PID)..."
            kill "$WEBSERVER_PID" 2>/dev/null || true
            rm -f "$WEBSERVER_PID_FILE"
        fi
    else
        # Dev mode: kill main process and its children
        echo "Stopping dev server (PID: $MAIN_PID)..."
        kill "$MAIN_PID" 2>/dev/null || true
        
        # Also try to kill any remaining dagster processes
        pkill -f "dagster dev" 2>/dev/null || true
        pkill -f "dagster-webserver" 2>/dev/null || true
        pkill -f "dagster-daemon" 2>/dev/null || true
    fi

    # Wait for processes to stop
    sleep 2
    
    # Clean up PID files
    rm -f "$PID_FILE"
    
    echo "$APP_NAME stopped."
}

restart() {
    stop
    sleep 1
    start
}

status() {
    if [ -f "$PID_FILE" ]; then
        MAIN_PID=$(cat "$PID_FILE")
        if kill -0 "$MAIN_PID" 2>/dev/null; then
            echo "$APP_NAME is running"
            echo "  Main PID: $MAIN_PID"
            
            if [ "$MODE" = "prod" ]; then
                if [ -f "$DAEMON_PID_FILE" ]; then
                    DAEMON_PID=$(cat "$DAEMON_PID_FILE")
                    if kill -0 "$DAEMON_PID" 2>/dev/null; then
                        echo "  Daemon PID: $DAEMON_PID"
                    else
                        echo "  Daemon: not running"
                    fi
                fi
                
                if [ -f "$WEBSERVER_PID_FILE" ]; then
                    WEBSERVER_PID=$(cat "$WEBSERVER_PID_FILE")
                    if kill -0 "$WEBSERVER_PID" 2>/dev/null; then
                        echo "  Webserver PID: $WEBSERVER_PID"
                    else
                        echo "  Webserver: not running"
                    fi
                fi
            fi
            
            echo "  Log file: $LOG_FILE"
            echo "  Access UI: http://localhost:3000"
        else
            echo "$APP_NAME is not running (stale PID file)"
            rm -f "$PID_FILE" "$DAEMON_PID_FILE" "$WEBSERVER_PID_FILE"
        fi
    else
        echo "$APP_NAME is not running."
    fi
}

# -------------------------------
# MAIN
# -------------------------------
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status} [dev|prod]"
        exit 1
        ;;
esac
