#!/bin/bash

# Grepx Prefect Server Run Script
# Convenience script for starting/stopping the server

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

case "$1" in
    start)
        ./start.sh
        ;;
    stop)
        ./stop.sh
        ;;
    restart)
        ./stop.sh
        sleep 2
        ./start.sh
        ;;
    status)
        PREFECT_PID_FILE=".prefect/prefect-server.pid"
        if [ -f "$PREFECT_PID_FILE" ]; then
            PID=$(cat "$PREFECT_PID_FILE")
            if ps -p $PID > /dev/null 2>&1; then
                echo "Prefect server is running (PID: $PID)"
                exit 0
            else
                echo "Prefect server is not running"
                exit 1
            fi
        else
            echo "Prefect server is not running"
            exit 1
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac
