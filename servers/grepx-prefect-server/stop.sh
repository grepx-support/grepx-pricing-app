#!/bin/bash

# Grepx Prefect Server Stop Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Set Prefect home directory
export PREFECT_HOME="${SCRIPT_DIR}/.prefect"

echo "========================================="
echo "Stopping Grepx Prefect Server"
echo "========================================="

PREFECT_PID_FILE="$PREFECT_HOME/prefect-server.pid"

if [ ! -f "$PREFECT_PID_FILE" ]; then
    echo "Prefect server is not running (no PID file found)"
    exit 0
fi

PID=$(cat "$PREFECT_PID_FILE")

if ! ps -p $PID > /dev/null 2>&1; then
    echo "Prefect server is not running (process not found)"
    rm -f "$PREFECT_PID_FILE"
    exit 0
fi

echo "Stopping Prefect server (PID: $PID)..."
kill $PID

# Wait for process to terminate
for i in {1..10}; do
    if ! ps -p $PID > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Force kill if still running
if ps -p $PID > /dev/null 2>&1; then
    echo "Force killing Prefect server..."
    kill -9 $PID
fi

rm -f "$PREFECT_PID_FILE"

echo ""
echo "========================================="
echo "Prefect Server Stopped"
echo "========================================="
