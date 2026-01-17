#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Installing test dependencies..."
pip install -q -r "$SCRIPT_DIR/requirements.txt"

echo ""
echo "=========================================="
echo "Running Database Server Tests"
echo "=========================================="
echo ""

# Check if server is running
if ! curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "ERROR: Database server not running!"
    echo "Please start the server first:"
    echo "  cd ..; ./run.sh"
    echo ""
    echo "Or run diagnostics:"
    echo "  ./check_logs.sh"
    exit 1
fi

echo "Server is running. Starting tests..."
echo ""

# Run test client
cd "$SCRIPT_DIR/.."
python -m test.test_client

# Show where logs are
echo ""
echo "=========================================="
echo "Test logs created in:"
echo "  $SCRIPT_DIR/test_log_*.txt"
echo ""
echo "To view latest log:"
echo "  cat $SCRIPT_DIR/test_log_*.txt | tail -100"
echo ""
echo "=========================================="
