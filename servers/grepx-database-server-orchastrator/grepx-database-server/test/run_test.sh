#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$SERVER_DIR/../../.." && pwd)"

# Convert Git Bash path to Windows path
if [[ "$PROJECT_ROOT" == /[a-z]/* ]]; then
    drive_letter="${PROJECT_ROOT:1:1}"
    rest_of_path="${PROJECT_ROOT:2}"
    PROJECT_ROOT="${drive_letter^^}:${rest_of_path}"
fi

export PROJECT_ROOT

# Load environment
if [ -f "$PROJECT_ROOT/env.common" ]; then
    echo "Loading environment from $PROJECT_ROOT/env.common"
    source "$PROJECT_ROOT/env.common"
fi

# Expand PROJECT_ROOT in GREPX_MASTER_DB_URL
if [[ "$GREPX_MASTER_DB_URL" == *'${PROJECT_ROOT}'* ]]; then
    export GREPX_MASTER_DB_URL="${GREPX_MASTER_DB_URL//\$\{PROJECT_ROOT\}/$PROJECT_ROOT}"
fi

echo "Project Root: $PROJECT_ROOT"
echo "Database URL: $GREPX_MASTER_DB_URL"
echo ""

# Activate venv
if [ -f "$SERVER_DIR/venv/bin/activate" ]; then
    source "$SERVER_DIR/venv/bin/activate"
elif [ -f "$SERVER_DIR/venv/Scripts/activate" ]; then
    source "$SERVER_DIR/venv/Scripts/activate"
else
    echo "ERROR: Virtual environment not found. Run setup.sh first."
    exit 1
fi

# Run test
cd "$SCRIPT_DIR"
python test_basic_operations.py
