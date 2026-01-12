#!/bin/bash
# setup.sh - One-time project setup

set -euo pipefail

VENV_DIR="venv"

# Function to find Python executable
find_python() {
    # Check for various Python executable names, including Windows-specific ones
    for py_version in python3.12 python3.11 python3.10 python3.9 python3 python py; do
        if command -v "$py_version" >/dev/null 2>&1; then
            # Test if the command actually works
            if "$py_version" --version >/dev/null 2>&1; then
                echo "$py_version"
                return 0
            fi
        fi
    done
    echo "ERROR: No working Python interpreter found" >&2
    exit 1
}

PYTHON_CMD=$(find_python)
echo "Using Python interpreter: $PYTHON_CMD"

# Create virtual environment if missing
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    "$PYTHON_CMD" -m venv "$VENV_DIR"
fi

# Cross-platform activation
if [ -f "$VENV_DIR/bin/activate" ]; then
    ACTIVATE_PATH="$VENV_DIR/bin/activate"
elif [ -f "$VENV_DIR/Scripts/activate" ]; then
    ACTIVATE_PATH="$VENV_DIR/Scripts/activate"
else
    echo "ERROR: Could not find virtual environment activation script."
    exit 1
fi

source "$ACTIVATE_PATH"

# Upgrade pip / setuptools / wheel
"$PYTHON_CMD" -m pip install --upgrade pip setuptools wheel

# Install project dependencies
pip install -r requirements.txt

echo "Setup complete. You can now run ./run.sh"
