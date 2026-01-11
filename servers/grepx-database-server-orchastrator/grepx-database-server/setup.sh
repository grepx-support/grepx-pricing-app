#!/bin/bash
# setup.sh - Setup task generator server

set -euo pipefail

VENV_DIR="venv"

# Create virtual environment if missing
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3.12 -m venv "$VENV_DIR"
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

# Upgrade pip
python3.12 -m pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

echo "Setup complete. You can now run ./run.sh"
