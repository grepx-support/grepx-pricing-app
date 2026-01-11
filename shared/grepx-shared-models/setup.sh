#!/bin/bash

set -euo pipefail

echo "Setting up grepx-shared-models..."

VENV_DIR="venv"

# Create virtual environment if missing
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python -m venv "$VENV_DIR"
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
python -m pip install --upgrade pip setuptools wheel

# Install project dependencies
pip install -r requirements.txt


echo "grepx-shared-models setup complete"

