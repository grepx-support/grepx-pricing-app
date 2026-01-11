#!/bin/bash

set -euo pipefail

VENV_DIR="venv"

# Load common environment variables
if [ -f ../../env.common ]; then
    source ../../env.common
fi

# Default Python version if not set
PYTHON_VERSION=${PYTHON_VERSION:-python3.12}

# Handle clean option
if [ "${1:-}" = "clean" ]; then
    echo "Cleaning grepx-shared-models..."
    if [ -d "$VENV_DIR" ]; then
        echo "Removing virtual environment..."
        rm -rf "$VENV_DIR"
        echo "Clean complete."
    else
        echo "No virtual environment found."
    fi
    exit 0
fi

echo "Setting up grepx-shared-models..."
echo "Using Python: $PYTHON_VERSION"

# Create virtual environment if missing
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    $PYTHON_VERSION -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists, skipping creation..."
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
echo "Upgrading pip, setuptools, wheel..."
$PYTHON_VERSION -m pip install --upgrade pip setuptools wheel

# Install project dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

echo "grepx-shared-models setup complete"

