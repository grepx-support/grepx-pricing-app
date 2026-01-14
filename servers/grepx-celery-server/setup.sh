#!/bin/bash
# setup.sh - One-time project setup

set -euo pipefail

VENV_DIR="venv"

# Set PROJECT_ROOT before loading common environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../" && pwd)"

# Export PROJECT_ROOT for env.common to use
export PROJECT_ROOT

# Load common environment variables
if [ -f ../../env.common ]; then
    source ../../env.common
fi

# Default Python version if not set
if [ -n "${PYTHON_VERSION:-}" ]; then
    PYTHON_VERSION=$PYTHON_VERSION
else
    # Try to find appropriate Python version
    if command -v python3.12 &> /dev/null; then
        PYTHON_VERSION=python3.12
    elif command -v python3 &> /dev/null; then
        PYTHON_VERSION=python3
    elif command -v python &> /dev/null; then
        PYTHON_VERSION=python
    else
        echo "Error: Python not found. Please install Python 3.9+ and ensure it's in your PATH."
        exit 1
    fi
fi

# Handle clean option
if [ "${1:-}" = "clean" ]; then
    echo "Cleaning grepx-celery-server..."
    if [ -d "$VENV_DIR" ]; then
        echo "Removing virtual environment..."
        rm -rf "$VENV_DIR"
        echo "Clean complete."
    else
        echo "No virtual environment found."
    fi
    exit 0
fi

echo "Setting up grepx-celery-server..."
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

echo "Setup complete. You can now run ./run.sh"
