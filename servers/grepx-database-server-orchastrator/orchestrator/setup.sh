#!/bin/bash
# orchestrator/setup.sh

set -e

# Set PROJECT_ROOT before loading common environment variables
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Export PROJECT_ROOT for env.common to use
export PROJECT_ROOT

cd "$(dirname "$0")"

# Load common environment variables from root
if [ -f ../../../env.common ]; then
    source ../../../env.common
fi

# Source common paths
source ./common.sh

VENV_DIR="venv"

# Handle clean option
if [ "${1:-}" = "clean" ]; then
    echo "Cleaning grepx-database-server-orchestrator..."
    if [ -d "$VENV_DIR" ]; then
        echo "Removing virtual environment..."
        rm -rf "$VENV_DIR"
        echo "Clean complete."
    else
        echo "No virtual environment found."
    fi
    exit 0
fi

echo "Setting up grepx-database-server-orchestrator..."

# Use PYTHON_VERSION from env.common if available, otherwise use find_python
if [ -n "${PYTHON_VERSION:-}" ]; then
    PYTHON=$PYTHON_VERSION
    echo "Using Python from env.common: $PYTHON"
else
    # Try to find appropriate Python version
    if command -v python3.12 &> /dev/null; then
        PYTHON=python3.12
    elif command -v python3 &> /dev/null; then
        PYTHON=python3
    elif command -v python &> /dev/null; then
        PYTHON=python
    else
        echo "Error: Python not found. Please install Python 3.9+ and ensure it's in your PATH."
        exit 1
    fi
fi

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    $PYTHON -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists, skipping creation..."
fi

echo "Upgrading pip..."
$VENV_PYTHON -m pip install --upgrade pip

echo "Installing requirements..."
$VENV_PIP install -r requirements.txt

echo ""
echo "Setup complete!"
echo "Next steps:"
echo "  ./run.sh --install-libs    # Install all libraries"
echo "  ./run.sh -p price_app      # Deploy specific project"
echo "  ./run.sh -a                # Deploy all projects"
echo "  ./run.sh -l                # List all projects"
