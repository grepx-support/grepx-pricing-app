#!/bin/bash
# orchestrator/setup.sh

set -e

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
    PYTHON=$(find_python)
    echo "Using Python from find_python: $PYTHON"
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
