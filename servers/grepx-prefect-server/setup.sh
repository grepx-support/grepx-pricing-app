#!/bin/bash

# Grepx Prefect Server Setup Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Handle clean option
if [ "${1:-}" = "clean" ]; then
    echo "========================================="
    echo "Cleaning Grepx Prefect Server"
    echo "========================================="

    if [ -d "venv" ]; then
        echo "Removing virtual environment..."
        rm -rf venv
    fi

    if [ -d ".prefect" ]; then
        echo "Removing Prefect home directory..."
        rm -rf .prefect
    fi

    echo "Clean complete!"
    exit 0
fi

echo "========================================="
echo "Grepx Prefect Server Setup"
echo "========================================="

# Check Python version
echo "Checking Python version..."
PYTHON_CMD=${PYTHON_VERSION:-python3.12}

if ! command -v $PYTHON_CMD &> /dev/null; then
    echo "Error: $PYTHON_CMD not found"
    echo "Please install Python 3.12 or set PYTHON_VERSION environment variable"
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD --version)
echo "Using: $PYTHON_VERSION"

# Create virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    $PYTHON_CMD -m venv venv
else
    echo "Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt

echo ""
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
echo "To activate the virtual environment:"
echo "  source venv/bin/activate"
echo ""
echo "To run the Prefect server:"
echo "  ./run.sh"
echo ""
