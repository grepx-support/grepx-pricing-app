#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/venv"

echo "Setting up Task Generator Server..."
echo ""

# Create virtual environment
if [ ! -d "$VENV_DIR" ]; then
    echo "▶ Creating virtual environment..."
    python -m venv "$VENV_DIR"
else
    echo "▶ Removing old virtual environment..."
    rm -rf "$VENV_DIR"
    python -m venv "$VENV_DIR"
fi

# Cross-platform activation
if [ -f "$VENV_DIR/bin/activate" ]; then
    ACTIVATE_PATH="$VENV_DIR/bin/activate"
elif [ -f "$VENV_DIR/Scripts/activate" ]; then
    ACTIVATE_PATH="$VENV_DIR/Scripts/activate"
else
    echo "❌ ERROR: Could not find virtual environment activation script."
    exit 1
fi

source "$ACTIVATE_PATH"
echo "✓ Virtual environment created"

# Upgrade pip
echo ""
echo "▶ Upgrading pip..."
python -m pip install --upgrade pip setuptools wheel 2>&1 | grep -i "successfully\|already" || true
echo "✓ pip upgraded"

# Install dependencies
echo ""
if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
    echo "▶ Installing requirements..."
    pip install -r "$SCRIPT_DIR/requirements.txt"
    echo "✓ Dependencies installed"
else
    echo "⚠️  requirements.txt not found"
fi

echo ""
echo "✓ Setup complete! You can now run ./run.sh"
