#!/bin/bash

set -e

echo "Setting up grepx-shared-models..."

if [ -d "venv" ]; then
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
    elif [ -f "venv/Scripts/activate" ]; then
        source venv/Scripts/activate
    fi
fi

pip install -e .

echo "grepx-shared-models setup complete"

