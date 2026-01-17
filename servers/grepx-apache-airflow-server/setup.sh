#!/usr/bin/env bash
set -e

echo "=========================================="
echo " Apache Airflow Setup (Python 3.12)"
echo "=========================================="

# --------------------------------------------------
# Resolve paths
# --------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

AIRFLOW_SERVER_DIR="$PROJECT_ROOT/servers/grepx-apache-airflow-server"
VENV_DIR="$AIRFLOW_SERVER_DIR/venv"
AIRFLOW_HOME="$AIRFLOW_SERVER_DIR/airflow_home"

export AIRFLOW_HOME

echo "Project root  : $PROJECT_ROOT"
echo "Airflow home  : $AIRFLOW_HOME"

# --------------------------------------------------
# Python
# --------------------------------------------------
PYTHON_CMD="python"

if ! command -v $PYTHON_CMD >/dev/null 2>&1; then
  echo "❌ Python not found in PATH"
  exit 1
fi

echo "Python        : $PYTHON_CMD"
$PYTHON_CMD --version

# --------------------------------------------------
# Airflow (Python 3.12 compatible)
# --------------------------------------------------
AIRFLOW_VERSION="3.1.5"
echo "Airflow ver   : $AIRFLOW_VERSION"

# --------------------------------------------------
# Virtualenv
# --------------------------------------------------
if [ ! -d "$VENV_DIR" ]; then
  echo "▶ Creating virtual environment..."
  $PYTHON_CMD -m venv "$VENV_DIR"
else
  echo "▶ Virtual environment already exists"
fi

# --------------------------------------------------
# Activate venv (Windows + Linux)
# --------------------------------------------------
if [ -f "$VENV_DIR/bin/activate" ]; then
  source "$VENV_DIR/bin/activate"
elif [ -f "$VENV_DIR/Scripts/activate" ]; then
  source "$VENV_DIR/Scripts/activate"
else
  echo "❌ Virtualenv activation script not found"
  exit 1
fi

echo "✓ Virtualenv activated"

# --------------------------------------------------
# Pip
# --------------------------------------------------
echo "▶ Upgrading pip..."
pip install --upgrade pip setuptools wheel

# --------------------------------------------------
# Install Airflow (NO CONSTRAINTS)
# --------------------------------------------------
echo "▶ Installing Apache Airflow..."
pip install "apache-airflow==${AIRFLOW_VERSION}"

# --------------------------------------------------
# Directories
# --------------------------------------------------
mkdir -p "$AIRFLOW_HOME/dags"
mkdir -p "$AIRFLOW_HOME/logs"
mkdir -p "$AIRFLOW_HOME/plugins"

# --------------------------------------------------
# Init DB
# --------------------------------------------------
echo "▶ Initializing Airflow DB..."
airflow db init

echo "=========================================="
echo " ✅ Apache Airflow setup complete"
echo "=========================================="
echo
echo "Next steps:"
echo "  airflow webserver"
echo "  airflow scheduler"
echo
