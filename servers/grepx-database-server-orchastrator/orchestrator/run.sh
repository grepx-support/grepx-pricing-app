#!/bin/bash
set -e

cd "$(dirname "$0")"

# Source common paths
source ./common.sh

if [ ! -d "venv" ]; then
    echo "Error: venv not found. Run ./setup.sh first"
    exit 1
fi

# Handle service commands by delegating to grepx-database-server
case "${1:-}" in
    status|start|stop|restart)
        # Delegate to the actual database server
        cd ../grepx-database-server
        ./run.sh "$1"
        ;;
    *)
        # Run orchestrator commands (deploy, list projects, etc.)
        $VENV_PYTHON orchestrator.py "$@"
        ;;
esac