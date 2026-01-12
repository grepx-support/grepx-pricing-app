#!/bin/bash

COMMAND=${1:-start}

echo "=== Running all servers: $COMMAND ==="

cd servers/grepx-celery-server
./run.sh $COMMAND
cd ../..

cd servers/grepx-dagster-server
./run.sh $COMMAND
cd ../..

cd servers/grepx-prefect-server
# For 'start' and 'restart' commands, run 'all' to start both server and agent
if [ "$COMMAND" = "start" ] || [ "$COMMAND" = "restart" ]; then
    ./run.sh all
elif [ "$COMMAND" = "deploy" ] || [ "$COMMAND" = "prefect-deploy" ]; then
    ./run.sh run-deployments
else
    ./run.sh $COMMAND
fi
cd ../..

# Deploy Prefect flows
prefect_deploy() {
    echo "Running Prefect deployments..."
    cd servers/grepx-task-generator-server || exit 1
    # Activate the environment and run the deployer
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
    elif [ -f "venv/Scripts/activate" ]; then
        source venv/Scripts/activate
    fi
    python src/main/task_generator/prefect_deployer.py
    cd ../..  # Return to main directory
}

# Only run deployments if explicit deploy command is given
if [ "$COMMAND" = "deploy" ] || [ "$COMMAND" = "prefect-deploy" ]; then
    sleep 5  # Wait a bit before running deployments
    prefect_deploy
fi

echo "=== All servers $COMMAND complete ==="
