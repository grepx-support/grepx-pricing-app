#!/bin/bash

load_env_file() {
    local env_file=$1
    if [ -f "$env_file" ]; then
        set -a
        source "$env_file"
        set +a
    fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

load_env_file "$SCRIPT_DIR/env.common"
load_env_file "$SCRIPT_DIR/servers/grepx-celery-server/env.celery"
load_env_file "$SCRIPT_DIR/servers/grepx-dagster-server/env.dagster"
load_env_file "$SCRIPT_DIR/servers/grepx-task-generator-server/env.generator"

