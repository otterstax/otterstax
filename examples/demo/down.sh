#!/usr/bin/env bash
# Tear down the demo stack and wipe volumes.
#
# ClickHouse only re-runs init scripts on a fresh volume, so wiping is needed
# whenever fixtures change.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

DOCKER_PREFIX=""
if ! docker ps >/dev/null 2>&1; then
    if sudo -n docker ps >/dev/null 2>&1; then DOCKER_PREFIX="sudo -n "; else echo "❌ docker"; exit 1; fi
fi
if ${DOCKER_PREFIX}docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="${DOCKER_PREFIX}docker compose"
else
    COMPOSE_CMD="${DOCKER_PREFIX}docker-compose"
fi

echo "=== Stopping demo stack and removing volumes ==="
$COMPOSE_CMD --profile full -f compose.yml down --volumes --remove-orphans
echo "✅ Demo stack stopped, volumes removed."
