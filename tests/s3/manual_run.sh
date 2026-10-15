#!/usr/bin/env bash
# Manual RustFS debug helper for the dev-container setup.
#
# The dev container runs on the external Docker network
# "otterstax_app_network_debug" (see .devcontainer/devcontainer.json). These
# commands start RustFS on the HOST and attach it to that same network so the
# code inside the dev container can reach it by hostname.
#
#   ./manual_run.sh start     # bring RustFS up + join the dev-container network
#   ./manual_run.sh stop      # tear RustFS down
#
# After `start`, RustFS is reachable from the dev container as:
#     rustfs1:9000   (bucket test-bucket-1)
#     rustfs2:9000   (bucket test-bucket-2)
# and from the host as localhost:9000 / localhost:9010.
#
# Override the network with DEVCON_NETWORK=... if your dev container differs.
#
# The previous C++ semi-integration tests (test_s3_manager, ~5-row people.*
# fixtures) have been removed — the s3 path is now covered end-to-end by the
# python integration tests in tests/test_{schema_}mysql_s3.py and friends,
# which bring up their own RustFS via compose.test.yml. This script stays for
# manual poking-around with the standalone two-RustFS stack.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="$HERE/docker-compose.yml"

DEVCON_NETWORK="${DEVCON_NETWORK:-otterstax_app_network_debug}"
CONTAINERS=("test-rustfs1:rustfs1" "test-rustfs2:rustfs2")   # container:alias

do_start() {
    command -v docker >/dev/null || { echo "docker not found — run 'start' on the host" >&2; exit 1; }

    # The dev-container network is 'external'; create it if it isn't there yet.
    if ! docker network inspect "$DEVCON_NETWORK" >/dev/null 2>&1; then
        echo "==> creating network $DEVCON_NETWORK"
        docker network create "$DEVCON_NETWORK" >/dev/null
    fi

    echo "==> starting RustFS services"
    docker compose -f "$COMPOSE" up -d --wait --remove-orphans rustfs1 rustfs2

    echo "==> seeding test buckets"
    docker compose -f "$COMPOSE" up -d --remove-orphans rustfs1-init rustfs2-init
    docker compose -f "$COMPOSE" wait rustfs1-init rustfs2-init >/dev/null

    echo "==> joining $DEVCON_NETWORK"
    for entry in "${CONTAINERS[@]}"; do
        local c="${entry%%:*}" alias="${entry##*:}"
        if docker inspect -f '{{json .NetworkSettings.Networks}}' "$c" | grep -q "\"$DEVCON_NETWORK\""; then
            echo "    $c already on $DEVCON_NETWORK"
        else
            docker network connect "$DEVCON_NETWORK" "$c" --alias "$alias"
            echo "    $c -> $DEVCON_NETWORK (alias $alias)"
        fi
    done

    echo "==> ready. From the dev container: rustfs1:9000 / rustfs2:9000"
    echo "    From the host:                  localhost:9000 / localhost:9010"
}

do_stop() {
    command -v docker >/dev/null || { echo "docker not found — run 'stop' on the host" >&2; exit 1; }
    echo "==> stopping RustFS stack"
    # Removing the containers auto-disconnects them from the dev network.
    docker compose -f "$COMPOSE" down -v --remove-orphans
}

case "${1:-}" in
    start) do_start ;;
    stop)  do_stop ;;
    *)
        echo "usage: $0 {start|stop}" >&2
        echo "  start   bring RustFS up + join the dev-container network" >&2
        echo "  stop    tear RustFS down" >&2
        exit 1
        ;;
esac
