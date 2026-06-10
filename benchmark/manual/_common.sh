#!/usr/bin/env bash
# Sourced by all benchmark/manual/ scripts — sets paths, helpers, constants.
# Not executed directly.

MANUAL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$MANUAL_DIR/.." && pwd)"
REPO_ROOT="$(cd "$BENCH_DIR/.." && pwd)"

IMAGE_TAG="${IMAGE_TAG:-bench}"

_compose_backends() {
    docker compose \
        -f "$BENCH_DIR/compose_backends.yml" \
        -p bench \
        "$@"
}

_compose_otterstax() {
    docker compose \
        -f "$BENCH_DIR/compose_backends.yml" \
        -f "$BENCH_DIR/compose_benchmark.yml" \
        -f "$BENCH_DIR/compose_manual.yml" \
        -p bench \
        "$@"
}

_wait_db_healthy() {
    local container=$1
    echo "Waiting for $container..."
    for i in $(seq 1 60); do
        status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "missing")
        if [ "$status" = "healthy" ]; then
            echo "  $container healthy"
            return 0
        fi
        echo "  $container: $status ($i/60)"
        sleep 5
    done
    echo "ERROR: $container did not become healthy in time" >&2
    return 1
}

_wait_otterstax() {
    echo "Waiting for OtterStax /health..."
    for i in $(seq 1 60); do
        if docker run --rm --network=bench_net benchmark-client:latest \
               curl -s -f http://bench_otterstax:8085/health >/dev/null 2>&1; then
            echo "  OtterStax healthy"
            return 0
        fi
        sleep 3
    done
    echo "ERROR: OtterStax did not become healthy in time" >&2
    return 1
}

_register_connections() {
    local mysql_url="http://bench_otterstax:8085/add_connection"
    local pg_url="http://bench_otterstax:8085/add_pg_connection"
    local ch_url="http://bench_otterstax:8085/add_ch_connection"

    _post_conn() {
        local url=$1 payload=$2 alias=$3
        for attempt in 1 2 3 4 5; do
            local resp code body
            resp=$(docker run --rm --network=bench_net benchmark-client:latest \
                       curl -s -w "\n%{http_code}" -X POST "$url" \
                       -H "Content-Type: application/json" \
                       -d "$payload" 2>/dev/null)
            code=$(echo "$resp" | tail -n1)
            body=$(echo "$resp" | sed '$d')
            if [ "$code" = "200" ] || [ "$code" = "201" ]; then
                echo "  Registered '$alias': $body"
                return 0
            fi
            echo "  '$alias': HTTP $code ($attempt/5) — retrying..."
            sleep 2
        done
        echo "  ERROR: failed to register '$alias'" >&2
        return 1
    }

    echo "Registering connections..."
    _post_conn "$mysql_url" \
        '{"alias":"mysql1","host":"bench_mariadb1","port":"3306","username":"user1","password":"password1","database":"benchdb1","table":""}' \
        "mysql1"
    _post_conn "$mysql_url" \
        '{"alias":"mysql2","host":"bench_mariadb2","port":"3306","username":"user2","password":"password2","database":"benchdb2","table":""}' \
        "mysql2"
    _post_conn "$pg_url" \
        '{"alias":"pg1","host":"bench_postgres1","port":"5432","username":"pguser","password":"pgpassword","database":"benchpg1","table":""}' \
        "pg1"
    _post_conn "$pg_url" \
        '{"alias":"pg2","host":"bench_postgres2","port":"5432","username":"pguser","password":"pgpassword","database":"benchpg2","table":""}' \
        "pg2"
    _post_conn "$ch_url" \
        '{"alias":"ch1","host":"bench_clickhouse1","port":"9000","username":"chuser","password":"chpassword","database":"benchch1","table":""}' \
        "ch1"
    _post_conn "$ch_url" \
        '{"alias":"ch2","host":"bench_clickhouse2","port":"9000","username":"chuser","password":"chpassword","database":"benchch2","table":""}' \
        "ch2"
}

_frontend_port() {
    case "$1" in
        mysql)    echo 8816 ;;
        postgres) echo 8817 ;;
        arrow)    echo 8815 ;;
        *) echo "Unknown frontend: $1" >&2; return 1 ;;
    esac
}

ALL_TESTS=(simple_select complex_select join_same_instance join_cross_engine join_all)
ALL_FRONTENDS=(mysql postgres arrow)
DEFAULT_FRONTENDS=(mysql postgres)
