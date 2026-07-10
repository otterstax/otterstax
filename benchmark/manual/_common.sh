#!/usr/bin/env bash
# Sourced by all benchmark/manual/ scripts — sets paths, helpers, constants.
# Not executed directly.

MANUAL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$MANUAL_DIR/.." && pwd)"
REPO_ROOT="$(cd "$BENCH_DIR/.." && pwd)"

IMAGE_TAG="${IMAGE_TAG:-bench}"
# Set to true (via start_service.sh --external) to add a seeded MinIO so the
# s3/file external_* benchmarks can run in the manual flow.
EXTERNAL_ENABLED="${EXTERNAL_ENABLED:-false}"

_compose_backends() {
    local _files=(-f "$BENCH_DIR/compose_backends.yml")
    [ "$EXTERNAL_ENABLED" = "true" ] && _files+=(-f "$BENCH_DIR/compose_minio.yml")
    docker compose "${_files[@]}" -p bench "$@"
}

_compose_otterstax() {
    local _files=(
        -f "$BENCH_DIR/compose_backends.yml"
        -f "$BENCH_DIR/compose_benchmark.yml"
        -f "$BENCH_DIR/compose_manual.yml"
    )
    [ "$EXTERNAL_ENABLED" = "true" ] && _files+=(-f "$BENCH_DIR/compose_minio.yml")
    docker compose "${_files[@]}" -p bench "$@"
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

# Register the bench MinIO alias for the s3 external-table benchmarks.
# Alias/bucket/endpoint must match benchmarks/external_common.py + compose_minio.yml.
_register_s3_credentials() {
    local url="http://bench_otterstax:8085/s3/add_credentials"
    local payload='{"alias":"bench_minio","access_key":"minioadmin","secret_key":"minioadmin","region":"us-east-1","endpoint":"bench_minio:9000"}'
    echo "Registering s3 credentials (bench_minio)..."
    for attempt in 1 2 3 4 5 6 7 8 9 10; do
        code=$(docker run --rm --network=bench_net benchmark-client:latest \
                   curl -s -o /dev/null -w "%{http_code}" -X GET "$url" \
                   -H "Content-Type: application/json" -d "$payload" 2>/dev/null)
        if [ "$code" = "200" ] || [ "$code" = "201" ]; then
            echo "  Registered s3 alias 'bench_minio'"
            return 0
        fi
        echo "  s3 creds: HTTP $code ($attempt/10) — retrying..."
        sleep 2
    done
    echo "  ERROR: failed to register s3 credentials" >&2
    return 1
}

# Generate the s3/file external-table fixtures into benchmark/data/fixtures.
_generate_external_fixtures() {
    echo "Generating external-table fixtures..."
    mkdir -p "$BENCH_DIR/data/fixtures"
    docker run --rm \
        -v "$BENCH_DIR/data/fixtures:/app/data/fixtures" \
        -v "$BENCH_DIR/bench.yaml:/app/bench.yaml:ro" \
        -e PYTHONUNBUFFERED=1 \
        benchmark-client:latest \
        python /app/data/generate_external_fixtures.py --out /app/data/fixtures
}

_frontend_port() {
    case "$1" in
        mysql)    echo 8816 ;;
        postgres) echo 8817 ;;
        arrow)    echo 8815 ;;
        *) echo "Unknown frontend: $1" >&2; return 1 ;;
    esac
}

# DEFAULT_TESTS run by default; external_* are opt-in (need MinIO via --external)
# but remain valid --bench values.
DEFAULT_TESTS=(simple_select complex_select join_same_instance join_cross_engine join_all)
ALL_TESTS=("${DEFAULT_TESTS[@]}"
           external_load external_join external_dump
           external_join_cross external_join_all)
ALL_FRONTENDS=(mysql postgres arrow)
DEFAULT_FRONTENDS=(mysql postgres)
