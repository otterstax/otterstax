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
# Set to true (via start_service.sh --kafka) to add a redpanda broker + a seeded
# topic so the kafka_* benchmarks can run in the manual flow.
KAFKA_ENABLED="${KAFKA_ENABLED:-false}"

_compose_backends() {
    local _files=(-f "$BENCH_DIR/compose_backends.yml")
    [ "$EXTERNAL_ENABLED" = "true" ] && _files+=(-f "$BENCH_DIR/compose_minio.yml")
    [ "$KAFKA_ENABLED" = "true" ] && _files+=(-f "$BENCH_DIR/compose_kafka.yml")
    docker compose "${_files[@]}" -p bench "$@"
}

_compose_otterstax() {
    local _files=(
        -f "$BENCH_DIR/compose_backends.yml"
        -f "$BENCH_DIR/compose_benchmark.yml"
        -f "$BENCH_DIR/compose_manual.yml"
    )
    [ "$EXTERNAL_ENABLED" = "true" ] && _files+=(-f "$BENCH_DIR/compose_minio.yml")
    [ "$KAFKA_ENABLED" = "true" ] && _files+=(-f "$BENCH_DIR/compose_kafka.yml")
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
    # No HTTP health port anymore — probe the MySQL wire port (8816). Connections
    # are read from the mounted connection config file at startup.
    echo "Waiting for OtterStax wire port (8816)..."
    for i in $(seq 1 60); do
        if docker run --rm --network=bench_net benchmark-client:latest \
               python -c "import socket; socket.create_connection(('bench_otterstax', 8816), 2)" >/dev/null 2>&1; then
            echo "  OtterStax ready"
            return 0
        fi
        sleep 3
    done
    echo "ERROR: OtterStax did not become ready in time" >&2
    return 1
}

# Connections (mysql1/mysql2/pg1/pg2/ch1/ch2) are registered from the mounted
# connection config file (benchmark/config.yaml) at server startup — there
# is no runtime registration API. Kept as a no-op so call sites need no change.
_register_connections() {
    echo "Connections read from benchmark/config.yaml at startup (no runtime registration)."
}

# The bench MinIO s3 alias ('bench_minio') is likewise declared in
# benchmark/config.yaml and registered at startup. No-op here.
_register_s3_credentials() {
    echo "s3 alias 'bench_minio' read from benchmark/config.yaml at startup."
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

# Generate the kafka JSON dataset into benchmark/data/fixtures.
# Uses the image-baked /app/bench.yaml (NOT the host file) so the row count/topic
# match what the runner reads — editing the kafka block needs --rebuild, the same
# contract as the rest of bench.yaml.
_generate_kafka_fixtures() {
    echo "Generating kafka JSON dataset..."
    mkdir -p "$BENCH_DIR/data/fixtures"
    docker run --rm \
        -v "$BENCH_DIR/data/fixtures:/app/data/fixtures" \
        -e PYTHONUNBUFFERED=1 \
        benchmark-client:latest \
        python /app/data/generate_kafka_fixtures.py --out /app/data/fixtures
}

# Seed the kafka topic from the generated dataset (once, after the broker is up).
_seed_kafka() {
    echo "Seeding kafka topic..."
    docker run --rm --network=bench_net \
        -v "$BENCH_DIR/data/fixtures:/fixtures:ro" \
        -e PYTHONUNBUFFERED=1 \
        benchmark-client:latest \
        python /app/data/seed_kafka.py --broker bench_kafka:9092 \
        --file /fixtures/kafka_events.ndjson
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
           external_join_cross external_join_all
           kafka_ingest kafka_produce kafka_stream)
ALL_FRONTENDS=(mysql postgres arrow)
DEFAULT_FRONTENDS=(mysql postgres)
