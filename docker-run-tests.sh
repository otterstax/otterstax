#!/bin/bash

set -e  # Exit on any error

# Get project name (directory name, used as Docker Compose project prefix)
PROJECT_NAME=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')
# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# Tracy flag parsing
# ---------------------------------------------------------------------------
ENABLE_TRACY=false
TRACY_SEP=false
TRACY_OUTPUT_DIR=""
TRACY_FILE=""
TRACY_CAPTURE_CONTAINER=""
BUILD_JOBS=0  # 0 means use nproc inside the container

_usage() {
    cat <<'EOF'
Usage: ./docker-run-tests.sh [OPTIONS]

Run the full OtterStax integration test suite inside Docker.

Options:
  --tracy         Enable Tracy profiling. Captures a single combined profile for
                  the entire test run. Output: tracy_profiles/<timestamp>/otterstax.tracy
  --tracy-sep     Enable per-test Tracy profiling. Each test gets its own capture
                  file; otterstax is restarted between tests for a clean disconnect.
                  Output: tracy_profiles/<timestamp>/<test_name>.tracy
  -j N            Parallel jobs for Docker image build (default: nproc inside container)
  -h, --help      Show this help message and exit

Notes:
  - --tracy and --tracy-sep are mutually exclusive; --tracy-sep takes precedence.
  - The server image must be built with Tracy support (WITH_TRACY=true) for either
    flag to produce output. The script sets this automatically.
  - Tracy output directories are never deleted by the script; old runs accumulate
    under tracy_profiles/.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            _usage
            exit 0
            ;;
        --tracy)
            ENABLE_TRACY=true
            TRACY_OUTPUT_DIR="$SCRIPT_DIR/tracy_profiles/$(date +%Y%m%d_%H%M%S)"
            TRACY_FILE="$TRACY_OUTPUT_DIR/otterstax.tracy"
            mkdir -p "$TRACY_OUTPUT_DIR"
            echo "📊 Tracy profiling enabled — output: $TRACY_OUTPUT_DIR"
            shift
            ;;
        --tracy-sep)
            ENABLE_TRACY=true
            TRACY_SEP=true
            TRACY_OUTPUT_DIR="$SCRIPT_DIR/tracy_profiles/$(date +%Y%m%d_%H%M%S)"
            mkdir -p "$TRACY_OUTPUT_DIR"
            echo "📊 Tracy per-test profiling enabled — output: $TRACY_OUTPUT_DIR"
            shift
            ;;
        -j)
            BUILD_JOBS="$2"
            shift 2
            ;;
        -j*)
            BUILD_JOBS="${1#-j}"
            shift
            ;;
        *)
            echo "Unknown option: $1" >&2
            _usage
            exit 1
            ;;
    esac
done
export BUILD_JOBS

# Ensure tracy-capture is stopped and its output noted on any exit.
_tracy_cleanup() {
    if [ -n "$TRACY_CAPTURE_CONTAINER" ]; then
        echo "📊 Tracy cleanup: stopping otterstax for clean disconnect..."
        compose stop test-otterstax 2>/dev/null || true
        # Give tracy-capture up to 10s to finalize the file after disconnect.
        for i in {1..10}; do
            docker inspect "$TRACY_CAPTURE_CONTAINER" --format '{{.State.Running}}' 2>/dev/null | grep -q true || break
            sleep 1
        done
        docker stop "$TRACY_CAPTURE_CONTAINER" 2>/dev/null || true
        docker rm   "$TRACY_CAPTURE_CONTAINER" 2>/dev/null || true
        TRACY_CAPTURE_CONTAINER=""
        [ -f "$TRACY_FILE" ] && echo "📊 Tracy file saved: $TRACY_FILE"
    fi
}
trap _tracy_cleanup EXIT INT TERM

# Detect which compose command to use (docker compose vs docker-compose)
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    echo "❌ Neither 'docker compose' nor 'docker-compose' is available. Please install Docker Compose."
    exit 1
fi

compose() {
    $COMPOSE_CMD -f compose.test.yml "$@"
}

compose_exec() {
    # -T avoids pseudo-TTY allocation issues in non-interactive CI runners.
    compose exec -T "$@"
}

# Retry configuration: allow overriding via env vars (e.g. WAIT_RETRIES=60)
WAIT_RETRIES=${WAIT_RETRIES:-120}
WAIT_SLEEP=${WAIT_SLEEP:-2}

# Activate virtualenv if available
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# Function to check database readiness
# When building from scratch, docker compose may not have enough time to assemble everything within the given timeout
wait_for_database_init() {
    local container=$1
    local user=$2
    local password=$3
    local database=$4
    local table=$5

    echo "🕒 Waiting for table $table initialization in $container..."
    for i in {1..120}; do
        if compose_exec $container mariadb -u $user -p$password $database -e "SELECT 1 FROM $table LIMIT 1;" 2>/dev/null; then
            echo "✅ Table $table is ready in $container"
            return 0
        fi
        echo "⏳ Waiting for table $table... ($i/60)"
        sleep 5
    done
    echo "❌ Timeout waiting for table $table in $container"
    return 1
}

# Function to check MariaDB logs
check_mariadb_logs() {
    local container=$1
    echo "📋 Logs for $container:"
    compose logs $container | tail -20
}

# Function to check existing tables
check_database_tables() {
    local container=$1
    local user=$2
    local password=$3
    local database=$4

    echo "📊 Tables in $database ($container):"
    # Check if it's a PostgreSQL container
    if [[ "$container" == *"postgres" ]]; then
        compose_exec $container psql -U $user -d $database -c "\dt" 2>/dev/null || echo "❌ Failed to connect to PostgreSQL"
    # Check if it's a ClickHouse container
    elif [[ "$container" == *"clickhouse"* ]]; then
        compose_exec $container clickhouse-client --user $user --password $password --database $database --query "SHOW TABLES" 2>/dev/null || echo "❌ Failed to connect to ClickHouse"
    else
        compose_exec $container mariadb -u $user -p$password $database -e "SHOW TABLES;" 2>/dev/null || echo "❌ Failed to connect to database"
    fi
}

# Function to check ClickHouse table readiness
wait_for_ch_table() {
    local container=$1
    local user=$2
    local password=$3
    local database=$4
    local table=$5

    echo "🕒 Waiting for table $table initialization in $container..."
    for i in {1..120}; do
        if compose_exec $container clickhouse-client --user $user --password $password --database $database --query "SELECT 1 FROM $table LIMIT 1" 2>/dev/null; then
            echo "✅ Table $table is ready in $container"
            return 0
        fi
        echo "⏳ Waiting for table $table... ($i/60)"
        sleep 5
    done
    echo "❌ Timeout waiting for table $table in $container"
    return 1
}

# Function to check PostgreSQL table readiness
wait_for_pg_table() {
    local container=$1
    local user=$2
    local database=$3
    local table=$4

    echo "🕒 Waiting for table $table initialization in $container..."
    for i in {1..120}; do
        if compose_exec $container psql -U $user -d $database -c "SELECT 1 FROM $table LIMIT 1;" 2>/dev/null; then
            echo "✅ Table $table is ready in $container"
            return 0
        fi
        echo "⏳ Waiting for table $table... ($i/60)"
        sleep 5
    done
    echo "❌ Timeout waiting for table $table in $container"
    return 1
}

# Create log directory
mkdir -p logs
echo ""
echo "=== Step 1: Cleaning up previous state ==="
echo ""
compose down --volumes --remove-orphans 2>/dev/null || true
# Clean up init directories to ensure fresh data
rm -rf init/mariadb1/* init/mariadb2/* init/postgres/* init/clickhouse/* 2>/dev/null || true

# Rebuild Docker images to pick up latest script changes
echo "🔨 Rebuilding Docker images..."
export DOCKER_BUILDKIT=1

if $ENABLE_TRACY; then
    export WITH_TRACY=true
fi

if [ -n "${IMAGE_TAG}" ]; then
    # CI / sanitizer path: test-otterstax image is pre-built with specific
    # build-args (e.g. ENABLE_ASAN / ENABLE_TSAN) by the workflow
    echo "ℹ️  IMAGE_TAG=${IMAGE_TAG} set; skipping test-otterstax build"
    compose build test-client test-spark-client minio-init
else
    compose build test-client test-spark-client test-otterstax minio-init
fi

echo "✅ Previous containers and volumes removed"

echo ""
echo "=== Step 2: Starting databases ==="
echo ""
compose up -d mariadb1 mariadb2 postgres1 clickhouse1

# MinIO for the s3 external-table tests; minio-init seeds test-bucket then exits.
echo "🪣 Starting MinIO + seeding test-bucket..."
compose up -d minio minio-init

echo ""
echo "=== Step 3: Waiting for databases to be ready ==="
echo ""

# Wait for MariaDB1 to be ready (use mariadb client to verify SQL connectivity)
echo "🕒 Waiting for MariaDB1 to be ready..."
for ((i=1;i<=WAIT_RETRIES;i++)); do
    if compose_exec mariadb1 sh -c "(command -v mysqladmin >/dev/null 2>&1 && mysqladmin -h 127.0.0.1 -u user1 -ppassword1 ping >/dev/null 2>&1) || mariadb -h 127.0.0.1 -u user1 -ppassword1 -e 'SELECT 1;'" >/dev/null 2>&1; then
        echo "✅ MariaDB1 is ready"
        break
    fi
    echo "⏳ Waiting for MariaDB1... ($i/${WAIT_RETRIES})"
    sleep ${WAIT_SLEEP}
    if [ $i -eq ${WAIT_RETRIES} ]; then
        echo "❌ Timeout waiting for MariaDB1"
        compose logs mariadb1 | tail -20
        exit 1
    fi
done

# Wait for MariaDB2 to be ready (use mariadb client to verify SQL connectivity)
echo "🕒 Waiting for MariaDB2 to be ready..."
for ((i=1;i<=WAIT_RETRIES;i++)); do
    if compose_exec mariadb2 sh -c "(command -v mysqladmin >/dev/null 2>&1 && mysqladmin -h 127.0.0.1 -u user2 -ppassword2 ping >/dev/null 2>&1) || mariadb -h 127.0.0.1 -u user2 -ppassword2 -e 'SELECT 1;'" >/dev/null 2>&1; then
        echo "✅ MariaDB2 is ready"
        break
    fi
    echo "⏳ Waiting for MariaDB2... ($i/${WAIT_RETRIES})"
    sleep ${WAIT_SLEEP}
    if [ $i -eq ${WAIT_RETRIES} ]; then
        echo "❌ Timeout waiting for MariaDB2"
        compose logs mariadb2 | tail -20
        exit 1
    fi
done

# Wait for PostgreSQL to be ready
echo "🕒 Waiting for PostgreSQL to be ready..."
for ((i=1;i<=WAIT_RETRIES;i++)); do
    if compose_exec postgres1 pg_isready -U pguser -d pgdb >/dev/null 2>&1; then
        echo "✅ PostgreSQL is ready"
        break
    fi
    echo "⏳ Waiting for PostgreSQL... ($i/${WAIT_RETRIES})"
    sleep ${WAIT_SLEEP}
    if [ $i -eq ${WAIT_RETRIES} ]; then
        echo "❌ Timeout waiting for PostgreSQL"
        compose logs postgres1 | tail -20
        exit 1
    fi
done

# Wait for ClickHouse to be ready
echo "🕒 Waiting for ClickHouse to be ready..."
for i in {1..30}; do
    if compose_exec clickhouse1 clickhouse-client --user chuser --password chpassword --query "SELECT 1" >/dev/null 2>&1; then
        echo "✅ ClickHouse is ready"
        break
    fi
    echo "⏳ Waiting for ClickHouse... ($i/30)"
    sleep 2
    if [ $i -eq 30 ]; then
        echo "❌ Timeout waiting for ClickHouse"
        compose logs clickhouse1 | tail -20
        exit 1
    fi
done

# Give databases extra time to be fully ready
echo "⏳ Giving databases extra time to stabilize..."
sleep 5

echo ""
echo "=== Step 4: Creating test data ==="
echo ""
# Create test data by connecting directly to running databases
# Use --use-aliases to ensure proper network connectivity
compose run --rm --no-deps --use-aliases test-client python create_test_data.py

if [ $? -ne 0 ]; then
    echo "❌ Test data creation failed!"
    echo "📋 Checking database status..."
    compose_exec mariadb1 mariadb -u user1 -ppassword1 db1 -e "SELECT 1;" 2>&1 || echo "MariaDB1 not accessible"
    compose_exec mariadb2 mariadb -u user2 -ppassword2 db2 -e "SELECT 1;" 2>&1 || echo "MariaDB2 not accessible"
    compose_exec postgres1 psql -U pguser -d pgdb -c "SELECT 1;" 2>&1 || echo "PostgreSQL not accessible"
    compose_exec clickhouse1 clickhouse-client --user chuser --password chpassword --database chdb --query "SELECT 1" 2>&1 || echo "ClickHouse not accessible"
    exit 1
fi

echo ""
echo "=== Step 5: Verifying data creation ==="
echo ""
# Check that tables were created
check_database_tables mariadb1 user1 password1 db1
check_database_tables mariadb2 user2 password2 db2
check_database_tables postgres1 pguser "" pgdb
check_database_tables clickhouse1 chuser chpassword chdb

echo ""
echo "=== Step 6: Checking specific tables ==="
echo ""
wait_for_database_init mariadb1 user1 password1 db1 campaigns
wait_for_database_init mariadb2 user2 password2 db2 impressions
wait_for_pg_table postgres1 pguser pgdb products
wait_for_ch_table clickhouse1 chuser chpassword chdb orders

# Wait for the one-shot minio-init to finish seeding test-bucket so the s3
# external-table tests find their fixtures. It exits after seeding.
echo "🕒 Waiting for MinIO bucket seeding to complete..."
for i in {1..60}; do
    status=$(docker inspect test-minio-init --format '{{.State.Status}}' 2>/dev/null || echo "missing")
    if [ "$status" = "exited" ]; then
        echo "✅ MinIO test-bucket seeded"
        break
    fi
    sleep 2
    if [ $i -eq 60 ]; then
        echo "⚠️  minio-init did not finish in time; s3 tests may retry"
        compose logs minio-init | tail -10 2>/dev/null || true
    fi
done

echo ""
echo "=== Step 7: Starting otterstax ==="
echo ""
compose up -d test-otterstax

# Wait for otterstax to be healthy with retry logic
echo "🕒 Waiting for otterstax to be healthy..."
for ((i=1;i<=WAIT_RETRIES;i++)); do
    if compose_exec test-otterstax curl -s -f http://localhost:8085/health >/dev/null 2>&1; then
        echo "✅ Otterstax is healthy"
        break
    fi
    echo "⏳ Waiting for otterstax... ($i/${WAIT_RETRIES})"
    sleep ${WAIT_SLEEP}
    if [ $i -eq ${WAIT_RETRIES} ]; then
        echo "❌ Timeout waiting for otterstax"
        compose logs test-otterstax | tail -30
        exit 1
    fi
done

# Give otterstax extra time to fully initialize all backends
echo "⏳ Giving otterstax extra time to initialize backends..."
sleep 10

if $ENABLE_TRACY && ! $TRACY_SEP; then
    echo ""
    echo "=== Step 7b: Starting Tracy capture ==="
    echo ""
    echo "📋 Tracy port check (bash /dev/tcp inside container):"
    compose_exec test-otterstax bash -c "
        timeout 2 bash -c '</dev/tcp/localhost/8086' 2>/dev/null \
            && echo 'Port 8086 OPEN — Tracy IS listening' \
            || echo 'Port 8086 CLOSED — Tracy is NOT listening'
        timeout 2 bash -c '</dev/tcp/localhost/8085' 2>/dev/null \
            && echo 'Port 8085 OPEN (health port sanity check OK)' \
            || echo 'Port 8085 CLOSED (health port — unexpected)'
    " 2>/dev/null || echo "(exec failed)"
    echo "📋 Tracy port check (nc from host via exposed port):"
    nc -z -w 2 localhost 8086 2>/dev/null && echo "Port 8086 reachable from host" || echo "Port 8086 NOT reachable from host"
    echo "📋 otterstax server stdout (last 20 lines):"
    compose logs --no-color --tail 20 test-otterstax 2>/dev/null || true
    echo ""
    # Run tracy-capture as a sidecar container (the binary lives in the image, not on the macOS host).
    # It connects to test-otterstax via the Docker network and writes to the mounted profiles dir.
    TRACY_CAPTURE_CONTAINER="tracy-capture-${PROJECT_NAME}-$$"
    docker run -d \
        --name "$TRACY_CAPTURE_CONTAINER" \
        --network "${PROJECT_NAME}_test_app_network" \
        -v "$TRACY_OUTPUT_DIR:/tracy_profiles" \
        "otterstax_app:${IMAGE_TAG:-latest}" \
        tracy-capture -a test-otterstax -p 8086 -o /tracy_profiles/otterstax.tracy -f
    sleep 2
    if docker inspect "$TRACY_CAPTURE_CONTAINER" --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
        echo "✅ Tracy capture running in container $TRACY_CAPTURE_CONTAINER"
    else
        echo "⚠️  tracy-capture container exited immediately"
        docker logs "$TRACY_CAPTURE_CONTAINER" 2>/dev/null | tail -10
        docker rm "$TRACY_CAPTURE_CONTAINER" 2>/dev/null || true
        TRACY_CAPTURE_CONTAINER=""
    fi
fi

echo ""
echo "=== Step 8: Running main tests ==="
echo ""

TEST_RC=0

if $TRACY_SEP; then
    # Per-test Tracy capture: each test gets its own tracy-capture sidecar.
    # otterstax is restarted between tests so tracy-capture receives a clean TCP
    # disconnect and can finalise its .tracy file before the next test begins.
    TESTS=(
        "FlightSQL Schema (MySQL backend):test_schema_flightsql_client_mysql_backend.py"
        "FlightSQL Schema (PostgreSQL backend):test_schema_flightsql_client_pg_backend.py"
        "FlightSQL Schema (ClickHouse backend):test_schema_flightsql_client_ch_backend.py"
        "Cross-backend Schema:test_schema_cross_backend.py"
        "Cross-backend JOIN Schema:test_schema_cross_backend_join.py"
        "FlightSQL Client (MySQL backend):test_flightsql_client_mysql_backend.py"
        "FlightSQL Client (PostgreSQL backend):test_flightsql_client_pg_backend.py"
        "FlightSQL Client (ClickHouse backend):test_flightsql_client_ch_backend.py"
        "MySQL Client (MySQL backend):test_mysql_client_mysql_backend.py"
        "PostgreSQL Client (MySQL backend):test_pg_client_mysql_backend.py"
        "MySQL Client (PostgreSQL backend):test_mysql_client_pg_backend.py"
        "PostgreSQL Client (PostgreSQL backend):test_pg_client_pg_backend.py"
        "MySQL Client (ClickHouse backend):test_mysql_client_ch_backend.py"
        "PostgreSQL Client (ClickHouse backend):test_pg_client_ch_backend.py"
        "FlightSQL Client (MySQL backend, mutable):test_flightsql_client_mysql_backend_mutable.py"
        "Cross-backend Queries (MySQL wire):test_cross_backend_queries_mysql.py"
        "Cross-backend Queries (PostgreSQL wire):test_cross_backend_queries_pg.py"
        "Schema MySQL Client (file external):test_schema_mysql_file.py"
        "Schema MySQL Client (s3 external):test_schema_mysql_s3.py"
        "MySQL Client (file external):test_mysql_file.py"
        "MySQL Client (file external, ndjson):test_mysql_file_ndjson.py"
        "MySQL Client (s3 external):test_mysql_s3.py"
        "MySQL Client (JOIN sql ⋈ s3 parquet → s3 csv):test_mysql_join_sql_s3_to_s3.py"
        "MySQL Client (JOIN otterbrix-local ⋈ s3 parquet):test_mysql_join_otb_local_s3.py"
        "MySQL Client (JOIN sql backend ⋈ otterbrix-local, string key):test_mysql_join_otb_local_backend.py"
        "MySQL Client (JOIN s3 parquet ⋈ file csv ⋈ otterbrix-local):test_mysql_join_otb_local_s3_file.py"
    )

    SEP_PASSED=0
    SEP_FAILED=0

    for entry in "${TESTS[@]}"; do
        test_name="${entry%%:*}"
        test_file="${entry#*:}"
        tracy_out="${test_file%.py}.tracy"

        echo ""
        echo "========================================="
        echo " [Tracy-Sep] $test_name"
        echo "========================================="

        # (Re)start otterstax fresh for this test.
        compose up -d test-otterstax
        echo "⏳ Waiting for otterstax to be healthy..."
        _skip=false
        for ((i=1;i<=WAIT_RETRIES;i++)); do
            if compose_exec test-otterstax curl -s -f http://localhost:8085/health >/dev/null 2>&1; then
                echo "✅ Otterstax healthy"
                break
            fi
            sleep ${WAIT_SLEEP}
            if [ $i -eq ${WAIT_RETRIES} ]; then
                echo "❌ Timeout waiting for otterstax — skipping $test_name"
                compose stop test-otterstax 2>/dev/null || true
                compose rm -f test-otterstax 2>/dev/null || true
                SEP_FAILED=$((SEP_FAILED + 1))
                _skip=true
                break
            fi
        done
        if $_skip; then continue; fi

        # Add all database connections.
        compose run --rm --use-aliases test-client bash -c "
            cd /app
            ./add_connections.sh
            curl -s -X POST 'http://test-otterstax:8085/add_pg_connection' \
                -H 'Content-Type: application/json' -d @connection_postgres.json >/dev/null 2>&1 || true
            curl -s -X POST 'http://test-otterstax:8085/add_ch_connection' \
                -H 'Content-Type: application/json' -d @connection_clickhouse.json >/dev/null 2>&1 || true
            sleep 3
        "

        # Start a dedicated tracy-capture sidecar for this test.
        SEP_CAPTURE="tracy-sep-${PROJECT_NAME}-$$-$((SEP_PASSED + SEP_FAILED))"
        docker run -d \
            --name "$SEP_CAPTURE" \
            --network "${PROJECT_NAME}_test_app_network" \
            -v "$TRACY_OUTPUT_DIR:/tracy_profiles" \
            "otterstax_app:${IMAGE_TAG:-latest}" \
            tracy-capture -a test-otterstax -p 8086 -o "/tracy_profiles/${tracy_out}" -f
        sleep 2
        if ! docker inspect "$SEP_CAPTURE" --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
            echo "⚠️  tracy-capture failed to start for $test_name"
            docker rm "$SEP_CAPTURE" 2>/dev/null || true
            SEP_CAPTURE=""
        fi

        # Run the individual test.
        compose run --rm --use-aliases test-client bash -c "cd /app && python $test_file"
        _test_rc=$?

        if [ $_test_rc -eq 0 ]; then
            echo "✅ PASSED: $test_name"
            SEP_PASSED=$((SEP_PASSED + 1))
        else
            echo "❌ FAILED: $test_name (exit code $_test_rc)"
            SEP_FAILED=$((SEP_FAILED + 1))
        fi

        # Stop otterstax so tracy-capture receives a clean TCP disconnect and
        # can finalise the output file before we move to the next test.
        compose stop test-otterstax 2>/dev/null || true

        if [ -n "$SEP_CAPTURE" ]; then
            echo "⏳ Waiting for tracy-capture to finalise ${tracy_out}..."
            for i in {1..30}; do
                if ! docker inspect "$SEP_CAPTURE" --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
                    echo "✅ Tracy capture done (${i}s)"
                    break
                fi
                sleep 1
                if [ $i -eq 30 ]; then
                    echo "⚠️  tracy-capture did not exit within 30s — stopping"
                    docker stop "$SEP_CAPTURE" 2>/dev/null || true
                fi
            done
            docker logs "$SEP_CAPTURE" 2>&1 | tail -5
            docker rm "$SEP_CAPTURE" 2>/dev/null || true
            SEP_CAPTURE=""
            if [ -f "$TRACY_OUTPUT_DIR/${tracy_out}" ]; then
                echo "📊 Saved: $TRACY_OUTPUT_DIR/${tracy_out}"
            else
                echo "⚠️  Tracy file not found: $TRACY_OUTPUT_DIR/${tracy_out}"
            fi
        fi

        # Remove the stopped container so compose up recreates it fresh next iteration.
        compose rm -f test-otterstax 2>/dev/null || true
    done

    echo ""
    echo "=== Tracy-Sep Test Summary: $SEP_PASSED passed, $SEP_FAILED failed ==="
    [ $SEP_FAILED -gt 0 ] && TEST_RC=1 || TEST_RC=0
else
    # Standard mode: run all tests in a single container via the startup script.
    # Use test-spark-client (a superset of test-client: adds OpenJDK + pyspark) so
    # the Spark Connect E2E tests run alongside the rest of the suite — startup.sh
    # runs them only when `import pyspark` succeeds (skipped in the plain client).
    compose run --rm --use-aliases test-spark-client bash -c "/app/startup.sh"
    TEST_RC=$?
fi

echo ""
echo "=== Test run exit code: $TEST_RC ==="
if [ $TEST_RC -ne 0 ]; then
    echo "⚠️  Some tests failed. The CI run will continue (exit code preserved in logs)."
else
    echo "✅ All tests passed inside the container."
fi

if $ENABLE_TRACY && [ -n "$TRACY_CAPTURE_CONTAINER" ]; then
    echo ""
    echo "=== Step 8b: Stopping Tracy capture ==="
    echo ""
    # Stop otterstax FIRST — this closes the TCP connection to tracy-capture,
    # which is its natural signal to finalize and write the profile file.
    # Killing tracy-capture directly (SIGTERM) causes it to exit without flushing.
    echo "⏳ Stopping otterstax so tracy-capture receives a clean disconnect..."
    compose stop test-otterstax 2>/dev/null || true

    # Wait up to 30 s for tracy-capture to exit naturally after the disconnect.
    echo "⏳ Waiting for tracy-capture to finalize the profile..."
    for i in {1..30}; do
        if ! docker inspect "$TRACY_CAPTURE_CONTAINER" --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
            echo "✅ Tracy capture finished (${i}s)"
            break
        fi
        sleep 1
        if [ $i -eq 30 ]; then
            echo "⚠️  tracy-capture did not exit within 30s — stopping it now"
            docker stop "$TRACY_CAPTURE_CONTAINER" 2>/dev/null || true
        fi
    done

    echo "📋 Tracy capture log:"
    docker logs "$TRACY_CAPTURE_CONTAINER" 2>&1 | tail -20
    docker rm "$TRACY_CAPTURE_CONTAINER" 2>/dev/null || true
    TRACY_CAPTURE_CONTAINER=""

    if [ -f "$TRACY_FILE" ]; then
        echo "📊 Tracy capture saved: $TRACY_FILE"
        echo "   Open with: tracy-profiler $TRACY_FILE"
    else
        echo "⚠️  Expected Tracy file not found: $TRACY_FILE"
    fi
fi

echo ""
echo "=== Step 9: Cleanup ==="
echo ""
compose down --volumes --remove-orphans
rm -rf .volumes 2>/dev/null || true

echo "✅ Tests completed."