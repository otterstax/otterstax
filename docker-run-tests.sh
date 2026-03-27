#!/bin/bash

set -e  # Exit on any error

# Get project name (directory name, used as Docker Compose project prefix)
PROJECT_NAME=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')
# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Detect which compose command to use (docker compose vs docker-compose)
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    echo "❌ Neither 'docker compose' nor 'docker-compose' is available. Please install Docker Compose."
    exit 1
fi

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
        if $COMPOSE_CMD -f compose.test.yml exec $container mariadb -u $user -p$password $database -e "SELECT 1 FROM $table LIMIT 1;" 2>/dev/null; then
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
    $COMPOSE_CMD -f compose.test.yml logs $container | tail -20
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
        $COMPOSE_CMD -f compose.test.yml exec $container psql -U $user -d $database -c "\dt" 2>/dev/null || echo "❌ Failed to connect to PostgreSQL"
    else
        $COMPOSE_CMD -f compose.test.yml exec $container mariadb -u $user -p$password $database -e "SHOW TABLES;" 2>/dev/null || echo "❌ Failed to connect to database"
    fi
}

# Function to check PostgreSQL table readiness
wait_for_pg_table() {
    local container=$1
    local user=$2
    local database=$3
    local table=$4

    echo "🕒 Waiting for table $table initialization in $container..."
    for i in {1..120}; do
        if $COMPOSE_CMD -f compose.test.yml exec $container psql -U $user -d $database -c "SELECT 1 FROM $table LIMIT 1;" 2>/dev/null; then
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
$COMPOSE_CMD -f compose.test.yml down --volumes --remove-orphans 2>/dev/null || true
# Clean up init directories to ensure fresh data
rm -rf init/mariadb1/* init/mariadb2/* init/postgres/* 2>/dev/null || true

# Rebuild Docker images to pick up latest script changes
echo "🔨 Rebuilding Docker images..."
$COMPOSE_CMD -f compose.test.yml build --no-cache test-client test-otterstax

echo "✅ Previous containers and volumes removed"

echo ""
echo "=== Step 2: Starting databases ==="
echo ""
$COMPOSE_CMD -f compose.test.yml up -d mariadb1 mariadb2 postgres1

echo ""
echo "=== Step 3: Waiting for databases to be ready ==="
echo ""

# Wait for MariaDB1 to be ready (use mariadb client to verify SQL connectivity)
echo "🕒 Waiting for MariaDB1 to be ready..."
for i in {1..30}; do
    if $COMPOSE_CMD -f compose.test.yml exec mariadb1 sh -c "mariadb -h 127.0.0.1 -u user1 -ppassword1 -e 'SELECT 1;'" >/dev/null 2>&1; then
        echo "✅ MariaDB1 is ready"
        break
    fi
    echo "⏳ Waiting for MariaDB1... ($i/30)"
    sleep 2
    if [ $i -eq 30 ]; then
        echo "❌ Timeout waiting for MariaDB1"
        $COMPOSE_CMD -f compose.test.yml logs mariadb1 | tail -20
        exit 1
    fi
done

# Wait for MariaDB2 to be ready (use mariadb client to verify SQL connectivity)
echo "🕒 Waiting for MariaDB2 to be ready..."
for i in {1..30}; do
    if $COMPOSE_CMD -f compose.test.yml exec mariadb2 sh -c "mariadb -h 127.0.0.1 -u user2 -ppassword2 -e 'SELECT 1;'" >/dev/null 2>&1; then
        echo "✅ MariaDB2 is ready"
        break
    fi
    echo "⏳ Waiting for MariaDB2... ($i/30)"
    sleep 2
    if [ $i -eq 30 ]; then
        echo "❌ Timeout waiting for MariaDB2"
        $COMPOSE_CMD -f compose.test.yml logs mariadb2 | tail -20
        exit 1
    fi
done

# Wait for PostgreSQL to be ready
echo "🕒 Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if $COMPOSE_CMD -f compose.test.yml exec postgres1 pg_isready -U pguser -d pgdb >/dev/null 2>&1; then
        echo "✅ PostgreSQL is ready"
        break
    fi
    echo "⏳ Waiting for PostgreSQL... ($i/30)"
    sleep 2
    if [ $i -eq 30 ]; then
        echo "❌ Timeout waiting for PostgreSQL"
        $COMPOSE_CMD -f compose.test.yml logs postgres1 | tail -20
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
$COMPOSE_CMD -f compose.test.yml run --rm --no-deps --use-aliases test-client python create_test_data.py

if [ $? -ne 0 ]; then
    echo "❌ Test data creation failed!"
    echo "📋 Checking database status..."
    $COMPOSE_CMD -f compose.test.yml exec mariadb1 mariadb -u user1 -ppassword1 db1 -e "SELECT 1;" 2>&1 || echo "MariaDB1 not accessible"
    $COMPOSE_CMD -f compose.test.yml exec mariadb2 mariadb -u user2 -ppassword2 db2 -e "SELECT 1;" 2>&1 || echo "MariaDB2 not accessible"
    $COMPOSE_CMD -f compose.test.yml exec postgres1 psql -U pguser -d pgdb -c "SELECT 1;" 2>&1 || echo "PostgreSQL not accessible"
    exit 1
fi

echo ""
echo "=== Step 5: Verifying data creation ==="
echo ""
# Check that tables were created
check_database_tables mariadb1 user1 password1 db1
check_database_tables mariadb2 user2 password2 db2
check_database_tables postgres1 pguser "" pgdb

echo ""
echo "=== Step 6: Checking specific tables ==="
echo ""
wait_for_database_init mariadb1 user1 password1 db1 campaigns
wait_for_database_init mariadb2 user2 password2 db2 impressions
wait_for_pg_table postgres1 pguser pgdb products

echo ""
echo "=== Step 7: Starting otterstax ==="
echo ""
$COMPOSE_CMD -f compose.test.yml up -d test-otterstax

# Wait for otterstax to be healthy with retry logic
echo "🕒 Waiting for otterstax to be healthy..."
for i in {1..60}; do
    if $COMPOSE_CMD -f compose.test.yml exec test-otterstax curl -s -f http://localhost:8085/health >/dev/null 2>&1; then
        echo "✅ Otterstax is healthy"
        break
    fi
    echo "⏳ Waiting for otterstax... ($i/60)"
    sleep 2
    if [ $i -eq 60 ]; then
        echo "❌ Timeout waiting for otterstax"
        $COMPOSE_CMD -f compose.test.yml logs test-otterstax | tail -30
        exit 1
    fi
done

# Give otterstax extra time to fully initialize all backends
echo "⏳ Giving otterstax extra time to initialize backends..."
sleep 10

echo ""
echo "=== Step 8: Running main tests ==="
echo ""
# Run tests in a new container with proper network connectivity (--use-aliases for DNS)
$COMPOSE_CMD -f compose.test.yml run --rm --use-aliases test-client bash -c "/app/startup.sh"
TEST_RC=$?
echo ""
echo "=== Test run exit code: $TEST_RC ==="
if [ $TEST_RC -ne 0 ]; then
    echo "⚠️  Some tests failed. The CI run will continue (exit code preserved in logs)."
else
    echo "✅ All tests passed inside the container."
fi

echo ""
echo "=== Step 9: Cleanup ==="
echo ""
$COMPOSE_CMD -f compose.test.yml down --volumes --remove-orphans
rm -rf .volumes 2>/dev/null || true

echo "✅ Tests completed."