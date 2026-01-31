#!/bin/bash

set -e  # Exit on any error

# Get project name (directory name, used as Docker Compose project prefix)
PROJECT_NAME=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')

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
        if docker compose -f compose.test.yml exec $container mariadb -u $user -p$password $database -e "SELECT 1 FROM $table LIMIT 1;" 2>/dev/null; then
            echo "✅ Table $table is ready in $container"
            return 0
        fi
        echo "⏳ Waiting for table $table... ($i/60)"
        sleep 5
    done
    echo "❌ Timeout waiting for table $table in $container"
    return 1
}

# Function to check volume contents
check_volume_contents() {
    echo "🔍 Checking volume contents:"

    # Use the actual volume names that Docker Compose creates (project_name + volume_name)
    local volume1="${PROJECT_NAME}_mariadb1_init"
    local volume2="${PROJECT_NAME}_mariadb2_init"
    
    echo "=== Volume $volume1 ==="
    docker run --rm -v $volume1:/data alpine sh -c "
        echo 'Files in volume:'
        ls -la /data/
        echo 'Contents of init.sql (first 10 lines):'
        if [ -f /data/init.sql ]; then
            head -10 /data/init.sql
        else
            echo '❌ File init.sql not found!'
        fi
    " || echo "❌ Failed to check volume $volume1"
    
    echo "=== Volume $volume2 ==="
    docker run --rm -v $volume2:/data alpine sh -c "
        echo 'Files in volume:'
        ls -la /data/
        echo 'Contents of init.sql (first 10 lines):'
        if [ -f /data/init.sql ]; then
            head -10 /data/init.sql
        else
            echo '❌ File init.sql not found!'
        fi
    " || echo "❌ Failed to check volume $volume2"
}

# Function to check MariaDB logs
check_mariadb_logs() {
    local container=$1
    echo "📋 Logs for $container:"
    docker compose -f compose.test.yml logs $container | tail -20
}

# Function to check existing tables
check_database_tables() {
    local container=$1
    local user=$2
    local password=$3
    local database=$4

    echo "📊 Tables in $database ($container):"
    docker compose -f compose.test.yml exec $container mariadb -u $user -p$password $database -e "SHOW TABLES;" 2>/dev/null || echo "❌ Failed to connect to database"
}

# Create log directory
mkdir -p logs
echo ""
echo "=== Step 1: Cleaning up previous state ==="
echo ""
docker compose -f compose.test.yml down --volumes --remove-orphans 2>/dev/null || true
echo "✅ Previous containers and volumes removed"

echo ""
echo "=== Step 2: Checking volumes before creating data ==="
echo ""
check_volume_contents

echo ""
echo "=== Step 3: Creating test data from Docker container ==="
echo ""
# Use --no-deps to prevent MariaDB from starting before init scripts are written
docker compose -f compose.test.yml run --rm --no-deps test-client python create_test_data.py

echo ""
echo "=== Step 4: Checking volumes after creating data ==="
echo ""
check_volume_contents

echo ""
echo "=== Step 5: Starting databases with init scripts ==="
echo ""
# Force recreate to ensure init scripts are executed
docker compose -f compose.test.yml up -d --force-recreate mariadb1 mariadb2

echo ""
echo "=== Step 6: Waiting for MariaDB to start ==="
echo ""

# Wait for MariaDB1 to be ready
echo "🕒 Waiting for MariaDB1 to be ready..."
for i in {1..30}; do
    if docker compose -f compose.test.yml exec mariadb1 mariadb-admin ping -h localhost -u user1 -ppassword1 --silent; then
        echo "✅ MariaDB1 is ready"
        break
    fi
    echo "⏳ Waiting for MariaDB1... ($i/30)"
    sleep 2
    if [ $i -eq 30 ]; then
        echo "❌ Timeout waiting for MariaDB1"
        check_mariadb_logs mariadb1
        exit 1
    fi
done

# Wait for MariaDB2 to be ready
echo "🕒 Waiting for MariaDB2 to be ready..."
for i in {1..30}; do
    if docker compose -f compose.test.yml exec mariadb2 mariadb-admin ping -h localhost -u user2 -ppassword2 --silent; then
        echo "✅ MariaDB2 is ready"
        break
    fi
    echo "⏳ Waiting for MariaDB2... ($i/30)"
    sleep 2
    if [ $i -eq 30 ]; then
        echo "❌ Timeout waiting for MariaDB2"
        check_mariadb_logs mariadb2
        exit 1
    fi
done

echo ""
echo "=== Step 7: Checking database initialization ==="
echo ""
echo "⏳ Giving MariaDB time to execute init scripts..."
sleep 10

echo "📋 Checking tables in databases:"
check_database_tables mariadb1 user1 password1 db1
check_database_tables mariadb2 user2 password2 db2

echo "📋 MariaDB logs after initialization:"
check_mariadb_logs mariadb1
check_mariadb_logs mariadb2

echo ""
echo "=== Step 8: Checking specific tables ==="
echo ""
wait_for_database_init mariadb1 user1 password1 db1 campaigns
wait_for_database_init mariadb2 user2 password2 db2 impressions

echo ""
echo "=== Step 9: Starting otterstax ==="
echo ""
docker compose -f compose.test.yml up -d test-otterstax

echo ""
echo "=== Step 10: Running main tests ==="
echo ""
docker compose -f compose.test.yml run --rm test-client bash -c "/app/startup.sh"

echo ""
echo "=== Step 11: Cleanup ==="
echo ""
docker compose -f compose.test.yml down --volumes --remove-orphans

echo "✅ Tests completed."