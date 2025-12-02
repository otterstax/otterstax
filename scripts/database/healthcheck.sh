#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 OtterStax


# Database configurations
declare -A DB_CONFIG=(
    [1_HOST]="127.0.0.1"
    [1_PORT]="3101"
    [1_USER]="user1"
    [1_PASS]="password1"
    [1_DB]="db1"
    
    [2_HOST]="127.0.0.1"
    [2_PORT]="3102"
    [2_USER]="user2"
    [2_PASS]="password2"
    [2_DB]="db2"
    
    [3_HOST]="127.0.0.1"
    [3_PORT]="3103"
    [3_USER]="user3"
    [3_PASS]="password3"
    [3_DB]="db2"
)

# Check if instance number was provided
if [ -z "$1" ]; then
    echo "Usage: $0 <instance_number> (1, 2, or 3)"
    exit 1
fi

INSTANCE=$1

# Validate instance number
if [[ ! "$INSTANCE" =~ ^[1-3]$ ]]; then
    echo "Error: Invalid instance number. Use 1, 2, or 3."
    exit 1
fi

# Get config for selected instance
HOST="${DB_CONFIG[${INSTANCE}_HOST]}"
PORT="${DB_CONFIG[${INSTANCE}_PORT]}"
USER="${DB_CONFIG[${INSTANCE}_USER]}"
PASS="${DB_CONFIG[${INSTANCE}_PASS]}"
DB="${DB_CONFIG[${INSTANCE}_DB]}"

# Test connection
if mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASS" "$DB" -e "SELECT 1;" >/dev/null 2>&1; then
    echo "✅ MariaDB instance $INSTANCE is running and accessible"
    echo "   Host: $HOST:$PORT"
    echo "   Version: $(mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASS" "$DB" -e "SELECT VERSION();" -s)"
    echo "   Database: $DB"
    echo "   Uptime: $(mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASS" "$DB" -e "SHOW STATUS LIKE 'Uptime';" -s | awk '{print $2}') seconds"
else
    echo "❌ Could not connect to MariaDB instance $INSTANCE"
    echo "   Check:"
    echo "   1. Container is running: docker ps | grep mariadb_instance$INSTANCE"
    echo "   2. Port mapping: docker port mariadb_instance$INSTANCE"
    echo "   3. Credentials are correct"
    exit 1
fi