#!/bin/bash
# Default: use test-otterstax hostname for Docker network connectivity
# Use --local flag for local access (0.0.0.0)
HOST="test-otterstax"

while [[ $# -gt 0 ]]; do
    case $1 in
        --local)
            HOST="0.0.0.0"
            shift
            ;;
        *)
            echo "Usage: $0 [--local]"
            echo "  --local  Use 0.0.0.0 instead of test-otterstax hostname"
            exit 1
            ;;
    esac
done

MYSQL_URL="http://${HOST}:8085/add_connection"
PG_URL="http://${HOST}:8085/add_pg_connection"

# Function to send connection request with retry
send_connection_request() {
    local url=$1
    local file=$2
    local max_retries=5
    local retry_delay=2
    
    for i in $(seq 1 $max_retries); do
        echo "Sending request with $file (attempt $i/$max_retries)..."
        response=$(curl -s -w "\n%{http_code}" -X POST "$url" -H "Content-Type: application/json" -d @"$file" 2>/dev/null)
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n -1)
        
        if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
            echo "✅ Success: $body"
            return 0
        elif [ "$http_code" = "000" ]; then
            echo "⏳ Server not ready, retrying in ${retry_delay}s..."
            sleep $retry_delay
        else
            echo "⚠️ Unexpected response: $http_code - $body"
            sleep $retry_delay
        fi
    done
    
    echo "❌ Failed to send connection request after $max_retries attempts"
    return 1
}

# MySQL connections
for FILE in connection_maria_db1.json connection_maria_db2.json ; do
    send_connection_request "$MYSQL_URL" "$FILE" || echo "Warning: Connection $FILE may have failed"
    echo ""
done

# PostgreSQL connection
FILE="connection_postgres.json"
send_connection_request "$PG_URL" "$FILE" || echo "Warning: PostgreSQL connection may have failed"
echo ""
