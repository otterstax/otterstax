#!/bin/bash

# Use test-otterstax hostname for Docker network connectivity
URL="http://test-otterstax:8085/health"

echo "Starting HTTP health check..."

# Retry logic for health check
max_retries=30
retry_delay=2

for i in $(seq 1 $max_retries); do
    echo "Health check attempt $i/$max_retries..."
    response=$(curl -s -w "\n%{http_code}" "$URL" 2>/dev/null)
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n -1)
    
    if [ "$http_code" = "200" ]; then
        echo "✅ Server is healthy: $body"
        exit 0
    elif [ "$http_code" = "000" ]; then
        echo "⏳ Server not reachable, retrying in ${retry_delay}s..."
        sleep $retry_delay
    else
        echo "⚠️ Unexpected response: $http_code - $body"
        sleep $retry_delay
    fi
done

echo "❌ Health check failed after $max_retries attempts"
exit 1
