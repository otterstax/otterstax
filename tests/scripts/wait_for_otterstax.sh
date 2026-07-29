#!/bin/bash

# Wait until the OtterStax FlightSQL wire port accepts TCP connections.
# There is no HTTP management/health port anymore — connections are loaded from
# the connection config file at server startup, so wire-port readiness is the
# signal we wait on.

HOST="test-otterstax"
PORT="8815"

echo "Waiting for OtterStax wire port ${HOST}:${PORT}..."

max_retries=30
retry_delay=2

for i in $(seq 1 $max_retries); do
    echo "Readiness attempt $i/$max_retries..."
    if timeout 2 bash -c "</dev/tcp/${HOST}/${PORT}" 2>/dev/null; then
        echo "✅ Server is accepting connections on ${HOST}:${PORT}"
        exit 0
    fi
    echo "⏳ Server not reachable, retrying in ${retry_delay}s..."
    sleep $retry_delay
done

echo "❌ Readiness check failed after $max_retries attempts"
exit 1
