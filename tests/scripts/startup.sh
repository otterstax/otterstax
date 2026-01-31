#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Run health check script
echo "Starting HTTP health check..."
./http_healthcheck.sh

# Check data presence in the database
echo "Checking data presence in the database..."
python query_data.py

# Run connection setup script
echo "Adding connections..."
./add_connections.sh

# Run the Python test schema
echo "Running Python test client..."
python test_schema.py

# Run the Python test client
echo "Running Python test client..."
python test_client.py

# Run the Python test mysql client
echo "Running Python test mysql client..."
python  mysql_test_client.py

# Run the Python test postgres client
echo "Running Python test postgres client..."
python  postgres_test_client.py

# Run the Python test client mutable
echo "Running Python test client..."
python test_client_mutable.py


