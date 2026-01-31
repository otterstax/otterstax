#!/bin/bash

URL="http://0.0.0.0:8085/add_connection"

for FILE in connection_maria_db1_local.json connection_maria_db2_local.json connection_maria_db3_local.json; do
    echo "Sending request with $FILE..."
    curl -X POST "$URL" -H "Content-Type: application/json" -d @"$FILE"
    echo -e "\n"
    sleep 1  # Optional: Add a delay between requests
done