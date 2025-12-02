#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 OtterStax


URL="http://otterstax_app:8085/check_connection"

for FILE in connection_maria_db1.json connection_maria_db2.json; do
    echo "Sending request with $FILE..."
    curl -X GET "$URL" -H "Content-Type: application/json" -d @"$FILE"
    echo -e "\n"
    sleep 1  # Optional: Add a delay between requests
done