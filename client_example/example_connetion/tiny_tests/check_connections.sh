#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 OtterStax


URL="http://0.0.0.0:8085/check_connection"

for FILE in connection_logins.json connection_users.json connection_salaries.json; do
    echo "Sending request with $FILE..."
    curl -X GET "$URL" -H "Content-Type: application/json" -d @"$FILE"
    echo -e "\n"
    sleep 1  # Optional: Add a delay between requests
done