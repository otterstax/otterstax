#!/bin/bash

URL="http://0.0.0.0:8085/health"

echo "Sending healthcheck request..."
curl -X GET "$URL" -H "Content-Type: application/json"
echo -e "\n"
sleep 1  # Optional: Add a delay between requests