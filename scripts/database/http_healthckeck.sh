#!/bin/bash

URL="http://otterstax_app:8085/health"

echo "Sending healthcheck request..."
curl -X GET "$URL" -H "Content-Type: application/json"
echo -e "\n"
sleep 1  # Optional: Add a delay between requests