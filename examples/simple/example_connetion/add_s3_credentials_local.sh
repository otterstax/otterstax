#!/bin/bash

# Same as add_s3_credentials.sh but uses the local payload (endpoint pointing at
# localhost:9000) for the case where otterstax runs as a local binary against a
# host-published MinIO port.

URL="http://0.0.0.0:8085/s3/add_credentials"
FILE="connection_s3_local.json"

echo "Registering s3 alias from $FILE..."
curl -X GET "$URL" -H "Content-Type: application/json" -d @"$FILE"
echo -e "\n"
