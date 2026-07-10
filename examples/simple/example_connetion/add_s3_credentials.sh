#!/bin/bash

# Register an s3 alias against the otterstax server. After this runs the alias
# 's3' is available to CREATE EXTERNAL TABLE / COPY ... TO statements that use
# `s3_alias = 's3'` (see ../example_9.txt and ../example_11.txt).
#
# The endpoint /s3/add_credentials is a GET with a JSON body — see
# connectors/api_server/connection_server.cpp.

URL="http://0.0.0.0:8085/s3/add_credentials"
FILE="connection_s3.json"

echo "Registering s3 alias from $FILE..."
curl -X GET "$URL" -H "Content-Type: application/json" -d @"$FILE"
echo -e "\n"
