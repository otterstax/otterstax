#!/bin/bash
# Register the demo MinIO bucket as an s3_alias against an otterstax server.
#
# Usage:
#   ./add_s3_credentials.sh           # docker mode: otterstax inside compose network,
#                                     #   minio addressed by docker-DNS (demo-minio:9000)
#   ./add_s3_credentials.sh --local   # bench mode: otterstax running as a local binary,
#                                     #   minio addressed via host-published port (localhost:3206)
#
# Like add_connections.sh, the HTTP API is reached at http://localhost:8085 in
# both cases — only the endpoint inside the payload differs.
#
# The OtterStax server exposes `/s3/add_credentials` as **GET with a JSON body**
# (see connectors/api_server/connection_server.cpp). Once registered, the alias
# 'demo_s3' is used by step_7 / step_8 / step_9 SQL (and any other CREATE
# EXTERNAL TABLE / COPY ... TO statement that hits this bucket).

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --local) LOCAL=true; shift ;;
        *) echo "Usage: $0 [--local]"; exit 1 ;;
    esac
done

URL_BASE="http://localhost:8085"

if $LOCAL; then
    S3_JSON="connection_s3_local.json"
else
    S3_JSON="connection_s3.json"
fi

echo ">> GET ${URL_BASE}/s3/add_credentials  payload=${S3_JSON}"
curl --silent --show-error --fail \
     -X GET "${URL_BASE}/s3/add_credentials" \
     -H "Content-Type: application/json" \
     --data @"${SCRIPT_DIR}/${S3_JSON}"
echo

echo "✅ Demo s3 alias 'demo_s3' registered"
