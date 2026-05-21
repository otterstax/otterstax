#!/bin/bash
# Register the three demo backends against an otterstax server.
#
# Usage:
#   ./add_connections.sh           # docker mode: otterstax inside compose network,
#                                  #   backends addressed by docker-DNS names
#   ./add_connections.sh --local   # bench mode: otterstax running as a local binary,
#                                  #   backends addressed via host-published ports
#
# In both cases the HTTP API is reached at http://localhost:8085 (port is
# always published to the host).  What differs is the JSON payload: docker
# mode uses docker-DNS hostnames (demo-mariadb / demo-postgres /
# demo-clickhouse), local mode uses localhost with the host-published backend
# ports (3201 / 3202 / 3204).

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
    MYSQL_JSON="connection_mysql_local.json"
    PG_JSON="connection_pg_local.json"
    CH_JSON="connection_ch_local.json"
else
    MYSQL_JSON="connection_mysql.json"
    PG_JSON="connection_pg.json"
    CH_JSON="connection_ch.json"
fi

post() {
    local endpoint="$1"
    local payload="$2"
    echo ">> POST ${URL_BASE}${endpoint}  payload=${payload}"
    curl --silent --show-error --fail \
         -X POST "${URL_BASE}${endpoint}" \
         -H "Content-Type: application/json" \
         -d @"${SCRIPT_DIR}/${payload}"
    echo
}

post "/add_connection"    "$MYSQL_JSON"
post "/add_pg_connection" "$PG_JSON"
post "/add_ch_connection" "$CH_JSON"

echo "✅ All demo connections registered"
