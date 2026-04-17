#!/usr/bin/env bash
# Bring up the demo stack.
#
# Usage:
#   examples/demo/up.sh            # full stack: otterstax + 3 backends inside docker
#   examples/demo/up.sh --local    # bench mode: 3 backends only; run otterstax locally
#
# After exit (full mode), server listens on:
#   PG  wire   localhost:8817   (psql -h localhost -p 8817 -U demo demo)
#   MySQL wire localhost:8816
#   FlightSQL  localhost:8815
#   HTTP API   localhost:8085   (POST /add_connection, GET /health)

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

LOCAL=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --local) LOCAL=true; shift ;;
        *) echo "Usage: $0 [--local]"; exit 1 ;;
    esac
done

# --- docker / sudo plumbing ---
DOCKER_PREFIX=""
if ! docker ps >/dev/null 2>&1; then
    if sudo -n docker ps >/dev/null 2>&1; then
        DOCKER_PREFIX="sudo -n "
        echo "ℹ️  Using 'sudo docker' (current user lacks docker socket access)"
    else
        echo "❌ Cannot connect to docker."
        exit 1
    fi
fi
if ${DOCKER_PREFIX}docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="${DOCKER_PREFIX}docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="${DOCKER_PREFIX}docker-compose"
else
    echo "❌ Neither 'docker compose' nor 'docker-compose' available."
    exit 1
fi

# --- 1. Generate init SQL ---
echo "=== 1. Generating demo data ==="
if [ -f ../../.venv/bin/activate ]; then source ../../.venv/bin/activate; fi
python ./generate_data.py

# --- 2. Bring up containers ---
echo ""
if $LOCAL; then
    echo "=== 2. Starting demo backends + MinIO only (bench/local mode) ==="
    $COMPOSE_CMD -f compose.yml up -d demo-mariadb demo-postgres demo-clickhouse demo-minio demo-minio-init
else
    echo "=== 2. Starting full demo stack (compose.yml --profile full) ==="
    $COMPOSE_CMD --profile full -f compose.yml up -d --build
fi

# --- 3. Wait for healthchecks ---
echo ""
echo "=== 3. Waiting for healthy containers ==="
if $LOCAL; then
    WAIT_SVCS="demo-mariadb demo-postgres demo-clickhouse demo-minio"
else
    WAIT_SVCS="demo-mariadb demo-postgres demo-clickhouse demo-minio demo-otterstax"
fi
for svc in $WAIT_SVCS; do
    echo -n "   $svc"
    for i in $(seq 1 90); do
        status=$(${DOCKER_PREFIX}docker inspect -f '{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "starting")
        if [ "$status" = "healthy" ]; then
            echo " ✅"
            break
        fi
        echo -n "."
        sleep 2
        if [ "$i" -eq 90 ]; then
            echo " ❌ timeout (last status: $status)"
            $COMPOSE_CMD -f compose.yml logs "$svc" | tail -30
            exit 1
        fi
    done
done

# --- 4. Register connections (full mode only) ---
if ! $LOCAL; then
    echo ""
    echo "=== 4. Registering demo connections (mysql, pg, ch, s3) ==="
    # otterstax runs INSIDE the docker network — use docker-DNS JSONs (no --local).
    ./connections/add_connections.sh
    ./connections/add_s3_credentials.sh
fi

# --- 5. Print usage ---
if $LOCAL; then
cat <<'EOF'

================================================================================
✅  Demo backends + MinIO are up:

  MariaDB:    localhost:3201   user=demo pass=demo db=bill
  Postgres:   localhost:3202   user=demo pass=demo db=shop
  ClickHouse: localhost:3204   user=demo pass=demo db=ev
  MinIO:      localhost:3206   user=minioadmin pass=minioadmin bucket=demo-bucket
              (console: http://localhost:3207)

  Now start your local otterstax server:
    ./build/server --port-flight 8815 --port-mysql 8816 --port-postgres 8817 --port-http 8085

  Then register connections (in another terminal once server is up):
    examples/demo/connections/add_connections.sh    --local
    examples/demo/connections/add_s3_credentials.sh --local

  Run all demo queries (steps 1-9, incl. s3 load + dump):
    examples/demo/run-queries.sh

  Tear down with: examples/demo/down.sh
================================================================================
EOF
else
cat <<'EOF'

================================================================================
✅  Demo stack is up.

  PostgreSQL wire (used by demo):
    psql -h localhost -p 8817 -U demo demo

  Other wires:
    mysql -h localhost -P 8816 -u demo -pdemo
    HTTP API:  http://localhost:8085
    MinIO console: http://localhost:3207  (user=minioadmin pass=minioadmin)

  Run demo SQL files step-by-step (paths relative to repo root):

    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_1.sql
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_2.sql
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_3a_ddl.sql
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_3b_insert.sql
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_3c_select.sql
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_3d_main.sql
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_4.sql
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_5.sql
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_6.sql
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_7.sql   # s3 csv load
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_8.sql   # s3 parquet load
    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/step_9.sql   # JOIN + dump back to s3

  Or all at once:
    examples/demo/run-queries.sh

  After running steps 3a-3b / 7-8 (which create otter.warehouses / otter.regions /
  otter.promos), run cleanup before re-doing the demo:

    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/cleanup.sql

  Tear everything down (releases volumes — clickhouse re-init needs this,
  and the MinIO bucket is recreated on next `up`):

    examples/demo/down.sh

================================================================================
EOF
fi
