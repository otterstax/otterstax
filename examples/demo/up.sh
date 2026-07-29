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
#
# Connections are no longer registered over HTTP — they are read once at server
# startup from the single config file (config.yaml, mounted into the container
# by compose.yml). Edit that file and restart to change them.

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
    echo "=== 2. Starting demo backends + MinIO + Kafka only (bench/local mode) ==="
    $COMPOSE_CMD -f compose.yml up -d demo-mariadb demo-postgres demo-clickhouse demo-minio demo-minio-init demo-kafka
else
    echo "=== 2. Starting full demo stack (compose.yml --profile full) ==="
    $COMPOSE_CMD --profile full -f compose.yml up -d --build
fi

# --- 3. Wait for healthchecks ---
echo ""
echo "=== 3. Waiting for healthy containers ==="
if $LOCAL; then
    WAIT_SVCS="demo-mariadb demo-postgres demo-clickhouse demo-minio demo-kafka"
else
    WAIT_SVCS="demo-mariadb demo-postgres demo-clickhouse demo-minio demo-kafka demo-otterstax"
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

# --- 4. Connections ---
# In full mode, config.yaml is mounted into the otterstax container and read at
# startup (server settings + connections) — nothing to register here. In local
# mode you start the server yourself pointing at config_local.yaml (see below).

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
  Kafka:      localhost:19093  (redpanda; streaming demo broker)

  Now start your local otterstax server (single config file with the
  host-published backend ports + connections):
    ./build/server --config examples/demo/config_local.yaml

  Connections (mysql/pg/ch/s3) load from that same file at startup — no
  separate registration step.

  Run all demo queries (steps 1-9, incl. s3 load + dump):
    examples/demo/run-queries.sh

  Run the Kafka streaming act — step by step (source ingestion, JOIN kafka⋈ch⋈pg,
  stream, fan-in); walk them in order:
    examples/demo/kafka/1_ingestion/run.sh --local
    examples/demo/kafka/2_join/run.sh      --local
    …  (see examples/demo/kafka/README.md; or run_all.sh --local for a smoke test)

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

  Run the Kafka streaming act — step by step (see examples/demo/kafka/README.md):
    examples/demo/kafka/1_ingestion/run.sh
    examples/demo/kafka/2_join/run.sh
    …  (or examples/demo/kafka/run_all.sh for a smoke test)

  After running steps 3a-3b / 7-8 (which create otter.warehouses / otter.regions /
  otter.promos), run cleanup before re-doing the demo:

    psql -h localhost -p 8817 -U demo demo -f examples/demo/sql/cleanup.sql

  Tear everything down (releases volumes — clickhouse re-init needs this,
  and the MinIO bucket is recreated on next `up`):

    examples/demo/down.sh

================================================================================
EOF
fi
