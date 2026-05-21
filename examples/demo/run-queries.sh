#!/usr/bin/env bash
# Run all demo SQL steps against the OtterStax PostgreSQL wire.
# Works for both full-docker and local (bench) mode — the PG wire is always
# published to localhost:8817.
#
# Usage:
#   examples/demo/run-queries.sh

set -euo pipefail
cd "$(dirname "$0")"

URI="postgresql://demo:demo@localhost:8817/demo"

for f in sql/step_*.sql; do
    echo "============================================================"
    echo "=== $f"
    echo "============================================================"
    psql -P pager=off "$URI" -f "$f"
    echo
done
