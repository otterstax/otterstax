#!/usr/bin/env bash
# Run all demo SQL steps on a fresh demo engine after S3 seeding succeeds.
# For a rerun with retained types, follow Re-running in examples/demo/CLAUDE.md.
# Works for both full-docker and local (bench) mode — the PG wire is always
# published to 127.0.0.1:8817.
#
# Usage:
#   examples/demo/run-queries.sh

set -euo pipefail
cd "$(dirname "$0")"

URI="postgresql://demo:demo@127.0.0.1:8817/local"

for f in sql/step_*.sql; do
    echo "============================================================"
    echo "=== $f"
    echo "============================================================"
    psql -v ON_ERROR_STOP=1 -P pager=off "$URI" -f "$f"
    echo
done
