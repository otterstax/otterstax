#!/usr/bin/env bash
# Talk to a running OtterStax server over its PostgreSQL wire with psql.
#
# The OtterStax wire IGNORES authentication — any user / db / password is
# accepted (it's a federation router, not a real PG server). So the defaults
# below "just work" for a local engine; override host/port only for a remote one.
#   OTTERSTAX_HOST     (default: localhost)
#   OTTERSTAX_PORT     (default: 8817)
#   OTTERSTAX_USER     (default: otterstax)   # ignored by the wire
#   OTTERSTAX_DB       (default: otterstax)   # ignored by the wire
#   OTTERSTAX_PASSWORD (default: empty)       # ignored by the wire
#
# Usage:
#   psql.sh -c "SELECT 1"            # one statement
#   psql.sh -f query.sql             # a .sql file
#   echo "SELECT 1" | psql.sh        # SQL from stdin
#   psql.sh                          # reads stdin
#
# Any extra args after the recognised ones are passed straight to psql.
set -euo pipefail

HOST="${OTTERSTAX_HOST:-localhost}"
PORT="${OTTERSTAX_PORT:-8817}"
USER="${OTTERSTAX_USER:-otterstax}"
DB="${OTTERSTAX_DB:-otterstax}"
PASS="${OTTERSTAX_PASSWORD:-}"

if ! command -v psql >/dev/null 2>&1; then
    echo "❌ psql not found on PATH." >&2
    echo "   Install it (macOS: brew install libpq, then add /opt/homebrew/opt/libpq/bin to PATH)." >&2
    exit 1
fi

status=0
PGPASSWORD="$PASS" psql -P pager=off \
    "postgresql://${USER}@${HOST}:${PORT}/${DB}" "$@" || status=$?

# psql exit code 2 == connection could not be established or was lost mid-query.
# On OtterStax that usually means the engine isn't up, or a query SIGSEGV'd it.
if [ "$status" -eq 2 ]; then
    echo "" >&2
    echo "⚠️  OtterStax connection failed or was lost (psql exit 2)." >&2
    echo "    Likely the engine is down or a query crashed it (some federated" >&2
    echo "    queries can SIGSEGV the server — see SKILL.md 'Engine crashes')." >&2
    echo "    Recover: restart the server (a plain restart may crash-loop on WAL" >&2
    echo "    replay — recreate the process/container fresh), then RE-REGISTER your" >&2
    echo "    backend connections via the HTTP API (they live in memory and are lost)." >&2
fi
exit "$status"