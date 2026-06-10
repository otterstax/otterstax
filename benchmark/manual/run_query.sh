#!/usr/bin/env bash
# Execute a SQL file against a running OtterStax instance and print results.
# Start the service first with: ./benchmark/manual/start_service.sh
#
# Usage:
#   ./benchmark/manual/run_query.sh --frontend mysql|postgres|arrow <query.sql>
#
# Options:
#   --frontend F    mysql | postgres | arrow (required)
#   --name N        Label for result files (default: basename of query file)
#   --out-dir DIR   Output directory (default: benchmark_manual/<YYYYMMDD_HHMMSS>)
#   <query.sql>     Path to a file containing the SQL to execute
#
# The script mounts the query file into the benchmark-client container and
# runs it against bench_otterstax on bench_net.  Results are printed to stdout
# and saved to <out-dir>/summary.md and <out-dir>/<name>.json.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

FRONTEND=""
QUERY_FILE=""
QUERY_NAME=""
OUT_DIR="$REPO_ROOT/benchmark_manual/$(date +%Y%m%d_%H%M%S)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --frontend)    FRONTEND="$2"; shift 2 ;;
        --frontend=*)  FRONTEND="${1#*=}"; shift ;;
        --name)        QUERY_NAME="$2"; shift 2 ;;
        --name=*)      QUERY_NAME="${1#*=}"; shift ;;
        --out-dir)     OUT_DIR="$2"; shift 2 ;;
        --out-dir=*)   OUT_DIR="${1#*=}"; shift ;;
        -h|--help)
            sed -n '2,16p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0 ;;
        -*)
            echo "Unknown option: $1" >&2; exit 1 ;;
        *)
            QUERY_FILE="$1"; shift ;;
    esac
done

if [ -z "$FRONTEND" ]; then
    echo "ERROR: --frontend is required (mysql | postgres | arrow)" >&2
    exit 1
fi
if [ -z "$QUERY_FILE" ]; then
    echo "ERROR: query file argument is required" >&2
    exit 1
fi
if [ ! -f "$QUERY_FILE" ]; then
    echo "ERROR: query file not found: $QUERY_FILE" >&2
    exit 1
fi

QUERY_FILE="$(cd "$(dirname "$QUERY_FILE")" && pwd)/$(basename "$QUERY_FILE")"
[ -z "$QUERY_NAME" ] && QUERY_NAME="$(basename "${QUERY_FILE%.sql}")"

# Verify OtterStax is reachable
if ! docker run --rm --network=bench_net benchmark-client:latest \
       curl -s -f http://bench_otterstax:8085/health >/dev/null 2>&1; then
    echo "ERROR: OtterStax is not running. Start it first:" >&2
    echo "  ./benchmark/manual/start_service.sh" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
GIT_COMMIT=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")

SQL=$(cat "$QUERY_FILE")

echo "=== $FRONTEND / $QUERY_NAME ==="
echo ""

docker run --rm \
    --network=bench_net \
    -v "$OUT_DIR:/results" \
    -v "$SCRIPT_DIR/execute_query.py:/app/execute_query.py:ro" \
    -e GIT_COMMIT="$GIT_COMMIT" \
    -e PYTHONUNBUFFERED=1 \
    benchmark-client:latest \
    python /app/execute_query.py \
        --frontend "$FRONTEND" \
        --out-dir /results \
        --query-name "$QUERY_NAME" \
        "$SQL"

echo ""
echo "Results saved to: $OUT_DIR"
