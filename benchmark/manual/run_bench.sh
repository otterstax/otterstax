#!/usr/bin/env bash
# Run benchmark tests against an already-running OtterStax service.
# Start the service first with: ./benchmark/manual/start_service.sh
#
# Usage:
#   ./benchmark/manual/run_bench.sh [--frontend F]... [--bench T]... [--repetitions N] [--out-dir DIR]
#
# Options:
#   --frontend F    mysql | postgres | arrow  (repeatable; default: mysql postgres)
#   --bench T [T..] Test name(s) to run, space-separated (default: cross-backend tests).
#                   Values: simple_select complex_select join_same_instance
#                           join_cross_engine join_all
#                           external_load external_join external_dump
#                   (external_* require ./start_service.sh --external)
#   --repetitions N Reps per test (default: 3)
#   --out-dir DIR   Output directory (default: benchmark_manual/<YYYYMMDD_HHMMSS>)
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

FRONTENDS=()
BENCH_FILTER=()
REPETITIONS=3
OUT_DIR="$REPO_ROOT/benchmark_manual/$(date +%Y%m%d_%H%M%S)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --frontend)     FRONTENDS+=("$2"); shift 2 ;;
        --frontend=*)   FRONTENDS+=("${1#*=}"); shift ;;
        --bench)
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^- ]]; do
                BENCH_FILTER+=("$1"); shift
            done ;;
        --bench=*)      BENCH_FILTER+=("${1#*=}"); shift ;;
        --repetitions)  REPETITIONS="$2"; shift 2 ;;
        --repetitions=*) REPETITIONS="${1#*=}"; shift ;;
        --out-dir)      OUT_DIR="$2"; shift 2 ;;
        --out-dir=*)    OUT_DIR="${1#*=}"; shift ;;
        -h|--help)
            sed -n '2,18p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

[ ${#FRONTENDS[@]} -eq 0 ] && FRONTENDS=("${DEFAULT_FRONTENDS[@]}")

# Default to the cross-backend tests; --bench may also select external_* (which
# need the service started with ./start_service.sh --external).
TESTS=("${DEFAULT_TESTS[@]}")
if [ ${#BENCH_FILTER[@]} -gt 0 ]; then
    _filtered=()
    for _t in "${ALL_TESTS[@]}"; do
        for _b in "${BENCH_FILTER[@]}"; do
            [ "$_t" = "$_b" ] && { _filtered+=("$_t"); break; }
        done
    done
    TESTS=("${_filtered[@]}")
fi

# Verify OtterStax is reachable
if ! docker run --rm --network=bench_net benchmark-client:latest \
       curl -s -f http://bench_otterstax:8085/health >/dev/null 2>&1; then
    echo "ERROR: OtterStax is not running. Start it first:" >&2
    echo "  ./benchmark/manual/start_service.sh" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
GIT_COMMIT=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")

echo ""
echo "=== Manual benchmark run ==="
echo "    Frontends : ${FRONTENDS[*]}"
echo "    Tests     : ${TESTS[*]}"
echo "    Reps      : $REPETITIONS"
echo "    Out dir   : $OUT_DIR"
echo ""

PASS=0
FAIL=0

for frontend in "${FRONTENDS[@]}"; do
    port="$(_frontend_port "$frontend")"
    mkdir -p "$OUT_DIR/$frontend"

    for test in "${TESTS[@]}"; do
        echo "--- $frontend / $test ---"
        set +e
        docker run --rm \
            --network=bench_net \
            -v "$OUT_DIR:/results" \
            -e GIT_COMMIT="$GIT_COMMIT" \
            -e PYTHONUNBUFFERED=1 \
            benchmark-client:latest \
            python "/app/benchmarks/$frontend/$test.py" \
            --host bench_otterstax \
            --port "$port" \
            --repetitions "$REPETITIONS" \
            --out-dir "/results/$frontend"
        rc=$?
        set -e
        if [ $rc -eq 0 ]; then
            echo "  PASS: $frontend/$test"
            PASS=$((PASS + 1))
        else
            echo "  FAIL: $frontend/$test (exit $rc)"
            FAIL=$((FAIL + 1))
        fi
    done
done

# Generate summary — same format as automated run
echo ""
echo "=== Generating summary ==="
docker run --rm \
    -v "$OUT_DIR:/results" \
    -e GIT_COMMIT="$GIT_COMMIT" \
    -e PYTHONUNBUFFERED=1 \
    benchmark-client:latest \
    python /app/scripts/generate_summary.py /results

echo ""
echo "=== Done: $PASS passed, $FAIL failed ==="
echo "    Results: $OUT_DIR"
