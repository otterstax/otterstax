#!/usr/bin/env bash
# Stop all benchmark services.
#
# Usage:
#   ./benchmark/manual/stop_service.sh [--clean]
#
# Options:
#   --clean   Remove containers and DB volumes (full wipe). Without this flag
#             containers are only stopped; data survives for the next run.
#
# If a perf session was started via start_service.sh --perf, stop_service.sh
# automatically signals perf to finalise, copies benchmark.perf.data from the
# container, and converts it to a speedscope-compatible benchmark.perf file.
# Output directory is printed on screen when the service was started.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

CLEAN=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --clean) CLEAN=true; shift ;;
        -h|--help)
            sed -n '2,14p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

PERF_STATE_FILE="$BENCH_DIR/.perf_state"

# If a perf session is active, finalise it before stopping the container.
# Sending INT to perf (while OtterStax is still running) lets perf write its
# data_size footer cleanly; stopping the container first would truncate the file.
if [ -f "$PERF_STATE_FILE" ]; then
    # shellcheck source=/dev/null
    source "$PERF_STATE_FILE"
    echo "Stopping perf inside bench_otterstax..."
    docker exec bench_otterstax sh -c \
        'kill -INT $(pgrep -x perf) 2>/dev/null || true' 2>/dev/null || true
    echo "  Waiting for perf to finalise..."
    for i in $(seq 1 30); do
        docker exec bench_otterstax sh -c 'pgrep -x perf >/dev/null 2>&1' \
            2>/dev/null || break
        sleep 1
    done
    if [ "${ENABLE_PERF_ALLOC}" = "true" ]; then
        docker exec bench_otterstax sh -c \
            'perf probe --del probe_libc:malloc 2>/dev/null || true' 2>/dev/null || true
    fi
    if docker cp bench_otterstax:/tmp/benchmark.perf.data \
            "${PERF_OUT_DIR}/benchmark.perf.data" 2>/dev/null; then
        echo "  Perf data saved: ${PERF_OUT_DIR}/benchmark.perf.data"
        echo "Converting perf.data → benchmark.perf (speedscope)..."
        docker run --rm \
            -v "${PERF_OUT_DIR}:/results" \
            "otterstax_app:${IMAGE_TAG:-bench}" \
            sh -c '
                cd /results
                perf script -i benchmark.perf.data > benchmark.perf 2>&1 && \
                    echo "  benchmark.perf written ($(wc -l < benchmark.perf) lines)" || \
                    echo "  WARNING: perf script failed"
            '
        echo "  Drag benchmark.perf into speedscope.app to explore"
        echo "  Files in: ${PERF_OUT_DIR}/"
    else
        echo "  WARNING: perf.data not found inside bench_otterstax (/tmp/benchmark.perf.data)"
        docker exec bench_otterstax cat /tmp/perf.log 2>/dev/null | tail -10 || true
    fi
    rm -f "$PERF_STATE_FILE"
fi

echo "Stopping OtterStax..."
_compose_otterstax stop bench-otterstax 2>/dev/null || true
_compose_otterstax rm -f bench-otterstax 2>/dev/null || true

if $CLEAN; then
    echo "Stopping backends and removing volumes..."
    _compose_backends down --volumes --remove-orphans
    docker network rm bench_net 2>/dev/null || true
    echo "All containers and volumes removed."
else
    echo "Stopping backends (data preserved)..."
    _compose_backends stop
    echo "Done. Run start_service.sh --no-init to restart without reinitialising data."
fi
