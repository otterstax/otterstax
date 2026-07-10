#!/usr/bin/env bash
# Start all benchmark services (backends + OtterStax), initialise data,
# and register connection aliases.
#
# After this script exits, Tracy can connect to localhost:8086 and the
# wire-protocol frontends are accessible on the host:
#   MySQL wire    → localhost:8816
#   PostgreSQL    → localhost:8817
#   FlightSQL     → localhost:8815
#   HTTP conn API → localhost:8085
#
# Usage:
#   ./benchmark/manual/start_service.sh [--no-init] [--rebuild] [--perf] [--perf-alloc] [--tracy] [-j N] [--image-tag TAG]
#
# Options:
#   --external   Also start a seeded MinIO and generate s3/file fixtures so the
#                external_load / external_join / external_dump / external_join_cross /
#                external_join_all benchmarks can run (registers the bench_minio
#                s3 alias). Adds compose_minio.yml.
#   --no-init    Skip benchmark data initialisation (reuse existing data).
#   --rebuild    Force rebuild of both Docker images (otterstax_app + benchmark-client)
#                even if they already exist. Use after C++ source changes.
#   --perf       CPU call-graph profiling (perf, 99 Hz, dwarf unwind).
#                Starts perf inside the OtterStax container after launch.
#                Output saved by stop_service.sh to benchmark_manual/<ts>/.
#                Rebuilds the image with perf tools if they are missing.
#   --perf-alloc Like --perf but also attaches a malloc uprobe so allocation
#                call-sites appear in the call graph. Implies --perf.
#   --tracy      Build/use an OtterStax image compiled with Tracy instrumentation.
#                Defaults image tag to "bench-tracy" so the Tracy and non-Tracy
#                builds coexist without overwriting each other.
#                Rebuilds the image whenever --tracy is specified.
#                Connect the Tracy GUI to localhost:8086 after the service starts.
#                No capture is started — Tracy GUI must be opened manually.
#   -j N         Parallel cmake jobs for the otterstax_app build (default: nproc).
#   --image-tag  OtterStax image tag (default: bench, or bench-tracy with --tracy).
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

SKIP_INIT=false
ENABLE_TRACY=false
ENABLE_PERF=false
ENABLE_PERF_ALLOC=false
FORCE_REBUILD=false
CUSTOM_TAG=false
BUILD_JOBS=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --external)     EXTERNAL_ENABLED=true; shift ;;
        --no-init)      SKIP_INIT=true; shift ;;
        --rebuild)      FORCE_REBUILD=true; shift ;;
        --perf)         ENABLE_PERF=true; shift ;;
        --perf-alloc)   ENABLE_PERF=true; ENABLE_PERF_ALLOC=true; shift ;;
        --tracy)        ENABLE_TRACY=true; shift ;;
        --image-tag)    IMAGE_TAG="$2"; CUSTOM_TAG=true; shift 2 ;;
        --image-tag=*)  IMAGE_TAG="${1#*=}"; CUSTOM_TAG=true; shift ;;
        -j)             BUILD_JOBS="$2"; shift 2 ;;
        -j*)            BUILD_JOBS="${1#-j}"; shift ;;
        -h|--help)
            sed -n '2,32p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if $ENABLE_TRACY; then
    export WITH_TRACY=true
    # Use a dedicated tag so Tracy and non-Tracy builds coexist.
    # --image-tag overrides this when the user wants a custom name.
    if ! $CUSTOM_TAG; then
        IMAGE_TAG="bench-tracy"
    fi
    echo "Building otterstax_app:${IMAGE_TAG} with Tracy enabled..."
    docker build -t "otterstax_app:${IMAGE_TAG}" \
        -f "$REPO_ROOT/Dockerfile.test" \
        --build-arg WITH_TRACY=true \
        --build-arg WITH_PERF="${ENABLE_PERF}" \
        --build-arg BUILD_JOBS="${BUILD_JOBS}" \
        "$REPO_ROOT"
elif $FORCE_REBUILD; then
    echo "Building otterstax_app:${IMAGE_TAG} ..."
    docker build -t "otterstax_app:${IMAGE_TAG}" \
        -f "$REPO_ROOT/Dockerfile.test" \
        --build-arg WITH_PERF="${ENABLE_PERF}" \
        --build-arg BUILD_JOBS="${BUILD_JOBS}" \
        "$REPO_ROOT"
elif ! docker image inspect "otterstax_app:${IMAGE_TAG}" >/dev/null 2>&1; then
    echo "ERROR: image otterstax_app:${IMAGE_TAG} not found." >&2
    echo "  Build it with: ./benchmark/manual/start_service.sh --rebuild" >&2
    echo "  Build with Tracy: ./benchmark/manual/start_service.sh --tracy" >&2
    exit 1
fi

# If --perf was requested, verify the image was built with perf support; rebuild if not.
if $ENABLE_PERF; then
    if ! docker run --rm --entrypoint sh "otterstax_app:${IMAGE_TAG}" \
           -c 'command -v perf >/dev/null 2>&1'; then
        echo "  perf not found in image — rebuilding with perf tools..."
        docker build -t "otterstax_app:${IMAGE_TAG}" \
            -f "$REPO_ROOT/Dockerfile.test" \
            --build-arg WITH_PERF=true \
            --build-arg BUILD_JOBS="${BUILD_JOBS}" \
            "$REPO_ROOT"
    fi
    # Add SYS_ADMIN/PERFMON capabilities and disable seccomp for perf_event_open.
    _compose_otterstax() {
        local _files=(
            -f "$BENCH_DIR/compose_backends.yml"
            -f "$BENCH_DIR/compose_benchmark.yml"
            -f "$BENCH_DIR/compose_manual.yml"
            -f "$BENCH_DIR/compose_benchmark_perf.yml"
        )
        [ "$EXTERNAL_ENABLED" = "true" ] && _files+=(-f "$BENCH_DIR/compose_minio.yml")
        docker compose "${_files[@]}" -p bench "$@"
    }
fi

if $FORCE_REBUILD || ! docker image inspect "benchmark-client:latest" >/dev/null 2>&1; then
    echo "Building benchmark-client:latest ..."
    docker build -t benchmark-client:latest \
        -f "$BENCH_DIR/Dockerfile.benchmark" "$REPO_ROOT"
fi

_start_perf() {
    if $ENABLE_PERF_ALLOC; then
        echo "  Installing malloc uprobe inside bench_otterstax..."
        docker exec bench_otterstax sh -c '
            perf probe --del probe_libc:malloc 2>/dev/null || true
            libc=$(find /lib /usr/lib -name "libc.so.6" 2>/dev/null | head -1)
            perf probe -x "$libc" --add malloc > /tmp/perf_probe.log 2>&1
            cat /tmp/perf_probe.log
        '
        if ! docker exec bench_otterstax sh -c \
                'perf probe -l 2>/dev/null | grep -q malloc'; then
            echo "  WARNING: malloc uprobe not available — running cpu-clock only"
            ENABLE_PERF_ALLOC=false
        fi
    fi

    if $ENABLE_PERF_ALLOC; then
        docker exec -d bench_otterstax sh -c '
            exec perf record --call-graph dwarf -F 99 \
              -e cpu-clock \
              -e probe_libc:malloc \
              -p 1 \
              -o /tmp/benchmark.perf.data > /tmp/perf.log 2>&1
        '
    else
        docker exec -d bench_otterstax sh -c '
            exec perf record --call-graph dwarf -F 99 \
              -e cpu-clock \
              -p 1 \
              -o /tmp/benchmark.perf.data > /tmp/perf.log 2>&1
        '
    fi

    sleep 2
    if docker exec bench_otterstax sh -c 'pgrep -x perf >/dev/null 2>&1'; then
        echo "  perf running inside bench_otterstax"
    else
        echo "  WARNING: perf failed to start inside bench_otterstax"
        docker exec bench_otterstax cat /tmp/perf.log 2>/dev/null | tail -10
    fi
}

# Generate s3/file fixtures before `up` so MinIO can seed the bucket and
# bench-otterstax can mount /fixtures.
if $EXTERNAL_ENABLED; then
    echo ""
    echo "=== Generating external-table fixtures ==="
    _generate_external_fixtures
fi

# Ensure bench_net exists (created by compose_backends.yml on first up)
echo ""
echo "=== Starting backend databases ==="
_compose_backends up -d --remove-orphans

_wait_db_healthy bench_mariadb1
_wait_db_healthy bench_mariadb2
_wait_db_healthy bench_postgres1
_wait_db_healthy bench_postgres2
_wait_db_healthy bench_clickhouse1
_wait_db_healthy bench_clickhouse2
$EXTERNAL_ENABLED && _wait_db_healthy bench_minio
echo "Giving databases extra time to stabilise..."
sleep 5

if ! $SKIP_INIT; then
    echo ""
    echo "=== Initialising benchmark data ==="
    docker run --rm \
        --network=bench_net \
        -v "$BENCH_DIR/bench.yaml:/app/bench.yaml:ro" \
        -e PYTHONUNBUFFERED=1 \
        benchmark-client:latest \
        python /app/data/init_data.py
fi

echo ""
echo "=== Starting OtterStax ==="
export IMAGE_TAG
_compose_otterstax up -d bench-otterstax

_wait_otterstax
echo "Giving OtterStax extra time to initialise backends..."
sleep 5
_register_connections
$EXTERNAL_ENABLED && _register_s3_credentials
sleep 2

PERF_OUT_DIR=""
if $ENABLE_PERF; then
    echo ""
    echo "=== Starting perf ==="
    PERF_TS="$(date +%Y%m%d_%H%M%S)"
    PERF_OUT_DIR="$REPO_ROOT/benchmark_manual/${PERF_TS}"
    mkdir -p "$PERF_OUT_DIR"
    _start_perf
    # Write state file so stop_service.sh knows perf is active and where to save output.
    printf 'PERF_OUT_DIR=%s\nENABLE_PERF_ALLOC=%s\nIMAGE_TAG=%s\n' \
        "$PERF_OUT_DIR" "$ENABLE_PERF_ALLOC" "$IMAGE_TAG" \
        > "$BENCH_DIR/.perf_state"
fi

echo ""
echo "================================================================"
echo " OtterStax is ready."
echo ""
if $ENABLE_TRACY; then
echo " Tracy profiler  → open Tracy GUI and connect to  localhost:8086"
else
echo " Tracy profiler  → localhost:8086  (not built with Tracy; use --tracy)"
fi
echo " MySQL wire      → localhost:8816  (user=testuser pass=testpass)"
echo " PostgreSQL      → localhost:8817  (user=testuser pass=testpass)"
echo " FlightSQL       → localhost:8815"
echo " HTTP conn API   → localhost:8085"
echo ""
if $ENABLE_PERF; then
echo " perf recording  → stop_service.sh will save:"
echo "   ${PERF_OUT_DIR}/benchmark.perf.data"
echo "   ${PERF_OUT_DIR}/benchmark.perf  (drag into speedscope.app)"
echo ""
fi
if $EXTERNAL_ENABLED; then
echo " s3/file ready   → MinIO seeded (alias bench_minio, bucket bench-bucket)"
echo "   external bench: ./benchmark/manual/run_bench.sh \\"
echo "                    --bench external_load external_join external_dump external_join_cross external_join_all"
fi
echo " Run benchmarks : ./benchmark/manual/run_bench.sh"
echo " Run a query    : ./benchmark/manual/run_query.sh --frontend mysql query.sql"
echo " Stop services  : ./benchmark/manual/stop_service.sh"
echo "================================================================"
