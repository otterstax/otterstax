#!/usr/bin/env bash
# Stress-test entry point for OtterStax.
# Starts backends + OtterStax once, runs N escalating load stages (defined by a
# YAML profile or built-in defaults), then writes a degradation report comparing
# throughput, latency, and error rate across all stages.
# The service is kept alive between stages — only a cooling period separates them.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$BENCH_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
PROFILE=""
WORKERS_SMALL=1
WORKERS_MEDIUM=5
WORKERS_HEAVY=15
DURATION_SMALL=60
DURATION_MEDIUM=60
DURATION_HEAVY=90
SKIP_INIT=false
ENABLE_TRACY=false
TRACY_FILE=""
TRACY_CAPTURE_CONTAINER=""
TRACY_OUTPUT_DIR=""
ENABLE_PERF=false
ENABLE_PERF_ALLOC=false
PERF_FILE=""
OUT_DIR="$REPO_ROOT/benchmark_results/stress/$(date +%Y%m%d_%H%M%S)"
BUILD_JOBS=0
IMAGE_TAG="${IMAGE_TAG:-bench}"
FORCE_REBUILD=false
CLEAR=false

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
_usage() {
    cat <<'EOF'
Usage: ./benchmark/stress/run_stress_benchmarks.sh [OPTIONS]

Run the OtterStax stress test: three escalating load stages against the same
live service instance, finishing with a degradation report.

Profile:
  -p, --profile FILE   YAML profile defining stages (workers, duration, query pool).
                       When provided, --workers-* and --duration-* flags are ignored.
                       Built-in profiles: benchmark/stress/profiles/{default,select_only}.yaml

Stage knobs (ignored when --profile is given):
  --workers-small N    Workers per frontend, stage 1 (default: 3)
  --workers-medium N   Workers per frontend, stage 2 (default: 15)
  --workers-heavy N    Workers per frontend, stage 3 (default: 30)
  --duration-small N   Stage 1 active duration in seconds (default: 30)
  --duration-medium N  Stage 2 active duration in seconds (default: 60)
  --duration-heavy N   Stage 3 active duration in seconds (default: 90)

Output:
  --out-dir DIR        Result root (default: benchmark_results/stress/<YYYYMMDD_HHMMSS>)

Build control:
  --rebuild            Rebuild both Docker images even if they already exist.
  --no-init            Skip benchmark data initialisation (reuse existing DB volumes).
  --clear              Full reset: remove images + DB volumes, then rebuild from scratch.
  -j N                 Parallel cmake jobs for the otterstax_app build (default: auto).

Profiling:
  --tracy              Continuous Tracy capture for all stages.
                       Output: <out-dir>/benchmark.tracy
  --perf               CPU call-graph sampling (perf, 99 Hz, dwarf unwind).
                       Outputs: <out-dir>/benchmark.perf.data + benchmark.perf (speedscope)
  --perf-alloc         Like --perf but also attaches a malloc uprobe. Implies --perf.

Environment:
  IMAGE_TAG=<tag>      OtterStax image tag (default: bench)
  GIT_COMMIT=<sha>     Override commit hash written into summary files

Examples:
  # Default 3-stage run
  ./benchmark/stress/run_stress_benchmarks.sh

  # Quick smoke-test profile
  ./benchmark/stress/run_stress_benchmarks.sh -p benchmark/stress/profiles/select_only.yaml

  # Custom profile with Tracy instrumentation
  ./benchmark/stress/run_stress_benchmarks.sh --profile my_profile.yaml --tracy

  # Reduced-scale default run (no image rebuild needed)
  ./benchmark/stress/run_stress_benchmarks.sh \
    --workers-small 2 --workers-medium 5 --workers-heavy 10 \
    --duration-small 15 --duration-medium 30 --duration-heavy 45
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            _usage; exit 0 ;;
        -p|--profile)
            PROFILE="$2"; shift 2 ;;
        --profile=*|-p=*)
            PROFILE="${1#*=}"; shift ;;
        --workers-small)
            WORKERS_SMALL="$2"; shift 2 ;;
        --workers-small=*)
            WORKERS_SMALL="${1#*=}"; shift ;;
        --workers-medium)
            WORKERS_MEDIUM="$2"; shift 2 ;;
        --workers-medium=*)
            WORKERS_MEDIUM="${1#*=}"; shift ;;
        --workers-heavy)
            WORKERS_HEAVY="$2"; shift 2 ;;
        --workers-heavy=*)
            WORKERS_HEAVY="${1#*=}"; shift ;;
        --duration-small)
            DURATION_SMALL="$2"; shift 2 ;;
        --duration-small=*)
            DURATION_SMALL="${1#*=}"; shift ;;
        --duration-medium)
            DURATION_MEDIUM="$2"; shift 2 ;;
        --duration-medium=*)
            DURATION_MEDIUM="${1#*=}"; shift ;;
        --duration-heavy)
            DURATION_HEAVY="$2"; shift 2 ;;
        --duration-heavy=*)
            DURATION_HEAVY="${1#*=}"; shift ;;
        --out-dir)
            OUT_DIR="$2"; shift 2 ;;
        --out-dir=*)
            OUT_DIR="${1#*=}"; shift ;;
        --no-init)
            SKIP_INIT=true; shift ;;
        --tracy)
            ENABLE_TRACY=true; shift ;;
        --perf)
            ENABLE_PERF=true; shift ;;
        --perf-alloc)
            ENABLE_PERF=true; ENABLE_PERF_ALLOC=true; shift ;;
        --rebuild)
            FORCE_REBUILD=true; shift ;;
        --clear)
            CLEAR=true; FORCE_REBUILD=true; shift ;;
        -j)
            BUILD_JOBS="$2"; shift 2 ;;
        -j*)
            BUILD_JOBS="${1#-j}"; shift ;;
        *)
            echo "Unknown option: $1" >&2
            _usage
            exit 1 ;;
    esac
done

# Validate profile path if given
if [ -n "$PROFILE" ]; then
    PROFILE_ABS="$(realpath "$PROFILE" 2>/dev/null || echo "")"
    if [ -z "$PROFILE_ABS" ] || [ ! -f "$PROFILE_ABS" ]; then
        echo "ERROR: profile file not found: $PROFILE" >&2
        exit 1
    fi
    echo "Profile: $PROFILE_ABS"
fi

# Auto-cap BUILD_JOBS
if [ "$BUILD_JOBS" -eq 0 ] 2>/dev/null; then
    _docker_mem_mb=$(docker info --format '{{.MemTotal}}' 2>/dev/null \
                     | awk '{printf "%d", $1/1048576}')
    _docker_cpus=$(docker info --format '{{.NCPU}}' 2>/dev/null || echo 1)
    _mem_jobs=$(( ${_docker_mem_mb:-2048} / 1536 ))
    [ "$_mem_jobs" -lt 1 ] && _mem_jobs=1
    BUILD_JOBS=$(( _docker_cpus < _mem_jobs ? _docker_cpus : _mem_jobs ))
    echo "  Auto build jobs: ${BUILD_JOBS} (Docker: ${_docker_cpus} CPUs, ${_docker_mem_mb} MB RAM)"
fi

mkdir -p "$OUT_DIR"

if $ENABLE_TRACY; then
    TRACY_OUTPUT_DIR="$OUT_DIR"
    TRACY_FILE="$OUT_DIR/benchmark.tracy"
    echo "Tracy profiling enabled — output: $TRACY_FILE"
fi

GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")

# ---------------------------------------------------------------------------
# Docker Compose detection
# ---------------------------------------------------------------------------
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    echo "ERROR: neither 'docker compose' nor 'docker-compose' found." >&2
    exit 1
fi

_compose_backends() {
    $COMPOSE_CMD -f "$BENCH_DIR/compose_backends.yml" "$@"
}
_compose_otterstax() {
    local _files=(-f "$BENCH_DIR/compose_backends.yml" -f "$BENCH_DIR/compose_benchmark.yml")
    $ENABLE_PERF && _files+=(-f "$BENCH_DIR/compose_benchmark_perf.yml")
    $COMPOSE_CMD "${_files[@]}" "$@"
}

# ---------------------------------------------------------------------------
# Cleanup on exit
# ---------------------------------------------------------------------------
_cleanup() {
    if [ -n "$TRACY_CAPTURE_CONTAINER" ]; then
        echo "Tracy cleanup: stopping OtterStax for clean disconnect..."
        _compose_otterstax stop bench-otterstax 2>/dev/null || true
        for i in {1..15}; do
            docker inspect "$TRACY_CAPTURE_CONTAINER" \
                --format '{{.State.Running}}' 2>/dev/null | grep -q true || break
            sleep 1
        done
        docker stop "$TRACY_CAPTURE_CONTAINER" 2>/dev/null || true
        docker rm   "$TRACY_CAPTURE_CONTAINER" 2>/dev/null || true
        TRACY_CAPTURE_CONTAINER=""
        [ -f "$TRACY_FILE" ] && echo "Tracy file saved: $TRACY_FILE"
    fi
    if $ENABLE_PERF && [ -n "$PERF_FILE" ]; then
        echo "Perf cleanup: sending INT to perf inside bench_otterstax..."
        docker exec bench_otterstax sh -c \
            'kill -INT $(pgrep -x perf) 2>/dev/null || true' 2>/dev/null || true
        echo "  Waiting for perf to finalise..."
        for i in {1..30}; do
            docker exec bench_otterstax sh -c 'pgrep -x perf >/dev/null 2>&1' \
                2>/dev/null || break
            sleep 1
        done
        if docker cp bench_otterstax:/tmp/benchmark.perf.data "$PERF_FILE" 2>/dev/null; then
            echo "Perf data saved: $PERF_FILE"
            _convert_perf "$PERF_FILE"
        else
            echo "  WARNING: perf.data not found inside bench_otterstax"
            docker exec bench_otterstax cat /tmp/perf.log 2>/dev/null | tail -10 || true
        fi
        if $ENABLE_PERF_ALLOC; then
            docker exec bench_otterstax sh -c \
                'perf probe --del probe_libc:malloc 2>/dev/null || true' 2>/dev/null || true
        fi
    fi
    _compose_otterstax down --volumes --remove-orphans 2>/dev/null || true
}
trap _cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Helpers (identical to run_benchmark.sh)
# ---------------------------------------------------------------------------
_wait_db_healthy() {
    local container=$1
    local retries=60 delay=5
    echo "Waiting for $container..."
    for ((i=1; i<=retries; i++)); do
        status=$(docker inspect --format='{{.State.Health.Status}}' \
                 "$container" 2>/dev/null || echo "missing")
        [ "$status" = "healthy" ] && { echo "  $container healthy"; return 0; }
        echo "  $container: $status ($i/$retries)"
        sleep "$delay"
    done
    echo "  ERROR: timeout waiting for $container"
    _compose_backends logs "$container" 2>/dev/null | tail -20
    return 1
}

_wait_otterstax() {
    local retries=120 delay=2
    # No HTTP health port anymore — probe the MySQL wire port (8816). Connections
    # are read from the mounted connection config file at startup.
    echo "Waiting for OtterStax wire port (8816)..."
    for ((i=1; i<=retries; i++)); do
        if docker run --rm --network=bench_net benchmark-client:latest \
               python -c "import socket; socket.create_connection(('bench_otterstax', 8816), 2)" >/dev/null 2>&1; then
            echo "  OtterStax ready"
            return 0
        fi
        echo "  not ready ($i/$retries)"
        sleep "$delay"
    done
    echo "  ERROR: timeout waiting for OtterStax"
    _compose_otterstax logs bench-otterstax 2>/dev/null | tail -30
    return 1
}

# Connections (mysql1/mysql2/pg1/pg2/ch1/ch2) are registered from the mounted
# connection config file (benchmark/config.yaml) at server startup — there
# is no runtime registration API. Kept as a no-op so call sites need no change.
_register_connections() {
    echo "Connections read from benchmark/config.yaml at startup (no runtime registration)."
}

_start_otterstax() {
    _compose_otterstax up -d bench-otterstax
    _wait_otterstax
    echo "Giving OtterStax extra time to initialise backends..."
    sleep 5
    _register_connections
    sleep 2
}

_start_tracy() {
    local name=$1 output=$2
    docker run -d \
        --name "$name" \
        --network=bench_net \
        -v "$TRACY_OUTPUT_DIR:/tracy_profiles" \
        "otterstax_app:${IMAGE_TAG}" \
        tracy-capture -a bench_otterstax -p 8086 \
        -o "/tracy_profiles/$(basename "$output")" -f
    sleep 2
    if docker inspect "$name" --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
        echo "  tracy-capture running ($name)"
    else
        echo "  WARNING: tracy-capture exited immediately"
        docker logs "$name" 2>/dev/null | tail -10
        docker rm "$name" 2>/dev/null || true
    fi
}

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
              -e cpu-clock -e probe_libc:malloc \
              -p 1 -o /tmp/benchmark.perf.data > /tmp/perf.log 2>&1
        '
    else
        docker exec -d bench_otterstax sh -c '
            exec perf record --call-graph dwarf -F 99 \
              -e cpu-clock \
              -p 1 -o /tmp/benchmark.perf.data > /tmp/perf.log 2>&1
        '
    fi

    sleep 2
    if docker exec bench_otterstax sh -c 'pgrep -x perf >/dev/null 2>&1'; then
        echo "  perf running inside bench_otterstax"
    else
        echo "  WARNING: perf failed to start"
        docker exec bench_otterstax cat /tmp/perf.log 2>/dev/null | tail -10
        PERF_FILE=""
    fi
}

_convert_perf() {
    local perf_data=$1
    local out_dir
    out_dir="$(dirname "$perf_data")"
    echo "Converting perf.data → benchmark.perf (speedscope)..."
    docker run --rm \
        -v "$out_dir:/results" \
        "otterstax_app:${IMAGE_TAG}" \
        sh -c '
            cd /results
            perf script -i benchmark.perf.data > benchmark.perf 2>&1 && \
                echo "  benchmark.perf written ($(wc -l < benchmark.perf) lines)" || \
                echo "  WARNING: perf script failed"
        '
    echo "  Drag benchmark.perf into speedscope.app to explore"
}

_image_exists() { docker image inspect "$1" >/dev/null 2>&1; }

# ---------------------------------------------------------------------------
# Step 1: Build images
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 1: Building images ==="
echo ""

export DOCKER_BUILDKIT=1
export BUILD_JOBS
$ENABLE_TRACY && export WITH_TRACY=true
$ENABLE_PERF  && export WITH_PERF=true

if $CLEAR; then
    echo "=== --clear: removing images and volumes ==="
    _compose_backends down --volumes --remove-orphans 2>/dev/null || true
    docker rmi "benchmark-client:latest" 2>/dev/null && echo "  removed benchmark-client:latest" || true
    docker rmi "otterstax_app:${IMAGE_TAG}" 2>/dev/null && echo "  removed otterstax_app:${IMAGE_TAG}" || true
    echo ""
fi

if $FORCE_REBUILD || ! _image_exists "benchmark-client:latest"; then
    echo "Building benchmark-client:latest ..."
    docker build -t benchmark-client:latest \
        -f "$BENCH_DIR/Dockerfile.benchmark" \
        "$REPO_ROOT"
else
    echo "  benchmark-client:latest already exists — skipping (use --rebuild to force)"
fi

if $FORCE_REBUILD || ! _image_exists "otterstax_app:${IMAGE_TAG}"; then
    echo "Building otterstax_app:${IMAGE_TAG} ..."
    docker build -t "otterstax_app:${IMAGE_TAG}" \
        -f "$REPO_ROOT/Dockerfile.test" \
        --build-arg WITH_TRACY="${WITH_TRACY:-false}" \
        --build-arg WITH_PERF="${WITH_PERF:-false}" \
        --build-arg BUILD_JOBS="${BUILD_JOBS}" \
        "$REPO_ROOT"
else
    echo "  otterstax_app:${IMAGE_TAG} already exists — skipping (use --rebuild to force)"
fi

if $ENABLE_PERF; then
    if ! docker run --rm --entrypoint sh "otterstax_app:${IMAGE_TAG}" \
           -c 'command -v perf >/dev/null 2>&1'; then
        echo "  perf not found in image — rebuilding with perf tools..."
        docker build -t "otterstax_app:${IMAGE_TAG}" \
            -f "$REPO_ROOT/Dockerfile.test" \
            --build-arg WITH_TRACY="${WITH_TRACY:-false}" \
            --build-arg WITH_PERF=true \
            --build-arg BUILD_JOBS="${BUILD_JOBS}" \
            "$REPO_ROOT"
    fi
fi

# ---------------------------------------------------------------------------
# Step 2: Start backend databases
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 2: Starting backend databases ==="
echo ""

_compose_otterstax stop bench-otterstax 2>/dev/null || true
_compose_otterstax rm -f bench-otterstax 2>/dev/null || true
_compose_backends up -d --remove-orphans

for _db in bench_mariadb1 bench_mariadb2 bench_postgres1 bench_postgres2 \
           bench_clickhouse1 bench_clickhouse2; do
    _wait_db_healthy "$_db"
done

echo "Giving databases extra time to stabilise..."
sleep 5

# ---------------------------------------------------------------------------
# Step 3: Initialise benchmark data
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 3: Initialising benchmark data ==="
echo ""

if $SKIP_INIT; then
    echo "  --no-init: skipping data initialisation"
else
    docker run --rm --network=bench_net \
        -e PYTHONUNBUFFERED=1 \
        benchmark-client:latest \
        python /app/data/init_data.py
fi

# ---------------------------------------------------------------------------
# Step 4: Start OtterStax (stays alive for all stages)
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 4: Starting OtterStax ==="
echo ""

_start_otterstax

# ---------------------------------------------------------------------------
# Step 5: Start profiling (optional)
# ---------------------------------------------------------------------------
if $ENABLE_TRACY; then
    echo ""
    echo "=== Step 5a: Starting Tracy capture ==="
    TRACY_CAPTURE_CONTAINER="tracy-stress-$$"
    _start_tracy "$TRACY_CAPTURE_CONTAINER" "$TRACY_FILE"
fi

if $ENABLE_PERF; then
    echo ""
    echo "=== Step 5b: Starting perf inside OtterStax container ==="
    PERF_FILE="$OUT_DIR/benchmark.perf.data"
    _start_perf
fi

# ---------------------------------------------------------------------------
# Step 6: Run stress test (all stages in one Python process)
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 6: Running stress test ==="
echo "    Profile      : ${PROFILE:-built-in default}"
echo "    Out dir      : $OUT_DIR"
echo ""

# Build docker run args for profile mounting
PROFILE_MOUNT_ARGS=()
PROFILE_PY_ARGS=()
if [ -n "$PROFILE_ABS" ]; then
    PROFILE_MOUNT_ARGS=(-v "${PROFILE_ABS}:/app/stress_profile.yaml:ro")
    PROFILE_PY_ARGS=(--profile /app/stress_profile.yaml)
fi

docker run --rm \
    --network=bench_net \
    -v "$OUT_DIR:/results" \
    "${PROFILE_MOUNT_ARGS[@]}" \
    -e GIT_COMMIT="$GIT_COMMIT" \
    -e PYTHONUNBUFFERED=1 \
    benchmark-client:latest \
    python /app/stress/stress_main.py \
        --host bench_otterstax \
        --out-dir /results \
        --workers-small  "$WORKERS_SMALL" \
        --workers-medium "$WORKERS_MEDIUM" \
        --workers-heavy  "$WORKERS_HEAVY" \
        --duration-small  "$DURATION_SMALL" \
        --duration-medium "$DURATION_MEDIUM" \
        --duration-heavy  "$DURATION_HEAVY" \
        "${PROFILE_PY_ARGS[@]}"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "=== Stress test complete ==="
echo "    Results: $OUT_DIR"
echo "    Report : $OUT_DIR/degradation_report.md"
echo ""
