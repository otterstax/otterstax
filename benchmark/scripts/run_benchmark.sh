#!/usr/bin/env bash
# Single entry point for the OtterStax benchmark suite.
# Builds images, starts backend DBs and OtterStax, initialises data,
# registers connections, runs all benchmark scripts, writes results.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$BENCH_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
REPETITIONS=10
FRONTENDS=()
ENABLE_TRACY=false
TRACY_SEP=false
TRACY_OUTPUT_DIR=""
TRACY_FILE=""
TRACY_CAPTURE_CONTAINER=""
ENABLE_PERF=false
ENABLE_PERF_ALLOC=false
PERF_FILE=""
OUT_DIR="$REPO_ROOT/benchmark_results/$(date +%Y%m%d_%H%M%S)"
BUILD_JOBS=0
IMAGE_TAG="${IMAGE_TAG:-bench}"
FORCE_REBUILD=false   # set by --rebuild or --clear; bypasses image-existence check
CLEAR=false           # set by --clear; removes images and volumes before building
EXTERNAL_ENABLED=false  # set when any external_* test is selected (adds MinIO + fixtures)
KAFKA_ENABLED=false     # set when any kafka_* test is selected (adds redpanda + seeds a topic)

# external_* are opt-in via --bench: they need generated fixtures + MinIO, which
# only spin up when selected.  They cover s3/file loading, internal joins and
# dumps — no cross-backend / JOIN ALL workloads.
DEFAULT_TESTS=(simple_select complex_select join_same_instance join_cross_engine join_all)
ALL_TESTS=("${DEFAULT_TESTS[@]}"
           external_load external_join external_dump
           external_join_cross external_join_all
           kafka_ingest kafka_produce kafka_stream)
TESTS=("${DEFAULT_TESTS[@]}")
BENCH_FILTER=()   # populated by --bench; empty = run default tests
# arrow is excluded by default: OtterStax FlightSQL serializer crashes on JOIN
# result sets > ~1000 rows.  Re-add with --frontend arrow once the bug is fixed.
ALL_FRONTENDS=(mysql postgres arrow)
DEFAULT_FRONTENDS=(mysql postgres)
_frontend_port() {
    case "$1" in
        mysql)   echo 8816 ;;
        postgres) echo 8817 ;;
        arrow)   echo 8815 ;;
    esac
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
_usage() {
    cat <<'EOF'
Usage: ./benchmark/scripts/run_benchmark.sh [OPTIONS]

Run the OtterStax benchmark suite end-to-end inside Docker.
Both Docker images are reused automatically when they already exist.

Options:
  --repetitions N     Repetitions per sub-test query (default: 10)
  --frontend F        Frontend to run: mysql | postgres | arrow
                      May be repeated. Default: mysql postgres.
                      arrow is excluded by default (FlightSQL JOIN bug — see CLAUDE.md).
  --bench T           Test(s) to run: simple_select | complex_select |
                      join_same_instance | join_cross_engine | join_all |
                      external_load | external_join | external_dump |
                      external_join_cross | external_join_all |
                      kafka_ingest | kafka_produce | kafka_stream
                      May be repeated. Default: the five cross-backend tests.
                      external_* (s3/file: load, internal joins, dump) are opt-in
                      and auto-start MinIO + generate fixtures. mysql/postgres only.
                      kafka_* are opt-in and auto-start redpanda + seed a JSON
                      topic: kafka_ingest (SOURCE ingest, at-least-once + EOS),
                      kafka_produce (INSERT VALUES write path), kafka_stream
                      (continuous-query throughput).
  --out-dir DIR       Root directory for result files
                      (default: benchmark_results/<YYYYMMDD_HHMMSS>)

Build control:
  --rebuild           Rebuild both Docker images even if they already exist.
                      Use after changing otterstax_app C++ source or bench.yaml.
  --clear             Full reset: remove images + all backend DB volumes, then
                      build and run everything from scratch. Implies --rebuild.
  -j N                Parallel cmake jobs for the otterstax_app build.
                      Default: min(nCPU, floor(dockerRAM_MB / 1536)) — auto-capped
                      so each job has ~1.5 GB. Override when you know your limits.

Tracy profiling:
  --tracy             Continuous Tracy capture for the whole run.
                      Output: <out-dir>/benchmark.tracy
  --tracy-sep         Per-test Tracy capture; OtterStax is restarted between
                      tests for clean boundaries.
                      Output: <out-dir>/<frontend>_<test>.tracy
                      (--tracy-sep takes precedence over --tracy)
  --perf              Whole-run perf capture: CPU call-graph (dwarf), sampled
                      at 99 Hz. Requires image built with perf support
                      (auto-detected; rebuilds if missing).
                      Incompatible with --tracy-sep.
                      Output: <out-dir>/benchmark.perf.data
  --perf-alloc        Like --perf but also attaches a malloc uprobe
                      (perf probe on libc malloc, period=1000) so every
                      1000th malloc call is captured in the call graph.
                      Implies --perf. Useful for finding allocation hotspots.
                      Output: same files as --perf (probe events merged in)

  -h, --help          Print this help and exit

Environment:
  IMAGE_TAG=<tag>     OtterStax image tag to build/use (default: bench)
  GIT_COMMIT=<sha>    Override commit hash written into summary files

Examples:
  # Normal run (reuses existing images automatically)
  ./benchmark/scripts/run_benchmark.sh --repetitions 5

  # Force full clean rebuild
  ./benchmark/scripts/run_benchmark.sh --clear

  # Only postgres, only join tests, 3 reps
  ./benchmark/scripts/run_benchmark.sh --frontend postgres --bench join_same_instance join_cross_engine join_all --repetitions 3

  # Single test across default frontends
  ./benchmark/scripts/run_benchmark.sh --bench simple_select --repetitions 5

  # Rebuild images without wiping DB data
  ./benchmark/scripts/run_benchmark.sh --rebuild
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            _usage; exit 0 ;;
        --repetitions)
            REPETITIONS="$2"; shift 2 ;;
        --repetitions=*)
            REPETITIONS="${1#*=}"; shift ;;
        --frontend)
            FRONTENDS+=("$2"); shift 2 ;;
        --frontend=*)
            FRONTENDS+=("${1#*=}"); shift ;;
        --out-dir)
            OUT_DIR="$2"; shift 2 ;;
        --out-dir=*)
            OUT_DIR="${1#*=}"; shift ;;
        --tracy)
            ENABLE_TRACY=true
            shift ;;
        --tracy-sep)
            ENABLE_TRACY=true
            TRACY_SEP=true
            shift ;;
        --perf)
            ENABLE_PERF=true
            shift ;;
        --perf-alloc)
            ENABLE_PERF=true
            ENABLE_PERF_ALLOC=true
            shift ;;
        --bench)
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^- ]]; do
                BENCH_FILTER+=("$1"); shift
            done ;;
        --bench=*)
            BENCH_FILTER+=("${1#*=}"); shift ;;
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

[ ${#FRONTENDS[@]} -eq 0 ] && FRONTENDS=("${DEFAULT_FRONTENDS[@]}")

if $ENABLE_PERF && $TRACY_SEP; then
    echo "ERROR: --perf and --tracy-sep cannot be used together." >&2
    echo "       --tracy-sep restarts OtterStax between tests, which breaks perf attachment." >&2
    echo "       Run them in separate passes." >&2
    exit 1
fi

if [ ${#BENCH_FILTER[@]} -gt 0 ]; then
    _filtered=()
    for _t in "${ALL_TESTS[@]}"; do
        for _b in "${BENCH_FILTER[@]}"; do
            [ "$_t" = "$_b" ] && { _filtered+=("$_t"); break; }
        done
    done
    TESTS=("${_filtered[@]}")
fi

# External tests pull in MinIO + generated fixtures; kafka tests pull in redpanda
# + a seeded topic.  Detect once up front.
for _t in "${TESTS[@]}"; do
    case "$_t" in
        external_*) EXTERNAL_ENABLED=true ;;
        kafka_*)    KAFKA_ENABLED=true ;;
    esac
done

# Auto-cap BUILD_JOBS when the user did not set -j explicitly.
# C++ compilation peaks at ~1.5 GB per parallel job.  With Docker's memory
# budget we compute a safe upper bound and also cap at the CPU count.
if [ "$BUILD_JOBS" -eq 0 ] 2>/dev/null; then
    _docker_mem_mb=$(docker info --format '{{.MemTotal}}' 2>/dev/null \
                     | awk '{printf "%d", $1/1048576}')
    _docker_cpus=$(docker info --format '{{.NCPU}}' 2>/dev/null || echo 1)
    _mem_jobs=$(( ${_docker_mem_mb:-2048} / 1536 ))   # 1.5 GB per job
    [ "$_mem_jobs" -lt 1 ] && _mem_jobs=1
    BUILD_JOBS=$(( _docker_cpus < _mem_jobs ? _docker_cpus : _mem_jobs ))
    echo "  Auto build jobs: ${BUILD_JOBS} (Docker: ${_docker_cpus} CPUs, ${_docker_mem_mb} MB RAM)"
fi

mkdir -p "$OUT_DIR"

# Tracy paths are relative to OUT_DIR so they land next to the result files.
if $ENABLE_TRACY; then
    TRACY_OUTPUT_DIR="$OUT_DIR"
    if ! $TRACY_SEP; then
        TRACY_FILE="$TRACY_OUTPUT_DIR/benchmark.tracy"
        echo "Tracy profiling enabled (combined) — output: $TRACY_OUTPUT_DIR"
    else
        echo "Tracy per-test profiling enabled — output: $TRACY_OUTPUT_DIR"
    fi
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
    local _files=(-f "$BENCH_DIR/compose_backends.yml")
    # External tests need a seeded MinIO alongside the DB backends.
    $EXTERNAL_ENABLED && _files+=(-f "$BENCH_DIR/compose_minio.yml")
    # Kafka tests need a redpanda broker alongside the DB backends.
    $KAFKA_ENABLED && _files+=(-f "$BENCH_DIR/compose_kafka.yml")
    $COMPOSE_CMD "${_files[@]}" "$@"
}
_compose_otterstax() {
    local _files=(-f "$BENCH_DIR/compose_backends.yml" -f "$BENCH_DIR/compose_benchmark.yml")
    $EXTERNAL_ENABLED && _files+=(-f "$BENCH_DIR/compose_minio.yml")
    $KAFKA_ENABLED && _files+=(-f "$BENCH_DIR/compose_kafka.yml")
    # When --perf is active, include the perf overlay to add SYS_ADMIN/PERFMON
    # capabilities and disable seccomp on bench-otterstax so perf_event_open works.
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
        # INT (signal 2) perf while OtterStax is still running — perf writes its
        # data_size footer cleanly before we stop the container.
        # Note: use -INT not -SIGINT; sh (dash) only accepts short signal names.
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
            echo "  WARNING: perf.data not found inside bench_otterstax (/tmp/benchmark.perf.data)"
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
# Helper: wait for a Docker container healthcheck to reach "healthy"
# ---------------------------------------------------------------------------
_wait_db_healthy() {
    local container=$1
    local retries=60
    local delay=5
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

# ---------------------------------------------------------------------------
# Helper: poll the OtterStax MySQL wire port from inside the bench_net network.
# There is no HTTP health port anymore — connections load at startup from the
# mounted connection config file.
# ---------------------------------------------------------------------------
_wait_otterstax() {
    local retries=120
    local delay=2
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

# ---------------------------------------------------------------------------
# Helper: connections are registered from the mounted connection config file
# (benchmark/config.yaml) at server startup — no runtime API. Kept as a
# no-op so the call sites below need no change.
# ---------------------------------------------------------------------------
_register_connections() {
    echo "Connections read from benchmark/config.yaml at startup (no runtime registration)."
    $EXTERNAL_ENABLED && _register_s3_credentials
}

# The bench MinIO s3 alias ('bench_minio') is likewise declared in
# benchmark/config.yaml and registered at startup. No-op here.
_register_s3_credentials() {
    echo "s3 alias 'bench_minio' read from benchmark/config.yaml at startup."
}

# ---------------------------------------------------------------------------
# Helper: (re)start OtterStax, wait for health, register connections
# ---------------------------------------------------------------------------
_start_otterstax() {
    _compose_otterstax up -d bench-otterstax
    _wait_otterstax
    echo "Giving OtterStax extra time to initialise backends..."
    sleep 5
    _register_connections
    sleep 2
}

# ---------------------------------------------------------------------------
# Helper: ensure OtterStax is running; restart if crashed
# ---------------------------------------------------------------------------
_ensure_otterstax() {
    if docker run --rm --network=bench_net benchmark-client:latest \
           python -c "import socket; socket.create_connection(('bench_otterstax', 8816), 2)" >/dev/null 2>&1; then
        return 0
    fi
    echo "  WARNING: OtterStax unhealthy — restarting..."
    _compose_otterstax stop bench-otterstax 2>/dev/null || true
    _compose_otterstax rm -f bench-otterstax 2>/dev/null || true
    # ensure backends are up (they may have exited after an OtterStax crash)
    _compose_backends up -d
    for c in bench_mariadb1 bench_mariadb2 bench_postgres1 bench_postgres2 \
              bench_clickhouse1 bench_clickhouse2; do
        status=$(docker inspect --format='{{.State.Health.Status}}' "$c" 2>/dev/null || echo "missing")
        [ "$status" != "healthy" ] && _wait_db_healthy "$c"
    done
    _start_otterstax
}

# ---------------------------------------------------------------------------
# Helper: start a tracy-capture sidecar container
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Helper: start perf inside the running OtterStax container via docker exec.
# Running inside the container avoids the sidecar PID-namespace-death problem:
# with --pid=container:X, when X's PID 1 exits the kernel SIGKILLs everything
# in the namespace (including perf) before perf can write its data footer.
# docker exec -d starts perf as a sibling of the server, still alive when we
# SIGINT it during cleanup — before the container is stopped.
# ---------------------------------------------------------------------------
_start_perf() {
    # --call-graph dwarf records stack snapshots that work even for DSOs compiled
    # without frame pointers (Conan packages).
    # exec replaces the shell so pgrep finds "perf" directly and there is no
    # || fallback that would respawn a second perf when the first is killed.

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
        PERF_FILE=""
    fi
}

# ---------------------------------------------------------------------------
# Helper: convert perf.data → speedscope-compatible text
# ---------------------------------------------------------------------------
_convert_perf() {
    local perf_data=$1   # absolute path inside OUT_DIR
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
    echo "  Perf outputs in: $out_dir"
}

# ---------------------------------------------------------------------------
# Step 1: Build images
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 1: Building images ==="
echo ""

export DOCKER_BUILDKIT=1
export BUILD_JOBS          # always explicit — auto-capped above; never fall back to nproc
$ENABLE_TRACY && export WITH_TRACY=true
$ENABLE_PERF  && export WITH_PERF=true

_image_exists() { docker image inspect "$1" >/dev/null 2>&1; }

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

# If --perf was requested, verify the image was built with perf support; rebuild if not.
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
# Step 1b: Generate s3/file external-table fixtures (only when needed)
# Must happen before `up` so MinIO seeds the bucket and bench-otterstax mounts
# the fixtures.  Written to benchmark/data/fixtures on the host (bind-mounted).
# ---------------------------------------------------------------------------
if $EXTERNAL_ENABLED; then
    echo ""
    echo "=== Step 1b: Generating external-table fixtures ==="
    echo ""
    mkdir -p "$BENCH_DIR/data/fixtures"
    docker run --rm \
        -v "$BENCH_DIR/data/fixtures:/app/data/fixtures" \
        -e PYTHONUNBUFFERED=1 \
        benchmark-client:latest \
        python /app/data/generate_external_fixtures.py --out /app/data/fixtures
fi

# ---------------------------------------------------------------------------
# Step 1c: Generate the Kafka JSON dataset (only when a kafka_* test is selected)
# Written to data/fixtures on the host (bind-mounted); seeded into the topic in
# Step 3b after the broker is healthy.
# ---------------------------------------------------------------------------
if $KAFKA_ENABLED; then
    echo ""
    echo "=== Step 1c: Generating Kafka JSON dataset ==="
    echo ""
    mkdir -p "$BENCH_DIR/data/fixtures"
    docker run --rm \
        -v "$BENCH_DIR/data/fixtures:/app/data/fixtures" \
        -e PYTHONUNBUFFERED=1 \
        benchmark-client:latest \
        python /app/data/generate_kafka_fixtures.py --out /app/data/fixtures
fi

# ---------------------------------------------------------------------------
# Step 2: Start backend databases
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 2: Starting backend databases ==="
echo ""

# Stop any lingering bench_otterstax from a previous run before touching DBs.
# An open OtterStax connection holds locks that block DROP TABLE in init_data.py.
_compose_otterstax stop bench-otterstax 2>/dev/null || true
_compose_otterstax rm -f bench-otterstax 2>/dev/null || true

_compose_backends up -d --remove-orphans

_wait_db_healthy bench_mariadb1
_wait_db_healthy bench_mariadb2
_wait_db_healthy bench_postgres1
_wait_db_healthy bench_postgres2
_wait_db_healthy bench_clickhouse1
_wait_db_healthy bench_clickhouse2
$EXTERNAL_ENABLED && _wait_db_healthy bench_minio
$KAFKA_ENABLED && _wait_db_healthy bench_kafka

echo "Giving databases extra time to stabilise..."
sleep 5

# ---------------------------------------------------------------------------
# Step 3: Initialise benchmark data
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 3: Initialising benchmark data ==="
echo ""

docker run --rm --network=bench_net \
    benchmark-client:latest \
    python /app/data/init_data.py

# ---------------------------------------------------------------------------
# Step 3b: Seed the Kafka topic (only when a kafka_* test is selected)
# Explicit one-shot (NOT a compose service) so it runs exactly once — see
# compose_kafka.yml for why re-seeding on a mid-run restart must be avoided.
# ---------------------------------------------------------------------------
if $KAFKA_ENABLED; then
    echo ""
    echo "=== Step 3b: Seeding Kafka topic ==="
    echo ""
    docker run --rm --network=bench_net \
        -v "$BENCH_DIR/data/fixtures:/fixtures:ro" \
        -e PYTHONUNBUFFERED=1 \
        benchmark-client:latest \
        python /app/data/seed_kafka.py \
        --broker bench_kafka:9092 \
        --file /fixtures/kafka_events.ndjson
fi

# ---------------------------------------------------------------------------
# Step 4: Start OtterStax
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 4: Starting OtterStax ==="
echo ""

if $ENABLE_TRACY && ! $TRACY_SEP; then
    _start_otterstax
    TRACY_CAPTURE_CONTAINER="tracy-bench-$$"
    _start_tracy "$TRACY_CAPTURE_CONTAINER" "$TRACY_FILE"
elif ! $TRACY_SEP; then
    _start_otterstax
fi
# TRACY_SEP mode: OtterStax is started fresh per-test in Step 5.

if $ENABLE_PERF; then
    PERF_FILE="$OUT_DIR/benchmark.perf.data"
    _start_perf
fi

# ---------------------------------------------------------------------------
# Step 5: Run benchmark tests
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 5: Running benchmarks ==="
echo "    Frontends      : ${FRONTENDS[*]}"
echo "    Tests          : ${TESTS[*]}"
echo "    Reps           : $REPETITIONS"
echo "    Out dir        : $OUT_DIR"
echo ""

PASS=0
FAIL=0

for frontend in "${FRONTENDS[@]}"; do
    port="$(_frontend_port "$frontend")"
    for test in "${TESTS[@]}"; do

        echo ""
        echo "--- $frontend / $test (port $port) ---"

        if $TRACY_SEP; then
            tracy_out="${frontend}_${test}.tracy"
            sep_capture="tracy-sep-bench-$$-${frontend}-${test}"

            # Full startup sequence first — health check, backend init, registration.
            # All startup zones must be CLOSED before tracy-capture connects.
            # With on_demand=False, Tracy sends the full historical buffer on connect;
            # any zone still open at connect time gets a synthetic end in the buffer
            # and a real end later → "Zone is ended twice" → tracy-capture aborts.
            # Connecting after init is complete avoids this entirely.
            _compose_otterstax up -d bench-otterstax
            _wait_otterstax
            echo "Giving OtterStax extra time to initialise backends..."
            sleep 5
            _register_connections
            sleep 2

            # Port 8086 is already up — no need for -s retry flag.
            _start_tracy "$sep_capture" "$TRACY_OUTPUT_DIR/$tracy_out"
        else
            _ensure_otterstax
        fi

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

        if $TRACY_SEP; then
            # Stop OtterStax — closes the Tracy TCP socket, which signals tracy-capture
            # to flush and exit cleanly with the complete trace.
            _compose_otterstax stop bench-otterstax 2>/dev/null || true

            if docker inspect "$sep_capture" \
                    --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
                echo "  Waiting for tracy-capture to finalise $tracy_out..."
                for i in {1..30}; do
                    docker inspect "$sep_capture" \
                        --format '{{.State.Running}}' 2>/dev/null | grep -q true || break
                    sleep 1
                    [ "$i" -eq 30 ] && docker stop "$sep_capture" 2>/dev/null || true
                done
            fi
            docker logs "$sep_capture" 2>&1 | tail -20
            docker rm "$sep_capture" 2>/dev/null || true
            [ -f "$TRACY_OUTPUT_DIR/$tracy_out" ] \
                && echo "  Saved: $TRACY_OUTPUT_DIR/$tracy_out" \
                || echo "  WARNING: Tracy file not found: $tracy_out"

            _compose_otterstax rm -f bench-otterstax 2>/dev/null || true
        fi
    done
done

# ---------------------------------------------------------------------------
# Step 6: Write global summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 6: Writing global summary ==="
echo ""

docker run --rm \
    -v "$OUT_DIR:/results" \
    -e GIT_COMMIT="$GIT_COMMIT" \
    -e PYTHONUNBUFFERED=1 \
    benchmark-client:latest \
    python /app/scripts/generate_summary.py /results

echo ""
echo "=== Benchmark complete: $PASS passed, $FAIL failed ==="
echo "    Results: $OUT_DIR"
echo ""

[ $FAIL -gt 0 ] && exit 1 || exit 0
