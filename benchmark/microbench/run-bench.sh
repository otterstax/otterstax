#!/usr/bin/env bash
# run-bench.sh — Build (if needed) and run the OtterStax Google Benchmark microbenchmarks.
# Usage: ./benchmark/microbench/run-bench.sh [OPTIONS]
# Can also be run from the repo root.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
BUILD_DIR="build/Release"
RESULTS_ROOT="benchmark_results/microbench"
REPETITIONS=5
FILTER=""
RECONFIGURE=false
PARALLEL_JOBS="$(nproc 2>/dev/null || echo 4)"

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
_usage() {
    cat <<'EOF'
Usage: ./benchmark/microbench/run-bench.sh [OPTIONS]

Build and run the Google Benchmark microbenchmarks (otterstax_bench).
Results are saved to benchmark_results/microbench/<timestamp>/.

Options:
  --repetitions N      Repetitions per benchmark (default: 5)
  --filter REGEX       Run only benchmarks matching REGEX
                       (passed to --benchmark_filter)
  --reconfigure        Force cmake reconfigure even if already configured
  --build-dir DIR      CMake build directory (default: build/Release)
  --out-dir DIR        Results root (default: benchmark_results/microbench)
  -j N                 Parallel build jobs (default: nproc)
  -h, --help           Show this help

Examples:
  # All benchmarks, 5 reps
  ./benchmark/microbench/run-bench.sh

  # Only translator benchmarks, 10 reps
  ./benchmark/microbench/run-bench.sh --repetitions 10 --filter "BM_ch_to_chunk|BM_mysql"

  # Force reconfigure (e.g. after conanfile.py change)
  ./benchmark/microbench/run-bench.sh --reconfigure
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --repetitions)  REPETITIONS=$2;    shift 2 ;;
        --filter)       FILTER=$2;         shift 2 ;;
        --reconfigure)  RECONFIGURE=true;  shift   ;;
        --build-dir)    BUILD_DIR=$2;      shift 2 ;;
        --out-dir)      RESULTS_ROOT=$2;   shift 2 ;;
        -j)             PARALLEL_JOBS=$2;  shift 2 ;;
        -h|--help)      _usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; _usage; exit 1 ;;
    esac
done

BENCH_BIN="$BUILD_DIR/benchmark/microbench/otterstax_bench"
TOOLCHAIN="$BUILD_DIR/generators/conan_toolchain.cmake"

# ---------------------------------------------------------------------------
# Step 1: cmake configure
# ---------------------------------------------------------------------------
_needs_configure() {
    # Reconfigure if forced, if no cache exists, or if BUILD_BENCHMARKS is off
    [[ "$RECONFIGURE" == true ]] && return 0
    [[ ! -f "$BUILD_DIR/CMakeCache.txt" ]] && return 0
    grep -q "BUILD_BENCHMARKS:BOOL=ON" "$BUILD_DIR/CMakeCache.txt" || return 0
    return 1
}

if _needs_configure; then
    echo "==> Configuring cmake (BUILD_BENCHMARKS=ON)..."
    cmake -S . -B "$BUILD_DIR" \
        -DBUILD_BENCHMARKS=ON \
        -DCMAKE_TOOLCHAIN_FILE="$TOOLCHAIN"
else
    echo "==> CMake already configured with BUILD_BENCHMARKS=ON — skipping configure."
fi

# ---------------------------------------------------------------------------
# Step 2: Build
# ---------------------------------------------------------------------------
echo "==> Building otterstax_bench (parallel: $PARALLEL_JOBS)..."
/usr/local/bin/cmake --build "$BUILD_DIR" --parallel "$PARALLEL_JOBS" --target otterstax_bench

# ---------------------------------------------------------------------------
# Step 3: LD_LIBRARY_PATH — collect all conan lib dirs
# ---------------------------------------------------------------------------
CONAN_LIBS="$(find /conan/.conan2/p/b/*/p/lib -maxdepth 0 2>/dev/null | tr '\n' ':')"
export LD_LIBRARY_PATH="${CONAN_LIBS}${LD_LIBRARY_PATH:-}"

# ---------------------------------------------------------------------------
# Step 4: Create timestamped results directory
# ---------------------------------------------------------------------------
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="$RESULTS_ROOT/$TIMESTAMP"
mkdir -p "$OUT_DIR"

JSON_OUT="$OUT_DIR/bench_results.json"
LOG_OUT="$OUT_DIR/bench_output.txt"

echo "==> Running benchmarks (repetitions=$REPETITIONS)..."
[[ -n "$FILTER" ]] && echo "    filter: $FILTER"
echo "    output: $OUT_DIR"
echo ""

# ---------------------------------------------------------------------------
# Step 5: Run benchmarks
# ---------------------------------------------------------------------------
BENCH_ARGS=(
    "--benchmark_repetitions=$REPETITIONS"
    "--benchmark_report_aggregates_only=true"
    "--benchmark_format=console"
    "--benchmark_out_format=json"
    "--benchmark_out=$JSON_OUT"
)
[[ -n "$FILTER" ]] && BENCH_ARGS+=("--benchmark_filter=$FILTER")

# The PostgreSQL raw_parser has a global-state teardown that causes a segfault
# at process exit — all benchmarks complete before it fires.  We tolerate exit
# code 139 (SIGSEGV) as a known-benign crash.
set +e
"$BENCH_BIN" "${BENCH_ARGS[@]}" 2>&1 | tee "$LOG_OUT"
EXIT_CODE=$?
set -e

if [[ $EXIT_CODE -ne 0 && $EXIT_CODE -ne 139 ]]; then
    echo ""
    echo "ERROR: otterstax_bench exited with code $EXIT_CODE" >&2
    exit "$EXIT_CODE"
fi

# ---------------------------------------------------------------------------
# Step 6: Fix truncated JSON (written at process exit, may be cut off by crash)
# ---------------------------------------------------------------------------
python3 - "$JSON_OUT" <<'PYEOF'
import sys, json

path = sys.argv[1]
if not path:
    sys.exit(0)

try:
    raw = open(path).read()
except FileNotFoundError:
    print(f"WARNING: {path} not found — no JSON output.", file=sys.stderr)
    sys.exit(0)

try:
    json.loads(raw)
except json.JSONDecodeError:
    last_close = raw.rfind('\n    }')
    if last_close >= 0:
        fixed = raw[:last_close + 6] + '\n  ]\n}\n'
        with open(path, 'w') as f:
            f.write(fixed)
        print("(Fixed truncated JSON output.)")
    else:
        print(f"WARNING: could not repair {path}", file=sys.stderr)
        sys.exit(0)
PYEOF

# ---------------------------------------------------------------------------
# Step 7: Print summary table
# ---------------------------------------------------------------------------
python3 - "$JSON_OUT" <<'PYEOF' | tee -a "$LOG_OUT"
import sys, json

path = sys.argv[1]
try:
    d = json.load(open(path))
except Exception as e:
    print(f"WARNING: could not parse results JSON: {e}", file=sys.stderr)
    sys.exit(0)

means   = {b['name'].replace('_mean',   ''): b for b in d['benchmarks'] if b['name'].endswith('_mean')}
stddevs = {b['name'].replace('_stddev', ''): b for b in d['benchmarks'] if b['name'].endswith('_stddev')}
cvs     = {b['name'].replace('_cv',     ''): b for b in d['benchmarks'] if b['name'].endswith('_cv')}

def fmt_time(ns):
    if ns < 1_000:
        return f"{ns:.1f} ns"
    if ns < 1_000_000:
        return f"{ns/1000:.2f} µs"
    if ns < 1_000_000_000:
        return f"{ns/1e6:.2f} ms"
    return f"{ns/1e9:.2f} s"

print()
print(f"{'Benchmark':<44} {'mean':>10}  {'±stddev':>10}  {'CV':>6}  {'items/s':>10}")
print('─' * 88)
for name, b in means.items():
    t      = b['real_time']
    sd     = stddevs.get(name, {}).get('real_time', 0)
    cv_pct = cvs.get(name, {}).get('real_time', 0) * 100
    ips    = b.get('items_per_second', 0)
    ips_s  = f"{ips/1e6:.1f}M" if ips >= 1e6 else (f"{ips/1e3:.1f}k" if ips else '')
    print(f"{name:<44} {fmt_time(t):>10}  {fmt_time(sd):>10}  {cv_pct:>5.1f}%  {ips_s:>10}")

print()
PYEOF

echo "Results saved to: $OUT_DIR"
echo "  Console log : $LOG_OUT"
echo "  JSON        : $JSON_OUT"
