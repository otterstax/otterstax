#!/usr/bin/env bash
# A/B end-to-end benchmark: a baseline build of OtterStax against the working tree.
#
# Builds two otterstax_app images (baseline, working tree), runs
# benchmark/scripts/run_benchmark.sh for each, interleaved base/new/base/new...
# so drift on the machine hits both alike, then compares the medians.
#
# Usage:
#   ab_bench.sh [--base index|REF] [--runs N] [--name NAME] [-j N] [--rebuild] [--dry-run]
#               [-- RUN_BENCHMARK_ARGS...]
#
#   --base index   baseline = the git index (staged changes)           [default]
#   --base REF     baseline = a commit / branch / tag (git archive REF)
#   --runs N       runs per side                                        [default 3]
#   --name NAME    result dir benchmark_results/ab_NAME, image tags ab_NAME_{base,new}
#                                                                       [default: timestamp]
#   -j N           cmake jobs per image build   [default: min(nCPU, dockerRAM_MB / 1536)]
#   --rebuild      rebuild the images even if the tags exist
#   --dry-run      preflight + print the plan, run nothing
#   -- ARGS        passed to run_benchmark.sh   [default: --frontend postgres --repetitions 5]
#                  (never pass --out-dir: this script sets an absolute one per run)
#
# Output: benchmark_results/ab_NAME/{base,new}/run<i>/ (+ run<i>.log) and compare.md.
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$SKILL_DIR" rev-parse --show-toplevel)"

BASE=index
RUNS=3
NAME="$(date +%Y%m%d_%H%M%S)"
JOBS=""
REBUILD=false
DRY=false
BENCH_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --base) BASE="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --name) NAME="$2"; shift 2 ;;
        -j) JOBS="$2"; shift 2 ;;
        --rebuild) REBUILD=true; shift ;;
        --dry-run) DRY=true; shift ;;
        --) shift; BENCH_ARGS=("$@"); break ;;
        -h|--help) sed -n '2,24p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown option: $1 (see --help)" >&2; exit 64 ;;
    esac
done
[[ ${#BENCH_ARGS[@]} -eq 0 ]] && BENCH_ARGS=(--frontend postgres --repetitions 5)
for a in "${BENCH_ARGS[@]}"; do
    [[ "$a" == --out-dir* ]] && { echo "do not pass --out-dir; ab_bench.sh sets one per run" >&2; exit 64; }
done

OUT="$REPO/benchmark_results/ab_$NAME" # absolute: run_benchmark.sh mounts it with docker -v
TAG_BASE="ab_${NAME}_base"
TAG_NEW="ab_${NAME}_new"

# ---- preflight -------------------------------------------------------------
docker info >/dev/null 2>&1 || { echo "docker is not running" >&2; exit 1; }

# run_benchmark.sh runs under compose project "benchmark"; the manual scripts
# (benchmark/manual/*.sh) use "-p bench" with the SAME container names, so any
# bench_* container of another project makes every run fail with "Conflict".
conflicts="$(docker ps -a --filter name=bench_ \
    --format '{{.Names}} {{.Label "com.docker.compose.project"}} {{.Status}}' | awk '$2 != "benchmark"')"
if [[ -n "$conflicts" ]]; then
    echo "bench_* containers from another compose project block run_benchmark.sh:" >&2
    echo "$conflicts" | sed 's/^/  /' >&2
    echo "Remove them (ask the user first), e.g.:" >&2
    echo "  docker compose -p bench -f $REPO/benchmark/compose_backends.yml down   # keeps volumes" >&2
    exit 2
fi

if [[ -z "$JOBS" ]]; then
    read -r ncpu mem < <(docker info --format '{{.NCPU}} {{.MemTotal}}')
    by_ram=$(( mem / 1024 / 1024 / 1536 ))
    JOBS=$(( by_ram < ncpu ? by_ram : ncpu ))
    (( JOBS < 1 )) && JOBS=1
fi

if [[ "$BASE" != index ]]; then
    git -C "$REPO" rev-parse --verify --quiet "$BASE^{commit}" >/dev/null ||
        { echo "unknown git ref: $BASE" >&2; exit 64; }
fi

echo "A/B benchmark '$NAME'"
echo "  baseline : $BASE  -> otterstax_app:$TAG_BASE"
echo "  new      : working tree -> otterstax_app:$TAG_NEW"
echo "  runs     : $RUNS per side, interleaved; build jobs: $JOBS"
echo "  bench    : run_benchmark.sh ${BENCH_ARGS[*]}"
echo "  results  : $OUT"
$DRY && exit 0

# ---- images ----------------------------------------------------------------
image_exists() { docker image inspect "otterstax_app:$1" >/dev/null 2>&1; }

build() { # tag context
    if ! $REBUILD && image_exists "$1"; then
        echo "otterstax_app:$1 exists, reusing (--rebuild to force)"
        return
    fi
    echo "building otterstax_app:$1 from $2 (log: $OUT/build_$1.log)"
    docker build -t "otterstax_app:$1" -f "$2/Dockerfile.test" \
        --build-arg BUILD_JOBS="$JOBS" "$2" >"$OUT/build_$1.log" 2>&1 ||
        { tail -20 "$OUT/build_$1.log" >&2; exit 1; }
}

mkdir -p "$OUT/base" "$OUT/new"
BASE_SRC="$(mktemp -d)"
trap 'rm -rf "$BASE_SRC"' EXIT
if [[ "$BASE" == index ]]; then
    git -C "$REPO" checkout-index -a --prefix="$BASE_SRC/"
else
    git -C "$REPO" archive "$BASE" | tar -x -C "$BASE_SRC"
fi
# One build at a time: two parallel builds double the RAM the jobs cap assumes.
build "$TAG_BASE" "$BASE_SRC"
build "$TAG_NEW" "$REPO"

# ---- interleaved runs ------------------------------------------------------
for i in $(seq 1 "$RUNS"); do
    for side in base new; do
        tag="$TAG_BASE"; [[ $side == new ]] && tag="$TAG_NEW"
        log="$OUT/$side/run$i.log"
        echo "$(date +%H:%M:%S) $side run $i/$RUNS ..."
        IMAGE_TAG="$tag" "$REPO/benchmark/scripts/run_benchmark.sh" \
            --out-dir "$OUT/$side/run$i" "${BENCH_ARGS[@]}" >"$log" 2>&1 || true
        if ! grep -q 'Benchmark complete' "$log" || grep -qE 'invalid characters|Conflict\.' "$log"; then
            echo "run failed, stopping (log: $log):" >&2
            tail -8 "$log" >&2
            exit 1
        fi
        echo "  $(grep -m1 'Benchmark complete' "$log")"
    done
done

# ---- comparison ------------------------------------------------------------
python3 "$SKILL_DIR/compare_bench.py" "base=$OUT/base" "new=$OUT/new" | tee "$OUT/compare.md"
echo
echo "comparison: $OUT/compare.md"
echo "images kept: otterstax_app:$TAG_BASE otterstax_app:$TAG_NEW (~11 GB each; docker rmi when done)"
