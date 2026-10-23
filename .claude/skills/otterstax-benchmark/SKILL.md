---
name: otterstax-benchmark
description: Use when asked to run the OtterStax end-to-end benchmarks, check a change for a performance regression, or compare latency between two builds (working tree vs staged/commit/branch). Also when run_benchmark.sh fails with a container-name "Conflict" or "includes invalid characters for a local volume name".
version: 1.0.0
---

# OtterStax benchmarks

`benchmark/scripts/run_benchmark.sh` runs the end-to-end suite in Docker: it starts
the backend DBs, runs OtterStax from an `otterstax_app:<IMAGE_TAG>` image, runs the
tests, and writes `<out-dir>/<frontend>/<test>.json`. One run on its own says nothing
about a regression. That needs a baseline image built from the other tree on the same
machine at the same time, with runs interleaved.

## Regression check: `ab_bench.sh`

```bash
S=.claude/skills/otterstax-benchmark
$S/ab_bench.sh --dry-run                       # preflight + plan only
$S/ab_bench.sh --name pool_fix                 # baseline = git index (staged)
$S/ab_bench.sh --base main --name vs_main      # baseline = a ref
$S/ab_bench.sh --name joins -- --frontend postgres --bench join_concurrent --repetitions 5
```

The script exports the baseline tree (`git checkout-index` / `git archive`, so the
working tree is never touched). It builds the two images one at a time, with jobs
capped to Docker's RAM, and runs base/new interleaved (`--runs 3` by default). It stops
at the first failed run. Last, it writes the median comparison to
`benchmark_results/ab_NAME/compare.md` (`compare_bench.py`: per sub-test medians, the
deltas, and a geometric mean per test). Run it with `run_in_background`. A postgres-only
full suite takes about 10 minutes per run, and each image build takes about 10 minutes.

## Single run

```bash
IMAGE_TAG=bench ./benchmark/scripts/run_benchmark.sh --frontend postgres --repetitions 5 \
    --out-dir "$PWD/benchmark_results/<name>"
```

`--help` lists the tests (`--bench`), suites and profiling options (`--tracy`, `--perf`).

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `container name "/bench_…" is already in use` | `bench_*` containers left by the manual stack (`benchmark/manual/*.sh`, compose project `bench`). `run_benchmark.sh` uses project `benchmark` with the same names. | Ask the user, then `docker compose -p bench -f benchmark/compose_backends.yml down` (volumes kept). `ab_bench.sh` checks for this first. |
| `includes invalid characters for a local volume name`; every test fails at once | `--out-dir` is relative, and the path is passed to `docker run -v` | Use an absolute path |
| Every run "fails" in seconds | The loop didn't check the log | Look for `Benchmark complete` in each run's log before you trust any numbers |
| `Killed` / `ResourceExhausted` during the build | Too many cmake jobs for Docker's RAM | `-j` = min(nCPU, dockerRAM_MB / 1536) |
| Numbers differ from an old run | Different day, image or DB state | Compare only runs from the same A/B session |
| Image reused with stale code | `run_benchmark.sh` skips the build when the tag exists | Use a fresh tag, or `--rebuild` |

## Reporting

- Report the per-test geometric means and every sub-test beyond ±10%. Rerun those
  before calling something a regression: concurrent tests are noisy.
- Repo docs get only "Typical latency" ranges. Run provenance and numbers belong in
  the chat or `.local_data/`.
- The images are about 11 GB each. Offer `docker rmi` when you're done.
