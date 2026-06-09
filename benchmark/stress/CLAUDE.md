# OtterStax Stress Test — Developer Guide

## What this directory is

A concurrency stress test that complements the serial latency harness in
`benchmark/scripts/run_benchmark.sh`.  All three stages run the **same query pool**;
only the number of parallel workers changes (1 → 5 → 15 per frontend).  Stage 1 is a
sequential baseline (1 worker) so the degradation ratios (S2/S1, S3/S1) reflect pure
concurrency cost, not query-mix differences.  The service is kept alive between stages —
all stages hit the same running instance.

---

## Directory layout

```text
stress/
├── CLAUDE.md                  # this file
├── run_stress_benchmarks.sh   # shell entry point (Docker orchestration + profiling)
├── stress_main.py             # Python CLI; orchestrates all stages
├── profiles.py                # StageConfig dataclass, YAML loading, built-in defaults
├── stress_runner.py           # threaded worker engine (persistent connections)
├── report.py                  # aggregation, JSON output, degradation markdown report
├── __init__.py                # package marker
└── profiles/
    ├── default.yaml           # 1/5/15 workers, all 4 query types (built-in defaults)
    └── select_only.yaml       # 1/5/15 workers, simple + complex selects only (no joins)
```

---

## Quick start

```bash
# Default 3-stage run
./benchmark/stress/run_stress_benchmarks.sh

# Selects only (no joins) — same 1/5/15 concurrency ramp
./benchmark/stress/run_stress_benchmarks.sh -p benchmark/stress/profiles/select_only.yaml

# Custom profile
./benchmark/stress/run_stress_benchmarks.sh --profile /path/to/my_profile.yaml

# With perf profiling
./benchmark/stress/run_stress_benchmarks.sh --perf

# With Tracy instrumentation
./benchmark/stress/run_stress_benchmarks.sh --tracy

# Reduced scale without rebuilding images
./benchmark/stress/run_stress_benchmarks.sh \
  --workers-small 2 --workers-medium 5 --workers-heavy 10 \
  --duration-small 15 --duration-medium 30 --duration-heavy 45
```

Results land in `benchmark_results/stress/<YYYYMMDD_HHMMSS>/`.

---

## CLI reference (`run_stress_benchmarks.sh`)

```text
-p, --profile FILE   YAML profile defining all stages. Overrides --workers-* and
                     --duration-* flags.
--workers-small N    Workers per frontend, stage 1 (default: 3)  [ignored with -p]
--workers-medium N   Workers per frontend, stage 2 (default: 15) [ignored with -p]
--workers-heavy N    Workers per frontend, stage 3 (default: 30) [ignored with -p]
--duration-small N   Stage 1 active duration in seconds (default: 30)  [ignored with -p]
--duration-medium N  Stage 2 active duration in seconds (default: 60)  [ignored with -p]
--duration-heavy N   Stage 3 active duration in seconds (default: 90)  [ignored with -p]
--out-dir DIR        Result root (default: benchmark_results/stress/<ts>)
--no-init            Skip data initialisation (reuse existing DB volumes)
--rebuild            Force rebuild of both Docker images
--clear              Wipe images + DB volumes, then rebuild
-j N                 Parallel build jobs (default: auto-capped)
--tracy              Continuous Tracy capture → <out-dir>/benchmark.tracy
--perf               CPU call-graph perf → benchmark.perf.data + benchmark.perf
--perf-alloc         Like --perf + malloc uprobe for allocation hotspots
IMAGE_TAG=<tag>      OtterStax image tag (env var; default: bench)
```

---

## YAML profile format

```yaml
cooling_s: 10     # seconds between stages (default: 10)

stages:
  - name: baseline        # used as output subdirectory name (required)
    label: "Stage 1"      # human-readable banner (optional; defaults to name)
    workers_per_frontend: 1
    duration_s: 60
    ramp_secs: 2          # optional; default: max(2, duration_s / 6)
    query_pool:
      simple_select:     1   # relative weight (positive integer)
      complex_select:    1
      join_same_b:       1
      join_cross_engine: 1

  - name: heavy
    label: "Stage 2"
    workers_per_frontend: 15
    duration_s: 90
    ramp_secs: 15
    query_pool:           # same keys, same weights as stage 1
      simple_select:     1
      complex_select:    1
      join_same_b:       1
      join_cross_engine: 1
```

The query pool **must be identical across all stages** so that the degradation ratios
(S2/S1, S3/S1) reflect concurrency cost only.  Each worker shuffles its own copy of the
pool independently so concurrent workers hit different queries.

### Valid `query_pool` keys

| Key | Description | Typical latency |
| --- | ----------- | --------------- |
| `simple_select` | `LIMIT 100` point reads across all 6 backends | 2–50 ms |
| `complex_select` | `GROUP BY + HAVING + ORDER BY LIMIT 1000` | 5–55 ms |
| `join_same_b` | Same-instance join, Group B only (~5 k rows) | 1–3 s |
| `join_same_instance` | Same-instance join, both groups (up to 60 k rows) | 3–9 s |
| `join_cross_engine` | Cross-engine join, Group B tables | 3–5 s |

> **`join_all` is not available as a pool key.** A single `join_all` query takes 47–90 s
> (full-table scan, no predicate pushdown) and would make per-stage statistics
> uninterpretable.

---

## Output

```text
benchmark_results/stress/<YYYYMMDD_HHMMSS>/
├── degradation_report.md     # main output: throughput, latency, errors + per-query-type breakdown
├── benchmark.tracy           # (--tracy) full capture of all stages
├── benchmark.perf.data       # (--perf) raw perf data
├── benchmark.perf            # (--perf) speedscope-compatible text
├── baseline/
│   ├── mysql_results.json    # aggregated stats + by_query_type + per-worker summary
│   └── postgres_results.json
├── medium/ ...
└── heavy/  ...
```

Each `*_results.json` includes a `by_query_type` field:

```json
{
  "by_query_type": {
    "simple_select":   { "count": 312, "qps": 5.2, "p50_ms": 18.4, "p95_ms": 42.1, "p99_ms": 61.3 },
    "complex_select":  { "count": 305, "qps": 5.1, "p50_ms": 25.7, "p95_ms": 58.0, "p99_ms": 79.2 },
    "join_same_b":     { "count": 41,  "qps": 0.7, "p50_ms": 1340, "p95_ms": 2100, "p99_ms": 2800 },
    "join_cross_engine": { ... }
  }
}
```

The `degradation_report.md` contains four sections:

1. **Throughput** — QPS per frontend + total, S2/S1 and S3/S1 ratio columns
2. **Latency (combined)** — p50/p95/p99/max across all frontends, ratio columns
3. **Errors** — query count, error count, error rate, connection errors
4. **Per-query-type Breakdown** — one sub-table per category (count, QPS, p50/p95/p99 + latency ratios)

A `⚠` marks p50/p95/p99 values that exceed 3× the baseline in the latency ratio columns.

---

## Python module internals

### `profiles.py`

- `StageConfig` dataclass: name, label, workers_per_frontend, duration_s, ramp_secs,
  query_weights, query_pool, frontends.
- `load_profile(path) → (list[StageConfig], cooling_s)` — parses and validates a YAML
  file.  Raises `ValueError` with a descriptive message on any schema violation.
- `build_stages(...) → (list[StageConfig], cooling_s)` — returns the three built-in
  defaults; used when no `--profile` flag is given.
- `_build_pool(weights)` — repeats each query category N times proportional to its
  weight, then shuffles.  SQL strings are imported from
  `benchmark/benchmarks/queries.py` (no duplication).

### `stress_runner.py`

- Each worker thread: opens **one persistent connection**, cycles through its shuffled
  copy of the pool round-robin, records per-query latency in ms, reconnects on error
  (up to 3 retries with 0.5 s back-off), exits when `stop_event` fires.
- Workers are staggered by `ramp_secs / max(total_workers-1, 1)` seconds each to avoid
  a thundering-herd at start.
- `run_stage(stage_config, host) → {frontend: [WorkerStats]}` — starts all threads,
  waits `duration_s + ramp_secs`, sets `stop_event`, joins all threads.

### `report.py`

- `build_stage_result(stage_config, raw_stats) → StageResult` — QPS = total_queries /
  duration_s (comparable across stages regardless of ramp timing).  Aggregates
  per-category latencies from `WorkerStats.latencies_by_category` into
  `FrontendStats.per_query_type` (per frontend) and `StageResult.per_query_type`
  (combined across all frontends).
- `write_stage_json(result, raw_stats, out_dir)` — per-frontend JSON with aggregated
  stats, per-worker summaries, and a `by_query_type` dict containing `{count, qps,
  p50_ms, p95_ms, p99_ms}` for each query category executed in that stage.
- `write_degradation_report(stages, out_dir)` — markdown with:
  - Overall throughput table (QPS per frontend + total, ratio columns)
  - Combined latency table (p50/p95/p99/max, ratio columns)
  - Error counts table
  - **"Per-query-type Breakdown"** section: one sub-table per category showing count,
    QPS, p50/p95/p99 across all stages with S2/S1 and S3/S1 ratio columns.

### `stress_main.py`

CLI entry point.  Parses args → loads profile or builds defaults → loops over stages
(run → aggregate → write JSON → print summary → cool down) → writes degradation report.

---

## Adding a custom profile

1. Copy `benchmark/stress/profiles/default.yaml` as a starting point.
2. Adjust `workers_per_frontend`, `duration_s`, and `query_pool` weights for each stage.
3. Run:

   ```bash
   ./benchmark/stress/run_stress_benchmarks.sh --no-init -p benchmark/stress/profiles/my_profile.yaml
   ```

   (`--no-init` reuses existing DB data so you skip the ~2 min initialisation step.)

## Adding a new query category

1. Add the SQL list to `benchmark/benchmarks/queries.py` (so it stays shared with the
   serial benchmark harness).
2. Register the new key in `VALID_POOL_KEYS` in `stress/profiles.py`.
3. Document it in the `query_pool` table above and in `STRESS_BENCHMARK_PLAN.md`.
