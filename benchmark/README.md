# OtterStax Benchmark

This directory contains three independent benchmark systems:

| System | What it measures | Requires |
| --- | --- | --- |
| **Microbenchmarks** (`microbench/`) | C++ hot-path latency (translators, parser, schema) | Local build only |
| **End-to-end benchmarks** (`scripts/`, `benchmarks/`) | Serial query latency over wire protocols | Docker + DB containers |
| **Stress test** (`stress/`) | Concurrent load: throughput, latency percentiles, error rate across escalating stages | Docker + DB containers |

---

## Microbenchmarks (Google Benchmark)

No Docker needed — builds and runs locally in seconds.

```bash
# All benchmarks, 5 reps
./benchmark/microbench/run-bench.sh

# Only parser benchmarks, 10 reps
./benchmark/microbench/run-bench.sh --repetitions 10 --filter "BM_parse"

# Force cmake reconfigure (e.g. after conanfile.py change)
./benchmark/microbench/run-bench.sh --reconfigure
```

Results land in `benchmark_results/microbench/<YYYYMMDD_HHMMSS>/`:

- `bench_output.txt` — full console output
- `bench_results.json` — machine-readable results (Google Benchmark JSON format)

### What is measured

| Benchmark | Description | Typical mean |
| --- | --- | --- |
| `BM_ch_to_chunk_{100,1k,10k,100k}` | ClickHouse Block → `data_chunk_t` | 14 µs – 16 ms |
| `BM_ch_to_chunk_multiblock_10k` | Multi-block merge path | ~1.6 ms |
| `BM_ch_to_struct` | Schema extraction from ClickHouse Block | ~270 ns |
| `BM_chunk_to_arrow_schema_{10,50}col` | `data_chunk_t` schema → Arrow schema | 540 ns – 2.6 µs |
| `BM_mysql_type_mapping_all` | Full MySQL column-type enum mapping | ~33 ns |
| `BM_parse_{simple_select,join_3table,cross_backend,subquery}` | `GreenplumParser::parse()` | 2–5 µs |
| `BM_prepare_sql_{simple,cross_backend,subquery}` | `prepare_sql()` subquery extraction | 1.3–2.7 µs |

### CLI reference

```text
--repetitions N   Reps per benchmark (default: 5)
--filter REGEX    Run only benchmarks matching REGEX (passed to --benchmark_filter)
--reconfigure     Force cmake reconfigure even if already configured
--build-dir DIR   CMake build directory (default: build/Release)
--out-dir DIR     Results root (default: benchmark_results/microbench)
-j N              Parallel build jobs (default: nproc)
```

---

## End-to-end benchmarks (Docker)

End-to-end query latency benchmark for the OtterStax federated SQL server.
Tests run against six real database containers (2× MariaDB, 2× PostgreSQL, 2× ClickHouse)
and a RustFS bucket, via the MySQL wire, PostgreSQL wire and Spark Connect frontends —
every frontend runs the same tests.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- `otterstax_app:bench` image — build it once:

  ```bash
  ./benchmark/scripts/run_benchmark.sh --rebuild
  ```

  After that, images are reused automatically on every run.

---

## Automated run

Run everything end-to-end — builds images if needed, starts databases, initialises
data, runs all benchmarks, and writes results.

```bash
# Default: mysql + postgres + spark frontends, every test except kafka_*, 10 reps each
./benchmark/scripts/run_benchmark.sh

# Only the concurrent tests / only the single-session ones (default: --full, both)
./benchmark/scripts/run_benchmark.sh --concurrent --repetitions 5
./benchmark/scripts/run_benchmark.sh --single --repetitions 5

# Quick smoke test
./benchmark/scripts/run_benchmark.sh --repetitions 1 --bench simple_select

# Specific frontend and tests
./benchmark/scripts/run_benchmark.sh \
  --frontend mysql --bench join_all join_cross_engine --repetitions 3

# Full clean slate (wipes DB volumes; the saved benchmark data is kept)
./benchmark/scripts/run_benchmark.sh --clear

# Generate the saved benchmark data again
./benchmark/scripts/run_benchmark.sh --regen-data
```

The backend rows are generated once into `benchmark/data/dataset/` and every
run loads that same data, so results from different runs and different images
compare over identical tables. See "Saved benchmark data" in `benchmark/CLAUDE.md`.

Results land in `benchmark_results/<YYYYMMDD_HHMMSS>/`.

### Tracy profiling (automated)

```bash
./benchmark/scripts/run_benchmark.sh --tracy-sep --bench join_all --repetitions 1
```

Produces `benchmark_results/<run>/mysql_join_all.tracy` (one file per test).
Open with [Tracy](https://github.com/wolfpld/tracy) to inspect zone timings.

---

## Manual / interactive mode

Use this when you want to connect Tracy while queries run, or execute ad-hoc SQL.

### 1. Start services

```bash
./benchmark/manual/start_service.sh
```

This starts all six database containers, OtterStax, initialises benchmark data, and
registers the six connection aliases. All ports are published to the host:

| Purpose        | Address          |
|----------------|------------------|
| Tracy profiler | `localhost:8086` |
| MySQL wire     | `localhost:8816` |
| PostgreSQL     | `localhost:8817` |
| FlightSQL      | `localhost:8815` |

Connections are read from `benchmark/config.yaml` at server startup (there
is no HTTP management port).

On subsequent starts, skip data re-initialisation:

```bash
./benchmark/manual/start_service.sh --no-init
```

### 2. Connect Tracy (optional)

Open the Tracy GUI and connect to `localhost:8086`.
Any benchmark or query you run will stream profiling zones in real time.

### 3. Run benchmarks

```bash
# All default tests
./benchmark/manual/run_bench.sh

# Specific tests
./benchmark/manual/run_bench.sh --frontend mysql --bench join_all --repetitions 1
```

Results land in `benchmark_manual/<YYYYMMDD_HHMMSS>/` with the same `.txt` and `.json`
files as the automated run, plus a `summary.md`.

### 4. Run a custom query

```bash
echo "SELECT * FROM mysql1.benchdb1.campaigns LIMIT 10" > /tmp/q.sql
./benchmark/manual/run_query.sh --frontend mysql /tmp/q.sql
```

Output is printed as a table. `summary.md` and `<query_name>.json` are saved to
`benchmark_manual/<YYYYMMDD_HHMMSS>/`.

`summary.md` entry format:

```text
Generated  : 2026-06-01 14:32:11 UTC
Commit     : d8fdb70...
q          : OK
rows       : 10
elapsed_ms : 45.2
```

### 5. Stop services

```bash
./benchmark/manual/stop_service.sh          # stop, keep DB data
./benchmark/manual/stop_service.sh --clean  # stop + wipe all volumes
```

---

## Test categories

| Test                 | What it measures                              | Typical latency |
|----------------------|-----------------------------------------------|-----------------|
| `simple_select`      | `LIMIT 100` point-read per table              | 28–51 ms        |
| `complex_select`     | `GROUP BY` + `HAVING` + `ORDER BY LIMIT 1000` | 33–63 ms        |
| `join_same_instance` | JOIN within the same engine instance          | 36–74 ms        |
| `join_cross_engine`  | JOIN across two different engine instances    | 36 ms–1 s       |
| `join_all`           | 3-way JOIN: MySQL + PostgreSQL + ClickHouse   | 0.18–0.48 s     |

> **Note:** a cross-backend JOIN such as `join_all` reads each side's table whole and
> filters in-process — its `WHERE` is not pushed down to the backends (a statement over
> one backend runs whole on it, `WHERE` included).

---

## Per-backend suite (`benchmarks/backends/suite.py`)

The same four tests for each backend — ClickHouse (`ch_`), MySQL (`mysql_`),
PostgreSQL (`pg_`) — in the shapes the cross-backend tests leave out, plus
`join_concurrent`. Part of the default run; `suite.py <test> --frontend F`.

| Test | What it measures | Sub-tests | Typical latency |
|------|------------------|-----------|-----------------|
| `<b>_select_wide` | ~60 k-row results (decoding + row streaming) | `impressions_<alias>_all`, `daily_stats_<alias>_all` | 77–163 ms |
| `<b>_insert` | `INSERT … VALUES` into the backend's `bench_inserts`; row count = the affected rows the frontend reports | `insert_{100,1000}_rows` | 2–8 ms |
| `<b>_prepare` | the same statement plain vs prepared (the prepare sends the backend a describe probe) | `{agg,select}_<alias>_{plain,prepared}` | 23–118 ms |
| `<b>_concurrent` | N sessions at once (N = 1, 4, 8): wall time of the batch and per-session latency | `{agg,select}_<alias>_x{1,4,8}`, `…_x{4,8}_each` | 21–78 ms |
| `join_concurrent` | N sessions running the same cross-backend JOIN at once | `{mysql2_x_pg2,ch2_x_mysql2,pg1_ch1_mysql2}_x{1,4,8}`, `…_x{4,8}_each` | 30 ms–3.5 s |
| `join_wide` | cross-backend JOINs whose cost is fetching each side (a whole 60 k-row table from MySQL and from PostgreSQL, one-row aggregate) | `count_mysql1_imp_x_pg1_stats`, `sum_mysql1_imp_x_pg1_imp` | TBD |
| `join_wide_concurrent` | the same from N sessions at once (N = 1, 4, 8) | `…_x{1,4,8}`, `…_x{4,8}_each` | TBD |

---

## External-table tests (s3/file, `benchmarks/s3/`)

Six workloads that exercise the `CREATE EXTERNAL TABLE` / `COPY ... TO` grammar
extensions; they are part of the default run.
- Each runs both a local-file source (the `/fixtures` mount) and an s3 source
  (seeded RustFS), so file vs s3 are directly comparable in the sub-test
  breakdown.
- Each script takes `--frontend mysql|postgres|spark`.

| Test                  | Workload                                                                                | Sub-tests | Typical latency |
|-----------------------|-----------------------------------------------------------------------------------------|-----------|-----------------|
| `external_load`       | `CREATE EXTERNAL TABLE` — table dropped between reps to time a cold load                | `{file,s3}_{parquet,csv,ndjson}` | 103–148 ms |
| `external_join`       | `regions`(parquet) ⋈ `web_events`(csv) on `campaign_id` — internal otterbrix-on-otterbrix join | `{file,s3}_join` | 108–131 ms |
| `external_dump`       | `COPY (SELECT * FROM <loaded>) TO <target>` — writer + upload                           | `{file,s3}_{parquet,csv,ndjson}` | 50–77 ms |
| `external_join_cross` | external `regions` ⋈ otterbrix-internal `weights` (`CREATE TABLE` + `INSERT`)           | `{file,s3}_cross` | 37–38 ms |
| `external_join_all`   | s3 parquet `regions` ⋈ file csv `web_events` ⋈ internal `weights`, all on `campaign_id` | `s3parquet_filecsv_internal` | 76–81 ms |
| `external_load_concurrent` | N sessions run `CREATE EXTERNAL TABLE` at once (N = 1, 4, 8) from `web_events.csv`: wall time of the batch and per-session latency | `{file,s3}_csv_x{1,4,8}`, `…_x{4,8}_each` | 0.15–0.39 s |

`external_join_cross` / `external_join_all` deliberately load every side into
otterbrix-internal storage (external load + a hand-built `bigint` engine table).
A direct backend.int32 ⋈ s3.int64 JOIN silently returns zero rows
("JOIN-key width sensitivity" in `tests/CLAUDE.md`), so the benchmarks follow the same staged
shape the python tests use.

Selecting any `external_*` test automatically:

1. Generates fixtures into `benchmark/data/fixtures/` via `data/generate_external_fixtures.py`
   (once: later runs reuse them while `bench.yaml` is unchanged).
2. Adds `compose_rustfs.yml` to the stack (RustFS + a one-shot that seeds `bench-bucket`).

The `bench_rustfs` s3 alias is declared in `benchmark/config.yaml` and
registered at server startup (no runtime registration step).

```bash
# All six external tests, both frontends
./benchmark/scripts/run_benchmark.sh \
  --bench external_load external_join external_dump external_join_cross external_join_all \
          external_load_concurrent \
  --repetitions 5

# Just the s3-join shapes, mysql wire
./benchmark/scripts/run_benchmark.sh --frontend mysql \
  --bench external_join_cross external_join_all
```

Manual flow: start with `--external` so RustFS + fixtures + s3 alias are ready
before `run_bench.sh` selects the tests:

```bash
./benchmark/manual/start_service.sh --external
./benchmark/manual/run_bench.sh \
  --bench external_load external_join external_dump external_join_cross external_join_all
```

Fixture sizes come from the `external:` block in `bench.yaml` (defaults:
`regions.parquet` ≈ 4 k rows, `web_events.csv` ≈ 20 k rows, `campaigns.ndjson` =
`num_campaigns`). All `int` columns are int64 to avoid the JOIN-key width trap.

---

## Connection aliases

All SQL must qualify table names with the registered alias:

```sql
SELECT * FROM mysql1.benchdb1.campaigns LIMIT 5;
SELECT * FROM pg1.benchpg1.public.impressions LIMIT 5;
SELECT * FROM ch1.benchch1.daily_stats LIMIT 5;
```

| Alias  | Engine     | Database  | Group | Size        |
|--------|------------|-----------|-------|-------------|
| mysql1 | MariaDB    | benchdb1  | A     | ~60 k rows  |
| mysql2 | MariaDB    | benchdb2  | B     | ~5 k rows   |
| pg1    | PostgreSQL | benchpg1  | A     | ~60 k rows  |
| pg2    | PostgreSQL | benchpg2  | B     | ~5 k rows   |
| ch1    | ClickHouse | benchch1  | A     | ~60 k rows  |
| ch2    | ClickHouse | benchch2  | B     | ~5 k rows   |

---

## Stress test (Docker)

Runs N parallel connections per frontend across three escalating load stages against the
same live service instance, then writes a degradation report comparing QPS, latency
percentiles, and error rate.

```bash
# Default 3-stage run (1 → 5 → 15 workers per frontend, same query pool)
./benchmark/stress/run_stress_benchmarks.sh

# Selects only (no joins) — same 1/5/15 concurrency ramp
./benchmark/stress/run_stress_benchmarks.sh -p benchmark/stress/profiles/select_only.yaml

# Custom YAML profile
./benchmark/stress/run_stress_benchmarks.sh --profile my_profile.yaml

# With CPU profiling
./benchmark/stress/run_stress_benchmarks.sh --perf
```

Results land in `benchmark_results/stress/<YYYYMMDD_HHMMSS>/`. Each stage writes
`<frontend>_results.json` with overall stats and a `by_query_type` breakdown (count,
QPS, p50/p95/p99 per category). The `degradation_report.md` compares all stages side
by side — overall throughput and latency, plus a per-category sub-table for each query
type.

See [`stress/CLAUDE.md`](stress/CLAUDE.md) for full developer documentation.

---

For detailed developer documentation see [CLAUDE.md](CLAUDE.md).
