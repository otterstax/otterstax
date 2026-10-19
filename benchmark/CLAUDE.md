# OtterStax Benchmark — Developer Guide

## What this directory is

Three independent benchmark systems:

- **`microbench/`** — Google Benchmark C++ microbenchmarks for translation, parsing, and schema hot paths. No Docker required; runs locally against the build tree.
- **`scripts/run_benchmark.sh` + `benchmarks/`** — End-to-end query latency harness. Measures query latency through the MySQL wire, PostgreSQL wire and Spark Connect frontends: cross-backend queries, the per-backend suite (`benchmarks/backends/suite.py`) and S3/file external tables (`benchmarks/s3/`).
- **`stress/run_stress_benchmarks.sh` + `stress/`** — Concurrent load stress test. Runs many parallel connections across three escalating stages and produces a degradation report (throughput, latency percentiles, error rate).

---

## Microbenchmarks (Google Benchmark)

### Quick start

```bash
# All benchmarks, 5 reps — reconfigures cmake if needed, builds, runs, saves results
./benchmark/microbench/run-bench.sh

# Filter to a subset
./benchmark/microbench/run-bench.sh --filter "BM_parse"

# Force cmake reconfigure (e.g. after conanfile.py change)
./benchmark/microbench/run-bench.sh --reconfigure
```

Results land in `benchmark_results/microbench/<YYYYMMDD_HHMMSS>/`:

- `bench_output.txt` — full console output with human-readable table
- `bench_results.json` — Google Benchmark JSON (repaired automatically if truncated by exit crash)

### CLI reference (`run-bench.sh`)

```text
--repetitions N   Reps per benchmark (default: 5)
--filter REGEX    Run only matching benchmarks (--benchmark_filter)
--reconfigure     Force cmake reconfigure even if BUILD_BENCHMARKS=ON is cached
--build-dir DIR   CMake build directory (default: build/Release)
--out-dir DIR     Results root (default: benchmark_results/microbench)
-j N              Parallel build jobs (default: nproc)
```

### How to build manually (without the script)

```bash
cmake -S . -B build/Release -DBUILD_BENCHMARKS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake --build build/Release --parallel 5 --target otterstax_bench

# LD_LIBRARY_PATH must include all conan lib dirs (handled automatically by run-bench.sh)
LD_LIBRARY_PATH=$(find /conan/.conan2/p/b/*/p/lib -maxdepth 0 | tr '\n' ':') \
  ./build/Release/benchmark/microbench/otterstax_bench --benchmark_filter="BM_ch"
```

### Known issue: exit crash (SIGSEGV, exit 139)

The PostgreSQL raw_parser (`raw_parser()` from the Greenplum SQL parser) has global
state that is cleaned up during process exit in a way that races with spdlog's file-sink
destructor. All benchmarks complete before the crash fires. `run-bench.sh` tolerates
exit code 139 and repairs the truncated JSON automatically.

### Adding a new microbenchmark

1. Add `BM_*` functions to an existing file in `benchmark/microbench/` or create a new `.cpp` file.
2. Register new `.cpp` files in `benchmark/microbench/CMakeLists.txt` under `add_executable(otterstax_bench ...)`.
3. If the benchmark uses `GreenplumParser`, ensure `logger_init` (the static `LoggerInit` in `bench_parser.cpp`) is already linked in — the parser requires named spdlog loggers to be registered before construction.

---

## End-to-end benchmarks (Docker)

An end-to-end benchmark harness for OtterStax. It measures query latency through
three client frontends (MySQL wire, PostgreSQL wire, Spark Connect), each running
the same tests. A default run covers three groups of tests:
- the cross-backend tests, `join_concurrent` included;
- the per-backend suite — the same four tests for ClickHouse, MySQL and
  PostgreSQL (`<backend>_select_wide` / `_insert` / `_prepare` / `_concurrent`,
  `benchmarks/backends/suite.py`);
- the S3/file suite (`external_*`, `benchmarks/s3/`).

The Kafka tests are opt-in (MySQL and PostgreSQL frontends only). `--suite`
(`--full` / `--concurrent` / `--single`) runs every selected test, only the
`*_concurrent` ones, or every other one.

## End-to-end quick start

```bash
# Normal run — images are reused automatically if they already exist
./benchmark/scripts/run_benchmark.sh --repetitions 5

# Full clean slate: wipe images + DB volumes, build and run from scratch
./benchmark/scripts/run_benchmark.sh --clear

# Rebuild images only (keeps existing DB volumes / data)
./benchmark/scripts/run_benchmark.sh --rebuild

# Single frontend
./benchmark/scripts/run_benchmark.sh --frontend postgres --repetitions 3

# Only the concurrent tests (or only the single-session ones)
./benchmark/scripts/run_benchmark.sh --concurrent --repetitions 5
./benchmark/scripts/run_benchmark.sh --single --repetitions 5

# Only specific tests
./benchmark/scripts/run_benchmark.sh --bench join_all join_cross_engine

# Regenerate summaries from existing JSON without re-running benchmarks
docker run --rm \
  -v "$PWD/benchmark_results/<run>:/results" \
  -v "$PWD/benchmark/benchmarks:/app/benchmarks" \
  -v "$PWD/benchmark/scripts:/app/scripts" \
  -e PYTHONUNBUFFERED=1 \
  benchmark-client:latest \
  python /app/scripts/generate_summary.py /results
```

Results land in `benchmark_results/<YYYYMMDD_HHMMSS>/`.

## CLI reference

```text
--repetitions N     Reps per sub-test (default: 10)
--frontend F        mysql | postgres | spark  (repeatable; default: all three)
--suite S           full (default) | concurrent | single — every selected
                    test, only the *_concurrent ones, or every other one
--full | --concurrent | --single
                    Shorthands for --suite full / concurrent / single
--bench T [T ...]   Test(s) to run (space-separated; default: every test
                    except kafka_*).
                    Values: simple_select complex_select join_same_instance
                            join_cross_engine join_all join_concurrent
                            <b>_select_wide <b>_insert <b>_prepare
                            <b>_concurrent   (<b> = ch | mysql | pg)
                            external_load external_join external_dump
                            external_join_cross external_join_all
                            external_load_concurrent
                            kafka_ingest kafka_produce kafka_stream
                    Selecting any external_* (s3/file) test auto-starts
                    RustFS, generates fixtures and uses the bench_rustfs
                    s3 alias.
                    kafka_ingest is opt-in too: it auto-starts a redpanda broker,
                    generates a JSON dataset, and seeds it into a topic. No REST
                    registration (the broker is named in CREATE SOURCE directly).
--out-dir DIR       Result root (default: benchmark_results/<timestamp>)
--rebuild           Force image rebuild even if images exist
--clear             Remove images + DB volumes, then rebuild from scratch
-j N                Parallel build jobs for otterstax_app
--tracy             Continuous Tracy capture → <out-dir>/benchmark.tracy
--tracy-sep         Per-test Tracy capture → <out-dir>/<frontend>_<test>.tracy
--perf              CPU call-graph sampling (perf, 99 Hz, dwarf unwind).
                    Outputs benchmark.perf.data + benchmark.perf (speedscope).
--perf-alloc        Like --perf but also attaches a malloc uprobe so allocation
                    call-sites appear in the call graph. Implies --perf.
IMAGE_TAG=<tag>     OtterStax image tag (default: bench)
GIT_COMMIT=<sha>    Override commit hash in summaries
```

## Manual workflow (Tracy + interactive queries)

The `benchmark/manual/` scripts let you start services once, connect Tracy,
then run benchmarks or arbitrary queries interactively.

### Start everything

```bash
./benchmark/manual/start_service.sh
# With --no-init to skip data re-initialisation (reuse existing data):
./benchmark/manual/start_service.sh --no-init
# Force rebuild of both Docker images (use after C++ source changes):
./benchmark/manual/start_service.sh --rebuild
./benchmark/manual/start_service.sh --rebuild --no-init
# With Tracy instrumentation compiled in (builds image automatically if absent):
./benchmark/manual/start_service.sh --tracy
./benchmark/manual/start_service.sh --tracy --no-init
# With CPU call-graph profiling (perf, 99 Hz, dwarf unwind):
./benchmark/manual/start_service.sh --perf
# With perf + malloc uprobe (allocation hotspot profiling):
./benchmark/manual/start_service.sh --perf-alloc
```

After the script exits, all ports are published to the host:

| Purpose          | Host address       |
|------------------|--------------------|
| Tracy profiler   | `localhost:8086`   |
| MySQL wire       | `localhost:8816`   |
| PostgreSQL wire  | `localhost:8817`   |
| FlightSQL/Arrow  | `localhost:8815`   |

Connections are read from `benchmark/config.yaml` at server startup (mounted
into `bench_otterstax` by `compose_benchmark.yml`) — there is no HTTP conn API.

Open Tracy and connect to `localhost:8086` at any time while the service is running.

### Run benchmarks

```bash
# All default tests, mysql + postgres
./benchmark/manual/run_bench.sh

# Specific tests and frontend
./benchmark/manual/run_bench.sh --frontend mysql --bench join_all --bench join_cross_engine --repetitions 3
```

Results land in `benchmark_manual/<YYYYMMDD_HHMMSS>/`.

### Run a custom query

```bash
echo "SELECT * FROM mysql1.benchdb1.campaigns LIMIT 10" > /tmp/my.sql
./benchmark/manual/run_query.sh --frontend mysql /tmp/my.sql
```

Results are printed as a table and saved to `benchmark_manual/<ts>/summary.md` and `<name>.json`.

### Stop everything

```bash
./benchmark/manual/stop_service.sh           # stops, keeps DB data
./benchmark/manual/stop_service.sh --clean   # stops + wipes all volumes
```

If a `--perf` or `--perf-alloc` session was active, `stop_service.sh` automatically:

1. Sends INT to perf (lets it write the data footer cleanly)
2. Copies `/tmp/benchmark.perf.data` from the container to `benchmark_manual/<ts>/`
3. Converts it to a speedscope-compatible `benchmark.perf` text file
4. Prints the output directory path

### Manual result format

```text
benchmark_manual/<YYYYMMDD_HHMMSS>/
├── summary.md              # run_bench.sh: same format as automated benchmark summary
├── db_info.md              # schema reference (written by generate_summary.py)
└── <frontend>/
    ├── summary.md          # per-frontend stats table (Generated, Commit, test rows)
    ├── <test>.txt          # per-test timing + SQL  (same as automated run)
    └── <test>.json         # machine-readable stats (same as automated run)
```

For **`run_query.sh`** (custom SQL), results land in a separate timestamped dir:

```text
benchmark_manual/<YYYYMMDD_HHMMSS>/
├── summary.md          # one entry per query invocation:
│                       #   Generated  : 2026-06-01 14:32:11 UTC
│                       #   Commit     : d8fdb70...
│                       #   my_query   : OK | ERROR
│                       #   rows       : 500
│                       #   elapsed_ms : 88445.0
└── <query_name>.json   # machine-readable record with the same fields
```

## Directory layout

```text
benchmark/
├── bench.yaml                   # Row-count and repetition config
├── CLAUDE.md                    # This file
├── compose_backends.yml         # 6 DB containers (bench_net)
├── compose_benchmark.yml        # OtterStax container; publishes :8086 to host
├── compose_manual.yml           # Overlay: also publishes 8815/8816/8817 to host
├── compose_rustfs.yml           # Overlay: RustFS + seed job for external_* tests
├── compose_kafka.yml            # Overlay: redpanda broker for kafka_ingest (seed is a run_benchmark.sh step, not a service)
├── Dockerfile.benchmark         # Python image: data init + benchmark scripts (+ confluent-kafka)
├── data/
│   ├── init_data.py             # Creates tables and inserts Faker data in all 6 DBs
│   ├── generate_external_fixtures.py  # Writes s3/file fixtures from bench.yaml `external:`
│   ├── generate_kafka_fixtures.py     # Writes kafka_events.ndjson from bench.yaml `kafka:`
│   ├── seed_kafka.py            # Creates the topic + produces the ndjson (run once at bring-up)
│   ├── fixtures/                # Generated regions.parquet/web_events.csv/campaigns.ndjson/kafka_events.ndjson
│   └── requirements.txt         # Python deps for the benchmark image
├── benchmarks/
│   ├── common.py                # Timing, stats, result serialisation, write_db_info;
│   │                            #   load_frontend / frontend_from_argv / suite_main /
│   │                            #   concurrent_batch for the connector suites
│   ├── queries.py               # All SQL strings (shared across all frontends and suites)
│   ├── external_common.py       # External-table runners (load/join/dump/…/load_concurrent) + external_main
│   ├── kafka_common.py          # Kafka runners (kafka_ingest/produce/stream) + sql_* builders + kafka_main
│   ├── mysql/connector.py       # mysql-connector-python, port 8816 (+ connect(), prepare hooks)
│   ├── postgres/connector.py    # psycopg2, port 8817 (+ connect()); psycopg 3 for the prepare hooks
│   ├── spark/connector.py       # pyspark-client 4.2 (Spark Connect), port 15002; a DB-API-shaped
│   │                            #   shim over spark.sql(), results fetched as Arrow
│   ├── {mysql,postgres,spark}/  # Client frontends: one .py file per cross-backend test
│   │                            #   (kafka_*: mysql and postgres only)
│   │   ├── simple_select.py
│   │   ├── complex_select.py
│   │   ├── join_same_instance.py
│   │   ├── join_cross_engine.py
│   │   ├── join_all.py
│   │   └── kafka_{ingest,produce,stream}.py  # thin, call kafka_main
│   ├── backends/suite.py        # Per-backend suite + join_concurrent: suite.py <test> --frontend F
│   └── s3/                      # S3/file suite; each script takes --frontend mysql|postgres|spark
│       └── external_{load,join,dump,join_cross,join_all,load_concurrent}.py  # thin, call external_main
├── manual/
│   ├── _common.sh               # Shared helpers sourced by all manual scripts
│   ├── start_service.sh         # Start DBs + OtterStax, register connections
│   ├── stop_service.sh          # Stop services (--clean wipes volumes)
│   ├── run_bench.sh             # Run benchmarks against running service
│   ├── run_query.sh             # Execute a SQL file, print tabular result
│   └── execute_query.py         # Python backend for run_query.sh
└── scripts/
    ├── run_benchmark.sh         # Host-side orchestration (full end-to-end)
    └── generate_summary.py      # Post-run: regenerate summaries from JSON
```

## Data layout (`bench.yaml`)

Two independent database groups sharing `campaign_id` 1–1000:

| Group | Alias              | Size              | Tables                              |
|-------|--------------------|-------------------|-------------------------------------|
| A     | mysql1 / pg1 / ch1 | big (~60 k rows)  | campaigns, impressions, daily_stats |
| B     | mysql2 / pg2 / ch2 | small (~5 k rows) | products, orders, events            |

Group A is used by: simple_select, complex_select, join_same_instance (camp×imp), join_all.
Group B is used by: simple_select, complex_select, join_same_instance (prod×ord), join_cross_engine.

The `external:` block sizes the s3/file fixtures (see "External-table tests"
below); it shares the same `campaign_id` space as Group A but its data lives in
generated files, not the DB backends.

Changing `bench.yaml` requires rebuilding the client image (`--rebuild` or `--clear`)
because `bench.yaml` is baked into the image at build time. (External fixtures
are the exception — they are bind-mounted, so editing `external:` only requires
re-running an `external_*` benchmark, which regenerates them.)

## Test categories

| Test                | Description                                           | Typical latency |
|---------------------|-------------------------------------------------------|-----------------|
| simple_select       | LIMIT 100 point-read per table                        | 28–51 ms        |
| complex_select      | GROUP BY + HAVING + ORDER BY LIMIT 1000               | 33–63 ms        |
| join_same_instance  | JOIN two tables in the same engine instance           | 36–74 ms        |
| join_cross_engine   | JOIN Group B tables across different engine instances | 36 ms–1 s       |
| join_all            | 3-engine JOIN (mysql1 × pg1/ch1 × ch2/mysql2)         | 0.18–0.48 s     |

`join_all` reads all ~60 k Group A rows regardless of the
`WHERE campaign_id <= 50` filter, because there is no predicate pushdown to
backends.

## Per-backend suite (`benchmarks/backends/suite.py`)

The cross-backend tests reach every backend with small results, one query at a
time. This suite runs the same four tests on each backend — ClickHouse (`ch_`),
MySQL (`mysql_`) and PostgreSQL (`pg_`) — in the shapes those tests leave out,
through every frontend. It is part of the default run.

| Test | Workload | Sub-tests | Typical latency |
|------|----------|-----------|-----------------|
| `<b>_select_wide` | ~60 k-row results from the backend's group-A `impressions` / `daily_stats` (decoding + row streaming dominate) | `impressions_<alias>_all`, `daily_stats_<alias>_all` | 78–161 ms |
| `<b>_insert` | `INSERT … VALUES` into the backend's group-B `bench_inserts`; the run's row count is the affected-row count the frontend reports (Spark Connect reports none: -1) | `insert_{100,1000}_rows` | 3–9 ms |
| `<b>_prepare` | the same statement plain and prepared, a fresh connection per rep (untimed); the difference is the prepare path (the describe probe OtterStax sends the backend). Prepared is `COM_STMT_PREPARE` (mysql), a named Parse through psycopg 3 (postgres — psycopg2 has no protocol-level prepare), `df.schema` before the collect (spark, AnalyzePlan) | `{agg,select}_<alias>_{plain,prepared}` | 23–119 ms |
| `<b>_concurrent` | N sessions send the same statement at once (N = 1, 4, 8) | `{agg,select}_<alias>_x{1,4,8}` (wall time of the batch), `…_x{4,8}_each` (per-session latency) | 23–198 ms |
| `join_concurrent` | N sessions run the same cross-backend JOIN at once: MySQL × PG, ClickHouse × MySQL, PG × ClickHouse × MySQL | `{mysql2_x_pg2,ch2_x_mysql2,pg1_ch1_mysql2}_x{1,4,8}`, `…_x{4,8}_each` | 32 ms–8.1 s |

- **Where the SQL lives:** `queries.py` — `BACKEND_SUITES` (per backend: the wide
  selects, the INSERT target, an aggregate over ~60 k rows and a 100-row select)
  and `JOIN_CONCURRENT`. The ClickHouse sub-test names are the ones `ch_*` always
  had, so older runs compare directly.
- **The `bench_inserts` tables** (`ch2.benchch2`, `mysql2.benchdb2`,
  `pg2.benchpg2.public`) are created empty by `data/init_data.py` before OtterStax
  starts, so startup discovery registers them. They have no key: the benchmark
  reuses ids across insert sizes and frontends.
- **Picking the frontend:** `suite.py <test> --frontend F` loads the frontend's
  `connector.py` (`common.suite_main`); `run_benchmark.sh` passes both per
  frontend (`_test_script`). A connector provides `connect`,
  `make_fetch_factory` and, for `_prepare`, `prepare_connect` / `statement_cursor`.
- **Why concurrency matters:** a `_concurrent` test's wall time shows how far
  sessions run side by side.

## External-table tests (s3/file, `benchmarks/s3/`)

These workloads exercise the `CREATE EXTERNAL TABLE` / `COPY ... TO` grammar
extensions and are part of the default run.
- **Scope:** loading, internal joins, dumps and concurrent loads only. No
  cross-backend or JOIN ALL shapes.
- **Two sources each:** every workload runs once against a local file (the
  `/fixtures` mount) and once against s3 (seeded RustFS). File vs s3 are
  directly comparable in the sub-test breakdown.
- **How they run:** each script in `benchmarks/s3/` takes
  `--frontend mysql|postgres` and calls `external_common.external_main`.

| Test            | Workload                                                        | Sub-tests | Typical latency |
|-----------------|-----------------------------------------------------------------|-----------|-----------------|
| `external_load` | `CREATE EXTERNAL TABLE` — load a fixture (table DROPped between reps for a cold load) | `{file,s3}_{parquet,csv,ndjson}` | 103–148 ms |
| `external_join` | `regions`(parquet) ⋈ `web_events`(csv) on `campaign_id` — **internal** otterbrix-on-otterbrix join (both sides loaded in untimed setup) | `{file,s3}_join` | 108–131 ms |
| `external_dump` | `COPY (SELECT * FROM <loaded>) TO <target>` — writer + upload (source loaded once, untimed) | `{file,s3}_{parquet,csv,ndjson}` | 50–77 ms |
| `external_join_cross` | external `regions` (s3/file) ⋈ otterbrix-internal `weights` (`CREATE TABLE`+`INSERT`) — the supported s3-join shape from `tests/test_mysql_join_otb_local_s3.py` | `{file,s3}_cross` | 37–38 ms |
| `external_join_all` | s3 parquet `regions` ⋈ file csv `web_events` ⋈ internal `weights`, all on `campaign_id` — three origins joined at once | `s3parquet_filecsv_internal` | 76–81 ms |
| `external_load_concurrent` | N sessions run `CREATE EXTERNAL TABLE` at once (N = 1, 4, 8), each into its own table, from `web_events.csv` (the largest fixture). Every s3/file load goes through the `S3Manager` / `FileManager` actors under their mutexes, so the wall time shows whether loads run side by side | `{file,s3}_csv_x{1,4,8}` (wall time of the batch), `…_x{4,8}_each` (per-session latency) | 0.15–1.17 s |

`external_join_cross` / `external_join_all` deliberately load every side into
otterbrix-internal storage first (external load + a hand-built `bigint` engine
table). A direct backend.campaign_id (int32) ⋈ s3.campaign_id (int64) silently
returns zero rows (the JOIN-key width trap: `tests/CLAUDE.md`, "JOIN-key width sensitivity"),
so the benchmarks follow the staged shape the tests use. `weights` covers
campaign_id 1..min(200, max) — bounded join cardinality regardless of fixture
scale.

Selecting any `external_*` test makes `run_benchmark.sh`:

1. Generate fixtures into `benchmark/data/fixtures/` via `data/generate_external_fixtures.py`
   (a container run of `benchmark-client`, before the stack comes up).
2. Add `compose_rustfs.yml` to the backend + otterstax compose sets (RustFS +
   a one-shot that seeds `bench-bucket` from the same fixtures).

The `bench_rustfs` s3 alias itself is declared in `benchmark/config.yaml`
(the `s3:` section) and registered at server startup like the six DB backends —
no runtime registration step.

The `file` and `s3` sources read the **identical** fixtures: bench-otterstax
mounts `data/fixtures` at `/fixtures`, and RustFS is seeded from the same dir.

```bash
# All six external tests, both frontends
./benchmark/scripts/run_benchmark.sh \
  --bench external_load external_join external_dump external_join_cross external_join_all \
          external_load_concurrent \
  --repetitions 5

# Just the s3-join shapes, mysql wire
./benchmark/scripts/run_benchmark.sh --frontend mysql --bench external_join_cross external_join_all
```

Manual flow: start with `--external` so RustFS + fixtures + s3 alias are ready,
then select the tests:

```bash
./benchmark/manual/start_service.sh --external
./benchmark/manual/run_bench.sh \
  --bench external_load external_join external_dump external_join_cross external_join_all
```

### External fixtures (`data/generate_external_fixtures.py`)

Benchmark-scale sibling of `tests/s3/fixtures/generate_external_fixtures.py`,
sized from the `external:` block in `bench.yaml`. `num_campaigns` defaults to
`group_a`, so the fixtures share the backend `campaign_id` space (they could
JOIN the backends, but the benchmarks deliberately keep joins internal).

| File              | Format  | Rows                                   | Columns (all ints are int64) |
|-------------------|---------|----------------------------------------|------------------------------|
| `regions.parquet` | parquet | `num_campaigns × regions_per_campaign` | region_id, campaign_id, region_name, country, population, ad_spend |
| `web_events.csv`  | csv     | `num_campaigns × events_per_campaign`  | event_id, campaign_id, product_id, event_type, session_seconds, value |
| `campaigns.ndjson`| ndjson  | `num_campaigns`                        | campaign_id, campaign_name, budget, status |

`regions ⋈ web_events` keys on int64 `campaign_id` on both sides — no width
mismatch (the silent zero-row JOIN trap when int32 meets int64). Defaults:
`regions_per_campaign=4`, `events_per_campaign=20` → ~4 k / ~20 k rows and an
~80 k-row internal join. Editing `bench.yaml` requires regenerating fixtures
(automatic on the next external run; the `benchmark-client` image is unaffected
because fixtures are bind-mounted, not baked in).

## Kafka benchmarks (`kafka_ingest` / `kafka_produce` / `kafka_stream`)

Three opt-in tests exercising the Kafka **runtime** (`integration/kafka/` — the
`KafkaManager` actor + librdkafka consumer/poller/producer). Split by feature the
way `external_load`/`external_join`/`external_dump` are (one test = one feature =
one result file). All share the redpanda broker + the pre-seeded topic, brought
up only when a `kafka_*` test is selected, the way RustFS comes up for the
`external_*` tests (redpanda is the analogue of RustFS; the topic is seeded at
bring-up the way the s3 bucket is).

| Test / sub-test | Workload |
|-----------------|----------|
| `kafka_ingest` / `ingest_alo` | SOURCE ingest throughput, at-least-once. Each rep `CREATE SOURCE`s over the pre-seeded topic with a **fresh** name (group_id defaults to `otterstax_<name>` + `OFFSET_RESET='earliest'` ⇒ each rep is an independent cold re-ingest), then polls `count(*)` until every row lands. `elapsed` = CREATE SOURCE → full ingest; txt reports `rec/s`. |
| `kafka_ingest` / `ingest_eos` | Same, with `TRANSACTIONAL=true`. The alo↔eos delta is the cost of exactly-once (each batch is a `BEGIN → insert(data) → upsert(offsets) → COMMIT` engine transaction). |
| `kafka_produce` / `produce` | Write path — `INSERT INTO kafka.<src> VALUES <PRODUCE_ROWS rows>` publishes the batch to the object's topic; the timed statement is one transactional produce+flush. |
| `kafka_stream` / `stream` | Continuous-query throughput — `CREATE STREAM AS SELECT ... FROM <src>` (pass-all filter, output count == num_records), then drain the stream's output topic until every row has flowed through (poll → transform → produce). Needs a broker consumer, so the runner imports `confluent-kafka`. |

```bash
# all three kafka tests, both default frontends
./benchmark/scripts/run_benchmark.sh --bench kafka_ingest kafka_produce kafka_stream --repetitions 5

# just ingest, postgres only
./benchmark/scripts/run_benchmark.sh --frontend postgres --bench kafka_ingest

# manual flow: bring up broker + seed once, then run interactively (+ Tracy)
./benchmark/manual/start_service.sh --kafka
./benchmark/manual/run_bench.sh --bench kafka_ingest kafka_produce kafka_stream
```

Selecting any `kafka_*` test makes `run_benchmark.sh`:

1. Generate the JSON dataset into `benchmark/data/fixtures/kafka_events.ndjson`
   via `data/generate_kafka_fixtures.py` (Step 1c, sized from `bench.yaml` `kafka:`).
2. Add `compose_kafka.yml` (the `bench_kafka` redpanda broker) to the backend +
   otterstax compose sets, and wait for it healthy.
3. Seed the topic **once** with `data/seed_kafka.py` (Step 3b, an explicit
   `docker run` — NOT a compose one-shot, so `_ensure_otterstax`'s backend `up -d`
   on a mid-run restart can't re-produce and double the topic; Kafka produce is
   not idempotent, unlike the RustFS seed's `aws s3 cp`).

The runner (`benchmarks/kafka_common.py`, driven by the thin
`benchmarks/{mysql,postgres}/kafka_{ingest,produce,stream}.py`) issues SQL to
OtterStax over the wire; `kafka_stream` additionally drains the output topic to
count rows. Every Kafka statement is built by a `sql_*` helper at the top of
`kafka_common.py`. The broker address OtterStax uses is embedded in
`CREATE SOURCE ... BOOTSTRAP_SERVERS='bench_kafka:9092'`.

**First `kafka_*` run needs a client-image rebuild.** `seed_kafka.py` and the
stream runner import `confluent-kafka`, added to `Dockerfile.benchmark`. If a
imports `confluent-kafka`, added to `Dockerfile.benchmark`. If a
`benchmark-client:latest` image from before this change is still cached,
run once with `--rebuild` (or `--clear`) so the seed step doesn't fail with
`ModuleNotFoundError: confluent_kafka`.

### Dataset (`data/generate_kafka_fixtures.py`)

Primitive columns only — `json_to_chunk` supports exactly
INTEGER / BIGINT / DOUBLE / STRING / BOOLEAN:

| Column | Type | Notes |
|--------|------|-------|
| `id` | BIGINT | sequential 0..num_records-1 (distinct) |
| `campaign_id` | BIGINT | 1..1000 (shares the group_a campaign_id space) |
| `event_type` | STRING | view / click / purchase |
| `amount` | DOUBLE | named `amount` (not `value`) to dodge the reserved word in SELECT |
| `ts` | BIGINT | monotonic epoch-ms |

Sized from `bench.yaml`:

```yaml
kafka:
  num_records: 50000     # rows produced == expected ingest count (baked into the client image — --rebuild after editing)
  topic: bench_events
```

### Standalone / local run (no docker harness)

The runner and the seed/generate scripts take `--local` / `--broker`, so the
whole thing runs against a hand-started redpanda + a local Debug server without
the docker image build:

```bash
BENCH_YAML=/path/to/bench.yaml python benchmark/data/generate_kafka_fixtures.py --out /tmp/fx
BENCH_YAML=/path/to/bench.yaml python benchmark/data/seed_kafka.py --broker 127.0.0.1:19092 --file /tmp/fx/kafka_events.ndjson
# ... start ./build/Debug/server ...
BENCH_YAML=/path/to/bench.yaml python benchmark/benchmarks/postgres/kafka_ingest.py --local --repetitions 5 --out-dir /tmp/out/postgres
```

### Hot-path instrumentation

The Kafka runtime carries `OTX_ZONE_N` zones (`utility/tracy_profiler.hpp`) on
every actor handler and the per-batch consume/convert/insert/produce hot path
(`KafkaManager::execute/produce/…`, `kafka::json_to_chunk`,
`kafka::poller::ingest_*`, `kafka::consumer::poll_batch`, `kafka::producer::*`),
so `--tracy` / `--tracy-sep` capture the ingestion pipeline the same as the query
path. The macros are no-ops unless the image is built with Tracy.

## Tracy profiling

OtterStax exposes a Tracy server on port **8086**. Two profiling modes:

**Automated (CI / full run):**

```bash
./benchmark/scripts/run_benchmark.sh --tracy-sep --bench join_all
```

Each test gets its own `.tracy` file. `tracy-capture` runs inside the
`otterstax_app` container and saves to `<out-dir>/<frontend>_<test>.tracy`.

**Interactive (manual exploration):**

1. `./benchmark/manual/start_service.sh`
2. Open Tracy → connect to `localhost:8086`
3. Run a query or benchmark — zones appear live in the Tracy UI
4. `./benchmark/manual/stop_service.sh` when done

**Why `ZoneScopedN` is absent from `mysql::Connector::runQuery_`:**
The MySQL connector uses `co_await` (Boost.Asio) which can resume on a different
io_context thread. Tracy zones opened before the suspension would close on the wrong
thread, causing "Zone is ended twice". The outer `mysql::ConnectorManager::executeQuery`
zone (single-threaded, non-coroutine) provides equivalent coverage.

## Result files

```text
benchmark_results/<run>/
├── summary.md        # Global: all frontends, total elapsed, slowest/fastest run, git commit
├── db_info.md        # Schema reference: all 6 DBs, tables, columns, row counts
├── mysql/
│   ├── summary.md    # Frontend-level summary table
│   ├── simple_select.txt   # Per-sub-test timing rows + SQL + stats (human-readable)
│   ├── simple_select.json  # Machine-readable (source for generate_summary.py)
│   └── ...
└── postgres/  ...
```

## Connector pattern

The per-frontend cross-backend scripts (five per frontend) are identical in structure:

```python
from common import benchmark_main
from queries import SIMPLE_SELECT
from connector import FRONTEND, DEFAULT_PORT, make_fetch_factory

if __name__ == "__main__":
    benchmark_main("simple_select", FRONTEND, DEFAULT_PORT, SIMPLE_SELECT, make_fetch_factory)
```

`make_fetch_factory(host, port) → make_fetch(sql) → fetch()`.
Every frontend reconnects per repetition — intentional, avoids stale connections after
OtterStax crashes between reps.
For Spark Connect a connection is a `SparkSession` of its own (`builder.remote(...).create()`),
so its timed fetch includes opening and releasing the session, as the mysql / postgres
fetches include their connect.

## Known OtterStax limitations affecting benchmarks

### A JOIN's described schema is not projected (Spark JOIN tests fail)

`Scheduler::prepare_schema` of a JOIN over backend tables answers every column of
the joined tables, not the SELECT list, while the executed rows are projected. A
Spark Connect client checks the rows against the described schema, so every
`spark.sql()` JOIN fails ("Target schema's field names are not matching the
table's field names") — on the spark frontend `join_same_instance`,
`join_cross_engine`, `join_all` and `join_concurrent` record errors. A MySQL
prepared statement over such a JOIN fails the same way (client error 2057);
single-table statements are unaffected.

### No predicate pushdown

OtterStax fetches entire tables from each backend and filters in-process (OtterBrix).
`WHERE` clauses in SQL **do not** reduce rows fetched from backends. So `join_all`
reads all ~60 k Group A rows even with `WHERE campaign_id <= 50 LIMIT 500`, and its
cost grows with table size, not with the result.

### Connection aliases required

Every backend is registered by alias (`mysql1`, `mysql2`, `pg1`, `pg2`, `ch1`, `ch2`)
at server startup from `benchmark/config.yaml` (mounted into `bench_otterstax`).
There is no runtime registration API — to change the registered aliases, edit that
file and restart the stack.

## Common issues

**Build fails with conan.otterbrix.com timeout**
The `otterstax_app` image can't be built when the private conan remote is unreachable.
If the image already exists, just run without `--rebuild` — images are reused by default.
If you need a clean build and the remote is down, wait for it to come back.

**Wrong row counts after changing bench.yaml**
`bench.yaml` is baked into the benchmark-client image at build time. After editing it,
run `--rebuild` (keeps DB volumes) or `--clear` (also wipes DB data and reinitialises).

**"Zone is ended twice" in Tracy captures**
This was caused by `ZoneScopedN` inside MySQL connector coroutines that cross
io_context thread boundaries at `co_await` suspension points. Fixed by removing
the zone from `mysql::Connector::runQuery_`. See the Tracy profiling section above.

---

## Stress test (`scripts/run_stress_benchmarks.sh`)

Runs N escalating load stages against the same live service instance and writes
a final `degradation_report.md` comparing throughput, latency percentiles, and
error rates across stages.  The service is **never restarted between stages**;
only a configurable cooling period separates them.

### Stress test quick start

```bash
# Default 3-stage run (1 → 5 → 15 workers per frontend, same query pool)
./benchmark/stress/run_stress_benchmarks.sh

# Selects only (no joins) — same 1/5/15 concurrency ramp
./benchmark/stress/run_stress_benchmarks.sh -p benchmark/stress/profiles/select_only.yaml

# Custom profile
./benchmark/stress/run_stress_benchmarks.sh --profile my_profile.yaml

# With perf profiling
./benchmark/stress/run_stress_benchmarks.sh --perf

# Reduced scale without rebuilding images
./benchmark/stress/run_stress_benchmarks.sh \
  --workers-small 1 --workers-medium 3 --workers-heavy 8
```

Results land in `benchmark_results/stress/<YYYYMMDD_HHMMSS>/`.

### CLI reference (`run_stress_benchmarks.sh`)

```text
-p, --profile FILE   YAML profile defining all stages. Overrides --workers-* and
                     --duration-* flags. Built-in profiles in benchmark/stress/profiles/.
--workers-small N    Workers per frontend, stage 1 (default: 1)   [ignored with -p]
--workers-medium N   Workers per frontend, stage 2 (default: 5)   [ignored with -p]
--workers-heavy N    Workers per frontend, stage 3 (default: 15)  [ignored with -p]
--duration-small N   Stage 1 active duration in seconds (default: 60)  [ignored with -p]
--duration-medium N  Stage 2 active duration in seconds (default: 60)  [ignored with -p]
--duration-heavy N   Stage 3 active duration in seconds (default: 90)  [ignored with -p]
--out-dir DIR        Result root (default: benchmark_results/stress/<ts>)
--no-init            Skip data initialisation (reuse existing DB volumes)
--rebuild            Force rebuild of both Docker images
--clear              Wipe images + DB volumes, then rebuild
-j N                 Parallel build jobs (default: auto)
--tracy              Continuous Tracy capture → <out-dir>/benchmark.tracy
--perf               CPU call-graph perf capture → benchmark.perf.data + benchmark.perf
--perf-alloc         Like --perf but also attaches malloc uprobe
IMAGE_TAG=<tag>      OtterStax image tag (env var; default: bench)
```

### YAML profile format

```yaml
cooling_s: 10     # seconds of quiet time between stages (default: 10)

stages:
  - name: baseline        # output subdirectory name
    label: "Stage 1"      # optional banner text
    workers_per_frontend: 1
    duration_s: 60
    ramp_secs: 2          # optional; default: max(2, duration_s / 6)
    query_pool:
      simple_select:     1   # weight (relative integer) — use same pool on all stages
      complex_select:    1
      join_same_b:       1
      join_cross_engine: 1
```

Valid `query_pool` keys:

| Key | Description | Typical latency |
| --- | ----------- | --------------- |
| `simple_select` | `LIMIT 100` point reads across all 6 backends | 28–51 ms |
| `complex_select` | `GROUP BY + HAVING + ORDER BY LIMIT 1000` | 33–63 ms |
| `join_same_b` | Same-instance join, Group B only (~5 k rows) | 36–55 ms |
| `join_same_instance` | Same-instance join, both groups (up to 60 k rows) | 36–74 ms |
| `join_cross_engine` | Cross-engine join, Group B tables | 36 ms–1 s |

Built-in profiles in `benchmark/stress/profiles/`:

- `default.yaml` — matches the built-in defaults (1/5/15 workers, all 4 query types)
- `select_only.yaml` — simple + complex selects only, no joins (1/5/15 workers)

### Output structure

```text
benchmark_results/stress/<YYYYMMDD_HHMMSS>/
├── degradation_report.md     # throughput, latency, errors + per-query-type breakdown across stages
├── benchmark.tracy           # (--tracy)
├── benchmark.perf.data       # (--perf)
├── benchmark.perf            # (--perf) speedscope-compatible
├── baseline/
│   ├── mysql_results.json    # includes by_query_type: {category: {count, qps, p50/p95/p99}}
│   └── postgres_results.json
├── medium/
│   └── ...
└── heavy/
    └── ...
```

### Python module layout (`benchmark/stress/`)

| Module | Role |
| ------ | ---- |
| `profiles.py` | `StageConfig` dataclass, YAML loading (`load_profile`), built-in defaults (`build_stages`) |
| `stress_runner.py` | Threaded worker engine (`run_stage`) — persistent connections, round-robin query pool, per-category latency tracking, reconnect on error |
| `report.py` | Aggregation (`build_stage_result`), JSON output with `by_query_type`, degradation markdown report with per-category breakdown section |
| `stress_main.py` | CLI entry point; orchestrates all stages and writes final report |
