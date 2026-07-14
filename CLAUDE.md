# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

OtterStax is a federated SQL query server. Clients connect via MySQL wire protocol (8816), PostgreSQL wire protocol (8817), or Apache Arrow FlightSQL (8815). Queries are either executed locally by the Otterbrix engine or dispatched to registered remote database backends (MariaDB/MySQL, PostgreSQL, ClickHouse). A REST API on port 8085 manages remote connections at runtime.

## Build Commands

```bash
# Install Conan deps
# IMPORTANT: if conanfile.py changed, remove ./build first before re-running
conan install conanfile.py --build missing -s build_type=Release

# Configure
cmake -S . -B build/Release \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake

# Build (5 parallel jobs)
/usr/local/bin/cmake --build /workspaces/otterstax/build/Release --parallel 5 --

# Build with tests enabled
cmake -S . -B build/Release -DBUILD_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
/usr/local/bin/cmake --build /workspaces/otterstax/build/Release --parallel 5 --

# Run a single test binary (Catch2)
./build/Release/test_system          # tests/system
./build/Release/test_unit_schema     # tests/unit/schema
./build/Release/test_unit_utility    # tests/unit/utility
./build/Release/test_mysql_front     # tests/mysql-front

# Build with sanitizers (not both at once)
cmake -S . -B build/Release -DENABLE_ASAN=ON -DBUILD_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake -S . -B build/Release -DENABLE_TSAN=ON -DBUILD_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake

# Adjust log verbosity (default ERROR)
cmake -S . -B build/Release -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG ...
```

## Docker Workflows

```bash
# Run all integration tests (handles DB startup timing)
chmod +x ./docker-run-tests.sh && ./docker-run-tests.sh

# Build containerized unit tests
docker build -f Dockerfile.test -t otterstax-test .

# Full stack for manual testing
python fixtures/generate_data.py
docker compose up

# Benchmark suite (builds images, starts DBs, runs tests, writes results)
./benchmark/scripts/run_benchmark.sh --repetitions 5

# Benchmark with CPU call-graph profiling (perf, 99 Hz dwarf unwind)
# Outputs: benchmark_results/<ts>/benchmark.perf.data + benchmark.perf (speedscope)
./benchmark/scripts/run_benchmark.sh --perf --frontend postgres --bench simple_select

# Benchmark with CPU + allocation hotspot profiling (malloc uprobe, every call site)
./benchmark/scripts/run_benchmark.sh --perf-alloc --frontend postgres --bench simple_select

# Manual interactive mode (start services, run queries/benchmarks by hand, then stop)
./benchmark/manual/start_service.sh            # start (reuses existing image)
./benchmark/manual/start_service.sh --rebuild  # force rebuild both images
./benchmark/manual/start_service.sh --perf     # start + perf recording (saved on stop)
./benchmark/manual/start_service.sh --perf-alloc  # + malloc uprobe
./benchmark/manual/stop_service.sh             # stop (saves perf.data if active)
```

## Architecture Overview

All components are actor-zeta actors (`actor_mixin` for the Scheduler,
`basic_actor` for Workers and the integration managers) communicating through
typed coroutine futures. The flow for every query:

```text
Frontend (MySQL/PG/FlightSQL)
  → Scheduler::execute()          — thin router, hashes session_hash → Worker
  → Worker::execute()             — owns the parser, runs the full pipeline
  → CatalogManager                — resolve schemas, set backend type on ParsedQueryData
  → [if remote] integration/{sql,postgresql,clickhouse}::ConnectionManager
       → connectors/{mysql,postgresql,clickhouse}::ConnectorManager::executeQuery()
       → otterbrix/translators/input/  — convert raw results to data_chunk_t
  → [if local] integration/otterbrix::OtterbrixManager
       → otterbrix/operators/execute_plan — delegate to Otterbrix engine
  → otterbrix/translators/output/chunk_to_arrow — convert to Arrow RecordBatch
  → Worker co_returns result_wrapper_t<session_payload>
  → frontend awaits via asio_future_bridge → sends response
```

The Scheduler is a session-affinity router over a pool of `Worker` actors
(spawned on an `actor_zeta::scheduler::sharing_scheduler`); every session
keyed by `session_hash_t` always lands on `workers_[id % N]`. The Scheduler's
own thread runs an otterbrix-style event loop: `enqueue_impl` (any sender
thread) only pushes into a lock-free inbox and signals a CV; all coroutine
work happens on the loop thread. Frontends never block on a `cv_wrapper` —
they hold the future returned by `Scheduler::execute` and poll it from the
per-connection asio executor through `frontend/common/asio_future_bridge.hpp`.

### Key Types

- `session_hash_t` — unique ID threaded through every component for a single query (also the Worker routing key)
- `core::result_wrapper_t<session_payload>` (`Worker::session_result` / `Scheduler::session_result`) — the typed result returned through the actor-zeta future; carries either the payload or `core::error_t`
- `ParsedQueryData` / `ParsedQueryDataPtr` — output of the Otterbrix SQL parser; carries the logical plan, `backend_type_t`, and per-node backend assignments for mixed queries
- `backend_type_t` — `{Unknown, MySQL, PostgreSQL, ClickHouse, Mixed, Otterbrix}`; set by `CatalogManager::get_catalog_schema` after schema resolution

### Actor Message Pattern

```cpp
auto [needs_sched, future] = actor_zeta::send(
    target_address,
    &TargetActor::method,   // type-safe method pointer — no route enums
    arg1, arg2
);
auto result = co_await std::move(future);
```

Every handler is listed in the actor's `dispatch_traits` alias and the `behavior()` coroutine.

### Federated Query Syntax

Connection aliases act as the outermost database name qualifier:

```sql
SELECT * FROM alias1.db.schema.table JOIN alias2.db.schema.table2 ON ...
```

`alias1`/`alias2` are registered via `POST :8085/add_connection`.

## Profiling Instrumentation (Tracy) — MANDATORY

Every main function MUST be instrumented with a Tracy zone. This is not optional:
when you add or substantially edit a main function, you MUST add the macro.

A "main function" is any of:

- An actor handler (every method listed in a `dispatch_traits` / `behavior()` coroutine)
- A public/top-level entry point of a translation unit (free functions exposed via a header)
- An expensive operation (parsing, schema discovery, SQL generation, data translation, plan execution)

Rules:

- Include the profiler header: `#include "utility/tracy_profiler.hpp"` (placed with the
  project's own includes, after the unit's own header).
- Add `OTX_ZONE_N("Scope::function")` as the **first statement** of the function body,
  using a stable, qualified name (e.g. `"Worker::execute"`, `"catalog::get_catalog_schema"`,
  `"sql_gen::generate_query"`, `"parser::prepare_sql"`).
- The macros compile to **no-ops** unless Tracy is enabled, so there is no release-build cost.

Do NOT instrument:

- Recursive AST/tree walkers on a per-call basis (one zone per node floods the profiler) —
  instrument the non-recursive top-level entry point that drives them instead.
- Trivial getters/setters and O(1) map lookups (e.g. `schema_store_t::find`).

Available macros (see `utility/tracy_profiler.hpp`): `OTX_ZONE()`, `OTX_ZONE_N(name)`,
`OTX_FRAME()`, `OTX_PLOT(name, val)`, `OTX_MESSAGE(msg)`, `OTX_LOCKABLE(type, var)`.

## External Tables — File & S3

OtterStax can ingest data from local files and S3-compatible object stores
into otterbrix-internal storage, query it like any other table, and export
query results back out. Both the loader and the exporter are exposed as SQL
through grammar extensions registered on the parser side.

### Surface SQL

```sql
-- Load a local file into otterbrix-internal storage
CREATE EXTERNAL TABLE <db>.<table>
    WITH (location = '/path/to/file.parquet', format = 'parquet');

-- Load an S3 object (after registering credentials via REST — see below)
CREATE EXTERNAL TABLE <db>.<table>
    WITH (s3_alias = 'minio1',
          location = 's3://bucket/path/file.csv',
          format   = 'csv');

-- Export an arbitrary SELECT (inner query is re-parsed and executed by the engine)
COPY (SELECT col_a, col_b FROM <db>.<table>) TO '/tmp/out.ndjson'
    WITH (format = 'ndjson');

COPY (SELECT * FROM <db>.<table>) TO 's3://bucket/exported/out.csv'
    WITH (s3_alias = 'minio1', format = 'csv');
```

Supported formats (both `CREATE EXTERNAL TABLE` and `COPY ... TO`):

| Format    | Loader (`translators/input/`) | Writer (`translators/output/`) |
|-----------|-------------------------------|--------------------------------|
| `parquet` | `parquet_to_chunk` (Arrow + snappy + brotli + zlib + lz4 + zstd) | `chunk_to_parquet` |
| `csv`     | `csv_to_chunk`                | `chunk_to_csv`     |
| `ndjson`  | `ndjson_to_chunk`             | `chunk_to_ndjson`  |

`format` is optional in both statements — if omitted, it is auto-detected from
the location's file extension. Once loaded, the table is a normal
otterbrix-internal table; the parquet→engine schema/column types survive into
the engine catalog (int64, double, utf8 strings, etc.). The database segment
(`<db>` above) is auto-created by `CREATE EXTERNAL TABLE` if it doesn't exist.

### How it flows through the actor graph

```text
Frontend
  → Scheduler::execute()              — thin router, hashes session_hash → Worker
  → Worker::execute()                 — owns the parser, runs the full pipeline
  → [parser extension claims the statement → otterstax::external::external_node_t]
  → Worker::handle_external_statement
       → CREATE EXTERNAL TABLE:
           local path  → conn::file::FileManager::add_file → OtterbrixManager engine load
           s3:// URI   → db::S3Manager::download → FileManager::add_file
       → COPY (<inner>) TO ...:
           inner SQL re-parsed; result chunk runs through chunk_to_<fmt>;
           local path  → FileManager::dump_file
           s3:// URI   → S3Manager::upload (uses FileManager::dump_file under the hood)
  → returns empty session_payload via future; DDL/COPY return no rows
```

Implementation entry points:

- **Grammar extensions** — `otterbrix/parser/grammar_extention/s3/` and
  `.../file/`. Each ships a `*_gram.y` + `*_scan.l` + `*_extension.cpp` and is
  registered with the parser registry at `otterbrix/parser/parser.cpp`. The
  `cmake/otterbrix_parser_extension.cmake` helper macro builds them.
- **Connectors** — `connectors/s3/` (Arrow's `arrow::fs::S3FileSystem`,
  initialised via `arrow::fs::EnsureS3Initialized`) and `connectors/file/`
  (filesystem ingestion).
- **Integration actors** — `integration/s3/s3_manager.{hpp,cpp}` orchestrates
  download/upload by composing the raw s3 connector with `FileManager` (so
  COPY ... TO 's3://...' is a single round-trip).
- **REST API** — `GET :8085/s3/add_credentials` registers a named s3 alias
  (`alias`, `access_key`, `secret_key`, `region`, `endpoint`) used by the
  `s3_alias` option on `CREATE EXTERNAL TABLE` / `COPY ... TO`.

### Working JOIN shapes

All combinations below are joinable in a single SQL statement:

- otterbrix-internal table ⋈ s3 external (after `CREATE EXTERNAL TABLE`).
- s3 external ⋈ local-file external.
- Two `CREATE EXTERNAL TABLE`'d sources of any mix of formats (parquet ⋈ csv ⋈
  ndjson).
- Two registered backends (cross-backend, classified as `Mixed`).
- **Registered backend ⋈ otterbrix-internal table** in a single statement. The
  backend manager fetches its slice through the existing single-backend
  dispatch and inlines it as `node_raw_data`
  (`integration/sql/connection_manager.cpp:162-164` for MySQL; PG/CH have the
  equivalent). The engine then resolves the symbolic local side by its
  stamped `table_oid` (`catalog/catalog_manager.cpp:240-242`) and JOINs raw
  data against engine-resident rows. The live demo's
  `examples/demo/sql/step_4.sql` exercises exactly this shape every run.

After `CREATE EXTERNAL TABLE`, the loaded table behaves identically to one
created via `CREATE TABLE` — `INSERT INTO`, `SELECT`, JOIN, `COPY ... TO` all
work the same way.

**JOIN-key type widths must agree on both sides.** An equi-JOIN between an
int32 column and an int64 column silently drops every row in the engine — no
error, just an empty result. The two width footguns to watch for:

- Local `int` (int32) vs parquet `int64` — declare the local column `bigint`
  to match the parquet loader's column type.
- Backend `INT` (int32, as `mysql_to_chunk` surfaces it) vs a local `bigint`
  joined against it — either widen the backend column on the backend side or
  stage the backend slice into a local `bigint` table first
  (`tests/test_mysql_join_sql_s3_to_s3.py` does the staging path). Joining on
  a string key avoids the issue entirely, which is what `step_4.sql` and
  `tests/test_mysql_join_otb_local_backend.py` do. See `FIX_JOIN.md` for the
  full diagnosis.

### Round-trip example

```sql
-- Bring two datasets in
CREATE EXTERNAL TABLE eg.regions
    WITH (s3_alias='minio1', location='s3://test-bucket/regions.parquet', format='parquet');
CREATE EXTERNAL TABLE eg.weights
    WITH (location='/fixtures/weights.csv', format='csv');

-- JOIN them and persist the result into another otterbrix-internal table
CREATE TABLE eg.weighted_regions (region_id bigint, region_name string, weight double);
INSERT INTO eg.weighted_regions (region_id, region_name, weight)
    SELECT r.region_id, r.region_name, w.weight
    FROM   eg.regions r JOIN eg.weights w ON r.campaign_id = w.campaign_id;

-- Export to s3
COPY (SELECT * FROM eg.weighted_regions) TO 's3://test-bucket/out/weighted.csv'
    WITH (s3_alias='minio1', format='csv');
```

End-to-end coverage lives in `tests/test_{schema_}mysql_{file,s3}.py`,
`tests/test_mysql_file_ndjson.py`, `tests/test_mysql_join_sql_s3_to_s3.py`,
`tests/test_mysql_join_otb_local_s3.py`, and
`tests/test_mysql_join_otb_local_s3_file.py` (3-origin: s3 parquet ⋈ file csv ⋈
otterbrix-internal — shadow of `external_join_all` benchmark), all driven by
`docker-run-tests.sh`.

## Directory Map

| Directory | CMake target | Role |
| --------- | ------------ | ---- |
| `connectors/` | `connectors`, `s3`, `file`, `api_server` | Raw DB connections (Boost.MySQL, libpq, clickhouse-cpp), S3 (Arrow `S3FileSystem`), local-file ingestion, HTTP connection API |
| `catalog/` | `catalog` | Schema discovery + connection type registry (`CatalogManager`) |
| `integration/` | `integration` | Actor wrappers bridging Worker ↔ ConnectorManagers (incl. `db::S3Manager`) |
| `otterbrix/` | `otterbrix_local` (+ `otterbrix_s3_extension`, `otterbrix_file_extension`) | Parser, SQL generator, translators, plan execution, grammar extensions for `CREATE EXTERNAL TABLE` / `COPY ... TO` |
| `scheduler/` | `scheduler` | `Scheduler` router + `Worker` pool (full parse→catalog→backend→otterbrix pipeline, including external-statement dispatch) + schema computation utilities |
| `frontend/` | `flight_sql_server`, `mysql_server`, `postgres_server` | Wire-protocol frontends (await `Scheduler` futures via `asio_future_bridge.hpp`) |
| `utility/` | (header-only) | `session_payload`, `session`, `pipeline_error`, logger, profiler |
| `cmake/` | (helper macros) | `otterbrix_parser_extension.cmake` — builds the s3/file flex+bison grammar extensions |
| `tests/` | `test_system`, `test_unit_*`, `test_mysql_front` | Catch2 tests + python integration suite under `tests/test_*.py` |
| `integration/` | `integration` | Actor wrappers bridging Scheduler ↔ ConnectorManagers |
| `integration/kafka/` | `kafka_runtime` | `KafkaManager` actor + `detail/` impl (consumer/producer/poller/stream/reader); Kafka SOURCE/STREAM objects, librdkafka |
| `otterbrix/` | `otterbrix_local` | Parser, SQL generator, translators, plan execution |
| `otterbrix/parser/grammar_extension/kafka/` | `kafka_grammar` | Kafka DDL parser extension (flex+bison): `kafka_node_t`, `kafka_write_target` |
| `scheduler/` | `scheduler` | Query routing actor (`Scheduler`) + schema computation utilities |
| `frontend/` | `flight_sql_server`, `mysql_server`, `postgres_server` | Wire-protocol frontends |
| `utility/` | (header-only) | `cv_wrapper`, `session_payload`, `result_t`, `pipeline_error`, logger |
| `tests/` | `test_system`, `test_unit_*`, `test_mysql_front` | Catch2 tests |

## Known Constraints

- `ConnectorManager::addConnection/removeConnection` is **not thread-safe** (see TODOs in `connectors/mysql/manager.hpp:44-45`)
- One query per connection at a time (`integration/sql/connection_manager.cpp:81`)
- Array types support only single dimension (`otterbrix/query_generation/sql_query_generator.cpp:70`)
- Docker MariaDB volumes lag on cold start — `docker-run-tests.sh` has a 120 s wait

## Critical Dependency Versions

- Otterbrix 1.0.0a13-rc-3 (custom Conan remote: `http://conan.otterbrix.com`)
- Arrow 21.0.0 (with `with_flight_sql=True`, `with_s3=True`, `with_parquet=True`, `with_csv=True`, `with_json=True`, plus snappy/brotli/zlib/lz4/zstd compression codecs)
- Boost 1.87.0
- actor-zeta 1.2.0
