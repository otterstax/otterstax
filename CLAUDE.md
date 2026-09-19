# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

OtterStax is a federated SQL query server. Clients connect via MySQL wire protocol (8816), PostgreSQL wire protocol (8817), or Arrow Flight SQL (8815 — served by an in-house implementation, no Arrow Flight dependency). Queries are either executed locally by the Otterbrix engine or dispatched to registered remote database backends (MariaDB/MySQL, PostgreSQL, ClickHouse). There is a **single config file** (`config.yaml`) that holds the wire-server settings and, under a `connections:` section, every remote backend and s3 alias — read once at startup. That `connections:` section is the single source of truth for connections; there is no runtime add/remove API.

## Connection Config

Everything lives in **one YAML file, `config.yaml`** — the wire-server settings
and, under `connections:`, every remote backend and s3 alias. It is **read once
at server startup**; the `connections:` section is the single source of truth for
connections. There is **no runtime add/remove/update API** (the old HTTP server on
port 8085 was removed). To change connections, edit `config.yaml` and restart the
server. See `config/CLAUDE.md` for the full config-layer reference.

### The file and how it is located

The path comes from the `--config PATH` flag (default `config.yaml`), resolved
**relative to the server's working directory**. In containers the server runs from
`WORKDIR /app/build/Release` with no `--config`, so at runtime it reads
`/app/build/Release/config.yaml`, provided either by:

- **baking** it into the image (`Dockerfile.test` `COPY`s the test `config.yaml`
  there), or
- **bind-mounting** a stack-specific `config.yaml` onto that path (compose files)
  — a mount overrides anything baked in.

A missing file → the server starts with default ports and no connections.

### File format

```yaml
service:
  flight_sql: { host: "0.0.0.0", port: 8815 }
  mysql:      { port: 8816 }      # wire-server port (NOT a backend)
  postgres:   { port: 8817 }      # wire-server port (NOT a backend)
  connection_retry: { max_attempts: 10, delay_ms: 2000 }

connections:
  mysql:
    - { alias: mysql, host: demo-mariadb, port: "3306", username: demo, password: demo, database: bill, table: "" }
  postgresql:
    - { alias: pg, host: demo-postgres, port: "5432", username: demo, password: demo, database: shop, schema: shop, table: "" }
  clickhouse:
    - { alias: ch, host: demo-clickhouse, port: "9000", username: demo, password: demo, database: ev, table: "" }
  s3:
    - { alias: demo_s3, access_key: minioadmin, secret_key: minioadmin, region: us-east-1, endpoint: demo-minio:9000 }
```

Wire-server settings live under the top-level `service:` node so
`service.mysql`/`service.postgres` (**wire-server ports**) never collide with the
backends under `connections:` (`connections.mysql`/`connections.postgresql`) —
different nesting. All `connections:` sections are optional (missing → empty).
`port` is an **optional string** (empty → driver default). `schema` defaults to
`public`. s3 `region`/`session_token`/`endpoint` are optional; malformed YAML
aborts startup. `service.connection_retry` (optional; default `max_attempts: 1`,
`delay_ms: 1000` = one-shot) sets how many times startup retries opening a slow
backend. The `alias` is the outermost qualifier in federated SQL
(`SELECT ... FROM alias.db.schema.table ...`) and the `s3_alias` referenced by
`CREATE EXTERNAL TABLE` / `COPY ... TO`.

### Flow through the code

```text
main.cpp
  → config::ConfigReader.load(config.yaml)      → result_wrapper_t<ServiceConfig> { ports…, ConnectionsConfig connections }
        (internally: parse_connections(config["connections"], resource) → plain descriptor structs)
        (parse_connections validates every entry → the first incomplete one is returned as
         core::error_t{invalid_parameter} → main logs it and returns 1 before the engine, any actor
         or connector exists — ComponentManager is constructed only after the config is accepted)
  → conn::s3::subsystem_finalizer_t            — declared before ComponentManager: the s3 actor it spawns
                                                 initialises Arrow S3, and the process must FinalizeS3
                                                 after the actor graph is gone on every exit path
  → ComponentManager(make_create_config(...))   → engine + actor graph + worker pool
  → ComponentManager::register_connections(server_config.connections, server_config.connection_retry)
       mysql/pg/ch: ConnectorManager::addConnection(conn::api_server::*Params)   — opens the connector,
                    retried up to connection_retry.max_attempts (delay_ms between tries)
       s3         : actor send &conn::s3::ConnectorManager::add_credentials      — stores the alias (no retry)
```

- Parsing + validation live in the `config` target (`config/`), which has **no
  connector dependencies** — `parse_connections` produces plain
  `config::ConnectionsConfig` descriptor structs held inside `ServiceConfig` and
  **returns `core::error_t{invalid_parameter}` (built from `validation_error`)
  for the first incomplete entry, malformed port or non-scalar field**; a file
  that is not YAML is a `conversion_failure`. Nothing in `config/` throws: the
  yaml-cpp calls are the only throwing sites and each is converted where it is
  made (`config/yaml_scalar.hpp`, `YAML::LoadFile` in `ConfigReader::load`).
  `main.cpp` logs the error and returns 1, so an invalid connection aborts
  startup rather than coming up half-configured.
- Registration lives in `ComponentManager::register_connections` (it owns the
  connector managers), converting the pre-validated descriptors → connector param
  structs. Opening a backend is best-effort: it is retried up to
  `connection_retry.max_attempts`, and a backend that is still unreachable logs an
  error but does not abort startup (the other backends and local engine stay up).

### Where each stack's config.yaml lives

| Stack | File | Delivery |
|-------|------|----------|
| repo default / template | `config.yaml` (repo root) | not baked; copy/mount it or pass `--config` |
| demo (`examples/demo/`) | `config.yaml` / `config_local.yaml` (`--local`) | mounted by `examples/demo/compose.yml` |
| integration tests | `tests/scripts/config.yaml` | baked by `Dockerfile.test` |
| root manual stack | `scripts/database/config.yaml` | mounted by `compose.yml` |
| benchmarks | `benchmark/config.yaml` | mounted by `benchmark/compose_benchmark.yml` |
| examples/simple | `examples/simple/example_connetion/config{,_local}.yaml` | pass `--config <file>` |

Unit tests: `tests/unit/config/` (`test_unit_config`).

## Build resources — avoid OOM (check RAM + threads first)

**Before building (local or in Docker), size the parallelism to available RAM.**
Compiling otterbrix/Arrow TUs needs **~1.5–2 GB per job**; running `-j nproc`
blindly is the #1 cause of OOM kills (`c++: internal compiler error: Killed`) and
Docker BuildKit `ResourceExhausted` failures — especially in Docker Desktop where
the VM's RAM is far smaller than the host's.

- **Check first:**
  - Host: `nproc` (Linux) / `sysctl -n hw.ncpu` (macOS); RAM via `free -g` / `sysctl -n hw.memsize`.
  - **Docker** (what matters for image builds): `docker info --format '{{.NCPU}} CPUs / {{.MemTotal}} bytes'`. Docker Desktop's default is often 2–8 GB regardless of host RAM.
- **Pick jobs:** `JOBS = min(nCPU, floor(RAM_MB / 1536))` (≈1.5 GB/job). Example: a
  Docker VM with 8 GB → `floor(8192/1536)=5` jobs even on a 14-CPU host.
- **Apply it:**
  - Local: `cmake --build build/Release --parallel <JOBS>`
  - `docker build --build-arg BUILD_JOBS=<JOBS> -f Dockerfile.test ...`
  - `./docker-run-tests.sh -j <JOBS>`
  - `./benchmark/scripts/run_benchmark.sh` / `run_stress_benchmarks.sh` already
    **auto-cap** to this formula when `-j` is omitted (see `run_benchmark.sh`).
- If a build still gets `Killed`/`ResourceExhausted`, halve `JOBS` or raise the
  Docker Desktop memory limit (Settings → Resources).

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

# Run a single test binary (Catch2) — binaries sit under build/Release/tests/<dir>/
./build/Release/tests/system/test_system                    # tests/system
./build/Release/tests/unit/parser/test_parser               # tests/unit/parser
./build/Release/tests/unit/schema/test_schema               # tests/unit/schema
./build/Release/tests/unit/utility/test_utils               # tests/unit/utility
./build/Release/tests/unit/translators/test_unit_translators
./build/Release/tests/unit/config/test_unit_config
./build/Release/tests/unit/parser/grammar_extension/kafka/test_kafka_grammar
./build/Release/tests/mysql-front/test_mysql_front          # tests/mysql-front

# Build with sanitizers (not both at once)
cmake -S . -B build/Release -DENABLE_ASAN=ON -DBUILD_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake -S . -B build/Release -DENABLE_TSAN=ON -DBUILD_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
# ASAN caveat: the engine's core/pmr.hpp keys a public class layout off the
# consumer's __SANITIZE_ADDRESS__, so an ASAN build undefines that macro
# (CMakeLists.txt) to match the non-ASAN Conan engine package. That workaround
# is valid only while the engine is built without ASAN — an ASAN-built engine
# would need the macro left defined. The CI sanitizer workflows also export
# ENABLE_ASAN/ENABLE_TSAN to docker-run-tests.sh, which skips the Kafka
# crash-recovery (kill -9) step under sanitizers.

# Adjust log verbosity (default ERROR)
cmake -S . -B build/Release -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG ...
```

## Docker Workflows

> Cap build parallelism to Docker's RAM to avoid OOM — see **Build resources**
> above. Pass `-j <JOBS>` / `--build-arg BUILD_JOBS=<JOBS>` on the commands below.

```bash
# Run all integration tests (handles DB startup timing)
chmod +x ./docker-run-tests.sh && ./docker-run-tests.sh -j 5

# Build containerized unit tests
docker build -f Dockerfile.test --build-arg BUILD_JOBS=5 -t otterstax-test .

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
work happens on the loop thread. Frontends never block on shared state — they
hold the future returned by `Scheduler::execute` and poll it from the
per-connection asio executor through `frontend/common/asio_future_bridge.hpp`.
That bridge is the only supported way to pick up an actor result; the
`cv_wrapper` condvar handoff it replaced has been deleted.

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

`alias1`/`alias2` are the aliases registered in the connection config file (see **Connection Config** above).

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
- **Exception — coroutine handlers that can resume on another thread.** When a handler's
  coroutine can suspend at a `co_await` and resume on another thread, or interleave with
  other coroutines on one thread (`Worker` on the `sharing_scheduler`, the `Scheduler`
  event loop), open the zone as the first statement of a nested block
  `{ OTX_ZONE_N("Scope::function"); ... }` that closes before the first `co_await`.
  Tracy requires a zone to end on the thread that began it, in LIFO order; a zone spanning
  a suspension point breaks both and corrupts the capture. Only `assert`s, objects that
  must live for the whole coroutine (`Timer`, `erase_on_exit_t`) and the variables the
  block fills (the `unique_future`, the parsed query data) may precede that block; the
  awaited actor instruments its own work. A handler of an actor whose `enqueue_impl`
  drives the coroutine to completion on the sender's thread (e.g. `CatalogManager`,
  `S3Manager`, `FileManager`) keeps the zone as its first statement even across
  `co_await`: the awaited call runs nested on the same thread. If such an actor moves off
  that synchronous path, its handlers fall under this exception. See
  `scheduler/CLAUDE.md`, "Tracy zones in coroutines".
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
- **Connection config** — s3 aliases are declared in the `connections.s3:`
  section of `config.yaml` (`alias`, `access_key`, `secret_key`, `region`,
  `endpoint`) and registered at startup. The alias is then used by the
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
  `tests/test_mysql_join_otb_local_backend.py` do. The test-side conventions
  are in `tests/CLAUDE.md`, "JOIN-key width sensitivity".

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
| `connectors/` | `connectors`, `s3`, `file` | Raw DB connections (Boost.MySQL, libpq, clickhouse-cpp), S3 (Arrow `S3FileSystem`), local-file ingestion. `api_connections/` holds the `conn::api_server::*Params` structs consumed by `addConnection` |
| `config/` | `config` | Single `config.yaml` reader — wire-server settings + `connections:` section (single source of truth for backend/s3 connections). See `config/CLAUDE.md` |
| `catalog/` | `catalog` | Schema discovery + connection type registry (`CatalogManager`) |
| `integration/` | `integration` | Actor wrappers bridging Worker ↔ ConnectorManagers (incl. `db::S3Manager`); each backend actor is the sole driver of its ConnectorManager and answers the catalog's `discover` |
| `integration/kafka/` | `kafka_runtime` | `KafkaManager` actor + `detail/` impl (consumer/producer/poller/stream/reader); Kafka SOURCE/STREAM objects, librdkafka |
| `otterbrix/` | `otterbrix_local` (+ `otterbrix_s3_extension`, `otterbrix_file_extension`) | Parser, SQL generator, translators, plan execution, grammar extensions for `CREATE EXTERNAL TABLE` / `COPY ... TO` |
| `otterbrix/parser/grammar_extension/kafka/` | `kafka_grammar` | Kafka DDL parser extension (flex+bison): `kafka_node_t`, `kafka_write_target` |
| `scheduler/` | `scheduler` | `Scheduler` router + `Worker` pool (full parse→catalog→backend→otterbrix pipeline, including external-statement dispatch) + schema computation utilities |
| `frontend/` | `flight_sql`, `mysql_server`, `postgres_server` | Wire-protocol frontends (await `Scheduler` futures via `asio_future_bridge.hpp`). `flight_sql/` is the in-house Flight SQL server: asio-grpc handlers over gRPC + a vendored Arrow IPC on flatbuffers — no Arrow Flight dependency. |
| `utility/` | (header-only) | `session`, `wait_barrier` (connector error marshalling), `asio_error`, `table_info`, logger, profiler |
| `cmake/` | (helper macros) | `otterbrix_parser_extension.cmake` — builds the s3/file flex+bison grammar extensions |
| `tests/` | `test_system`, `test_parser`, `test_schema`, `test_utils`, `test_unit_translators`, `test_unit_config`, `test_kafka_grammar`, `test_mysql_front` | Catch2 tests + python integration suite under `tests/test_*.py` (binary names are the `project()` names in `tests/*/CMakeLists.txt`; see `tests/CLAUDE.md`) |

## Known Constraints

- `ConnectorManager::addConnection` is the only write to a connector registry and runs on the startup thread before any query reaches the integration actor that drives the manager (`connectors/mysql/manager.hpp`); there is no remove path
- One query per connection at a time: each alias owns a single `boost::mysql::any_connection` and nothing serializes overlapping statements on it (`Connector::runQuery_` in `connectors/mysql/connector.hpp`; the pg/ch connectors are shaped the same way)
- Array types support only single dimension (see the `TODO: multiple dimentions array` in `write_column_def`, `otterbrix/query_generation/sql_query_generator.cpp`)
- Docker MariaDB volumes lag on cold start — `docker-run-tests.sh` has a 120 s wait

## Critical Dependency Versions

- Otterbrix 1.0.0b2-rc-3 (custom Conan remote: `http://conan.otterbrix.com`; pinned by recipe revision in `conanfile.py`)
- Arrow 24.0.0 (with `with_flight_sql=True`, `with_s3=True`, `with_parquet=True`, `with_csv=True`, `with_json=True`, plus snappy/brotli/zlib/lz4/zstd compression codecs)
- Boost 1.88.0
- actor-zeta 1.2.0
- Catch2 3.15.1 (v3 — `find_package(Catch2 3)` required by the otterbrix recipe)
