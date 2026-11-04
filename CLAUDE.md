# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
It is the map; every module with a `CLAUDE.md` of its own holds the details.

## What This Project Is

OtterStax is a federated SQL server: wire frontends + the embedded otterbrix engine + remote backends.
Clients connect over the MySQL wire (8816), the PostgreSQL wire (8817), Arrow Flight SQL (8815, an
in-house server, no Arrow Flight dependency) or Spark Connect (15002, gRPC, for PySpark: `spark.sql()`,
DataFrames, `spark.catalog`). **Every statement runs on otterbrix.** A table of a remote backend
(MariaDB / MySQL, PostgreSQL, ClickHouse) is an ordinary row of otterbrix's catalog in a database of
a *connection*; otterbrix plans the statement and reaches the table's rows only through its storage
contract, which OtterStax answers with one backend actor per connection. One file, `config.yaml`,
read once at startup, holds the server settings and every connection.

## Connection Config

`config.yaml` — path from `--config PATH` (default `config.yaml`, relative to the working
directory; a missing file = default ports, no connections). Containers run from
`/app/build/Release` with no `--config`: the file is baked (`Dockerfile.test`) or bind-mounted
(compose files) onto `/app/build/Release/config.yaml`. There is no runtime add/remove API: edit and
restart. Full reference: `config/CLAUDE.md`.

```yaml
service:
  flight_sql:    { host: "0.0.0.0", port: 8815 }
  mysql:         { port: 8816 }     # wire-server port (NOT a backend)
  postgres:      { port: 8817 }     # wire-server port (NOT a backend)
  spark_connect: { host: "0.0.0.0", port: 15002, session_timeout_s: 3600 }  # 0 = never expire
  connection_retry: { max_attempts: 10, delay_ms: 2000 }   # default 1 / 1000 = one attempt
  engine:  { data_dir: /tmp/test_collection_sql/base, log_dir: "" }        # log_dir empty = data_dir
  runtime:                          # every thread pool; every key optional
    general: { threads: 3 }         # engine pools (>= 1)
    exec:    { threads: 3 }
    disk:    { threads: 3 }
    sessions: { threads: 0 }        # 0 = max(2, hardware threads)
    backends: { threads: 0 }        # 0 = max(2, hardware threads, sum of every pool_size)
    mysql_io: { threads: 0 }        # frontends: 0 = max(2, hardware threads)
    postgres_io: { threads: 0 }
    flight_sql_io: { threads: 0 }
    spark_connect_io: { threads: 0 }

connections:
  mysql:
    - { alias: mysql, host: demo-mariadb, port: "3306", username: demo, password: demo, database: bill, table: "" }
  postgresql:
    - { alias: pg, host: demo-postgres, port: "5432", username: demo, password: demo, database: shop, schema: shop, table: "" }
  clickhouse:
    - { alias: ch, host: demo-clickhouse, port: "9000", username: demo, password: demo, database: ev, table: "", pool_size: 2 }
  s3:
    - { alias: demo_s3, access_key: rustfsadmin, secret_key: rustfsadmin, region: us-east-1, endpoint: demo-rustfs:9000 }
```

- `port` is an optional string (empty = driver default). `pool_size` (backends, default 4,
  `config::default_pool_size`) = agents of the connection, one physical connection each.
- `schema` (PostgreSQL): a name = only that schema, empty = every user schema of the database.
  MySQL / ClickHouse connections have the one schema `public`.
- `table`: a name makes the connection **that table only** — the startup discovery and its
  reconciliation keep it alone, and a statement missing another table of the connection gets its
  own miss (no discovery is asked); empty = every base table.
- `alias`: the first part of a remote name and the connection's name in otterbrix. A backend
  alias must be an unquoted identifier `[a-z_][a-z0-9_]*`; backend aliases are one namespace
  (mysql / postgresql / clickhouse together), s3 aliases another (any spelling). `federation` and
  `main` are reserved (any case, any section).
- Refused at load (stderr, exit 1): malformed YAML, a section that is not a mapping, an unknown
  key at any level (`unknown key "pool_sise" in connections.mysql[0]`), an incomplete entry, a bad
  port / `pool_size` / thread count, a reserved, malformed or duplicate alias.
  An unreachable backend is not a config error: retried per `connection_retry`, then logged, the
  server starts. A MySQL server under `sql_mode` `NO_BACKSLASH_ESCAPES` is left out at once (no
  retry, no actor, not in the engine's map: a statement naming it is refused), the server starts.
  A PostgreSQL server under `standard_conforming_strings = off` needs nothing: a literal holding a
  backslash is written `E'…'`, which reads the same under either setting.

| Stack | File | Delivery |
|-------|------|----------|
| template | `config.yaml` (repo root) | copy / mount / `--config` |
| demo | `examples/demo/config.yaml`, `config_local.yaml` | mounted by `examples/demo/compose.yml` |
| quick start | `examples/quick_start/config.yaml` | mounted by `examples/quick_start/compose.yml` |
| integration tests | `tests/scripts/config.yaml` | baked by `Dockerfile.test` |
| root manual stack | `scripts/database/config.yaml` | mounted by `compose.yml` |
| benchmarks | `benchmark/config.yaml` | mounted by `benchmark/compose_benchmark.yml` |

### Startup (`main.cpp`)

`main.cpp` owns every part as locals: declaration order = build order, reverse = destruction
order (an aborted startup unwinds the same way).

```text
main.cpp
  → --config PATH; config::load_config        — a refusal to stderr, return 1 (no logger yet)
  → initialize_all_loggers(log_directory(engine))
  → conn::s3::subsystem_finalizer_t           — FinalizeS3 after everything else is gone
  → backends::backends_threads(...)           — the backends pool sized against the connections
  → runtime::runtime_t(runtime)               — memory resource + the five actor-zeta pools
  → backends::remote_backends_t::open(connections, connection_retry)
       per connection: pool_size backend_agent_t (connect), discovery on the first agent (retried),
       a backend_actor_t over its agents; an unreachable one keeps its actor and agents
       (they connect on the first statement); a bad descriptor → return 1
  → runtime::engine_t::open(runtime, engine, remote->primitives())
                                              — otterbrix with one backend address per connection
  → spawn conn::s3::ConnectorManager, FileManager, S3Manager, KafkaManager
  → backends::register_s3_aliases             — an error → return 1
  → backends::register_remote_tables          — the discovery into otterbrix's catalog (no SQL to a
                                                backend); an engine refusal → return 1
  → kafka->recover()
  → session::make_session_services(..., remote->connections())
  → session::session_factory_t
  → Flight SQL / MySQL / PostgreSQL / Spark Connect servers; flight->run() blocks until SIGTERM / SIGINT
```

Shutdown: servers → `~session_factory_t` (sessions drain 30 s, sessions pool stops) → Kafka, S3,
file, s3 connector → engine (final CHECKPOINT) → remote backends (agents cancelled, backends pool
stopped, actors and agents destroyed) → runtime → FinalizeS3.

## Build resources — avoid OOM (check RAM + threads first)

**Size the parallelism to RAM before building (local or Docker).** An otterbrix / Arrow TU needs
~1.5–2 GB per job; `-j nproc` is the #1 cause of `c++: internal compiler error: Killed` and BuildKit
`ResourceExhausted`, especially in Docker Desktop, whose VM is far smaller than the host.

- Check: host `nproc` / `sysctl -n hw.ncpu`, `free -g` / `sysctl -n hw.memsize`; Docker
  `docker info --format '{{.NCPU}} CPUs / {{.MemTotal}} bytes'`.
- `JOBS = min(nCPU, floor(RAM_MB / 1536))` — an 8 GB Docker VM → 5 jobs even on 14 CPUs.
- Apply: `cmake --build build/Release --parallel <JOBS>`, `docker build --build-arg BUILD_JOBS=<JOBS>`,
  `./docker-run-tests.sh -j <JOBS>`. `run_benchmark.sh` / `run_stress_benchmarks.sh` /
  `docker-run-tests.sh` / `benchmark/manual/start_service.sh` auto-cap without `-j`; inside the
  images `BUILD_JOBS=0` resolves to `min(nproc, MemTotal_MB / 1536)`.
- Still `Killed`: halve `JOBS` or raise the Docker Desktop memory limit.

## Build Commands

```bash
# Conan deps (if conanfile.py changed, remove ./build first). As the images build it (Dockerfile:
# cppstd 20, Dockerfile.test: gnu20), every package at the one standard: an abseil built at cppstd 17
# has no operator<=> on absl::int128, which otterbrix's core/date/date_types.hpp:143 compares with.
conan install conanfile.py --build missing -s build_type=Release -s compiler.cppstd=20

# Configure (+ tests) and build
cmake -S . -B build/Release -DBUILD_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake --build build/Release --parallel <JOBS>
# macOS / Homebrew: add -DBISON_EXECUTABLE=/opt/homebrew/opt/bison/bin/bison (the system bison is too old)

# The 12 Catch2 binaries (build/Release/tests/<dir>/; names are the project() names, tests/CLAUDE.md)
./build/Release/tests/system/test_system
./build/Release/tests/session/test_session
./build/Release/tests/runtime/test_runtime
./build/Release/tests/federation/test_federation
./build/Release/tests/mysql-front/test_mysql_front
./build/Release/tests/unit/connectors/test_unit_connectors
./build/Release/tests/unit/parser/test_parser
./build/Release/tests/unit/parser/grammar_extension/kafka/test_kafka_grammar
./build/Release/tests/unit/utility/test_utils
./build/Release/tests/unit/translators/test_unit_translators
./build/Release/tests/unit/config/test_unit_config
./build/Release/tests/unit/spark_connect/test_unit_spark_connect

# Sanitizers (not both at once)
cmake -S . -B build/Release -DENABLE_ASAN=ON -DBUILD_TESTS=ON -DCMAKE_TOOLCHAIN_FILE=...
cmake -S . -B build/Release -DENABLE_TSAN=ON -DBUILD_TESTS=ON -DCMAKE_TOOLCHAIN_FILE=...
# ASAN needs ASAN-built dependencies (asan-deps.profile, "The ASAN image" below): the engine's own
# headers key no layout off sanitizer macros, but gRPC's and abseil's do, and libotterbrix embeds
# and exports the abseil the executable calls. macOS: run with
# ASAN_OPTIONS=detect_container_overflow=0 (containers annotated by our code are freed into the
# uninstrumented engine's pool: false positives). LeakSanitizer is unsupported on Apple Silicon.
# The CI sanitizer workflows export ENABLE_ASAN / ENABLE_TSAN to docker-run-tests.sh, which skips
# the Kafka crash-recovery (kill -9) step.

# Log verbosity (default ERROR; the engine's log follows it)
cmake -S . -B build/Release -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG ...
```

### The TSAN image (`Dockerfile.test`, `TSAN_DEPS=true`)

`tsan-deps.profile` rebuilds the libraries that synchronize through atomics with
`-fsanitize=thread` (otterbrix, actor-zeta, librdkafka, gRPC, abseil, protobuf, re2), and nothing
instrumented runs at build time:

- protoc and grpc_cpp_plugin come from the build context: the profile adds
  `&: protobuf/<v>, grpc/<v>` as `[tool_requires]` (profile-only: in `conanfile.py` it would build a
  second grpc for the build context), and `protobuf::protoc` / `gRPC::grpc_cpp_plugin` resolve to
  them through `CMAKE_PROGRAM_PATH`;
- the TSAN grpc is built with `codegen=False` (OtterStax links `gRPC::grpc++` only);
- Catch2 discovery is `PRE_TEST` under `ENABLE_TSAN` (`tests/CMakeLists.txt`).

A TSAN executable re-executes itself with `personality(ADDR_NO_RANDOMIZE)` on arm64, which Docker's
default seccomp refuses, so the image's `ctest` step cannot pass inside `docker build` on Apple
Silicon (CI builds it on amd64). On arm64 build without it and run the tests in a container:

```bash
docker build -f Dockerfile.test --build-arg TSAN_DEPS=true --build-arg ENABLE_TSAN=ON \
  --build-arg RUN_TESTS=false --build-arg BUILD_JOBS=<JOBS> -t otterstax-tsan .
docker run --rm --security-opt seccomp=unconfined -w /app/build/Release otterstax-tsan \
  ctest -C Release --output-on-failure --timeout 120
```

### The ASAN image (`Dockerfile.test`, `ASAN_DEPS=true`)

An `ENABLE_ASAN=ON` build over the prebuilt packages is not sound: headers change layouts under
ASAN. gRPC's `GRPC_ASAN_ENABLED` adds `leak_checker` to `gpr_mu` (`grpc::ClientContext` 448 → 456
bytes: a bad-free in the core from the Spark gRPC client of `test_system`); abseil's
`ABSL_HAVE_ADDRESS_SANITIZER` turns on swisstable / btree generations (`CommonFields` gains a
generation pointer and crosses into the compiled library). `asan-deps.profile` therefore rebuilds
with `-fsanitize=address` gRPC, abseil, protobuf and re2 (every package that compiles abseil's
containers) and otterbrix (through its own `ENABLE_ASAN`: `libotterbrix` embeds and exports abseil +
re2, and the executable binds its raw_hash_set calls to that copy). actor-zeta, clickhouse-cpp
(abseil's `int128` only) and the rest stay prebuilt. The build tools come from the build context
as in the TSAN image (`[tool_requires]` protoc / grpc_cpp_plugin, grpc `codegen=False`): an ASAN
protoc would fail the codegen on Linux, LeakSanitizer turning a leak into exit code 23.
`ENABLE_ASAN=ON ./docker-run-tests.sh` builds this image (compose passes `ENABLE_ASAN` and
`ASAN_DEPS`); the ASAN workflow passes `ASAN_DEPS=true`. On a host:

```bash
conan install conanfile.py --build missing -pr:h asan-deps.profile -pr:b default \
  -s build_type=Release -s compiler.cppstd=20 -c tools.build:jobs=<JOBS> -of build/asan-deps
ln -sfn build/Release/lib build/asan-deps/lib   # the binaries' rpath lib/ → the generators' lib dir
cmake -S . -B build/asan-deps -DENABLE_ASAN=ON -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=build/asan-deps/build/Release/generators/conan_toolchain.cmake
```

## Docker Workflows

Cap the jobs (see **Build resources**).

```bash
./docker-run-tests.sh -j <JOBS>                     # integration suite + PySpark client matrix
docker build -f Dockerfile.test --build-arg BUILD_JOBS=<JOBS> -t otterstax-test .   # unit tests in the image
python3 fixtures/generate_data.py && docker compose up   # full stack for manual testing
./benchmark/scripts/run_benchmark.sh --repetitions 5
./benchmark/scripts/run_benchmark.sh --perf --frontend postgres --bench simple_select        # CPU call graph
./benchmark/scripts/run_benchmark.sh --perf-alloc --frontend postgres --bench simple_select  # + malloc uprobe
./benchmark/manual/start_service.sh [--rebuild] [--perf] [--perf-alloc]; ./benchmark/manual/stop_service.sh
```

## Architecture Overview

```text
Frontend (MySQL / PostgreSQL / Flight SQL / Spark Connect)          frontend/CLAUDE.md
  → query_session_t (one per client connection, sessions pool)      session/CLAUDE.md
       parser → ownership guards → one ENGINE session per connection: every statement, its
       transaction, its settings, its describe and prepared statements are the engine's
  → otterbrix: names, plan, optimizer, execution
       a local table → otterbrix's disk
       a table of a connection → that connection's backend_actor_t (storage_contract messages)
  → backend_actor_t (one per connection, its own loop thread)       otterbrix/federation/CLAUDE.md
       storage_offer: the blocks the dialect says whole (a SELECT block, a JOIN of the
       connection, an UPDATE / DELETE) become ONE statement (sql_gen); the rest is read
       by scans and written by INSERT … VALUES; the answer conformed to the planned types
  → backend_agent_t (pool_size per connection, backends pool; one physical connection each)
       runs the statement on its connector, translates the rows (tsl::*_to_chunk), wakes the actor
  → the answer back through the engine's cursor → the session → the frontend
       (asio_future_bridge.hpp; Spark: await_future.hpp)
```

- **Registration.** At startup `catalog::register_connections` turns the discovery into otterbrix
  catalog rows — `CREATE DATABASE c.d`, `CREATE SCHEMA c.d.s`, `CREATE TABLE c.d.s.t`, lower case — on
  the actors' registration session, so no SQL reaches a backend; the backend's own spelling is the
  actor's, kept in `federation.public.remote_names`. A restart reconciles the catalog with the new
  discovery (new tables registered, gone ones dropped, changed columns altered). A statement that
  misses a table of a configured connection (created on the backend later) registers it
  (`catalog::registration_target` → the actor's `discover` → `registration_plans`) and runs once
  more if its failure left the session outside a block. `catalog/CLAUDE.md`.
- **Transactions.** The engine keeps the block and stamps its status on every answer
  (`query_session_t::transaction_status()`: PostgreSQL's `I` / `T` / `E`, MySQL's
  `SERVER_STATUS_IN_TRANS`). SAVEPOINT / ROLLBACK TO / RELEASE are the engine's. A PostgreSQL / MySQL
  block holds one agent from its first statement (BEGIN before it, a read too) to its end;
  `SAVEPOINT s<k>` before each savepoint level's first statement; COMMIT = the engine's prepare.
- **Settings.** `SET` / `RESET` go to the engine, per session (it has `timezone`, `autocommit`,
  `search_path`); the frontends translate their own forms (MySQL `SET @@x`, `USE`, `time_zone`) and
  keep only the protocol parameters (PostgreSQL `client_encoding`, `DateStyle`, `application_name`,
  `extra_float_digits`, …; MySQL character sets). `frontend/CLAUDE.md`.
- **Prepared statements** are reusable: the session keeps the text and the engine's describe, and
  each execute binds and runs it again until `close_statement`.
- **Spark Connect**: `spark.sql()` text through `query_session_t::execute` (a query handed back
  unrun as its SQL relation), a DataFrame translated in the frontend
  (`spark_connect_server/plan_translator/`) through `execute_plan` / `prepare_plan`; one query session
  per Spark session.
- Frontends never block on shared state: they poll the session's future through
  `frontend/common/asio_future_bridge.hpp` (Spark: `spark_connect_server/await_future.hpp`), the only
  supported way to pick up an actor result.

### Key Types

- `session::query_session_t` / `session_ptr` — a client connection's session; `statement_id_t`
  per statement (`next_statement()`); `session_result` = `core::result_wrapper_t<session_payload>`
  (schema, chunks, `affected_rows`, tag, `transaction`, `setting_changes`, `savepoint`).
- `components::session::session_id_t` — the engine session (`engine_session()`), one per query session.
- `ParsedQueryData` / `ParsedQueryDataPtr` (`otterbrix/parser/parser.hpp`) — `plan`
  (`ExecutionPlanPtr`, `types/otterbrix.hpp`: otterbrix's `execution_plan_t`, names as written),
  `binder`, `tag`, `extension_kind`, `sql`, `awaiting_bind`.
- `qualified_name_t` — `connection.database.schema.table`, an empty part = not written.
- `backend_type_t` (`otterbrix/backend_type.hpp`) — `{MySQL, PostgreSQL, ClickHouse}`: the dialect
  `sql_gen` writes, the one spelling of a backend's kind.
- `federation::backend_actor_t` / `backend_agent_t` — the storage of a connection's tables / one
  physical connection; `remote_backends_t::primitives()` — the connection → actor address map
  `open_engine` takes.
- `catalog::configured_connection_t` / `connection_discovery_t` — a configured connection with its
  actor / what its startup discovery found.

### Actor Message Pattern

```cpp
auto [needs_sched, future] = actor_zeta::send(target_address, &TargetActor::method, arg1, arg2);
auto result = co_await std::move(future);
```

Every handler is listed in the actor's `dispatch_traits` alias and the `behavior()` coroutine.
actor-zeta 2.0: no blocking `get()` / `wait()` on a future, no `co_await` outside an actor coroutine.

### Names

Names are otterbrix's, as written (unquoted identifiers fold to lower case):

| Written | Means |
|---------|-------|
| `c.d.s.t` | table `t` of connection `c` (its database `d`, schema `s`: `public` on MySQL / ClickHouse) |
| `d.s.t` | a **local** table (three parts are always local) |
| `s.t`, `t` | completed by the session's place, `search_path` (default `main.public`) |

```sql
SELECT o.id, c.name
FROM mysql.bill.public.orders o JOIN pg.shop.shop.customers c ON o.customer_id = c.id;
```

- A column may be qualified by every suffix of its table's name (`c.d.s.t.col`).
- PostgreSQL wire: the StartupMessage `database` sets `search_path = '<db>.public'`; a database the
  engine does not have is `FATAL 3D000` with the hint `The default database is "main".` — clients
  connect with `dbname=main`. MySQL: the handshake database / `USE` / `COM_INIT_DB` set the same.
- Flight SQL catalogs are `d` (local) and `c.d` (a connection's database); a Spark database is a
  schema (`d.s` / `c.d.s`).
- `CREATE DATABASE <alias>` and DDL / DML on `federation.*` are refused (`catalog/ownership.hpp`).

## Profiling Instrumentation (Tracy) — MANDATORY

Every main function MUST carry a Tracy zone: an actor handler (every method in `dispatch_traits` /
`behavior()`), a public top-level entry point of a translation unit, an expensive operation
(parsing, discovery, SQL generation, translation, plan execution).

- `#include "utility/tracy_profiler.hpp"` (with the project's own includes, after the unit's header).
- `OTX_ZONE_N("Scope::function")` as the **first statement**, a stable qualified name
  (`"query_session::execute"`, `"catalog::registration_plans"`, `"backend_actor::storage_offer"`).
- **Coroutines that can resume on another thread or interleave on one** (`query_session_t` on the
  sessions pool, `backend_actor_t` on its loop): open the zone in a nested block
  `{ OTX_ZONE_N(...); ... }` that closes before the first `co_await` — a Tracy zone must end on the
  thread that began it, in LIFO order. Only `assert`s, objects living for the whole coroutine
  (`Timer`, `erase_on_exit_t`) and the variables the block fills may precede it; work after a
  `co_await` opens its own block. An actor whose `enqueue_impl` runs the coroutine to completion on
  the sender's thread (`S3Manager`, `FileManager`) keeps the zone as its first statement.
  `session/CLAUDE.md`, "Tracy zones in coroutines".
- Not instrumented: recursive tree walkers per node (instrument their entry point), trivial getters
  and O(1) lookups.
- Macros (no-ops unless Tracy is enabled): `OTX_ZONE()`, `OTX_ZONE_N(name)`, `OTX_FRAME()`,
  `OTX_PLOT(name, val)`, `OTX_MESSAGE(msg)`, `OTX_LOCKABLE(type, var)`.

## External Tables — File & S3

```sql
CREATE EXTERNAL TABLE eg.regions                       -- t, s.t, d.s.t or c.d.s.t, as CREATE TABLE
    WITH (s3_alias = 'rustfs1', location = 's3://bucket/regions.parquet', format = 'parquet');
CREATE EXTERNAL TABLE eg.weights WITH (location = '/fixtures/weights.csv');   -- format from the extension
COPY (SELECT * FROM eg.regions) TO 's3://bucket/out/regions.csv' WITH (s3_alias = 'rustfs1', format = 'csv');
```

- Formats `parquet` (snappy / brotli / zlib / lz4 / zstd), `csv`, `ndjson`, both directions; the
  `s3_alias` is a `connections.s3` entry.
- Both statements run on the client's engine session (its place, snapshot, transaction): a load
  outside a block is BEGIN / CREATE / INSERT / COMMIT, inside a block part of it. The schema of the
  target must exist (the database is never created); an existing table is `table_already_exists`.
- A loader answers a run of chunks of ≤ 1024 rows (`data_chunks_from_arrow`); an empty file makes a
  table with its columns and no row. S3 objects stream through `tsl::arrow_stream_t` (one owner,
  move-only; an output let go unclosed is aborted) with no local staging file; a COPY to a local
  path writes a temporary sibling renamed over the target only on success.
- Flow: `query_session_t::load_external_table` → `FileManager::load_file` (local) or
  `S3Manager::download` → `engine::create_table_plan` + `engine::load_table`; `copy_to` → the inner
  query (a query only) on the engine → `FileManager::dump_file` / `S3Manager::upload`.
- Details: `otterbrix/CLAUDE.md` (translators), `otterbrix/parser/grammar_extention/CLAUDE.md`
  (grammar), `connectors/CLAUDE.md` (s3 / file).

**JOIN-key widths must agree.** An equi-JOIN of an int32 column with an int64 one answers no rows,
silently: declare a local column `bigint` against parquet's int64, and widen or stage a backend
`INT` (int32) before joining it with a local `bigint` — or join on a string key
(`tests/CLAUDE.md`, "JOIN-key width sensitivity").

## Kafka

`CREATE / DROP SOURCE | STREAM` (grammar `otterbrix/parser/grammar_extension/kafka/`, runtime
`integration/kafka/`, `KafkaManager`). The objects are tables of the schema `main.kafka`
(`main.kafka.<obj>`); the bookkeeping is `federation.kafka.__sources` / `<source>__offsets`. An
INSERT whose target, completed by the session's place, is `main.kafka.<obj>` produces to Kafka.
`integration/kafka/CLAUDE.md`.

## Directory Map

| Directory | CMake target | Role |
| --------- | ------------ | ---- |
| `config/` | `config` | `config.yaml` reader: `service:` + `connections:` (no connector dependency). `config/CLAUDE.md` |
| `runtime/` | `runtime` | `runtime_t` (memory resource, the pools general / exec / disk / sessions / backends) and `engine_t` (the embedded otterbrix). `runtime/CLAUDE.md` |
| `backends/` | `backends` | `remote_backends_t` (agents + backend actor per connection, startup discovery with `connection_retry.hpp`, `primitives()`), `register_s3_aliases`, `register_remote_tables` |
| `otterbrix/federation/` | `otterstax_federation` | `backend_actor_t` (storage contract, `storage_offer`, transactions, DDL), `backend_agent_t`, `reachability_t`, `conform_to_result`, `federation.public.remote_names`. `otterbrix/federation/CLAUDE.md` |
| `catalog/` | `catalog`, `catalog_discovery` | discovery queries / replies (`discovery.hpp`), registration in otterbrix's catalog (`registration.hpp`), owned names (`ownership.hpp`). `catalog/CLAUDE.md` |
| `session/` | `session` | `query_session_t`, `session_factory_t`, `make_session_services`, `plan_inspection`. `session/CLAUDE.md` |
| `otterbrix/` | `otterbrix_local` | parser (`parser/`), `sql_gen` (`query_generation/`), translators (`translators/`), engine requests (`operators/engine_requests.hpp`), s3 / file grammar (`parser/grammar_extention/`). `otterbrix/CLAUDE.md` |
| `otterbrix/parser/grammar_extension/kafka/` | `kafka_grammar` | Kafka DDL grammar, Kafka names |
| `connectors/` | `connectors`, `s3`, `file` | MySQL (Boost.MySQL) / PostgreSQL (libpq) / ClickHouse (clickhouse-cpp) connectors, the s3 connector actor (`conn::s3::ConnectorManager`, Arrow `S3FileSystem`), `conn::file::FileManager`. `connectors/CLAUDE.md` |
| `integration/` | `integration`, `kafka_runtime` | `db::S3Manager` (s3 ↔ file streams); `kafka/` the Kafka runtime. `integration/CLAUDE.md`, `integration/kafka/CLAUDE.md` |
| `frontend/` | `common_server`, `flight_sql`, `mysql_server`, `postgres_server`, `spark_connect_server` | the four servers. `frontend/CLAUDE.md` |
| `types/` | (header-only) | `ExecutionPlanPtr`, `plan_root` |
| `utility/` | (header-only) | logger, Tracy macros, `asio_error`, `wait_barrier`, `thread_pool_manager`, `parse_port`, `table_info`, … `utility/CLAUDE.md` |
| `cmake/` | — | `otterbrix_parser_extension.cmake` (flex + bison extensions), conan hooks |
| `tests/` | the 12 binaries above | Catch2 suites + the python integration suite and PySpark client matrix. `tests/CLAUDE.md` |
| `benchmark/`, `examples/` | — | benchmark harness (`benchmark/CLAUDE.md`), demo and quick start stacks |

## Known Constraints

**Transactions and writes on a backend**

- A PostgreSQL / MySQL block holds one agent (connection) of its connection to its end: more open
  blocks than `pool_size` wait for a free agent up to 60 s, then `io_error`. A client that closes its
  connection inside a block (a driver with autocommit off) frees the agent at once: the session's
  close ends its transaction on every backend actor itself (`backend_actor_t::end_session`), not
  through otterbrix's ROLLBACK, which waits for one of otterbrix's executors (four, each running one
  statement at a time) — all of them may be running statements that wait for that agent. A COMMIT /
  ROLLBACK a client sends while every executor runs such a waiting statement still waits for an
  executor until those statements' checkout timeout (a waiting statement holds its otterbrix executor).
- A block writes to one connection: a write to a second connection in the same block is
  `unimplemented_yet` and fails the block (otterbrix's one write place).
- ClickHouse has no transactions: a write inside a block is refused (`unimplemented_yet`).
- A ClickHouse UPDATE / DELETE is a mutation: one that fails on a part is killed by the agent
  (`KILL MUTATION` of the one mutation ClickHouse names, so it is not retried forever) and the
  statement answers its error, but the parts it already rewrote keep the change (no rollback). When
  ClickHouse names several mutations failing together, none is killed (the error says so).
- An UPDATE / DELETE of a remote table runs only as ONE statement of its backend; one the dialect
  cannot say whole (a JOIN with another server or a local table, a correlated sub-query over one, an
  expression with no literal in the dialect) is `unimplemented_yet` — OtterStax numbers no remote
  row. An uncorrelated sub-query over a local table (`WHERE id IN (SELECT id FROM <local table>)`)
  is no obstacle: otterbrix computes it first and the backend gets its values (`IN (2, 3)`).
  RETURNING: PostgreSQL only.
- otterbrix casts an UPDATE value to its column's type, and a literal is BIGINT / DOUBLE: `SET i = i + 1`
  (INTEGER), `SET f = 2.5` (FLOAT) narrow, range-checked. MySQL / ClickHouse have no such CAST, so the
  backend's store does the check (otterbrix does not mark the assignment's cast): MySQL writes the value uncast, which only a
  session under `STRICT_TRANS_TABLES` / `STRICT_ALL_TABLES` refuses out of range (ER 1264) — a session
  without a strict `sql_mode` refuses such an UPDATE (`unimplemented_yet`); on a non-transactional
  (MyISAM) table under `STRICT_TRANS_TABLES` alone MySQL clamps after the first row, and a computed
  DOUBLE below FLOAT's smallest value is stored as 0 where the engine refuses it. ClickHouse:
  `if(isNull(x), NULL, accurateCast(x, 'Nullable(Int32)'))` for an integer (a NULL row stays NULL);
  a DOUBLE into Float32 only as a bound value
  (`f = f + 1` is `unimplemented_yet`). A fraction into an integer column stays `unimplemented_yet`
  on both (MySQL rounds a DECIMAL half away from zero, the engine to even).
- An INSERT that omits a column sends NULL for it: the backend's DEFAULT / serial / AUTO_INCREMENT is
  lost (NOT NULL → `not_null_violation`) — otterbrix leaves no column to the backend's DEFAULT. An INSERT … SELECT of one backend is
  not pushed down (its rows pass through the engine).
- Remote DDL (`CREATE / DROP / ALTER TABLE c.d.s.t`) runs only in autocommit (otterbrix refuses it
  inside a block, `unimplemented_yet`): otterbrix's storage contract has no DDL inside a transaction. MySQL / ClickHouse DDL commits on the
  backend at once (an engine failure afterwards is compensated by a DROP); a DROP TABLE / COLUMN is
  sent at COMMIT.
- `CREATE / DROP INDEX` on a remote table is refused by otterbrix (its storage contract has no index).
- `CREATE EXTERNAL TABLE` into a four-part target is not atomic: CREATE and INSERT are two
  autocommit statements.
- A backend's COMMIT that succeeded while otterbrix's own commit marker failed leaves the outcome in
  doubt (no two-phase commit).
- The count of a MySQL UPDATE is the rows it matched (read off the OK packet's info: Boost.MySQL sets
  no `CLIENT_FOUND_ROWS`); ClickHouse answers `written_rows` for an INSERT and 0 for a mutation.

**Connections and names**

- `table:` of a connection makes it that one table: the startup discovery and a miss register it
  alone (a miss of another table of it is the statement's own miss, no discovery sent), and DDL of
  any other table of it written in four parts — CREATE / ALTER / DROP TABLE, CREATE INDEX, CREATE
  EXTERNAL TABLE — is refused by the session before the engine and the backend (`invalid_parameter`
  `connection "<c>" is configured with table "<X>"`, `catalog::check_table_scope`; a short name in a
  place that is the connection's database is not seen). Without `table:` a miss registers any table
  of the connection's database (and, on PostgreSQL, of its configured schema or any schema when none
  is).
- Registration by a miss looks the table up by the written name, which otterbrix keeps in lower
  case: a backend table spelled with upper-case letters is found only by the startup discovery.
- A connection unreachable at startup keeps its actor and its catalog rows of the last discovery;
  a statement then fails at the connection. A failed connect marks the connection unreachable for
  10 s: until then every statement that needs a connect fails at once (`io_error` `connection
  "<alias>" is unreachable`). A table never registered on such a connection answers that failure
  of the connection (the miss's discovery could not reach the backend), not otterbrix's "does not
  exist"; a backend that answers without the table leaves the miss.
- A connection removed from `config.yaml` keeps its catalog databases (a warning in the BACKENDS log).
- A MySQL session under `sql_mode` `NO_BACKSLASH_ESCAPES` is refused (`unimplemented_yet`: the
  generated literals double a backslash): every agent reads `@@SESSION.sql_mode` of every session it
  opens, at startup and at every reconnect (`backend_agent_t::check_session`). At startup the
  connection is left out on that first answer (`open_failure_kind_t::unusable`, no retry): no actor,
  no address in the engine's map, not among the sessions' connections — a statement naming it is
  refused (`undefined_object`, its catalog rows of an earlier start included), the other connections
  serve. A server switched to it while OtterStax runs is found at the agent's next connect: that
  statement is refused before any SQL of it is sent, the session closed, and the next statement
  connects and asks again. PostgreSQL's `standard_conforming_strings` is not checked: a string
  literal holding a backslash is written as an escape string constant `E'…'` with the backslash
  doubled (`sql_gen`, `catalog::escape_sql_literal`), which reads the same under on and off.
- An agent runs its statement (and a reconnect: connect bounded by 10 s) on a backends-pool thread;
  MySQL / PostgreSQL bound no statement, so a hung backend holds that thread until the shutdown
  cancels the agents (no cancel of a running statement from the client).
- One `remote_backends_t` per `runtime_t`: its destructor stops the runtime's backends pool.
- `storage_total_rows` answers a constant estimate (1000), no query (otterbrix asks for no table size).

**Results and semantics**

- A pushed-down expression is evaluated by the backend with its semantics (division by zero: MySQL
  NULL, PostgreSQL an error, ClickHouse `inf`); the result *types* are always the engine's
  (`conform_to_result`).
- `count(*)` is UBIGINT (otterbrix): MySQL wire unsigned BIGINT (Connector/J `getObject` →
  `BigInteger`), PostgreSQL int8.
- Arrays of one dimension only (`TODO: multiple dimentions array`, `sql_query_generator.cpp`).

**Sessions and drivers**

- SET / SHOW a driver sends that the engine does not have is refused (otterbrix has neither the
  settings drivers send nor SHOW): MySQL Connector/J `SQL_SELECT_LIMIT`, Rails `sql_mode` /
  `wait_timeout` (`1149`), a PostgreSQL `SHOW` of anything but the client parameters
  (`application_name`, `client_encoding`, `extra_float_digits`) — `SHOW DateStyle`,
  `SHOW server_version`, … go to the engine, which has no SHOW.
- `DISCARD ALL` is refused (outside a block by the engine; inside `25001`): otterbrix has no
  DISCARD. pgbouncer: `server_reset_query = 'RESET ALL; DEALLOCATE ALL'`.
- No `NoticeResponse` (an `application_name` cut to 63 bytes, a `SET LOCAL` outside a block warn
  nothing).
- A client-parameter `SET` (`application_name`, …) run through Execute without BEGIN, followed by an
  error before Sync, is not undone (PostgreSQL's implicit pipeline transaction).
- The startup `TimeZone` is reported as the engine keeps it, `utc` (the engine's session settings
  cannot be read back).
- Server-wide settings are refused from a client (`unimplemented_yet`): `ALTER SYSTEM`, `ALTER
  DATABASE | ROLE … SET`, MySQL `SET GLOBAL | PERSIST | @@global.x`.
- Flight SQL keeps no transaction across RPCs: a statement leaving its session inside a block is
  rolled back and refused.
- A statement a frontend stopped waiting for (90 s, `asio_future_bridge.hpp`) runs to its end; inside
  a block the frontend fails the block behind it (MySQL `ER_QUERY_TIMEOUT`).
- A Spark session idle for `session_timeout_s` is closed (its block rolled back); its id is then
  refused `[INVALID_HANDLE.SESSION_CLOSED]`.
- A COPY rolled back leaves its file / object written (files are not transactional).
- Waiting is a poll: a settled actor-zeta future wakes nobody, so
  the query session (sessions pool) and the engine's executor spin while a statement waits — ~3 CPU-s
  per second of waiting (`[.engine-defect-otb-notify]`, `tests/session/test_session_lifetime.cpp`).
  The backend actor's loop is woken by its agents.
- Docker MariaDB volumes lag on cold start — `docker-run-tests.sh` waits up to 120 s.

## Critical Dependency Versions (`conanfile.py`)

- otterbrix `1.0.0b2-rc-4#0cdaa27b38b1da19885a7b44e01fa4e7` from `conan.otterbrix.com` — tag `1.0.0b2-rc-4`
  on otterbrix `main` `01ec5268` (PR #850 merged; the tree of `feat/otterstax-requests` @ `af99cccf`). The remote
  holds only a Linux x86_64 gcc 13 Release static binary; OtterStax links it shared (`otterbrix/*:shared=True`,
  `conanfile.py`), so `conan install … --build missing` builds it from the remote recipe against the abseil / re2 /
  boost overrides of `conanfile.py`. One `libotterbrix.so`. The TSAN image passes `--build="otterbrix/*"
  --build="actor-zeta/*"`, the ASAN image `--build="otterbrix/*"` (instrumentation).
- actor-zeta `2.0.0#887bc992cac0ead59e5bdf5c90c844f4` from `conan.otterbrix.com` (Linux x86_64 gcc 13
  binary; other configurations `--build missing`).
- Arrow 24.0.0 (shared; s3, parquet, csv, json; snappy / brotli / zlib / lz4 / zstd)
- Boost 1.88.0, gRPC 1.69.0 + asio-grpc 3.7.0 (`backend=boost`), libpq 15.4, clickhouse-cpp 2.6.1,
  librdkafka 2.14.2, yaml-cpp 0.7.0, spdlog 1.15.1, fmt 11.1.3, Catch2 3.15.1, tracy 0.13.1 (optional)
- Spark Connect protocol 4.2.0 (protos vendored in `frontend/spark_connect_server/proto/`; the server
  answers `SparkVersion` 4.2.0); PySpark clients 3.5.0–4.2.0 (20 versions) in the E2E matrix
  (`Dockerfile.spark-test`, `tests/CLAUDE.md`).
