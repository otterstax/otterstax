# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
It is the map; every module with a `CLAUDE.md` of its own holds the details.

## What This Project Is

OtterStax is a federated SQL server: wire frontends + the embedded otterbrix engine + remote backends.
Clients connect over the MySQL wire (8816), the PostgreSQL wire (8817), Arrow Flight SQL (8815, an
in-house server, no Arrow Flight dependency) or Spark Connect (15002, gRPC, for PySpark: `spark.sql()`,
DataFrames, `spark.catalog`). Otterbrix plans and executes queries against local and remote tables.
The wire frontends handle a limited set of SQL statements directly, such as PostgreSQL's
`SHOW DateStyle`. See `frontend/CLAUDE.md` for the supported cases.
A table of a remote backend (MariaDB / MySQL, PostgreSQL, ClickHouse) is an ordinary row of
otterbrix's catalog in a database of a *connection*. Otterbrix reaches the table's rows through its
storage contract, which OtterStax answers with one backend actor per connection. One file,
`config.yaml`, read once at startup, holds the server settings and every connection.

For SQL against a running server, start with `.claude/skills/otterstax-sql/SKILL.md`.
The quick start tables and runnable federated JOIN are in `examples/quick_start/README.md`
and the root `readme.md`. Quick start and demo share port 8817 and use different
connection aliases. Confirm the server's aliases before choosing an example.

## Connection Config

`config.yaml` — path from `--config PATH` (default `config.yaml`, relative to the working
directory). A missing file, or one without `service.engine.data_dir`, refuses the startup (stderr,
exit 1). Containers run from `/app/build/Release`: the test image (`Dockerfile.test`) bakes the file
there and passes no `--config`; the public image (`Dockerfile`, stage `runtime`) runs
`./server --config /otterstax/config.yaml`, a baked file that names only `data_dir: /otterstax/data`
(the volume), and compose files bind-mount their own over it. There is no runtime add/remove API:
edit and restart. Full reference: `config/CLAUDE.md`.

```yaml
service:
  flight_sql:    { host: "0.0.0.0", port: 8815 }
  mysql:         { port: 8816 }     # wire-server port (NOT a backend)
  postgres:      { port: 8817 }     # wire-server port (NOT a backend)
  spark_connect: { host: "0.0.0.0", port: 15002, session_timeout_s: 3600 }  # 0 = never expire
  connection_retry: { max_attempts: 10, delay_ms: 2000 }   # default 1 / 1000 = one attempt
  timezone: UTC                     # the server's: every session's TimeZone until its client sets one
  engine:  { data_dir: /tmp/test_collection_sql/base, log_dir: "" }        # data_dir required; log_dir empty = data_dir
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
  A MySQL / ClickHouse connection sees every user database of its server (each a namespace);
  its `database:` is the connection's default namespace (and, with `table:`, that table's database).
- `table`: a name makes the connection **that table only** — the startup discovery and its
  reconciliation keep it alone, and a statement missing another table of the connection gets its
  own miss (no discovery is asked); empty = every base table.
- `alias`: the first part of a remote name and the connection's name in otterbrix. A backend
  alias must be an unquoted identifier `[a-z_][a-z0-9_]*`; backend aliases are one namespace
  (mysql / postgresql / clickhouse together), s3 aliases another (any spelling). `local`,
  `federation` and `main` are reserved (any case, any section).
- Refused at load (stderr, exit 1): a missing file, no `service.engine.data_dir`, malformed YAML, a section that is not a mapping, an unknown
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
| demo | `examples/demo/config.yaml` | mounted by `examples/demo/compose.yml`; `examples/demo/config_local.yaml` is the local server's (`examples/demo/up.sh --local`) |
| quick start | `examples/quick_start/config.yaml` | mounted by `examples/quick_start/compose.yml` |
| integration tests | `tests/scripts/config.yaml` | baked by `Dockerfile.test` |
| root manual stack | `scripts/database/config.yaml` | mounted by `compose.yml` |
| benchmarks | `benchmark/config.yaml` | mounted by `benchmark/compose_benchmark.yml` |

### Startup (`main.cpp`)

`main.cpp` owns every part as locals: declaration order = build order, reverse = destruction
order (an aborted startup unwinds the same way).

```text
main.cpp
  → --config PATH; config::load_config        — a missing file, a refusal or no data_dir: stderr, return 1 (no logger yet)
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
  `./docker-run-tests.sh -j <JOBS>`. `benchmark/scripts/run_benchmark.sh` / `benchmark/stress/run_stress_benchmarks.sh` /
  `docker-run-tests.sh` / `benchmark/manual/start_service.sh` auto-cap without `-j`; inside the
  images `BUILD_JOBS=0` resolves to `min(nproc, MemTotal_MB / 1536)`.
- Still `Killed`: halve `JOBS` or raise the Docker Desktop memory limit.

## Build Commands

Install Bison and Flex before configuring a host build. The Kafka parser
requires both even with tests disabled.

```bash
# Conan deps (if conanfile.py changed, remove ./build first). Use one standard for all
# packages (Dockerfile.deps uses gnu20): an abseil built at cppstd 17 has no operator<=>
# on absl::int128, which otterbrix's core/date/date_types.hpp:144-146 compares with.
conan install conanfile.py --build missing -s build_type=Release -s compiler.cppstd=20

# Configure (+ tests) and build
cmake -S . -B build/Release -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON \
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
# The CI sanitizer workflows export ENABLE_ASAN / ENABLE_TSAN to docker-run-tests.sh. Its Kafka
# crash-recovery (kill -9) step runs in the plain suite and is skipped under Tracy / ASAN / TSAN.

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
docker build -f Dockerfile.test --build-arg DEPS_IMAGE="$(./scripts/deps-image.sh tsan)" \
  --build-arg TSAN_DEPS=true --build-arg ENABLE_TSAN=ON \
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
# The target is relative to build/asan-deps. Conan copies ASAN libraries into
# build/asan-deps/build/Release/lib, alongside its generators directory.
ln -sfn build/Release/lib build/asan-deps/lib
cmake -S . -B build/asan-deps -DENABLE_ASAN=ON -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=build/asan-deps/build/Release/generators/conan_toolchain.cmake
```

## Docker Workflows

Cap the jobs (see **Build resources**).
Install Python Faker before generating data for the manual stack.

### Dependency images (`Dockerfile.deps`)

`Dockerfile` and `Dockerfile.test` compile only OtterStax: they start `FROM ${DEPS_IMAGE}`, an
image that already holds the system packages, conan and every conan package of `conanfile.py`
(`-s compiler.cppstd=gnu20`). Three variants, three packages on ghcr.io:

| Variant | Image | Holds |
|---|---|---|
| `release` | `ghcr.io/otterstax/otterstax-deps:<tag>` | every package, plain |
| `asan` | `ghcr.io/otterstax/otterstax-deps-asan:<tag>` | `release` + the 5 packages `asan-deps.profile` instruments |
| `tsan` | `ghcr.io/otterstax/otterstax-deps-tsan:<tag>` | `release` + the 7 packages `tsan-deps.profile` instruments, g++-12 |

- The sanitizer images are built `FROM` the release one: a package its profile does not touch has
  the same conan package id, so it is taken from the cache (44 of 49 under ASAN, 42 under TSAN).
- `<tag>` is a hash of `Dockerfile.deps`, `conanfile.py`, the two profiles and the librdkafka conan
  hook (`scripts/deps-image.sh --tag`): a change of any names a new image, anything else reuses
  the published one.
- `./scripts/deps-image.sh [release|asan|tsan]` puts the image into the local store and prints its
  reference: pulled on amd64, built here otherwise (the registry holds Linux amd64 only — on an
  arm64 Mac the first run compiles every dependency, once for both Dockerfiles). It also tags
  `<name>:local`, the default `DEPS_IMAGE`, so a bare `docker build` / `docker compose build` works
  afterwards. `docker-run-tests.sh` and the benchmark scripts call it themselves.
- Both Dockerfiles still run `conan install`: over the image it only writes the generators; a
  package the image lacks (a `conanfile.py` newer than the image, Tracy) is built there.
- CI: the `Dependency images` workflow publishes the images of a commit's tag (a run whose images
  exist ends at once); every other workflow waits for its image (`.github/actions/deps-image`)
  and passes it as `DEPS_IMAGE`. A pull request from a fork cannot publish: it works while it
  leaves the tag's files alone.

```bash
./docker-run-tests.sh -j <JOBS>                     # integration suite + PySpark client matrix
./scripts/deps-image.sh release                     # once: the dependency image, tagged otterstax-deps:local
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
       parser → ownership guards → one ENGINE session per connection for query execution,
       transactions, engine settings, describe and prepared statements
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
  catalog rows — `CREATE DATABASE c.d` (one per connection: PostgreSQL its `database:`, MySQL /
  ClickHouse `def`), `CREATE SCHEMA c.d.ns` (a namespace of the backend: PostgreSQL schema, MySQL /
  ClickHouse database), `CREATE TABLE c.d.ns.t`, lower case — on
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
  `extra_float_digits`, `idle_session_timeout`, …; MySQL character sets, `wait_timeout`). `frontend/CLAUDE.md`.
- **Idle timeout.** A MySQL / PostgreSQL connection past its handshake is closed after
  `IDLE_TIMEOUT_SEC` (28800 s, `frontend/common/protocol_config.hpp`) without a message; each
  connection sets its own — MySQL `SET [SESSION] wait_timeout = N` (`@@wait_timeout`), PostgreSQL
  `SET idle_session_timeout = '5min'` (or its StartupMessage; `0` never) — and the handshake and a
  message's body keep `CONNECTION_TIMEOUT_SEC` (30 s).
- **Prepared statements** are reusable: the session keeps the text and the engine's describe, and
  each execute binds and runs it again until `close_statement`.
- **Spark Connect**: `spark.sql()` text through `query_session_t::execute` (a query handed back
  unrun as its SQL relation), a DataFrame translated in the frontend
  (`frontend/spark_connect_server/plan_translator/`) through `execute_plan` / `prepare_plan`; one query session
  per Spark session.
- Frontends never block on shared state: they poll the session's future through
  `frontend/common/asio_future_bridge.hpp` (Spark: `frontend/spark_connect_server/await_future.hpp`), the only
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

A table is `connection.namespace.table`: the first part is always a connection, or `local` for the
clients' own tables (unquoted identifiers fold to lower case).

| Written | Means |
|---------|-------|
| `uid.ns.t` | table `t` of connection `uid`; `ns` is the backend's database on MySQL / ClickHouse, its schema on PostgreSQL (whose database is the connection's `database:`) |
| `local.ns.t` | a local table: `local` is the engine database of the clients' tables, `ns` its schema |
| `s.t`, `t` | completed by the session's place — only one the client chose (below); without it, refused |

```sql
SELECT o.id, c.name FROM mysql.bill.orders o JOIN pg.shop.customers c ON o.customer_id = c.id;
```

- Through one connection every backend names two levels — PostgreSQL a schema of the database the
  connection is open on (it reads no other database), MySQL / ClickHouse a database of the server —
  so the alias is the SQL catalog and every name has three parts. Four parts are refused
  (`a table is named connection.namespace.table`, `query_session_t::guard_ownership`).
- Layout: a connection is one engine database (PostgreSQL: its `database:`; MySQL / ClickHouse: `def`,
  `catalog::def_database`), its namespaces that database's schemas; otterbrix reads a first part that
  is a connection as that database (`lookup_database`), so nothing rewrites a name. A MySQL /
  ClickHouse connection sees every user database of its server.
- A column is qualified by at most `uid.ns.t.col`; a table under an alias by its alias only.
- A quoted qualifier holding dots whose first segment is a connection or `local` is the name it spells
  (an ORM's one schema slot): `"pg.public"."items"` — and MySQL's `` `pg.public`.items `` — is
  `pg.public.items`; an alias `AS "pg.total"` and any other quoted name keep their dots
  (`catalog::split_dotted_names`, run on every text `query_session_t::parse_sql` parses).
- A call of a host function (`version()`, and `obj_description` / `col_description` (NULL) until otterbrix
  has them; registered in `local.public` at startup) written bare or as `pg_catalog.<name>()` is written
  `local.public.<name>()` before the parse, and `pg_table_is_visible(x)` as a sub-query over pg_catalog and
  the session's place
  (`catalog::qualify_host_calls`): otterbrix keeps no host function in pg_catalog, and a call naming
  `local.public` reaches it from any place.
- **The place** is the client's choice: the PostgreSQL StartupMessage `database`, the MySQL handshake
  database / `USE` / `COM_INIT_DB`, Spark's current database name `uid` (its default namespace:
  PostgreSQL's configured schema or `public`, MySQL / ClickHouse's configured database), `uid.ns`,
  `local` (`local.public`) or `local.ns` (`catalog::client_place`, `query_session_t::use`). At PostgreSQL
  startup an unknown connection alias gives `FATAL 3D000` and a missing namespace gives `FATAL 3F000`.
  MySQL returns `1049` in either case. The PostgreSQL StartupMessage sets the initial place.
  A client can change it with `SET search_path = 'local.ns'` using an existing namespace.
  The startup sets `ALTER SYSTEM SET search_path = ''` as the default. Short names fail while
  the session has no place.
- Namespace DDL lands in `local`: `CREATE / DROP SCHEMA x` is `local.x` wherever the session stands;
  `CREATE / DROP DATABASE x` is `local.x` for a MySQL or Spark client (its database is a schema) and
  refused for a PostgreSQL or Flight SQL one (its database is a connection; connections come from
  config.yaml) — `plan::place_namespace_ddl` (`session/query_session.hpp`).
- `local`, `main` and `federation` are reserved aliases; an alias that is the name of an existing
  local database is refused at startup (`catalog::register_connections`). No client makes or drops an
  engine database (the namespace DDL above), so a connection's and OtterStax's own are out of its
  reach; DDL / DML on `federation.*` and a DROP of `local.kafka` are refused (`catalog/ownership.hpp`).
- Flight SQL catalogs are still `d` (local) and `c.d` (a connection's database); a Spark database is
  `uid.ns` / `local.ns`.

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
CREATE EXTERNAL TABLE eg.regions                       -- t, s.t or x.s.t (three parts), as CREATE TABLE
    WITH (s3_alias = 'rustfs1', location = 's3://bucket/regions.parquet', format = 'parquet');
CREATE EXTERNAL TABLE eg.weights WITH (location = '/fixtures/weights.csv');   -- format from the extension
COPY (SELECT * FROM eg.regions) TO 's3://bucket/out/regions.csv' WITH (s3_alias = 'rustfs1', format = 'csv');
```

- Formats `parquet` (snappy / brotli / zlib / lz4 / zstd), `csv`, `ndjson`, both directions; the
  `s3_alias` is a `connections.s3` entry.
- Both statements use the client's engine session (its place, snapshot and transaction). A local
  load outside a block is BEGIN / CREATE / INSERT / COMMIT. Inside a block it participates in that
  block. A remote load uses separate autocommit statements, so a failed INSERT can leave an empty
  table. The target schema must exist (the database is never created). An existing table is
  `table_already_exists`.
- A loader answers a run of chunks of ≤ 1024 rows (`data_chunks_from_arrow`); an empty file makes a
  table with its columns and no row. S3 objects stream through `tsl::arrow_stream_t` (one owner,
  move-only; an output let go unclosed is aborted) with no local staging file; a COPY to a local
  path writes a temporary sibling renamed over the target only on success.
- Flow: `query_session_t::load_external_table` → `FileManager::load_file` (local) or
  `S3Manager::download` → `engine::create_table_plan` + `engine::load_table`; `copy_to` → the inner
  query (a query only) on the engine → `FileManager::dump_file` / `S3Manager::upload`.
- Details: `otterbrix/CLAUDE.md` (translators), `otterbrix/parser/grammar_extention/CLAUDE.md`
  (grammar), `connectors/CLAUDE.md` (s3 / file).

**Mixed integer JOIN keys.** Equal `INT` and `BIGINT` keys match on rc-7. Verified paths include
local tables, MariaDB `INT` joined to local `BIGINT`, and Parquet-loaded `BIGINT` joined to either
a local `INT` or a MariaDB `INT`.

## Kafka

`CREATE / DROP SOURCE | STREAM` (grammar `otterbrix/parser/grammar_extension/kafka/`, runtime
`integration/kafka/`, `KafkaManager`). The objects are named in the schema `local.kafka`
(`local.kafka.<obj>`, `kafka.<obj>` from a place in `local`); the bookkeeping is
`federation.kafka.__sources` / `<source>__offsets`. The session routes an INSERT to Kafka when
its target resolves to a registered object in `local.kafka`. `INSERT … VALUES` publishes once.
`INSERT … SELECT` requires a STREAM target and registers a continuous query.
See `integration/kafka/CLAUDE.md`.

## Application Integration

How an application or ORM uses OtterStax: `docs/integration.md` (the general approach, for any
language), with the framework code (Django, SQLAlchemy) and the runnable example
`examples/integration/python_orm/` in `examples/integration/CLAUDE.md`. In short:

- clients connect over the PostgreSQL wire, the database name is the session's place (`local`
  reaches every connection), and an ORM's schema slot takes `alias.namespace`
  (`catalog::split_dotted_names`);
- the backends own their schemas: no reflection (pg_catalog lacks the columns ORMs read), a remote
  `CREATE TABLE` without keys / defaults / `serial` and only in autocommit, no generated ids in `local`
  tables — so a framework's own tables (migrations, auth) live in an ordinary database beside
  OtterStax;
- one written backend per transaction; a backend table created after startup fails its first use
  inside a transaction (the miss's registration retries only outside a block).

A change to what a client driver sees — the wire frontends, pg_catalog and the catalog functions, name
resolution, transactions, generated ids, types — re-runs the example and updates both documents
(`examples/integration/CLAUDE.md`, "When the server changes").

## Directory Map

| Directory | CMake target | Role |
| --------- | ------------ | ---- |
| `config/` | `config` | `config.yaml` reader: `service:` + `connections:` (no connector dependency). `config/CLAUDE.md` |
| `runtime/` | `runtime` | `runtime_t` (memory resource, the pools general / exec / disk / sessions / backends) and `engine_t` (the embedded otterbrix). `runtime/CLAUDE.md` |
| `backends/` | `backends` | `remote_backends_t` (agents + backend actor per connection, startup discovery with `backends/connection_retry.hpp`, `primitives()`), `register_s3_aliases`, `register_remote_tables` |
| `otterbrix/federation/` | `otterstax_federation` | `backend_actor_t` (storage contract, `storage_offer`, transactions, DDL), `backend_agent_t`, `reachability_t`, `conform_to_result`, `federation.public.remote_names`. `otterbrix/federation/CLAUDE.md` |
| `catalog/` | `catalog`, `catalog_discovery` | discovery queries / replies (`catalog/discovery.hpp`), registration in otterbrix's catalog (`catalog/registration.hpp`), owned names (`catalog/ownership.hpp`). `catalog/CLAUDE.md` |
| `session/` | `session` | `query_session_t`, `session_factory_t`, `make_session_services`, `plan_inspection`. `session/CLAUDE.md` |
| `otterbrix/` | `otterbrix_local` | parser (`otterbrix/parser/`), `sql_gen` (`otterbrix/query_generation/`), translators (`otterbrix/translators/`), engine requests (`otterbrix/operators/engine_requests.hpp`), s3 / file grammar (`otterbrix/parser/grammar_extention/`). `otterbrix/CLAUDE.md` |
| `otterbrix/parser/grammar_extension/kafka/` | `kafka_grammar` | Kafka DDL grammar, Kafka names |
| `connectors/` | `connectors`, `s3`, `file` | MySQL (Boost.MySQL) / PostgreSQL (libpq) / ClickHouse (clickhouse-cpp) connectors, the s3 connector actor (`conn::s3::ConnectorManager`, Arrow `S3FileSystem`), `conn::file::FileManager`. `connectors/CLAUDE.md` |
| `integration/` | `integration`, `kafka_runtime` | `db::S3Manager` (s3 ↔ file streams); `integration/kafka/` the Kafka runtime. `integration/CLAUDE.md`, `integration/kafka/CLAUDE.md` |
| `frontend/` | `common_server`, `flight_sql`, `mysql_server`, `postgres_server`, `spark_connect_server` | the four servers. `frontend/CLAUDE.md` |
| `types/` | (header-only) | `ExecutionPlanPtr`, `plan_root` |
| `utility/` | (header-only) | logger, Tracy macros, `asio_error`, `wait_barrier`, `thread_pool_manager`, `parse_port`, `table_info`, … `utility/CLAUDE.md` |
| `cmake/` | — | conan hooks |
| `scripts/` | — | repository scripts: `scripts/format.sh` (the formatter, over `scripts/tools/code-fromat.py`), the root manual stack's `scripts/database/config.yaml` and `scripts/database/healthcheck.sh`, and `scripts/tools/doc_refs_check.py`, which checks that every path the documentation quotes is a tracked file or directory (CI runs `python3 scripts/tools/doc_refs_check.py .` from the repo root; exit 1 on an unresolved one) |
| `tests/` | the 12 binaries above | Catch2 suites + the python integration suite and PySpark client matrix. `tests/CLAUDE.md` |
| `benchmark/`, `examples/` | — | benchmark harness (`benchmark/CLAUDE.md`), demo and quick start stacks, `examples/integration/` (framework integration code and the runnable Django / SQLAlchemy example, `examples/integration/CLAUDE.md`) |

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
  RETURNING: sent to PostgreSQL and MySQL — the backend answers what it has (MariaDB INSERT / DELETE
  … RETURNING, MariaDB 13's UPDATE … RETURNING too; MySQL's and an older MariaDB's UPDATE … RETURNING are the server's
  own refusal); ClickHouse refused.
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
- An INSERT names only the columns the client wrote (`INSERT INTO t (id) …` is `INSERT … ("id") VALUES …`
  on the backend): an omitted column takes the backend's DEFAULT / serial / AUTO_INCREMENT. An INSERT … SELECT
  of one backend is not pushed down (its rows pass through the engine).
- Remote DDL (`CREATE / DROP / ALTER TABLE uid.ns.t`) runs only in autocommit (otterbrix refuses it
  inside a block, `unimplemented_yet`): otterbrix's storage contract has no DDL inside a transaction. MySQL / ClickHouse DDL commits on the
  backend at once (an engine failure afterwards is compensated by a DROP); a DROP TABLE / COLUMN is
  sent at COMMIT.
- `CREATE / DROP INDEX` on a remote table is refused by otterbrix (its storage contract has no index).
- `CREATE EXTERNAL TABLE` into a connection's table is not atomic: CREATE and INSERT are two
  autocommit statements.
- A backend's COMMIT that succeeded while otterbrix's own commit marker failed leaves the outcome in
  doubt (no two-phase commit).
- The count of a MySQL UPDATE is the rows it matched (read off the OK packet's info: Boost.MySQL sets
  no `CLIENT_FOUND_ROWS`); ClickHouse answers `written_rows` for an INSERT and 0 for a mutation.
- An INSERT's generated id (the MySQL wire's `last_insert_id`, `cursor.lastrowid`) is a MySQL backend's
  AUTO_INCREMENT only — its OK packet's `last_insert_id`, the first of the statement (otterbrix's
  `write_receipt_t::generated_id` → `cursor_t::generated_id()` → `session_payload::generated_id`); an
  INSERT into a PostgreSQL / ClickHouse connection or a local table reports 0.

**Connections and names**

- `table:` of a connection makes it that one table: the startup discovery and a miss register it
  alone (a miss of another table of it is the statement's own miss, no discovery sent), and DDL of
  any other table of it named `uid.ns.t` — CREATE / ALTER / DROP TABLE, CREATE INDEX, CREATE
  EXTERNAL TABLE — is refused by the session before the engine and the backend (`invalid_parameter`
  `connection "<c>" is configured with table "<X>"`, `catalog::check_table_scope`; a short name in a
  place that is the connection's database is not seen). Without `table:`, discovery on a miss looks
  for `uid.ns.t` in database `ns` on MySQL and ClickHouse. On PostgreSQL it uses the connection's
  configured database and schema `ns`, subject to any configured `schema:`.
- Discovery on a miss uses the table name from the statement, with unquoted names folded to lower
  case. For a connection scoped with `table:`, it uses the configured spelling, so it can find a
  mixed-case table on a miss as well as during startup discovery.
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
  cancels the agents. A client's cancel (PostgreSQL CancelRequest, MySQL `KILL QUERY` / `KILL CONNECTION`)
  stops the waiting connection's await and answers it cancelled (`frontend/common/cancel_registry.hpp`), but
  the statement runs on: the engine has no cancel.
- One `remote_backends_t` per `runtime_t`: its destructor stops the runtime's backends pool.
- `storage_total_rows` answers a constant estimate (1000), no query (otterbrix asks for no table size).

**Results and semantics**

- Backend dates and times are the engine's temporal types: MySQL DATE / DATETIME / TIMESTAMP / TIME as DATE /
  TIMESTAMP / TIMESTAMPTZ / TIME, PostgreSQL's as themselves (TIMETZ, INTERVAL too), ClickHouse Date / Date32 as
  DATE and DateTime / DateTime64 as TIMESTAMPTZ (Time stays text). Every agent session reads them in one form —
  MySQL `time_zone = '+00:00'`, PostgreSQL `DateStyle ISO`, `IntervalStyle postgres`, `TimeZone UTC`
  (`backend_agent_t::check_session`) — so instants are exact across backends; a value the engine's type has no room
  for (a MySQL zero date, a TIME past a day, PostgreSQL `infinity`) refuses the read. A pushed-down expression that
  depends on a zone (`ts::date`, `now()`) runs on the backend in UTC, not the client's TimeZone, until the agents
  follow the session's zone (otterbrix's `time_zone_t` exposes no zone name yet). A remote column registered as
  TEXT by an earlier version is re-typed by the next startup's reconciliation.
- A MySQL / MariaDB `TINYINT(1)` (a display width of 1: `BOOLEAN`, `BOOL`, a Django BooleanField) is
  registered and read as BOOLEAN, any value but 0 true (Connector/J's `tinyInt1isBit`): a TINYINT(1) that
  stores other small integers reads them as `true`. Every other TINYINT is an integer.
- A pushed-down expression is evaluated by the backend with its semantics (division by zero: MySQL
  NULL, PostgreSQL an error, ClickHouse `inf`); the result *types* are always the engine's
  (`conform_to_result`).
- `count(*)` is UBIGINT (otterbrix): MySQL wire unsigned BIGINT (Connector/J `getObject` →
  `BigInteger`), PostgreSQL int8.
- The backend DDL generator writes one-dimensional array types
  (`otterbrix/query_generation/sql_query_generator.cpp`). On rc-7, local `INT[][]` and ClickHouse nested
  arrays can be read as text through the PostgreSQL wire.
- `ARRAY(SELECT …)` is accepted only as a comparison operand. Compare it with a compatible array;
  use `x IN (SELECT …)` to test scalar membership. The engine evaluates the subquery and can push
  a comparison against the resulting literal array to the backend, as observed with ClickHouse.
  `x = ANY (ARRAY(SELECT …))`
  and an ARRAY sub-query in the SELECT list are `unimplemented_yet` (`ARRAY(SELECT ...) is supported only
  as a comparison operand`), a bare `ARRAY(SELECT …)` predicate is `sql_parse_error`
  (`unsupported subquery expression in this context`), `x IN ARRAY(SELECT …)` without parentheses is a
  grammar error and `x IN (ARRAY(SELECT …))` a `sql_parse_error` too (`unable to parse value`) (tag rc-7:
  `components/sql/transformer/impl/transfrom_common.cpp:631-648`, 1038-1043, 1232-1239,
  `components/sql/transformer/utils.cpp:1388-1390`, `components/sql/parser/gram.y:14255`, 15375-15382).
  No test pins the refusals.

**Sessions and drivers**

- MySQL `SET` of an unsupported engine setting is refused (`1149`), for example Connector/J's
  `SQL_SELECT_LIMIT` or Rails' `sql_mode`. The PostgreSQL frontend answers `SHOW` for its client
  parameters, fixed protocol parameters and server facts, including `SHOW DateStyle` and
  `SHOW server_version`. It passes other `SHOW` requests to the engine, which
  answers `SHOW TimeZone`, `SHOW search_path` and `SHOW autocommit`.
  See `frontend/CLAUDE.md` for the supported settings.
- `DISCARD ALL` is refused (outside a block by the engine; inside `25001`): otterbrix has no
  DISCARD. A pooler's reset must arrive as separate queries — the wire takes one statement
  per Query (`0A000` `multiple statements in one query are not supported`), so pgbouncer's
  `server_reset_query = 'RESET ALL; DEALLOCATE ALL'` is refused whole. `RESET ALL` alone runs
  on the engine; `DEALLOCATE ALL` alone fits a pooler that only needs its prepared statements
  cleared.
- No `NoticeResponse` (an `application_name` cut to 63 bytes, a `SET LOCAL` outside a block warn
  nothing).
- A client-parameter `SET` (`application_name`, …) run through Execute without BEGIN, followed by an
  error before Sync, is not undone (PostgreSQL's implicit pipeline transaction).
- The startup `TimeZone` is reported in the engine's canonical form, `utc` by default.
- Server-wide settings are refused from a client (`unimplemented_yet`): `ALTER SYSTEM`, `ALTER
  DATABASE | ROLE … SET`, MySQL `SET GLOBAL | PERSIST | @@global.x`.
- Flight SQL keeps no transaction across RPCs: a statement leaving its session inside a block is
  rolled back and refused.
- A statement a frontend stopped waiting for (90 s, `frontend/common/asio_future_bridge.hpp`) runs to its end; inside
  a block the frontend fails the block behind it (MySQL `ER_QUERY_TIMEOUT`). A MySQL / PostgreSQL
  connection that closes (quit, server stop, the client's socket gone) stops waiting at once and closes
  its session: a statement waiting for a backend agent leaves the queue, a running one runs to its end.
- A Spark session idle for `session_timeout_s` is closed (its block rolled back); its id is then
  refused `[INVALID_HANDLE.SESSION_CLOSED]`.
- A COPY rolled back leaves its file / object written (files are not transactional).
- Waiting is a poll: a settled actor-zeta future wakes nobody, so
  the query session (sessions pool) and the engine's executor spin while a statement waits, burning
  CPU for as long as it waits (`[.engine-defect-otb-notify]`, `tests/session/test_session_lifetime.cpp`).
  The backend actor's loop is woken by its agents. The frontends' own poll backs off
  (`await_backoff_t`: short steps first, then growing ones up to a cap).
- Docker MariaDB volumes lag on cold start — `docker-run-tests.sh` waits up to 120 attempts, 2 s
  apart (`WAIT_RETRIES` / `WAIT_SLEEP`).

## Critical Dependency Versions (`conanfile.py`)

- otterbrix `1.0.0b2-rc-7#01a9e7c6173c1b2f198da8efa2887499` from `conan.otterbrix.com` (source: the
  GitHub tag `1.0.0b2-rc-7`). The remote
  holds only a Linux x86_64 gcc 13 Release static binary; OtterStax links it shared (`otterbrix/*:shared=True`,
  `conanfile.py`), so `conan install … --build missing` builds it from the remote recipe against the abseil / re2 /
  boost overrides of `conanfile.py` (its own pins, abseil 20260107.1 / re2 20240702, cannot be taken: that
  abseil breaks protobuf 5.27 / gRPC 1.69). One `libotterbrix.so`. The TSAN image passes `--build="otterbrix/*"
  --build="actor-zeta/*"`, the ASAN image `--build="otterbrix/*"` (instrumentation).
- actor-zeta `2.0.1#b72880c342fcff50f62cc8c16d460cd1` from `conan.otterbrix.com` (Linux x86_64 gcc 13
  binary; other configurations `--build missing`).
- Arrow 24.0.0 (shared; s3, parquet, csv, json; snappy / brotli / zlib / lz4 / zstd)
- Boost 1.88.0, gRPC 1.69.0 + asio-grpc 3.7.0 (`backend=boost`), libpq 15.4, clickhouse-cpp 2.6.1,
  librdkafka 2.14.2, yaml-cpp 0.7.0, spdlog 1.15.1, fmt 11.1.3, Catch2 3.15.1, tracy 0.13.1 (optional)
- Spark Connect protocol 4.2.0 (protos vendored in `frontend/spark_connect_server/proto/`; the server
  answers `SparkVersion` 4.2.0); PySpark clients 3.5.0–4.2.0 (20 versions) in the E2E matrix
  (`Dockerfile.spark-test`, `tests/CLAUDE.md`).
