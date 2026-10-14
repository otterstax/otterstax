# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

OtterStax is a federated SQL query server. Clients connect via MySQL wire protocol (8816), PostgreSQL wire protocol (8817), Arrow Flight SQL (8815 — served by an in-house implementation, no Arrow Flight dependency), or Spark Connect (15002 — gRPC, for PySpark clients: `spark.sql()`, DataFrame operations, `spark.catalog`). Queries are either executed locally by the Otterbrix engine or dispatched to registered remote database backends (MariaDB/MySQL, PostgreSQL, ClickHouse). There is a **single config file** (`config.yaml`) that holds the wire-server settings and, under a `connections:` section, every remote backend and s3 alias — read once at startup. That `connections:` section is the single source of truth for connections; there is no runtime add/remove API.

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
  spark_connect: { host: "0.0.0.0", port: 15002, session_timeout_s: 3600 }   # Spark Connect gRPC server
  connection_retry: { max_attempts: 10, delay_ms: 2000 }
  # engine: { data_dir, log_dir }   — config/CLAUDE.md
  runtime:                          # every thread pool of the server; every key optional
    sessions:     { threads: 0 }    # actor-zeta pools: 0 = max(2, hardware threads)
    backends:     { threads: 0 }    # 0 = also >= the sum of every connection's pool_size
    connector_io: { threads: 0 }    # io threads of EACH connector manager;
                                    # 0 = the connections of its backend kind (sum of pool_size), >= 1
    mysql_io:     { threads: 0 }    # the frontends: wire servers' asio pools, Flight SQL /
    postgres_io:  { threads: 0 }    # Spark Connect gRPC threads; 0 = max(2, hardware threads)
    flight_sql_io:    { threads: 0 }
    spark_connect_io: { threads: 0 }

connections:
  mysql:
    - { alias: mysql, host: demo-mariadb, port: "3306", username: demo, password: demo, database: bill, table: "" }
  postgresql:
    - { alias: pg, host: demo-postgres, port: "5432", username: demo, password: demo, database: shop, schema: shop, table: "" }
  clickhouse:
    - { alias: ch, host: demo-clickhouse, port: "9000", username: demo, password: demo, database: ev, table: "", pool_size: 2 }
  s3:
    - { alias: demo_s3, access_key: minioadmin, secret_key: minioadmin, region: us-east-1, endpoint: demo-minio:9000 }
```

Wire-server settings live under the top-level `service:` node so
`service.mysql`/`service.postgres` (**wire-server ports**) never collide with the
backends under `connections:` (`connections.mysql`/`connections.postgresql`) —
different nesting. All `connections:` sections are optional (missing → empty).
`port` is an **optional string** (empty → driver default). `schema` defaults to
`public`. `table` empty = every base table of the database (MySQL / ClickHouse)
or schema (PostgreSQL); a name = that one table. Every backend entry
(mysql / postgresql / clickhouse) takes an optional `pool_size` (integer `>= 1`,
default 1: the connections kept open to it). s3
`region`/`session_token`/`endpoint` are optional; malformed YAML aborts startup.
`service.connection_retry` (optional; default `max_attempts: 1`, `delay_ms: 1000`
= one-shot) sets how many times startup retries opening a slow backend.
`service.spark_connect.session_timeout_s` (default 3600, 0 = never) closes a
Spark session that sent no RPC for that long. `service.runtime` sizes **every
thread pool of the server**: the five actor-zeta pools `runtime_t` owns (`general`
/ `exec` / `disk` for the engine, `sessions`, `backends`), `connector_io`, the
asio io pool each of the three connector managers owns (`threads: N` = N per
manager; 0, the default, = one per connection `connections:` opens to that
backend kind, at least one — a statement holds an io thread while it runs and a
connection runs one statement at a time), and the frontends' threads:
`mysql_io` / `postgres_io` (the wire servers' asio pools), `flight_sql_io` (the
Flight SQL gRPC threads) and `spark_connect_io` (the Spark Connect io threads),
each 0 = `max(2, hardware threads)`. `backends` 0 (the default) is at least that
and at least the sum of every remote connection's `pool_size`: a statement holds
a backends thread for its whole remote call, so fewer threads would idle
connections and let one slow alias take every thread; an explicit size below
that sum is kept and logged as a warning (`config/CLAUDE.md`). The `alias` is the outermost qualifier
in federated SQL (`SELECT ... FROM alias.db.schema.table ...`) and the `s3_alias`
referenced by `CREATE EXTERNAL TABLE` / `COPY ... TO`.

Refused at load (startup aborts, the reason on stderr): a section that is not a
mapping (`service: {mysql: 8816}`, a scalar connection entry), an incomplete entry, a
malformed port or `pool_size`, the reserved aliases `federation` and `kafka` (any
case, any section — the engine databases of the remote-table metadata and of the
Kafka objects), and a **duplicate
alias**: the mysql / postgresql / clickhouse aliases are ONE namespace compared
case-insensitively (an alias is the uid every federated name starts with, and
the federation folds it to lower case), so `Sales` under mysql and `sales` under
clickhouse collide; s3 aliases are a namespace of their own (two equal s3
aliases are refused, an s3 alias equal to a backend alias is fine). A MySQL
connection whose server runs with `sql_mode` `NO_BACKSLASH_ESCAPES` is refused
at discovery (`unimplemented_yet`: the generated literals double a backslash) —
that connection is left out, startup goes on.

### Flow through the code

```text
main.cpp
  → command line (--config PATH)                — a refused argument goes to stderr, return 1
  → config::load_config(&startup_resource, config_path)
                                                → result_wrapper_t<ServiceConfig> { ports…, engine, runtime,
                                                  connection_retry, ConnectionsConfig connections }
        (parse_connections validates every entry, then the aliases → the first problem is
         core::error_t{invalid_parameter}; main writes it to stderr and returns 1 — no logger
         and no module exists yet)
  → initialize_all_loggers(config::log_directory(server_config.engine))   — every log file under
                                                  service.engine.log_dir (default data_dir)
  → conn::s3::subsystem_finalizer_t            — declared before every module: the s3 connector actor
                                                 main spawns initialises Arrow S3, and the process must
                                                 FinalizeS3 after the modules are gone on every exit path
  → backends::backends_threads(runtime.backends.threads, connections)
                                                — the backends pool against the remote connections
                                                  (an explicit size below them: a warning it logs)
  → runtime::runtime_t(server_config.runtime)   — memory resource + every actor-zeta pool
  → backends::remote_backends_t(runtime.resource(),
        {.connector_threads = connector_io_threads(runtime.connector_io.threads, connections),
         .statements = runtime.backends_scheduler()})
    .open(connections, connection_retry)
       mysql/pg/ch: ConnectorManager::addConnection(conn::api_server::*Params, pool_size) + the
                    integration backend's discover (the connection's tables) — one attempt, retried
                    up to connection_retry.max_attempts (delay_ms between tries); a connection whose
                    discovery fails (or that the federation metadata cannot record) is removed from
                    its connector manager again; one that stays unreachable is left out (logged),
                    its uid still in the registry (its statements fail at the connection); a
                    descriptor the connector can never accept (a malformed port, pool_size < 1)
                    is checked before any connection opens → return 1. Which a failure is, the
                    step that failed says, never the error's code
       then federation::install_registry(every configured connection → its backend)
  → runtime::engine_t::open(runtime, server_config.engine, federation::make_primitives())
                                                — the embedded engine with the federation's hooks and
                                                  pushdown rule; a refusal → return 1
  → spawn conn::s3::ConnectorManager, conn::file::FileManager(dispatcher), db::S3Manager(s3, file),
          otterstax::kafka::KafkaManager(dispatcher, start_pollers = true)
                                                — main's locals after the engine: destroyed before it,
                                                  Kafka (declared last) first
  → backends::register_s3_aliases(resource, s3_connector, connections)
                                                — s3: add_credentials (no retry); an error → return 1
  → backends::publish_federation_metadata(resource, *engine, *remote)
                                                — a leftover mirror database of a uid dropped;
                                                  federation.remote_tables / remote_columns
                                                  written from the discovery, the rows of a
                                                  connection no longer in config.yaml (or of
                                                  another backend kind) removed (one transaction)
  → kafka->recover()
  → session::make_session_services(resource, dispatcher, s3, file, kafka)
                                                — an empty address → invalid_parameter → return 1
  → session::session_factory_t(runtime, &make_parser, services)
  → the four servers (Flight SQL, MySQL, PostgreSQL, Spark Connect) over the factory
       → start() MySQL / PostgreSQL / Spark Connect, then flight->start() (false → return 1),
         flight->run() — blocks until SIGTERM / SIGINT, then the shutdown in reverse order
```

- Parsing + validation live in the `config` target (`config/`), which has **no
  connector dependencies** — `parse_connections` produces plain
  `config::ConnectionsConfig` descriptor structs held inside `ServiceConfig` and
  **returns `core::error_t{invalid_parameter}` (built from `validation_error`)
  for the first incomplete entry, malformed port or non-scalar field**; a file
  that is not YAML is a `conversion_failure`. Nothing in `config/` throws: the
  yaml-cpp calls are the only throwing sites and each is converted where it is
  made (`config/yaml_scalar.hpp`, `YAML::LoadFile` in `config::load_config`).
  `main.cpp` writes the error to stderr and returns 1 — the file is read before
  the loggers exist (they go under `service.engine.log_dir`, which the file
  names), so `load_config` logs nothing — and an invalid connection aborts
  startup rather than coming up half-configured.
- Opening lives in `remote_backends_t::open` (`backends/remote_backends.hpp`,
  which owns the connector managers and the integration backends), converting the
  pre-validated descriptors → connector param structs (`pool_size` included) and
  discovering each connection's tables (MySQL: the database, or the one `table`;
  PostgreSQL: the schema, or the one `table`; ClickHouse: the database, or the one
  `table`). Opening a backend is best-effort: it is retried up to
  `connection_retry.max_attempts` (`backends/connection_retry.hpp`,
  `otterstax::backends::open_with_retry`), and a backend that is still unreachable
  (or whose discovery fails) logs an error but does not abort startup (the other
  backends and the local engine stay up). The discovery is written into the
  engine's federation metadata by `backends::publish_federation_metadata`
  (`backends/backends.hpp`) once the engine is open (a table the backend no longer
  has, or a connection no longer configured, loses its rows there); s3 aliases are
  stored by `backends::register_s3_aliases`. A remote table created on the backend while
  the server runs is found by the session's rediscovery on its first "does not
  exist" (`catalog/remote_rediscovery.hpp`).

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
./build/Release/tests/session/test_session                  # tests/session
./build/Release/tests/runtime/test_runtime                  # tests/runtime
./build/Release/tests/federation/test_federation            # tests/federation
./build/Release/tests/unit/connectors/test_unit_connectors
./build/Release/tests/unit/parser/test_parser               # tests/unit/parser
./build/Release/tests/unit/schema/test_schema               # tests/unit/schema
./build/Release/tests/unit/utility/test_utils               # tests/unit/utility
./build/Release/tests/unit/translators/test_unit_translators
./build/Release/tests/unit/config/test_unit_config
./build/Release/tests/unit/spark_connect/test_unit_spark_connect
./build/Release/tests/unit/parser/grammar_extension/kafka/test_kafka_grammar
./build/Release/tests/mysql-front/test_mysql_front          # tests/mysql-front

# Build with sanitizers (not both at once)
cmake -S . -B build/Release -DENABLE_ASAN=ON -DBUILD_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake -S . -B build/Release -DENABLE_TSAN=ON -DBUILD_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
# ASAN note: the engine's public headers no longer key any class layout off the
# consumer's sanitizer macros (core/pmr.hpp: one otterbrix_resource /
# arena_resource_t layout in every build; the ASAN backing store is picked when
# the engine library itself is built), so an ASAN app links the non-ASAN Conan
# engine package as is — no macro workaround. Engine-internal memory errors are
# only caught with an ASAN-built engine. On macOS (libc++ annotates containers
# under ASAN) run with ASAN_OPTIONS=detect_container_overflow=0: containers our
# instrumented code annotates are freed by the uninstrumented engine back into its
# pool, and actor-zeta coroutine frames later carved from that pool trip stale
# annotations (false positives). LeakSanitizer is unsupported on Apple Silicon.
# The CI sanitizer workflows also export
# ENABLE_ASAN/ENABLE_TSAN to docker-run-tests.sh, which skips the Kafka
# crash-recovery (kill -9) step under sanitizers.

# Adjust log verbosity (default ERROR; the engine's log runs at the same level)
cmake -S . -B build/Release -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG ...
```

### The TSAN image (`Dockerfile.test`, `TSAN_DEPS=true`)

`tsan-deps.profile` (the host profile) rebuilds the LIBRARIES that synchronize
through atomics with `-fsanitize=thread` (otterbrix, actor-zeta, librdkafka, gRPC,
abseil, protobuf, re2). Nothing instrumented runs at build time:

- **protoc and grpc_cpp_plugin come from the build context** (`-pr:b default`, no
  TSAN): the profile adds `&: protobuf/<host_version>, grpc/<host_version>` as
  `[tool_requires]`. CMakeToolchain puts their bin dirs on `CMAKE_PROGRAM_PATH`, and
  the host recipes' imported targets (`protobuf::protoc`, `gRPC::grpc_cpp_plugin`,
  used by `frontend/{flight_sql,spark_connect_server}/CMakeLists.txt`) locate the
  executable with `find_program`, so they resolve to those. Profile-only on purpose:
  in a plain build host == build, while the same `tool_requires` in `conanfile.py`
  would build a second grpc for the build context (its version ranges and the
  `cppstd=17` host settings do not carry over to the build profile).
- **The TSAN grpc is built with `codegen=False`** (`[options]` of the profile): with
  codegen on, grpc's own build runs the plugin it has just built (instrumented) for
  its reflection / channelz stubs. OtterStax links `gRPC::grpc++` only.
- **Catch2 discovery is `PRE_TEST` under `ENABLE_TSAN`** (`tests/CMakeLists.txt`):
  the default `POST_BUILD` discovery runs every test binary at build time.

Why: a TSAN executable re-executes itself with `personality(ADDR_NO_RANDOMIZE)` on
arm64 (gcc-12's libtsan does so in every aarch64 process with ASLR on), and Docker's
default seccomp profile refuses that call — inside `docker build` such a binary dies with
`ThreadSanitizer: CHECK failed: tsan_platform_linux.cpp:315 "((personality(...))) != ((-1))"`.
With the three rules above a plain `docker build` of the TSAN image gets through the
`conan install` and the build on arm64 (Docker Desktop on Apple Silicon). **Its
`ctest` step cannot pass there** — it runs the instrumented test binaries, and
`docker build` has no way to lift the seccomp filter (BuildKit's
`--security=insecure` needs an entitlement nobody grants by default). The TSAN CI
workflow (`integration-tests-tsan.yml`) builds on amd64 GitHub runners. On an arm64
host, build without the test step and run the tests in a container that allows the
call:

```bash
docker build -f Dockerfile.test --build-arg TSAN_DEPS=true --build-arg ENABLE_TSAN=ON \
  --build-arg RUN_TESTS=false --build-arg BUILD_JOBS=<JOBS> -t otterstax-tsan .
docker run --rm --security-opt seccomp=unconfined -w /app/build/Release otterstax-tsan \
  ctest -C Release --output-on-failure --timeout 120
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

### TEMPORARY: local otterbrix recipe in Docker builds (until OTB-tag)

`conanfile.py` pins `otterbrix/1.0.0b2-embedding.46fba865#66a161bad0259c1280d2a132e32e8bb3`.
It is not on a conan remote, it exists only in the developer's local Conan cache
(see **Critical Dependency Versions**). Docker images therefore get it from an
archive in the build context. `actor-zeta/2.0.0` (pinned as
`#887bc992cac0ead59e5bdf5c90c844f4`) is **not** part of this: it is on
`conan.otterbrix.com` and resolves like any other dependency (the remote holds a
Linux x86_64 gcc 13 binary; any other configuration builds it through
`--build missing`).

```bash
# Host side: archive the otterbrix recipe + sources (no binaries: a macOS binary
# is useless in a Linux image) into .conan-local/local_recipes.tgz (git-ignored).
# Rewritten only when the ref#revision changes, so Docker's layer cache holds.
scripts/conan/export_local_packages.sh            # --force to rewrite anyway
docker build -f Dockerfile.test --build-arg BUILD_JOBS=5 -t otterstax-test .
```

- `docker-run-tests.sh` (when it builds `test-otterstax`, i.e. no `IMAGE_TAG`),
  `benchmark/scripts/run_benchmark.sh`, `benchmark/stress/run_stress_benchmarks.sh`,
  `benchmark/manual/start_service.sh` and `examples/demo/up.sh` run the script
  before they build an image. An otterbrix package that is not in the local cache
  is a WARNING, not an error: the script prints the `conan create` command that
  builds it, keeps an existing archive (if any), exits 0, and the image resolves
  otterbrix from the remotes. That is the normal state after OTB-tag.
  Without a local or remote package the failure surfaces in the image's
  `conan install`. The script exits 1 only on real errors (no `conan` and no
  archive, or a failed save), and the callers treat that as fatal.
  A plain `docker build` / `docker compose up --build` (`compose.yml`,
  `examples/quick_start`, `benchmark/compose_benchmark.yml`) does not run it, so
  run it by hand first.
- `Dockerfile` and `Dockerfile.test` `COPY .conan-local/` (the directory is tracked
  through `.conan-local/.gitkeep`, so the COPY never fails). When
  `local_recipes.tgz` is present they `conan cache restore` it and add
  `--build="otterbrix/*"` to every `conan install` branch. The TSAN branch
  (`TSAN_DEPS=true`) always passes `--build="otterbrix/*" --build="actor-zeta/*"`
  (plus librdkafka): it builds them from source to instrument them with
  `-fsanitize=thread`, which is not part of this workaround and stays after
  OTB-tag. Without the archive the install falls back to the remotes.
  `BUILD_JOBS` is also passed as `-c tools.build:jobs=N`: otterbrix built from
  source is the heaviest step of the image, so size it with **Build resources**. `BUILD_JOBS=0` (the `Dockerfile.test` and compose default)
  no longer means a bare `nproc`: every build RUN in both Dockerfiles resolves it to
  `min(nproc, MemTotal_MB / 1536)` read from the build VM's `/proc/meminfo`.
  With no `-j`, `docker-run-tests.sh` and `benchmark/manual/start_service.sh` also
  cap the jobs on the host side to Docker's RAM, the way `run_benchmark.sh` does.
  The root `Dockerfile` takes `BUILD_JOBS` (default 5) too.
- **CI is not covered.** The GitHub workflows (`unit-tests.yml`,
  `integration-tests{,-asan,-tsan}.yml`) build `Dockerfile.test` from a clean
  checkout with no archive, so they cannot resolve the pinned otterbrix until OTB-tag lands.
- `.dockerignore` keeps `build/`, `cmake-build-*`, `.git` and the result/profile
  directories out of the build context. It must not exclude `.conan-local/`.

**Removing it once OTB-tag lands** (an official otterbrix tag on
`conan.otterbrix.com`; actor-zeta 2.0.0 is there already): point `conanfile.py`
at the tag, then delete `scripts/conan/`, `.conan-local/`, the `.conan-local`
block in `.gitignore` and `.dockerignore`, the `COPY .conan-local/` + restore
lines and the `_local_build` array in `Dockerfile` / `Dockerfile.test`, and the
`export_local_packages.sh` calls in
`docker-run-tests.sh`, the three benchmark scripts and `examples/demo/up.sh`. Keep
the `tools.build:jobs` wiring, the job auto-cap and the TSAN branch's `--build`
flags.

## Architecture Overview

`main.cpp` owns every part of the process directly, as locals: the declaration
order is the build order and its reverse the destruction order (RAII — an
aborted startup's early `return` unwinds the same way). Three modules, then the
actors and the servers:

```text
runtime_t         (runtime/)   — the memory resource and every thread pool (service.runtime)
remote_backends_t (backends/)  — the MySQL / PostgreSQL / ClickHouse connector managers and
                                 integration backends (the federation's backend_t
                                 implementations); opens and discovers the connections and
                                 installs the federation registry BEFORE the engine opens
engine_t          (runtime/)   — the embedded otterbrix engine on the runtime's general/exec/disk
                                 pools (service.engine), opened with the federation's primitives
                                 (host hooks + pushdown rule); destroying it is its final CHECKPOINT
s3 connector, FileManager, S3Manager, KafkaManager
                               — actors spawned after the engine (Kafka last); then
                                 backends::register_s3_aliases / publish_federation_metadata
                                 (backends/backends.hpp), kafka->recover(),
                                 session::make_session_services
session_factory_t (session/)   — the query sessions on the sessions pool; its destructor drains
                                 them (30 s), runtime_t::stop_sessions(), frees what the pool left
the four servers  (frontend/)  — Flight SQL, MySQL, PostgreSQL, Spark Connect over the factory;
                                 each stops in its destructor
```

Shutdown: the servers (each stops in its destructor; their connections close
their sessions) → `~session_factory_t` (the sessions drain, the sessions pool
stops, what it left is freed) → Kafka, S3, file, the s3 connector → engine
(final CHECKPOINT, engine pools stop) → remote backends (registry reset first,
then the integration backends wait for the statement jobs they posted to the
backends pool, then the connector managers and their io pools) → runtime (the
backends pool, then memory) → FinalizeS3.

Every client connection owns ONE query session (`session::query_session_t`, an
actor-zeta `basic_actor` on the runtime's sessions pool) and through it ONE
engine session. **Every statement runs on the engine** but the two forms the
engine cannot take yet (remote table DDL, a four-part DML target — both wait for
otterbrix); a remote table is a table the engine resolves through the federation
(`otterbrix/federation/`):

```text
Frontend (MySQL/PG/FlightSQL/Spark Connect)
  → query_session_t::execute()      — the connection's session: parser, prepared map
  → database-DDL guard              — catalog::check_database_ownership (uids, kafka, federation)
  → [table DDL on a remote table]   — generated in the backend's dialect and run by the
                                      backend the registry holds for the uid
                                      (backend_t::execute_ddl: DDL never reaches the
                                      engine's host hooks); the federation metadata follows —
                                      waits for otterbrix
  → [INSERT / UPDATE / DELETE whose target is written in four parts, uid.db.schema.t]
                                    — generated from the parsed plan and run on the backend
                                      directly (run_remote_dml → backend_t::write): the engine's
                                      transformer keeps no uid slot on a DML target — waits for
                                      otterbrix
  → engine dispatcher               — otterstax::engine::execute_statement on the connection's
                                      engine session (everything else, the 3-part DML form included)
       name the catalog does not know → federation need/decide hooks: the remote table is rows of
                                        the engine database `federation` (declared columns from
                                        federation.remote_columns) + a per-statement
                                        remote_storage_t
       pushdown rule (after_simplify)  → a subtree whose every table is a remote table of ONE
                                        server — a query block over one table or over a JOIN
                                        tree / derived table of that server, a JOIN alone, an
                                        INSERT / UPDATE / DELETE — becomes ONE statement in the
                                        backend's dialect where the dialect can say it
                                        (sql_gen::generate_query) — node_extension_t operators
       otherwise                       → the storage's scan (what the dialect cannot say, a JOIN
                                        with a local table or with another server — each side's
                                        own pushable subtree still pushed), insert / update /
                                        delete sinks (a row named by its primary key)
       backend_t::scan / write         → the integration backend posts the statement as a job on the
                                        runtime's backends pool (integration/statement_executor.hpp):
                                        ConnectorManager::executeQuery — a pooled connection, the
                                        manager's io threads — then the rows translated
                                        (tsl::mysql_to_chunk / pg_to_chunk / ch_to_chunk)
  → [a remote table missing / stale] — rediscovered through its backend (backend_t::discover), the metadata
                                        upserted, the statement retried once
  → session co_returns result_wrapper_t<session_payload>
  → frontend awaits via asio_future_bridge → sends response
       (Flight SQL: otterbrix/translators/output/chunk_to_arrow → Arrow RecordBatch)
```

Statements of one connection run strictly one at a time in arrival order (the
session's mailbox is its statement queue). Because each connection has its own
engine session, BEGIN / COMMIT / ROLLBACK over engine tables work on every
protocol with PostgreSQL semantics (an error aborts the block until ROLLBACK; a
COMMIT of a failed block is a rollback). A write to a remote table inside a block
runs on the backend at once (the backend takes no part in the block). See
`session/CLAUDE.md`.

The Spark Connect frontend enters the session two ways. SQL text goes through
`query_session_t::execute` like the other frontends: a `spark.sql()` statement
that is not a query runs at once, while a query is handed back to the client
unrun (as its SQL relation) and runs when the client acts on its DataFrame
(`prepare_schema` for `df.schema`). A DataFrame — translated into an otterbrix
logical plan inside the frontend (`frontend/spark_connect_server/plan_translator/`)
— goes through `query_session_t::execute_plan`, or `prepare_plan` for
`df.schema` (described, never executed). One query session per Spark session,
closed by ReleaseSession.

Frontends never block on shared state — they hold the future a session send
returns and poll it from the per-connection asio executor through
`frontend/common/asio_future_bridge.hpp` (the Spark Connect handlers, which run
on an `agrpc::GrpcExecutor`, through its gRPC form,
`frontend/spark_connect_server/await_future.hpp`). That bridge, in either form,
is the only supported way to pick up an actor result.

### Key Types

- `session::statement_id_t` — a statement on a query session (fresh per statement; the connection is the session itself); `session_hash_t` (`utility/session.hpp`) is the statement id as the file / s3 managers and Kafka receive it
- `core::result_wrapper_t<session_payload>` (`query_session_t::session_result`) — the typed result returned through the actor-zeta future; carries either the payload (rows, or `affected_rows` of a DML; `reported_rows()`) or `core::error_t`
- `components::session::session_id_t` — the engine session; one per query session (`query_session_t::engine_session()`)
- `ParsedQueryData` / `ParsedQueryDataPtr` — output of the parser: the logical plan (`otterbrix_params`; names carried whole: a remote name keeps its uid slot), the bindings of a prepared statement, the statement's text (`sql`, re-parsed for a retry or a describe), a table DDL statement's written target (`ddl_target`), a four-part DML target (`dml_target`), the claiming grammar extension (`extension_kind`)
- `backend_type_t` — `{MySQL, PostgreSQL, ClickHouse}`: the SQL dialect `sql_gen` writes, and the one spelling of a backend's kind everywhere (`federation::server_t::kind`, the discovery and the federation metadata, whose `backend` columns store it as `mysql` / `postgresql` / `clickhouse`)
- `federation::backend_t` / `registry_t` — how a configured connection runs a remote statement (`scan`, `write`, `discover`, `execute_ddl`; implemented by the integration backends); the immutable uid → backend map the hooks read, installed by `remote_backends_t::open`
- `remote_backends_options_t` / `connector_io_threads_t` (`backends/remote_backends.hpp`) — the connector factories (mocks in tests), the io threads of each connector manager, the backends pool

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
  using a stable, qualified name (e.g. `"query_session::execute"`, `"catalog::list_tables"`,
  `"sql_gen::generate_query"`, `"federation::rules::pushdown"`).
- **Exception — coroutine handlers that can resume on another thread.** When a handler's
  coroutine can suspend at a `co_await` and resume on another thread, or interleave with
  other coroutines on one thread (`query_session_t` on the sessions pool), open the zone as the first statement of a nested block
  `{ OTX_ZONE_N("Scope::function"); ... }` that closes before the first `co_await`.
  Tracy requires a zone to end on the thread that began it, in LIFO order; a zone spanning
  a suspension point breaks both and corrupts the capture. Only `assert`s, objects that
  must live for the whole coroutine (`Timer`, `erase_on_exit_t`) and the variables the
  block fills (the `unique_future`, the parsed query data) may precede that block; the
  awaited actor instruments its own work. A handler of an actor whose `enqueue_impl`
  drives the coroutine to completion on the sender's thread (e.g. `S3Manager`,
  `FileManager`) keeps the zone as its first statement even across
  `co_await`: the awaited call runs nested on the same thread. If such an actor moves off
  that synchronous path, its handlers fall under this exception. See
  `session/CLAUDE.md`, "Tracy zones in coroutines".
- The macros compile to **no-ops** unless Tracy is enabled, so there is no release-build cost.

Do NOT instrument:

- Recursive AST/tree walkers on a per-call basis (one zone per node floods the profiler) —
  instrument the non-recursive top-level entry point that drives them instead.
- Trivial getters/setters and O(1) map lookups (e.g. `registry_t::find`).

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

-- Load an S3 object (the s3_alias is a connections.s3 entry of config.yaml — see below)
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
  → query_session_t::execute()        — the connection's session: parser + pipeline
  → [parser extension claims the statement → otterstax::external::external_node_t]
  → query_session_t::load_external_table / copy_to (by the node's op)
       → CREATE EXTERNAL TABLE:
           local path  → conn::file::FileManager::add_file → engine::create_table (its own engine session)
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
- Two tables of ONE registered backend: the JOIN — with the WHERE / GROUP BY /
  HAVING / ORDER BY / LIMIT / DISTINCT above it and the derived tables under it —
  is pushed to that backend as one statement where its dialect runs it with the
  engine's semantics (MySQL has no FULL JOIN; ClickHouse takes only some ON
  conditions); what it cannot say stays with the engine over the JOIN's own remote
  statement or over a scan of each table (`otterbrix/federation/CLAUDE.md`, "The
  pushdown rule").
- Two registered backends: the engine reads each server's side (its own pushable
  subtree — a query block, a JOIN of its tables — as one statement, otherwise its
  storage's scan) and JOINs them in-process.
- **Registered backend ⋈ otterbrix-internal table** in a single statement. The
  engine resolves the local table from its own catalog and the remote one
  through the federation (`otterbrix/federation/`), reads the remote rows through
  the storage's scan and JOINs them against engine-resident rows. The live
  demo's `examples/demo/sql/step_4.sql` exercises exactly this shape every run.
- A remote table in a sub-query (`IN (SELECT ... FROM remote)`, `EXISTS`, a scalar
  sub-query, a derived table): the engine runs the sub-query like any other; a
  sub-query block over the tables of one server is pushed down whole.

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
| `catalog/` | `catalog` | Pure building blocks, no actor: the discovery reply and query builders (`discovery.hpp`), the federation metadata plans (`remote_metadata.hpp`), the rediscovery flow (`remote_rediscovery.hpp`), owned database names (`ownership.hpp`), the GetTables / spark.catalog listing (`introspection.hpp`). See `catalog/CLAUDE.md` |
| `integration/` | `integration` | The layer between the engine / the sessions and the ConnectorManagers (incl. the `db::S3Manager` actor); each backend (`db::MySQLManager`, `db::PostgressManager`, `db::ClickhouseManager`, plain objects, not actors) is the sole driver of its ConnectorManager and the federation's `backend_t`: `discover` / `execute_ddl` on the calling thread, `scan` / `write` as jobs on the runtime's backends pool (`statement_executor.hpp`). See `integration/CLAUDE.md` |
| `integration/kafka/` | `kafka_runtime` | `KafkaManager` actor + `detail/` impl (consumer/producer/poller/stream/reader); Kafka SOURCE/STREAM objects, librdkafka |
| `otterbrix/` | `otterbrix_local` (+ `otterbrix_s3_extension`, `otterbrix_file_extension`) | Parser, SQL generator (`sql_gen`, every backend dialect), translators, the engine requests (`operators/`), grammar extensions for `CREATE EXTERNAL TABLE` / `COPY ... TO`. See `otterbrix/CLAUDE.md` |
| `otterbrix/parser/grammar_extension/kafka/` | `kafka_grammar` | Kafka DDL parser extension (flex+bison): `kafka_node_t`, `kafka_write_target` |
| `runtime/` | `runtime` | `runtime_t` (memory resource + every thread pool) and `engine_t` (the embedded otterbrix engine: open / shutdown, `dispatcher_address()`). See `runtime/CLAUDE.md` |
| `backends/` | `backends` | `remote_backends_t` (connector managers + integration backends; `open` with `connection_retry.hpp`; the federation registry; `connector_io_threads` sizes the managers' io pools) and two startup steps as free functions (`backends.hpp`: `register_s3_aliases` — the s3 aliases into the s3 connector —, `publish_federation_metadata`); the S3 / file / Kafka actors are `main.cpp`'s locals |
| `session/` | `session` | `query_session_t` (one per client connection: parser, prepared statements, the database-DDL guard, remote DDL, the engine, rediscovery + one retry, external-statement and Kafka dispatch, the connection's engine session), `make_session_services` (the addresses a session sends to, none empty) and `session_factory_t` (whose destructor drains the sessions and stops the sessions pool). See `session/CLAUDE.md` |
| `otterbrix/operators/` | (in `otterbrix_local`) | `engine_requests.{hpp,cpp}`: the free functions every engine request goes through (`execute_statement`, `describe`, `create_table`, …), sent straight to the engine dispatcher |
| `otterbrix/federation/` | `otterstax_federation` | The host primitives the engine is opened with: name resolution (`need` / `decide`), `remote_storage_t` (scan + insert / update / delete sinks), the pushdown rule (`rules/pushdown`), the backend registry. See `otterbrix/federation/CLAUDE.md` |
| `frontend/` | `flight_sql`, `mysql_server`, `postgres_server`, `spark_connect_server` | The wire-protocol frontends, owned by `main.cpp` (each server stops in its destructor — the first step of the shutdown) (one query session per connection, awaited via `asio_future_bridge.hpp`). `flight_sql/` is the in-house Flight SQL server: asio-grpc handlers over gRPC, the data plane on the project's arrow core (`arrow::ipc` + `tsl::*` translators) — no Arrow Flight dependency. The Spark Connect gRPC server (asio-grpc; one query session per Spark session: `spark.sql()` through `query_session_t::execute`, DataFrame plans through `execute_plan`, awaited via `await_future.hpp`). See `frontend/CLAUDE.md` |
| `utility/` | (header-only) | `session` (`session_hash_t`), `wait_barrier` (connector error marshalling, `run_with_owned_handler`), `function_ref`, `thread_pool_manager` (a connector manager's io pool), `parse_port`, `asio_error`, `table_info`, logger, profiler. See `utility/CLAUDE.md` |
| `cmake/` | (helper macros) | `otterbrix_parser_extension.cmake` — builds the s3/file flex+bison grammar extensions |
| `tests/` | `test_system`, `test_session`, `test_runtime`, `test_federation`, `test_unit_connectors`, `test_parser`, `test_schema`, `test_utils`, `test_unit_translators`, `test_unit_config`, `test_unit_spark_connect`, `test_kafka_grammar`, `test_mysql_front` | Catch2 tests + python integration suite under `tests/test_*.py` (the `test_spark_client_*.py` files run in the PySpark client matrix, `Dockerfile.spark-test`; binary names are the `project()` names in `tests/*/CMakeLists.txt`; see `tests/CLAUDE.md`) |

## Known Constraints

- `ConnectorManager::addConnection` / `removeConnection` are the only writes to a connector registry and run on the startup thread (`remote_backends_t::open`) before any query reaches the integration backend that drives the manager (`connectors/mysql/manager.hpp`)
- MySQL / PostgreSQL / ClickHouse: each alias owns a pool of `pool_size` connections (`connections:` in `config.yaml`, default 1) and a statement checks one out for its whole run, so statements on one connection never overlap; with every connection busy the next statement waits up to the checkout timeout (60 s, then `io_error`; `connectors/CLAUDE.md`, "Connection pools"). The statements run on the manager's own io pool (`service.runtime.connector_io`)
- A rediscovery (`backend_t::discover`) and a remote DDL statement (`execute_ddl`) run synchronously on the query session's thread of the sessions pool, which waits for the backend's answer (`executeQuery(...).get()`, `session/CLAUDE.md`). MySQL and PostgreSQL bound only the connect (10 s), not a statement (PostgreSQL's TCP keepalive ends one only when the host went silent, ~60 s; ClickHouse's 10 s recv timeout bounds it), so a hung backend holds that pool thread. The running statements are aborted (`ConnectorManager::shutdown`) only by `~remote_backends_t`, after `frontends_t` stopped the sessions pool, which joins that thread: a server stopped while a hung backend holds a rediscovery or a remote DDL can wait for it indefinitely. PostgreSQL and ClickHouse run one `discover` at a time per backend kind (one mutex per manager, not per uid), so a slow alias delays the rediscovery of every other alias of its kind; MySQL takes no lock. The actors these backends replaced behaved the same (a synchronous enqueue)
- Row-level UPDATE / DELETE of a remote table (what the pushdown rule leaves to the engine: a JOIN, a sub-query, a value with no literal in the dialect) names each row by its **primary key** as the scan read it, one statement per row that must change exactly one row. A table without a discovered primary key — a key over a binary column (PostgreSQL `bytea`, MySQL `BINARY` / `VARBINARY` / `BLOB`) is not recorded, its raw bytes having no exact literal —, a key column whose value names no row exactly (FLOAT / DOUBLE, nested types; a DECIMAL — MySQL DECIMAL / PostgreSQL NUMERIC / ClickHouse Decimal — names its row by its exact text) and every ClickHouse table are refused at plan time (`unimplemented_yet`). There is no optimistic check of the other columns: a row deleted on the backend since the read, or whose key changed, fails the statement (`write_conflict`, the row statements before it staying committed), but a row whose non-key columns changed on the backend in between is still matched by its key and overwritten — a lost update (`otterbrix/federation/CLAUDE.md`)
- The count of a MySQL UPDATE is the rows it **matched**, read off the OK packet's info (`Rows matched: N  Changed: M  Warnings: W` on MySQL and MariaDB, translated by the session's `lc_messages`, so it is read by its numbers: the first of exactly three) — Boost.MySQL never sets `CLIENT_FOUND_ROWS`, so `affected_rows()` would count only the rows whose value changed; PostgreSQL counts the same way; ClickHouse answers `written_rows` for an INSERT and 0 for a mutation
- A pushed-down expression is evaluated by the backend, with the backend's semantics: division by zero is MySQL NULL, a PostgreSQL error, ClickHouse `inf` (`intDiv` an error), where the same expression the engine evaluates (a JOIN, a mix of servers) answers the engine's way. The pushdown spells the engine's semantics only where it can (NULL placement in ORDER BY, integer `/` as `DIV` / `intDiv`); the result *types* are always the engine's (`conform_to_result`), so a prepared schema and the executed one agree
- Array types support only single dimension (see the `TODO: multiple dimentions array` in `write_column_def`, `otterbrix/query_generation/sql_query_generator.cpp`)
- Docker MariaDB volumes lag on cold start — `docker-run-tests.sh` has a 120 s wait
- otterbrix has no close-session call: closing a query session ends its engine session through ROLLBACK (`query_session_t::end_session`, queued by `close()` when a block may be open), which clears what the engine keeps for the session. A session whose job the stopped sessions pool dropped at shutdown is freed by `session_factory_t`'s `reap()` without that ROLLBACK, so a block it left open is not rolled back. Per-session `SET` does not exist either: otterbrix applies a `SET` to every session (`manager_dispatcher_t::default_settings_`) and persists it in `pg_settings`, so no `SET` / `RESET` reaches the engine — the query session tells it by the raw statement (`VariableSetStmt`, `session/statement_kind.hpp`) and answers OK without effect on every protocol (`SET autocommit`, `SET timezone`, any parameter; an open block stays open, a failed one refuses it). It is passed on once otterbrix keeps settings per session (OTB-session-settings)
- Waits for otterbrix — transactions cover engine tables only (no storage transaction hooks): a write to a remote table inside `BEGIN` is committed by the backend at once and a ROLLBACK does not undo it; remote reads inside a block are not one snapshot of the backend; the engine-side rows of the same block still follow BEGIN / COMMIT / ROLLBACK. `SAVEPOINT` / `RELEASE` / `ROLLBACK TO` are refused (`unimplemented_yet`: PG `0A000`, MySQL `ER_NOT_SUPPORTED_YET`), failing an open block
- Waits for actor-zeta / otterbrix (OTB-notify) — waiting on a future is a poll: a settled future wakes nobody, so a cooperative actor suspended on a `co_await` (the query session on the sessions pool, the engine's executor on the exec pool) is requeued on every pass of its pool, and the frontends' bridges poll on a timer (1 ms; Spark 16 µs → 1 ms). A statement waiting one second costs ~3.4 CPU-seconds (`[.engine-defect-otb-notify]`), and the benchmark's ClickHouse JOINs run +23–25% slower than with the spin removed, +21–23% against the pre-migration baseline (`session/CLAUDE.md`, `docs/proposals/actor-zeta.md` AZ-waker / AZ-park, `docs/proposals/otterbrix-embedding.md` §4.13)
- Flight SQL keeps no transaction across RPCs: a statement that leaves its session inside a block (`BEGIN`, by text or prepared) is rolled back and refused ("a transaction cannot be opened over Flight SQL")
- A statement a frontend stopped waiting for (the await bridge's 90 s, `asio_future_bridge.hpp`) runs to its end in the session; inside a block the frontend fails the block behind it (MySQL answers `ER_QUERY_TIMEOUT`)
- A Spark session that sent no RPC for `service.spark_connect.session_timeout_s` (default 3600) is closed with its query session (an open block rolled back); a later RPC with its id is refused `FAILED_PRECONDITION` `[INVALID_HANDLE.SESSION_CLOSED]`
- `COPY ... TO` and the `CREATE EXTERNAL TABLE` load run on the file manager's own engine session, outside the connection's transaction
- Waits for otterbrix — remote DDL: DDL never reaches the engine's host hooks, so `CREATE TABLE` / `DROP TABLE` / `CREATE INDEX` / `DROP INDEX` on a remote table is generated in the backend's dialect and run by the session directly through the backend the registry holds for the uid (`run_remote_ddl` → `backend_t::execute_ddl`); the federation metadata follows (the created table rediscovered, the dropped one's rows removed). Kafka DDL and the S3 / file statements keep their own routes
- Waits for otterbrix — four-part DML targets: the engine's transformer keeps database, schema and table on an INSERT / UPDATE / DELETE target and no uid (`set_target`, external targets), so the engine cannot resolve `UPDATE uid.db.schema.t` through the federation. The session generates that form in the backend's dialect from the parsed plan and runs it on the backend directly (`run_remote_dml`, the parser's written `dml_target`), its count the backend's; a source on another server or a local one is refused by the generator, and so is a `RETURNING` list (`unimplemented_yet`, at execute and at prepare). The three-part form (`uid.db.t` for MySQL / ClickHouse, `uid.schema.t` for PostgreSQL — the engine reads three parts as database.schema.table and the federation takes the database slot for the uid) runs on the engine through the federation and takes any source (`INSERT ... SELECT` from a local table included). SELECT keeps every slot
- Waits for otterbrix — a prepared statement is described by the engine's plan-only EXPLAIN (no execution, no backend round-trip: the declared columns come from `federation.remote_columns`); a statement with unbound parameters or a set operation is described by the federated computation (`otterbrix/schema/schema_utils.hpp`) over a plan-only probe of each relation
- Known issue (G1, OTB-resolve-uid): otterbrix resolves a local table by (database, name) alone and asks the federation only for names its catalog left unresolved, so a local database named like a remote database captures the remote name (`bill.orders` answers `sales.bill.public.orders`). The ownership guard keeps user DDL off the uid names (`catalog/ownership.hpp`); a leftover mirror database of an older OtterStax is dropped at startup (`backends::publish_federation_metadata`)

## Critical Dependency Versions

- Otterbrix `1.0.0b2-embedding.46fba865` — a **local** Conan package built from otterbrix branch `feat/otterstax-embedding` (`conan create . --version 1.0.0b2-embedding.46fba865 -pr:h profiles/release -pr:b default -o "otterbrix/*:shared=True"`), pinned by recipe revision in `conanfile.py`; it replaces the `1.0.0b2-rc-3` remote package until otterbrix cuts a release tag from that branch (see `docs/proposals/otterbrix-embedding.md`, OTB-tag). Docker images cannot fetch it from the remote yet, so they build it from source out of the recipe archive written by `scripts/conan/export_local_packages.sh`. See **Docker Workflows**, "TEMPORARY: local otterbrix recipe". The package ships a single `libotterbrix.so`; the per-component libs such as `libotterbrix_log.so` are gone, and so is their `tsan.supp` entry
- Arrow 24.0.0 (with `with_s3=True`, `with_parquet=True`, `with_csv=True`, `with_json=True`, plus snappy/brotli/zlib/lz4/zstd compression codecs)
- Boost 1.88.0
- actor-zeta 2.0.0 (required by the otterbrix package), pinned as `actor-zeta/2.0.0#887bc992cac0ead59e5bdf5c90c844f4` in `conanfile.py` and taken from the `conan.otterbrix.com` remote like any other dependency: the remote has a Linux x86_64 gcc 13 binary, other configurations build from the recipe via `--build missing`. API constraints: no blocking `unique_future::get()/wait()`, no `co_await` on a future outside an actor coroutine. What OtterStax asks of actor-zeta (a waker on `unique_future`, parking an awaiting actor, destroying an actor on a live pool, restartable pools, the exception of a failed future): `docs/proposals/actor-zeta.md`
- Catch2 3.15.1 (v3 — `find_package(Catch2 3)` required by the otterbrix recipe)
- asio-grpc 3.7.0 (`backend=boost` in `conanfile.py`) over gRPC 1.69.0 — the Spark Connect server
- Spark Connect protocol 4.2.0 — the protos are vendored from apache/spark v4.2.0 into `frontend/spark_connect_server/proto/spark/connect/`; the server answers `SparkVersion` 4.2.0
- PySpark clients 3.5.0–4.2.0 — every released client (20 versions) runs the E2E matrix (`Dockerfile.spark-test`, see `tests/CLAUDE.md`)
