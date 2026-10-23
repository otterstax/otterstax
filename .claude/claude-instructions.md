# Claude Instructions for OtterStax

## Architecture Overview

This is a **federated SQL query server** that exposes four client frontends — Arrow
Flight SQL (8815, an in-house implementation), the MySQL wire (8816), the PostgreSQL
wire (8817) and Spark Connect (15002) — over one backend integrating:

- **Otterbrix**: the core query execution engine (external dependency)
- **Remote backends**: MySQL/MariaDB, PostgreSQL and ClickHouse, reached through
  per-alias connection pools, plus S3 and local files for external tables
- **Actor-based concurrency**: `actor-zeta` for message passing between components

### Core Components (Component Manager Pattern)

`ComponentManager` (`component_manager/`) is the central orchestrator; it spawns the
actors and owns the connection pools:

- `CatalogManager`: Manages database/table schemas from registered connections
- `Scheduler`: session-affinity router over a pool of `Worker` actors; each Worker runs
  the full pipeline (parse → catalog → backends → engine)
- `OtterbrixManager`: Wraps Otterbrix database operations
- `db::backend_registry_t` (`integration/backends.hpp`): the per-alias connection pools (MySQL, PostgreSQL, ClickHouse) and the S3 / local-file transfer pools; every Worker and the catalog build their backends (`db::backend_t`) over it

**Critical Pattern**: All components communicate via `actor_zeta::send(address, &Actor::method, args...)` using `actor_zeta::address_t` handles and C++20 coroutines (`co_await`, `co_return`). Sessions are tracked by `session_hash_t` IDs.

## Key Architectural Patterns

### Session-Based Query Execution Flow

1. Frontend receives query → generates unique `session_hash_t`
2. Frontend calls `Scheduler::execute` (and friends), which returns
   `actor_zeta::unique_future<core::result_wrapper_t<session_payload>>`
3. Scheduler routes by session hash → a `Worker` parses SQL and determines if the
   query is Otterbrix-local or federated remote
4. For federated: the Worker's own backend sends each slot's statement to the alias's connection pool and `co_await`s the answer (`connectors/CLAUDE.md`)
5. Results translated through `otterbrix/translators/` (backend results → `data_chunk_t`)
6. The connection awaits the future through
   `frontend/common/asio_future_bridge.hpp` — polled on the per-connection asio
   executor, no blocking get and no condition variable

**Session Management**: the payload type is `session_payload` in
`scheduler/session_data.hpp`, carried by value through the typed future.

### Federated Query Pattern (Critical!)

Tables behind a connection are named `alias.db.table` (or `alias.db.schema.table` for
PostgreSQL); see the root `CLAUDE.md`, "Federated Query Syntax":
```sql
SELECT * FROM my.bill.orders o
JOIN pg.shop.customers c ON o.customer_id = c.id
```

- `my` and `pg` are **connection aliases** (not databases!)
- Each alias maps to a connection declared in the `connections:` section of the
  single `config.yaml` (see `config.yaml` at the repo root, or `examples/demo/config.yaml`)
- Connections are read once at server startup from that file (default `config.yaml`,
  override with `--config PATH`). There is no runtime add/remove API.

### Parser and Query Generation

- `otterbrix/parser/parser.hpp`: Wraps Otterbrix SQL parser as `IParser` interface
- `otterbrix/query_generation/sql_query_generator.cpp`: Generates SQL for remote databases from parsed logical plans
- **External nodes**: Query plans mark nodes requiring remote execution (see `OtterbrixStatement::external_nodes`)

## Development Workflows

### Build resources — CHECK RAM + THREADS FIRST (avoid OOM)

**Before any build (local or Docker), cap parallelism to available RAM.** Each
compile job needs **~1.5–2 GB**; blindly using `-j nproc` OOM-kills the compiler
(`c++: ... Killed`) or fails a Docker build with `ResourceExhausted`. Docker
Desktop's VM RAM is usually much smaller than the host's, so check the Docker
limit, not just `nproc`:

```bash
nproc                                                   # host CPUs (macOS: sysctl -n hw.ncpu)
docker info --format '{{.NCPU}} CPUs / {{.MemTotal}} B' # what Docker builds actually get
```

Choose `JOBS = min(nCPU, floor(RAM_MB / 1536))` (~1.5 GB/job). Then pass it:
`cmake --build ... --parallel <JOBS>`, `docker build --build-arg BUILD_JOBS=<JOBS>`,
`./docker-run-tests.sh -j <JOBS>`. The benchmark scripts auto-cap with this
formula when `-j` is omitted. Example: an 8 GB Docker VM ⇒ 5 jobs even on 14 CPUs.

The default `-j nproc` OOMs the Docker build partway through (`ResourceExhausted:
cannot allocate memory`), so the `ctest`/integration steps never run and the failure
looks unrelated to your change. Always pass the capped value:
`docker build --build-arg BUILD_JOBS=<JOBS> -f Dockerfile.test -t otterstax-test .`
and `./docker-run-tests.sh -j <JOBS>`.

### Building & Running (Docker-First)
```bash
# Build with Conan dependencies (see conanfile.py for otterbrix/arrow versions)
docker compose up           # Runs app + 2 MariaDB, 1 PostgreSQL, 1 ClickHouse
# OR local build:
# IMPORTANT: if conanfile.py changed, remove the existing build dir first:
#   rm -rf ./build
conan install conanfile.py --build missing -s build_type=Release
cmake -S . -B build/Release \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
# --parallel N: N = min(nproc, floor(RAM_MB/1536)) — see "Build resources" above
cmake --build build/Release --parallel <JOBS>
```

### Testing (Critical Setup Steps)
```bash
# Use docker-run-tests.sh - it handles volume initialization timing issues.
# Cap build jobs to RAM to avoid OOM: ./docker-run-tests.sh -j <JOBS>
chmod +x ./docker-run-tests.sh
./docker-run-tests.sh -j <JOBS>

# Manual test workflow:
1. python fixtures/generate_data.py    # Creates MariaDB init SQL scripts
2. docker compose up -d                # Starts services (connections read from
                                       #   scripts/database/config.yaml at startup)
3. query it over any wire, e.g. the otterstax-sql skill (psql on 8817)
```

**Testing Anti-Pattern**: Docker volumes (`mariadb*_init`) can lag on cold starts. The test script includes `wait_for_database_init()` with 120-sec timeout - respect this when writing integration tests.

### Unit Tests Structure

- `tests/unit/`: Component-level tests (parser, schema, translators, config, connection pool, drivers against fake servers)
- `tests/system/`: Scheduler/Worker pipeline over the real Otterbrix engine with mock drivers (`Catch2`)
- `tests/mysql-front/`: MySQL protocol layer tests (plus PG packets and Flight SQL pieces)
- Build tests with `-DBUILD_TESTS=ON` in CMake; `tests/CLAUDE.md` has the details

### Benchmarks

- End-to-end suite: `benchmark/scripts/run_benchmark.sh` (Docker; pass an absolute `--out-dir`)
- Regression check of a change (baseline build vs working tree, interleaved runs): the
  `otterstax-benchmark` skill (`.claude/skills/otterstax-benchmark/ab_bench.sh`)
- See `benchmark/CLAUDE.md`

## Code Conventions

### Actor Communication Pattern
```cpp
// Send message to actor (returns future for coroutine-based actors)
auto [needs_sched, future] = actor_zeta::send(
    target_address,
    &Scheduler::execute,  // Type-safe method pointer (no route enums)
    session_id, sql_query
);
auto result = co_await std::move(future);
```

### Memory Management

- Use `std::pmr::memory_resource*` from Otterbrix dispatcher for all allocations
- Components use `actor_zeta::pmr::deleter_t` for unique_ptr cleanup
- `Scheduler`, `CatalogManager`, `OtterbrixManager` and `KafkaManager` derive from
  `actor_mixin<T>`; a `Worker` is a `basic_actor<Worker>` on a `sharing_scheduler`
  (`scheduler/CLAUDE.md`)

### Namespace Organization

- `frontend::{mysql,postgres,spark}`, `flight::` - the wire frontends
- `conn::` (+ `conn::s3`, `conn::file`), `mysql::`, `pg::`, `ch::` - connection pools and drivers (`connectors/`)
- `db::` - backends over the pools (`db::backend_t`, `db::backend_registry_t`) and `db::OtterbrixManager`
- `mysql::CatalogManager`, `otterstax::catalog` - schema discovery and the catalog
- `config::` - single config.yaml reader (server settings + connections; startup connection registration)
- `otterstax::kafka` - the Kafka runtime

### Thread Safety Notes

- `connection_pool_t::add` (and the transfer pools' `add_s3` / `set_files`) are the only writes to the pool registries and run on the startup thread before any query; there is no remove path
- A session's prepared statement lives in its Worker's `metadata_map_`, unguarded: sticky routing (`workers_[id % N]`) means one Worker handles every message of a session
- The `Scheduler`'s inbox is lock-free; all its coroutine work runs on its own loop thread
- Each pooled connection runs on its own pool thread; a MySQL driver runs Boost.MySQL's async operations on a private single-thread io_context

## Common Tasks

### Adding a New Actor Handler

1. Add handler coroutine method to actor class (returns `unique_future<T>`)
2. Add method pointer to `dispatch_traits<...>` in actor header
3. Add dispatch branch in `behavior()` coroutine
4. Callers use `actor_zeta::send(address, &Actor::method, args...)`

### Adding External Database Support

1. Implement `conn::IDriver` with a free `decode` (see `connectors/clickhouse/driver.hpp`)
2. Add a connection pool and metadata to `db::backend_registry_t` and a traits struct for `db::backend_t` (discovery included)
3. Register it in `ComponentManager::register_connections` and route it in `db::backend_set_t` (`execute`, `describe`) and the catalog
4. Add translator in `otterbrix/translators/input/` for result conversion

### Debugging Actor Messages

- Each component uses an `spdlog` logger with tag-based identification (`utility/logger.hpp`); verbosity is set at configure time (`-DSPDLOG_ACTIVE_LEVEL=...`, default ERROR)
- Session IDs are consistent across components - grep logs by `session_hash_t`
- Tracy zones (`utility/tracy_profiler.hpp`, `-DENABLE_TRACY=ON`) for timelines

## Dependencies & Constraints

### Critical External Dependencies

- **Otterbrix 1.0.0b2-rc-3**: Custom Conan remote at `https://conan.otterbrix.com`, pinned by recipe revision in `conanfile.py`
- **Arrow 24.0.0** with S3, Parquet, CSV and JSON (no Arrow Flight: Flight SQL is served in-house)
- **Boost 1.88.0**: Boost.MySQL and Asio (C++20 coroutines in the MySQL driver)
- **actor-zeta 1.2.0**: Custom actor framework with C++20 coroutines (not Akka/CAF)
- **Catch2 3.15.1**: v3 API (`catch2/catch_all.hpp`, `find_package(Catch2 3)`)
- **asio-grpc 3.7.0 / gRPC 1.69.0**: the Spark Connect and Flight SQL servers

### Port Assignments

- 8815: Flight SQL server
- 8816: MySQL wire protocol
- 8817: PostgreSQL wire protocol
- 15002: Spark Connect
- 3101-3104: the root `compose.yml` test backends (MariaDB ×2, PostgreSQL, ClickHouse)

### Known Limitations

- Array types only support a single dimension (the `TODO: multiple dimentions array` in `otterbrix/query_generation/sql_query_generator.cpp`)
- The root `CLAUDE.md`, "Known Constraints", lists the current engine and runtime constraints

## Anti-Patterns to Avoid

1. **Don't** call `actor_zeta::spawn` outside `ComponentManager` (or the Scheduler, which spawns its Workers) - breaks lifecycle management
2. **Don't** use `std::make_unique` for actors - use `actor_zeta::spawn` with pmr deleter
3. **Don't** block on actor send() - actors communicate via coroutines and futures
4. **Don't** share mutable state between Workers - a session is pinned to one Worker, and each Worker owns its parser and backends
5. **Don't** assume connection aliases are database names - they're logical identifiers for remote connections
