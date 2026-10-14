# Copilot Instructions for SQLFlight Server

## Architecture Overview

This is a **federated SQL query engine** that exposes multiple frontend protocols (Apache Arrow FlightSQL, MySQL wire protocol, PostgreSQL wire protocol) over a unified backend integrating:
- **Otterbrix**: The core query execution engine (external dependency)
- **MariaDB/MySQL pools**: Remote database connectors for federated queries
- **Actor-based concurrency**: Using `actor-zeta` for message-passing between components

### Core Components (process modules)

`main.cpp` owns every part directly, builds them in order and destroys them in reverse:
`runtime_t` (memory + thread pools) → `remote_backends_t` (connector managers, the
MySQL / PostgreSQL / ClickHouse integration backends, startup discovery, the federation
registry) → `engine_t` (the embedded Otterbrix engine, opened with the federation's
hooks) → the S3 / file / Kafka actors (then `backends::register_s3_aliases` /
`publish_federation_metadata`) → `session_factory_t` (its destructor drains the
sessions and stops their pool) → the four servers (each stops in its destructor).
- `catalog::` (`catalog/`): pure building blocks — remote tables are rows of the engine
  database `federation`, resolved by the federation's `need` / `decide` hooks
  (`otterbrix/federation/`)
- `query_session_t`: one actor per client connection; parses, classifies and runs every
  statement on the connection's own engine session (local or federated remote)
- `otterstax::engine::*` (`otterbrix/operators/engine_requests.hpp`): free functions sent
  straight to the engine dispatcher — there is no engine wrapper actor
- `MySQLManager` / `PostgressManager` / `ClickhouseManager`: delegate queries to the connector pools
- `ConnectorManager`: Manages async MySQL connections via Boost.MySQL

**Critical Pattern**: All components communicate via `actor_zeta::send(address, &Actor::method, args...)` using `actor_zeta::address_t` handles and C++20 coroutines (`co_await`, `co_return`). Sessions are tracked by `session_hash_t` IDs.

## Key Architectural Patterns

### Session-Based Query Execution Flow
1. Frontend receives query → generates unique `session_hash_t`
2. Frontend sends `query_session_t::execute` (and friends) to its connection's session, which returns
   `actor_zeta::unique_future<core::result_wrapper_t<session_payload>>`
3. The session parses SQL and runs it on the engine (remote DDL and four-part DML
   go to the backend directly)
4. The engine resolves a remote table through the federation and reads / writes it
   through the integration backend (`federation::backend_t`: `MySQLManager` → `ConnectorManager` → MySQL)
5. Results translated through `otterbrix/translators/` (backend results → `data_chunk_t`)
6. The connection awaits the future through
   `frontend/common/asio_future_bridge.hpp` — polled on the per-connection asio
   executor, no blocking get and no condition variable

**Session Management**: the payload type is `session_payload` in
`session/session_payload.hpp`, carried by value through the typed future.

### Federated Query Pattern (Critical!)
Queries use **connection aliases** as database names:
```sql
SELECT * FROM campaigns.db1.schema.campaigns
JOIN impressions.db2.schema.impressions ON campaigns.id = impressions.id
```
- `campaigns` and `impressions` are **connection aliases** (not databases!)
- Each alias maps to a connection declared in the `connections:` section of the
  single `config.yaml` (see `examples/simple/example_connetion/config.yaml`)
- Connections are read once at server startup from that file (default `config.yaml`,
  override with `--config PATH`). There is no runtime add/remove API.

### Parser and Query Generation
- `otterbrix/parser/parser.hpp`: Wraps Otterbrix SQL parser as `IParser` interface
- `otterbrix/query_generation/sql_query_generator.cpp`: Generates SQL for remote databases from parsed logical plans
- **Pushdown**: the federation's optimizer rule (`otterbrix/federation/rules/pushdown.hpp`) turns a query block over one remote table into one statement in the backend's dialect

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

**Verified on this machine:** the default `-j nproc` OOMs the Docker build
(`ResourceExhausted: cannot allocate memory`) — it dies partway through, so the
`ctest`/integration steps never run and the failure looks unrelated to your
change. `-j 4` builds cleanly. Always run:
`docker build --build-arg BUILD_JOBS=4 -f Dockerfile.test -t otterstax-test .`
and `./docker-run-tests.sh -j 4` (unit: 194 pass, integration: 27 pass at -j 4).

### Building & Running (Docker-First)
```bash
# Build with Conan dependencies (see conanfile.py for otterbrix/arrow versions)
docker compose up           # Runs app + 3 MariaDB instances
# OR local build:
# IMPORTANT: if conanfile.py changed, remove the existing build dir first:
#   rm -rf ./build
conan install conanfile.py --build missing -s build_type=Release
cmake -S . -B build/Release \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
# --parallel N: N = min(nproc, floor(RAM_MB/1536)) — see "Build resources" above
/usr/local/bin/cmake --build /workspaces/otterstax/build/Release --parallel 5 --
```

### Testing (Critical Setup Steps)
```bash
# Use docker-run-tests.sh - it handles volume initialization timing issues.
# Cap build jobs to RAM to avoid OOM: ./docker-run-tests.sh -j <JOBS>
chmod +x ./docker-run-tests.sh
./docker-run-tests.sh

# Manual test workflow:
1. fixtures/generate_data.py          # Creates MariaDB init SQL scripts
2. docker compose up -d                # Starts services (connections read from
                                       #   scripts/database/config.yaml at startup)
3. python examples/simple/flight_sql_example.py examples/simple/example_1.txt  # Executes federated query
```

**Testing Anti-Pattern**: Docker volumes (`mariadb*_init`) can lag on cold starts. The test script includes `wait_for_database_init()` with 120-sec timeout - respect this when writing integration tests.

### Unit Tests Structure
- `tests/unit/`: Component-level tests (mocked actors)
- `tests/system/`: Integration tests with real Otterbrix/MySQL (use `Catch2`)
- `tests/mysql-front/`: MySQL protocol layer tests
- Build tests with `-DBUILD_TESTS=ON` in CMake

## Code Conventions

### Actor Communication Pattern
```cpp
// Send message to actor (returns future for coroutine-based actors)
auto [needs_sched, future] = actor_zeta::send(
    target_address,
    &query_session_t::execute,  // Type-safe method pointer (no route enums)
    statement_id, sql_query
);
auto result = co_await std::move(future);
```

### Memory Management
- Use `std::pmr::memory_resource*` from Otterbrix dispatcher for all allocations
- Components use `actor_zeta::pmr::deleter_t` for unique_ptr cleanup
- Actors use `actor_mixin<T>` base class with inline message processing

### Namespace Organization
- `mysql::` / `pg::` / `ch::` - the connectors and their `ConnectorManager`s
- `mysql_front::` - MySQL protocol frontend implementation
- `config::` - single config.yaml reader (server settings + connections)
- `db::` - integration backends (`MySQLManager`, `PostgressManager`, `ClickhouseManager`) and the `S3Manager` actor
- `otterstax::federation` / `otterstax::catalog` / `otterstax::backends` - federation provider, catalog building blocks, process modules

### Thread Safety Notes
- `ConnectorManager::addConnection` / `removeConnection` are the only writes to a connector registry and run on the startup thread (`remote_backends_t::open`) before any query reaches the integration backend that drives the manager
- Statements of one connection run one at a time on its `query_session_t` (the mailbox is the queue)
- Each connector manager owns a `thread_pool_manager` io_context, sized by `service.runtime.connector_io`

## Common Tasks

### Adding a New Actor Handler
1. Add handler coroutine method to actor class (returns `unique_future<T>`)
2. Add method pointer to `dispatch_traits<...>` in actor header
3. Add dispatch branch in `behavior()` coroutine
4. Callers use `actor_zeta::send(address, &Actor::method, args...)`

### Adding External Database Support
1. Implement `IConnector` interface (see `connectors/mysql/connector.hpp`)
2. Add factory function to `ConnectorManager` constructor
3. Add an integration backend answering `discover` and implementing `federation::backend_t`, opened by `remote_backends_t::open`
4. Add translator in `otterbrix/translators/input/` for result conversion

### Debugging Actor Messages
- Use `arrow::util::ArrowLog::StartArrowLog()` (set in `main.cpp`)
- Each actor uses `spdlog` logger with tag-based identification
- Session IDs are consistent across components - grep logs by `session_hash_t`

## Dependencies & Constraints

### Critical External Dependencies
- **Otterbrix 1.0.0b2-embedding.46fba865**: a local Conan package pinned by recipe revision in `conanfile.py` (see the root `CLAUDE.md`, Critical Dependency Versions)
- **Arrow 24.0.0** (S3, parquet, csv, json; Flight SQL is served in-house, no Arrow Flight dependency)
- **Boost 1.88.0**: Required for C++20 coroutines in MySQL connector
- **actor-zeta 2.0.0**: Custom actor framework with C++20 coroutines (not Akka/CAF)
- **Catch2 3.15.1**: v3 API (`catch2/catch_all.hpp`, `find_package(Catch2 3)`)

### Port Assignments (Hardcoded)
- 8815: FlightSQL server
- 8816: MySQL wire protocol
- 8817: PostgreSQL wire protocol
- 3101-3103: MariaDB test instances

### Known Limitations (from TODOs)
- A statement waits up to 60 s for a free pooled connection of its alias (`connectors/common/connection_pool.hpp`); `pool_size` per alias in `config.yaml`
- Array types only support single dimension (`otterbrix/query_generation/sql_query_generator.cpp:70`)

## Anti-Patterns to Avoid

1. **Don't** call `actor_zeta::spawn` outside the owner of the actor (`remote_backends_t`, `main.cpp` / the test stack for the S3 / file / Kafka actors, `session_factory_t`) - breaks lifecycle management
2. **Don't** use `std::make_unique` for actors - use `actor_zeta::spawn` with pmr deleter
3. **Don't** block on actor send() - actors communicate via coroutines and futures
4. **Don't** destroy a `query_session_t` directly - drop its `session_ptr` (close protocol, `session/CLAUDE.md`)
5. **Don't** assume connection aliases are database names - they're logical identifiers for remote connections
