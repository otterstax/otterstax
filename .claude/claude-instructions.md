# Copilot Instructions for SQLFlight Server

## Architecture Overview

This is a **federated SQL query engine** that exposes multiple frontend protocols (Apache Arrow FlightSQL, MySQL wire protocol, PostgreSQL wire protocol) over a unified backend integrating:
- **Otterbrix**: The core query execution engine (external dependency)
- **MariaDB/MySQL pools**: Remote database connectors for federated queries
- **Actor-based concurrency**: Using `actor-zeta` for message-passing between components

### Core Components (Component Manager Pattern)

The system uses `ComponentManager` (`component_manager/`) as the central orchestrator spawning actor-based supervisors:
- `CatalogManager`: Manages database/table schemas from registered connections
- `Scheduler`: Routes queries to Otterbrix (local) or SQL connectors (remote)
- `OtterbrixManager`: Wraps Otterbrix database operations
- `SqlConnectionManager`: Delegates queries to MySQL connection pool
- `ConnectorManager`: Manages async MySQL connections via Boost.MySQL

**Critical Pattern**: All components communicate via `actor_zeta::send(address, &Actor::method, args...)` using `actor_zeta::address_t` handles and C++20 coroutines (`co_await`, `co_return`). Sessions are tracked by `session_hash_t` IDs.

## Key Architectural Patterns

### Session-Based Query Execution Flow
1. Frontend receives query → generates unique `session_hash_t`
2. Frontend spawns async task, registers `shared_flight_data` with Scheduler
3. Scheduler parses SQL → determines if Otterbrix-local or federated remote query
4. For federated: dispatches to `SqlConnectionManager` → `ConnectorManager` → MySQL
5. Results translated through `translators/` (MySQL results → Arrow format)
6. Completed data signaled via condition variable in `shared_flight_data`

**Session Management**: Use `utility/shared_flight_data.hpp` wrapper with CV for async wait/signal between components.

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
    &Scheduler::execute,  // Type-safe method pointer (no route enums)
    session_id, shared_data, sql_query
);
auto result = co_await std::move(future);
```

### Memory Management
- Use `std::pmr::memory_resource*` from Otterbrix dispatcher for all allocations
- Components use `actor_zeta::pmr::deleter_t` for unique_ptr cleanup
- Actors use `actor_mixin<T>` base class with inline message processing

### Namespace Organization
- `mysqlc::` - MySQL connector/catalog management
- `mysql_front::` - MySQL protocol frontend implementation
- `config::` - single config.yaml reader (server settings + connections; startup connection registration)
- `db_conn::` - Database integration actors (Otterbrix/SQL managers)

### Thread Safety Notes
- `ConnectorManager::addConnection/removeConnection` is **NOT thread-safe** (see TODOs in `connectors/mysql/mysql_manager.hpp:44-45`)
- `Scheduler` uses `std::mutex data_map_mtx_` for session map access
- Boost.MySQL connections run on `thread_pool_manager` io_context

## Common Tasks

### Adding a New Actor Handler
1. Add handler coroutine method to actor class (returns `unique_future<T>`)
2. Add method pointer to `dispatch_traits<...>` in actor header
3. Add dispatch branch in `behavior()` coroutine
4. Callers use `actor_zeta::send(address, &Actor::method, args...)`

### Adding External Database Support
1. Implement `IConnector` interface (see `connectors/mysql/mysql_connector.hpp`)
2. Add factory function to `ConnectorManager` constructor
3. Update `CatalogManager` schema discovery for new DB type
4. Add translator in `otterbrix/translators/input/` for result conversion

### Debugging Actor Messages
- Use `arrow::util::ArrowLog::StartArrowLog()` (set in `main.cpp`)
- Each actor uses `spdlog` logger with tag-based identification
- Session IDs are consistent across components - grep logs by `session_hash_t`

## Dependencies & Constraints

### Critical External Dependencies
- **Otterbrix 1.0.0a10-rc-10**: Custom Conan remote at `http://conan.otterbrix.com`
- **Arrow 19.0.1** with FlightSQL support (must set `with_flight_sql=True`)
- **Boost 1.87.0**: Required for C++20 coroutines in MySQL connector
- **actor-zeta 1.1.1**: Custom actor framework with C++20 coroutines (not Akka/CAF)

### Port Assignments (Hardcoded)
- 8815: FlightSQL server
- 8816: MySQL wire protocol
- 8817: PostgreSQL wire protocol
- 3101-3103: MariaDB test instances

### Known Limitations (from TODOs)
- No connection pool timeout handling (see `connectors/mysql/mysql_connector.hpp:115`)
- Single query per connection limitation (`db_integration/sql/connection_manager.cpp:81`)
- Array types only support single dimension (`otterbrix/query_generation/sql_query_generator.cpp:70`)

## Anti-Patterns to Avoid

1. **Don't** call `actor_zeta::spawn` outside `ComponentManager` - breaks lifecycle management
2. **Don't** use `std::make_unique` for actors - use `actor_zeta::spawn` with pmr deleter
3. **Don't** block on actor send() - actors communicate via coroutines and futures
4. **Don't** access session maps without mutex locks in `Scheduler`
5. **Don't** assume connection aliases are database names - they're logical identifiers for remote connections
