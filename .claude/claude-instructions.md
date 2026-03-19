# Copilot Instructions for SQLFlight Server

## Architecture Overview

This is a **federated SQL query engine** that exposes multiple frontend protocols (Apache Arrow FlightSQL, MySQL wire protocol, HTTP API) over a unified backend integrating:
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

**Critical Pattern**: All components communicate via `actor_zeta::send()` using `actor_zeta::address_t` handles and route enums (see `routes/*.hpp`). Sessions are tracked by `session_hash_t` IDs.

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
- Each alias maps to a registered MySQL connection (see `client_example/example_connetion/*.json`)
- Register connections via HTTP API: `POST localhost:8085/add_connection` with JSON payload

### Parser and Query Generation
- `otterbrix/parser/parser.hpp`: Wraps Otterbrix SQL parser as `IParser` interface
- `otterbrix/query_generation/sql_query_generator.cpp`: Generates SQL for remote databases from parsed logical plans
- **External nodes**: Query plans mark nodes requiring remote execution (see `OtterbrixStatement::external_nodes`)

## Development Workflows

### Building & Running (Docker-First)
```bash
# Build with Conan dependencies (see conanfile.py for otterbrix/arrow versions)
docker compose up           # Runs app + 3 MariaDB instances
# OR local build:
conan install conanfile.py --build missing -s build_type=Release
cmake .. -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake --build . -- -j$(nproc)
```

### Testing (Critical Setup Steps)
```bash
# Use docker-run-tests.sh - it handles volume initialization timing issues
chmod +x ./docker-run-tests.sh
./docker-run-tests.sh

# Manual test workflow:
1. fixtures/generate_data.py          # Creates MariaDB init SQL scripts
2. docker compose up -d                # Starts services
3. cd client_example && ./example_connetion/add_connections_maria_db.sh  # Registers connections
4. python client.py example_1.txt      # Executes federated query
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
// Send message to actor (async, no return)
actor_zeta::send(
    target_address,
    self_address,
    scheduler::handler_id(scheduler::route::execute),  // Route from routes/*.hpp
    session_id, shared_data, sql_query
);
```

### Memory Management
- Use `std::pmr::memory_resource*` from Otterbrix dispatcher for all allocations
- Components use `actor_zeta::pmr::deleter_t` for unique_ptr cleanup
- Never manually delete actor supervisors (handled by actor-zeta)

### Namespace Organization
- `mysqlc::` - MySQL connector/catalog management
- `mysql_front::` - MySQL protocol frontend implementation
- `http_server::` - HTTP API for connection management
- `db_conn::` - Database integration actors (Otterbrix/SQL managers)

### Thread Safety Notes
- `ConnectorManager::addConnection/removeConnection` is **NOT thread-safe** (see TODOs in `connectors/mysql/mysql_manager.hpp:44-45`)
- `Scheduler` uses `std::mutex data_map_mtx_` for session map access
- Boost.MySQL connections run on `thread_pool_manager` io_context

## Common Tasks

### Adding a New Query Route
1. Add enum to `routes/scheduler.hpp` or relevant route file
2. Add behavior in target actor's constructor (e.g., `Scheduler::behavior()`)
3. Implement handler method following `execute_*` pattern
4. Update senders to use new `handler_id(route::new_route)`

### Adding External Database Support
1. Implement `IConnector` interface (see `connectors/mysql/mysql_connector.hpp`)
2. Add factory function to `ConnectorManager` constructor
3. Update `CatalogManager` schema discovery for new DB type
4. Add translator in `otterbrix/translators/input/` for result conversion

### Debugging Actor Messages
- Use `arrow::util::ArrowLog::StartArrowLog()` (set in `main.cpp`)
- Each actor has `make_type()` returning string identifier for logging
- Session IDs are consistent across components - grep logs by `session_hash_t`

## Dependencies & Constraints

### Critical External Dependencies
- **Otterbrix 1.0.0a10-rc-10**: Custom Conan remote at `http://conan.otterbrix.com`
- **Arrow 19.0.1** with FlightSQL support (must set `with_flight_sql=True`)
- **Boost 1.87.0**: Required for C++20 coroutines in MySQL connector
- **actor-zeta 1.0.0a12**: Custom actor framework (not Akka/CAF)

### Port Assignments (Hardcoded)
- 8815: FlightSQL server
- 8816: MySQL wire protocol
- 8085: HTTP connection management API
- 3101-3103: MariaDB test instances

### Known Limitations (from TODOs)
- No connection pool timeout handling (see `connectors/mysql/mysql_connector.hpp:115`)
- Single query per connection limitation (`db_integration/sql/connection_manager.cpp:81`)
- Array types only support single dimension (`otterbrix/query_generation/sql_query_generator.cpp:70`)

## Anti-Patterns to Avoid

1. **Don't** call `actor_zeta::spawn_supervisor` outside `ComponentManager` - breaks lifecycle management
2. **Don't** use `std::make_unique` for actors - use `actor_zeta::spawn_supervisor` with pmr deleter
3. **Don't** block on actor send() - actors communicate asynchronously via message passing
4. **Don't** access session maps without mutex locks in `Scheduler`
5. **Don't** assume connection aliases are database names - they're logical identifiers for remote connections
