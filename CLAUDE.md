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

All components are `actor_zeta::actor_mixin<T>` actors communicating through typed coroutine futures. The flow for every query:

```text
Frontend (MySQL/PG/FlightSQL)
  → Scheduler::execute()          — parse SQL, determine backend_type_t
  → CatalogManager                — resolve schemas, set backend type on ParsedQueryData
  → [if remote] integration/{sql,postgresql,clickhouse}::ConnectionManager
       → connectors/{mysql,postgresql,clickhouse}::ConnectorManager::executeQuery()
       → otterbrix/translators/input/  — convert raw results to data_chunk_t
  → [if local] integration/otterbrix::OtterbrixManager
       → otterbrix/operators/execute_plan — delegate to Otterbrix engine
  → otterbrix/translators/output/chunk_to_arrow — convert to Arrow RecordBatch
  → session cv_wrapper signalled → frontend sends response
```

### Key Types

- `session_hash_t` — unique ID threaded through every component for a single query
- `shared_session_payload` (`shared_data<session_payload>`) — CV-wrapped result buffer; frontend blocks on `.wait()`, backend calls `.set_result()`
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

## Directory Map

| Directory | CMake target | Role |
| --------- | ------------ | ---- |
| `connectors/` | `connectors` | Raw DB connections (Boost.MySQL, libpq, clickhouse-cpp) + HTTP connection API |
| `catalog/` | `catalog` | Schema discovery + connection type registry (`CatalogManager`) |
| `integration/` | `integration` | Actor wrappers bridging Scheduler ↔ ConnectorManagers |
| `otterbrix/` | `otterbrix_local` | Parser, SQL generator, translators, plan execution |
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

- Otterbrix 1.0.0a10-rc-10 (custom Conan remote: `http://conan.otterbrix.com`)
- Arrow 19.0.1 (with `with_flight_sql=True`)
- Boost 1.87.0
- actor-zeta 1.1.1
