# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Test Suites

All C++ tests use Catch2 and are built with `-DBUILD_TESTS=ON`.

| Directory | Binary | What it tests |
|-----------|--------|---------------|
| `tests/system/` | `test_system` | Full Scheduler pipeline with real Otterbrix + mock connectors |
| `tests/unit/schema/` | `test_unit_schema` | `schema_utils` namespace |
| `tests/unit/utility/` | `test_unit_utility` | `cv_wrapper`, `session` helpers |
| `tests/mysql-front/` | `test_mysql_front` | MySQL protocol reader/writer, result-set encoding |

Python integration tests in `tests/` (e.g. `test_flightsql_client_mysql_backend.py`) run against live Docker containers via `docker-run-tests.sh`.

## Mock Layer (`tests/mock/`)

System tests build a full actor graph using real Otterbrix but injected mock connectors:

| Mock file | Replaces |
|-----------|----------|
| `mock/sql_db_connector.hpp` | `mysql::IConnector` — returns fixed result rows |
| `mock/pg_db_connector.hpp` | `pg::IConnector` |
| `mock/ch_db_connector.hpp` | `ch::IConnector` |
| `mock/otterbrix.hpp` | `IDataManager` (`SimpleMockOtterbrixManager`) |
| `mock/parser.hpp` | `IParser` — returns pre-built `ParsedQueryData` |
| `mock/mock_config.hpp` | Otterbrix config (`configuration::config::default_config()`) |

Connectors are injected via the `connector_factory` constructor parameter on `ConnectorManager`. Otterbrix is injected via `OtterbrixManager(resource, make_unique<SimpleMockOtterbrixManager>())`.

## Running a Single Test

```bash
# Run one Catch2 test by name
./build/test_system "[test name]"

# List all available test names
./build/test_system --list-tests

# Run with verbose output
./build/test_system -s
```

## Python Test Structure

Python tests share common helpers via `config.py` (connection params) and `query_data.py` (expected result fixtures). File names encode: `test_{protocol}_client_{backend}_backend[_mutable].py`. The `test_cross_backend_queries*.py` files exercise mixed-backend joins.

Run a single Python test file:
```bash
python -m pytest tests/test_flightsql_client_mysql_backend.py -v
```
(Requires live Docker stack — run `docker compose up -d` first and register connections via `add_connections_maria_db.sh`.)
