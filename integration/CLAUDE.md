# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`integration/` contains the actor-based bridge between `Scheduler` and the raw `ConnectorManager` / Otterbrix engine. Each manager is an `actor_mixin<T>` that translates actor messages into async calls on the underlying connection pool or Otterbrix instance.

## Managers

| Directory | Actor class | What it wraps |
|-----------|-------------|---------------|
| `sql/` | `db::MySQLManager` | `mysql::ConnectorManager` |
| `postgresql/` | `db::PostgressManager` | `pg::ConnectorManager` |
| `clickhouse/` | `db::ClickHouseManager` | `ch::ConnectorManager` |
| `otterbrix/` | `db::OtterbrixManager` | `IDataManager` (wraps `otterbrix::otterbrix_ptr`) |

## Execution Pattern

All four managers follow the same structure:

1. Receive a `session_hash_t` + `ParsedQueryDataPtr` (or `OtterbrixStatementPtr` for Otterbrix)
2. Call `sql_gen::generate_query()` (remote managers) or `data_manager_->execute_plan()` (Otterbrix)
3. Pass raw results through the appropriate `otterbrix/translators/input/` converter
4. Return the translated result as `otterstax::result<ParsedQueryDataPtr>` or `cursor_t_ptr`

## OtterbrixManager

`OtterbrixManager` accepts an `IDataManager` rather than a direct `otterbrix_ptr`. This decoupling is load-bearing for tests: `tests/mock/otterbrix.hpp` provides `SimpleMockOtterbrixManager`. Do not change the constructor to take `otterbrix_ptr` directly.

The `get_schema` handler is called during the schema-resolution phase (before actual execution) to determine output column types from Otterbrix's local catalog. It returns a `(cursor_t_ptr, ParsedQueryDataPtr)` pair so the Scheduler can build the schema without executing the query.

## Known Limitation

`integration/sql/connection_manager.cpp:81` — MySQLManager currently handles one query at a time per connection. Parallelism comes from having multiple connections registered, not from connection multiplexing.
