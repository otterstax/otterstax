# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`integration/` contains the actor-based bridge between the `Scheduler`'s `Worker` pool and the raw `ConnectorManager` / Otterbrix engine. Each manager is an `actor_mixin<T>` that translates actor messages into async calls on the underlying connection pool or Otterbrix instance. In the Scheduler→Worker pool architecture, every call into these managers is sent from a `Worker` (the `Scheduler` itself is just a thin router; see `scheduler/CLAUDE.md`).

## Managers

| Directory | Actor class | What it wraps |
|-----------|-------------|---------------|
| `sql/` | `db::MySQLManager` | `mysql::ConnectorManager` |
| `postgresql/` | `db::PostgressManager` | `pg::ConnectorManager` |
| `clickhouse/` | `db::ClickHouseManager` | `ch::ConnectorManager` |
| `otterbrix/` | `db::OtterbrixManager` | `IDataManager` (wraps `otterbrix::otterbrix_ptr`) |
| `kafka/` | `otterstax::kafka::KafkaManager` | librdkafka consumer/producer threads + Kafka SOURCE/STREAM objects (NOT a remote-DB connector — own runtime; has its own CLAUDE.md) |

## Execution Pattern

All four managers follow the same structure:

1. Receive a `session_hash_t` + `ParsedQueryDataPtr` (or `OtterbrixStatementPtr` for Otterbrix)
2. Call `sql_gen::generate_query()` (remote managers) or `data_manager_->execute_plan()` (Otterbrix)
3. Pass raw results through the appropriate `otterbrix/translators/input/` converter
4. Return the translated result as `otterstax::result<ParsedQueryDataPtr>` or `cursor_t_ptr`

## OtterbrixManager

`OtterbrixManager` accepts an `IDataManager` rather than a direct `otterbrix_ptr`. This decoupling is load-bearing for tests: `tests/mock/otterbrix.hpp` provides `SimpleMockOtterbrixManager`. Do not change the constructor to take `otterbrix_ptr` directly.

The `get_schema` handler is called during the schema-resolution phase (before actual execution) to determine output column types from Otterbrix's local catalog. It returns a `(cursor_t_ptr, ParsedQueryDataPtr)` pair so the calling `Worker` can build the schema (via `Worker::prepare_schema` → `finish_schema_value`) without executing the query.

## S3Manager (`s3/`) and the file path

`db::S3Manager` is **not** a query backend and does not follow the four-manager pattern above; it
orchestrates object I/O by bridging the raw S3 connector (`conn::s3::ConnectorManager`) and the
file-mapping connector (`conn::file::FileManager`):

- `download` — pulls an S3 object, then `FileManager::add_file` ingests it into a `database.table`.
- `upload` — runs a **pre-parsed `OtterbrixStatementPtr`** and writes its result to S3. It hands the
  statement to `FileManager::dump_file`, which executes it via `db::OtterbrixManager::execute` and dumps the
  resulting chunk. This replaced the old whole-table `OtterbrixManager::read_table` handler (removed, along
  with `IDataManager::read_table`), so `COPY (<select>) TO 's3://…'` can export an arbitrary query — not
  just a table.
- `ls` — lists objects for an alias.

Its handlers (and `FileManager`'s) `co_await` the cross-actor sends rather than `.take_ready()`, so they
stay correct if those callees ever move off the synchronous busy-wait path. End-to-end coverage lives in
the python integration suite (`tests/test_{schema_}mysql_s3.py`,
`tests/test_mysql_join_sql_s3_to_s3.py`, `tests/test_mysql_join_otb_local_s3.py`, and
`tests/test_mysql_join_otb_local_s3_file.py` — 3-origin: s3 parquet ⋈ file csv ⋈
otterbrix-internal), driven by `docker-run-tests.sh`.

## Known Limitation

`integration/sql/connection_manager.cpp:81` — MySQLManager currently handles one query at a time per connection. Parallelism comes from having multiple connections registered, not from connection multiplexing.
