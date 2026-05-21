# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`otterbrix/` is the glue layer between OtterStax and the external Otterbrix library. It does **not** contain business logic — it wraps Otterbrix types, translates data formats, and generates SQL strings for remote backends.

## Subdirectories

### `parser/`

`IParser` / `GreenplumParser` — thin interface over the Otterbrix SQL parser. `parse()` returns `ParsedQueryDataPtr` which carries:
- `otterbrix_params` (`OtterbrixStatementPtr`) — the logical plan with `external_nodes` marking which sub-trees must run remotely
- `binder` (`transform_result`) — parameter bindings for prepared statements
- `backend_type` — initially `Unknown`; filled in by `CatalogManager`
- `node_backend_types` — per-node backend for `Mixed` queries

`backend_type_t` enum is defined here because it must be visible to both the parser and all downstream actors.

### `query_generation/`

`sql_gen::generate_query()` — walks an Otterbrix logical plan node and produces a SQL string for a specific `backend_type_t`. Backend differences:
- MySQL: `database.collection` table reference
- PostgreSQL: `schema.collection` table reference
- ClickHouse: same as PostgreSQL currently

Call `sql_gen::table_reference(name, backend)` when you only need the table qualifier.

### `translators/input/`

Converts raw backend results into Otterbrix `data_chunk_t`:

| File | Input type | Output |
|------|-----------|--------|
| `mysql_to_chunk` | `boost::mysql::results` | `data_chunk_t` |
| `mysql_to_complex` | `boost::mysql::results` | `complex_logical_type` (schema only) |
| `pg_to_chunk` | `PGresult*` | `data_chunk_t` |
| `ch_to_chunk` | ClickHouse result | `data_chunk_t` |

`translators/internal/doc_to_chunk.cpp` handles conversion from Otterbrix document cursor rows.

### `translators/output/`

`chunk_to_arrow` — converts `data_chunk_t` + `complex_logical_type` schema into an Arrow `RecordBatch` for delivery to frontends.

### `operators/`

`IDataManager` / `OtterbrixDataManager` — interface injected into `db::OtterbrixManager`. Provides `execute_plan()` and `get_schema()` against a live `otterbrix::otterbrix_ptr`. The interface exists so tests can swap in `SimpleMockOtterbrixManager` (see `tests/mock/otterbrix.hpp`).

### `config.hpp`

Re-exports the Otterbrix configuration type used when initialising the engine in `main.cpp` and in system tests.

## Adding a New Backend Translator

1. Create `translators/input/<backend>_to_chunk.{hpp,cpp}`
2. Follow `mysql_to_chunk` as the template — map each column type to the corresponding `components::types::logical_type`
3. Register in `otterbrix/CMakeLists.txt` under both `OTTERBRIX_HEADERS` and `OTTERBRIX_SOURCES`
4. Use the new translator in the relevant `integration/<backend>/connection_manager.cpp`
