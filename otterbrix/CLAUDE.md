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

`parser/grammar_extention/` — pluggable parser extensions registered with the
core parser registry. Two ship by default: `s3/` and `file/`, both adding the
`CREATE EXTERNAL TABLE` and `COPY (...) TO ...` syntax. Each extension is a flex
+ bison pair (`*_scan.l` / `*_gram.y`) plus a small `*_extension.cpp` that
implements the registry hook and produces an `otterstax::external::external_node_t`
the `Worker` (behind the `Scheduler` router) routes via
`Worker::handle_external_statement`. See
[`parser/grammar_extention/CLAUDE.md`](parser/grammar_extention/CLAUDE.md) for the
extension recipe and `cmake/otterbrix_parser_extension.cmake` for the build helper.
#### `parser/grammar_extension/kafka/`

The Kafka DDL grammar extension (library `kafka_grammar`): a self-contained
flex+bison pair (`kafka_yy` prefix, isolated from the core parser) producing a
`kafka_node_t` for `CREATE/DROP SOURCE/STREAM`, registered into `GreenplumParser`
via `make_kafka_extension()`. Also defines `kafka_write_target` (detects
`INSERT INTO kafka.<obj>`) and `kafka_stream_source` / `kafka_find_aggregate`
(stream plan helpers). Namespace `otterstax::kafka`. The Kafka **runtime** that
consumes these nodes lives in `integration/kafka/` — see its CLAUDE.md.

### `query_generation/`

`sql_gen::generate_query()` — walks an Otterbrix logical plan node and produces a SQL string for a specific `backend_type_t`. Backend differences:
- MySQL: `database.collection` table reference
- PostgreSQL: `schema.collection` table reference
- ClickHouse: same as PostgreSQL currently

Call `sql_gen::table_reference(name, backend)` when you only need the table qualifier.

### `translators/input/`

Converts raw backend results or file bytes into Otterbrix `data_chunk_t`:

| File | Input type | Output |
|------|-----------|--------|
| `mysql_to_chunk` | `boost::mysql::results` | `data_chunk_t` |
| `mysql_to_complex` | `boost::mysql::results` | `complex_logical_type` (schema only) |
| `pg_to_chunk` | `PGresult*` | `data_chunk_t` |
| `ch_to_chunk` | ClickHouse result | `data_chunk_t` |
| `parquet_to_chunk` | parquet file or buffer | `data_chunk_t` (via Arrow + snappy/brotli/zlib/lz4/zstd) |
| `csv_to_chunk` | csv file or buffer | `data_chunk_t` |
| `ndjson_to_chunk` | ndjson file or buffer | `data_chunk_t` (TableReader; LSan note: Arrow's `BackgroundGenerator` worker thread races during teardown — suppressed in `tsan.supp`) |
| `arrow_to_chunk` | `arrow::RecordBatch` | `data_chunk_t` (shared shim used by parquet/csv/ndjson) |

The file-format loaders all take a `std::pmr::memory_resource*` and route every
allocation through it. **Lifetime trap:** the returned `data_chunk_t` keeps that
resource alive — if you borrow `otterbrix->dispatcher()->resource()`, the chunk
must die *before* the engine does, otherwise `synchronized_pool_resource`
deallocate sees freed memory (caught by TSan in
`tests/system/test_file_ingestion.cpp` previously).

`translators/internal/doc_to_chunk.cpp` handles conversion from Otterbrix document cursor rows.

### `translators/output/`

| File | Output |
|------|--------|
| `chunk_to_arrow` | `data_chunk_t` + `complex_logical_type` schema → Arrow `RecordBatch` for frontends |
| `chunk_to_parquet` | `data_chunk_t` → parquet file (snappy/brotli/zlib/lz4/zstd via Arrow) |
| `chunk_to_csv` | `data_chunk_t` → csv file |
| `chunk_to_ndjson` | `data_chunk_t` → ndjson file |

The three file-format writers are the `COPY (...) TO 'path'` companion to the
input loaders. `db::S3Manager::upload` and `conn::file::FileManager::dump_file`
pick the writer by `FileFormat` enum (auto-detected from the location's
extension when `format=` is omitted).

### `operators/`

`IDataManager` / `OtterbrixDataManager` — interface injected into `db::OtterbrixManager`. Provides `execute_plan()` and `get_schema()` against a live `otterbrix::otterbrix_ptr`. The interface exists so tests can swap in `SimpleMockOtterbrixManager` (see `tests/mock/otterbrix.hpp`).

### `config.hpp`

Re-exports the Otterbrix configuration type used when initialising the engine in `main.cpp` and in system tests.

## Adding a New Backend Translator

1. Create `translators/input/<backend>_to_chunk.{hpp,cpp}`
2. Follow `mysql_to_chunk` as the template — map each column type to the corresponding `components::types::logical_type`
3. Register in `otterbrix/CMakeLists.txt` under both `OTTERBRIX_HEADERS` and `OTTERBRIX_SOURCES`
4. Use the new translator in the relevant `integration/<backend>/connection_manager.cpp`
