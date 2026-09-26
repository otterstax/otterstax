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
- ClickHouse: `database.collection` with backticks, like MySQL. ClickHouse 23.8 has no `UPDATE`
  statement, so `UPDATE` is generated as the `ALTER TABLE ... UPDATE` mutation; both it and the
  lightweight `DELETE` require a `WHERE`, so a statement without a predicate gets `WHERE 1`

Call `sql_gen::table_reference(name, backend, resource)` when you only need the table qualifier. It answers
`core::result_wrapper_t<std::string>`: a backend other than MySQL / PostgreSQL / ClickHouse, or an empty name,
is `invalid_parameter` — there is no default dialect and no placeholder table name.

**Expressions.** One pair of recursive writers covers every clause: `write_operand` for a
`param_storage` operand (key / bound parameter / nested expression) and `write_expr` dispatching on
`expression_i::group()` — scalar (arithmetic, unary minus, bitwise, coalesce), aggregate, function,
cast, and a comparison used as a value. There is no separate UPDATE-SET tree: an assignment IS its
value expression, named by the column it assigns. What no dialect spells comes back as
`unimplemented_yet`, a malformed plan as `invalid_parameter`, each message naming the clause.
Notable dialect points:

- the SELECT list is read off `node_group_t` (the transformer moves every projected expression
  there), the grouping-key markers and the engine-internal `__group_key_*` / `__having_*` outputs
  excluded — those are written inline where HAVING or ORDER BY references them, and never projected;
- ORDER BY is taken from `sort_expression_t::operand()`, which must be a key (a computed key has no
  name on the backend);
- SQL LIKE arrives as a `regexp_like` function call carrying the LIKE pattern and its flags, so it is
  written as `[NOT] LIKE` / `[NOT] ILIKE` (MySQL, which has no ILIKE, gets `LOWER(...) LIKE LOWER(...)`
  for the case-insensitive form); a real regular expression is `unimplemented_yet`;
- `CAST` takes each dialect's own target-type spelling (MySQL `SIGNED` / `UNSIGNED` / `CHAR`, not a
  column type), and `TRY_CAST` is `unimplemented_yet`;
- `UPDATE ... FROM` / `DELETE ... USING` are pushed down when the plan's source child is a plain
  table of the same backend and `target.from_name` resolves it: PostgreSQL `UPDATE t SET … FROM s` /
  `DELETE FROM t USING s`, MySQL the multi-table `UPDATE t, s SET t.c = …` / `DELETE t FROM t, s`
  (the plan's join condition is the WHERE, so the comma forms are the faithful ones), with every
  column qualified by the table its key's side names. A source on another backend or a local one, a
  source that is not a plain table, and ClickHouse (whose mutations name one table) are refused —
  never silently emitted as a single-table statement.

### `translators/input/`

Converts raw backend results or file bytes into Otterbrix `data_chunk_t`:

| File | Input type | Output |
|------|-----------|--------|
| `mysql_to_chunk` | `boost::mysql::results` | `result_wrapper_t<data_chunk_t>` (`conversion_failure` on a MySQL column type without a mapping) |
| `mysql_to_struct` (same file) | `boost::mysql::results` | `result_wrapper_t<complex_logical_type>` (schema only). The STRUCT of the chunk's own types, so discovery and execute answer off ONE table: each arm of `to_local_translator` names both the engine type and the reader of its values. Every MySQL column type has an arm — the date/time family and JSON/ENUM/SET as the text MySQL prints (no wire frontend can encode a DATE/TIMESTAMP), BINARY/VARBINARY/BLOB/GEOMETRY as their bytes, BIT as the UBIGINT the wire sends — except `column_type::unknown`, which is `conversion_failure` in both paths rather than a guess |
| `pg_to_chunk` | `PGresult*` | `result_wrapper_t<data_chunk_t>` (`conversion_failure` on a malformed DML command tag) |
| `ch_to_chunk` | ClickHouse blocks + the named-type overrides (`system.columns` type strings) | `result_wrapper_t<data_chunk_t>`. A column's type is its override's (`Nullable(T)`, `LowCardinality(T)`, `SimpleAggregateFunction(f, T)` → `T`; `Bool` → BOOLEAN; `Array(T)` → a LIST of `T` whose rows keep their own length; `Tuple` → STRUCT; DateTime64, Decimal, Enum, IPv4/6, Int128/UInt128, Time, Map, … → STRING) when the wire column is a representation of it — the same type up to Nullable / LowCardinality / SimpleAggregateFunction, Tuple field names and `Bool` over the wire's UInt8 — and the wire type's otherwise: the overrides are keyed by the base table's column names, and a result column that only shares one (`AVG(x) AS x` is Float64, `toString(id) AS id` String, `count() AS x` UInt64, `x + 1 AS x` Int64) is not that column. `ch_to_struct` gives discovery the same answer. Every value is read as that type, never handed to the engine under another (rc-3's `set_value` aborts on one): `Bool` from the wire's UInt8, and under STRING the text of any wire scalar that has one — Date / Date32 `YYYY-MM-DD` (from the day number, `RawAt`), DateTime / DateTime64(p) `YYYY-MM-DD hh:mm:ss` with p fraction digits in UTC (the column's timezone is not applied), Time / Time64 `[-]hh:mm:ss[.f]`, Decimal exact with its scale's digits (as text it sorts lexicographically), Int128 / UInt128 decimal, Enum the item's name, IPv4 / IPv6 via `inet_ntop`, UUID, numbers. A value with no reading as its column's type — Map, the geo types, Nothing, an Enum value outside its items, a DateTime64 outside the years 0000–9999 — is `conversion_failure` naming the column, not a placeholder; a NULL is NULL whatever its wire type |
| `parquet_to_chunk` / `parquet_to_struct` | parquet file or buffer | `result_wrapper_t<data_chunk_t>` / `result_wrapper_t<complex_logical_type>` (via Arrow + snappy/brotli/zlib/lz4/zstd) |
| `csv_to_chunk` | csv file or buffer | `result_wrapper_t<data_chunk_t>` |
| `ndjson_to_chunk` | ndjson file or buffer | `result_wrapper_t<data_chunk_t>` (TableReader; LSan note: Arrow's `BackgroundGenerator` worker thread races during teardown — suppressed in `tsan.supp`) |
| `arrow_to_chunk` | `arrow::RecordBatch` | `result_wrapper_t<data_chunk_t>` (shared shim used by parquet/csv/ndjson). Two explicit switches — the column type and the value reading — the second keyed on the array's own `type_id()`, so a type the reader has no case for is `conversion_failure` naming the column, never another array's layout read through a wrong cast. `decimal128(p, 0)` is HUGEINT (the carrier our own `COPY ... TO` writes for it), `decimal128(p, s > 0)` a DECIMAL(p, s) holding the same unscaled integer; a scale outside `0 <= s <= p`, a precision past 38, a payload that does not fit its declared precision, and a non-finite decimal payload are refused rather than read as another number |
| `affected_rows_carrier.hpp` | affected-row count | column-less DML carrier chunks (`make_affected_rows_carrier`) |
| `chunk_windows` | one translated `data_chunk_t` | `split_to_capacity(resource, chunk)` → `std::pmr::vector<data_chunk_t>` of chunks within `DEFAULT_VECTOR_CAPACITY` (the chunk contract below) |

**Chunk contract.** The engine takes at most `DEFAULT_VECTOR_CAPACITY` (1024) rows per
`data_chunk_t`; a wider result is a run of such chunks sharing one column shape, and that
is also what the cursor hands the frontends. Every translator above builds its whole input —
a backend result set, a file — as ONE chunk, so it is cut into that run where it enters the
engine, by `tsl::split_to_capacity`: at the three backend managers' `node_raw_data`
substitution (`integration/{sql,postgresql,clickhouse}/connection_manager.cpp`) and in
`OtterbrixDataManager::insert_rows` (the file / s3 load path). A chunk within the bound is
moved in unchanged. A column-less chunk is never cut, whatever its cardinality: it is the
DML count carrier, which `capture_remote_dml_count` reads whole, and a zero-column window
would be the engine pipeline's drain sentinel. The windows are `data_chunk_t::partial_copy`
slices of the source's reference-counted buffers, so no row is copied.
`tests/unit/translators/test_chunk_windows.cpp` pins it per source (2500 rows),
`tests/system/test_backend_managers.cpp` at the substitution and through a real engine's cursor.

Nothing in `translators/` throws: a file that cannot be opened is `io_error`, a
body Arrow cannot parse or a value without a mapping is `conversion_failure`, a
bad argument is `invalid_parameter`. `translators/error.hpp` holds the
`make_error` / `arrow_error` helpers that build those `core::error_t` values on
the caller's resource.

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
| `chunk_to_arrow` | `to_arrow_schema(res, types)` / `to_arrow_schema(res, struct_t)` → `result_wrapper_t<shared_ptr<arrow::Schema>>` (`conversion_failure` on a physical type without an Arrow mapping); `chunk_to_record_batch(res, chunk)` → `result_wrapper_t<shared_ptr<arrow::RecordBatch>>` for frontends |
| `chunk_to_parquet` | `chunk_to_parquet(res, chunks, path)` → `result_wrapper_t<bool>` (snappy/brotli/zlib/lz4/zstd via Arrow) |
| `chunk_to_csv` | `chunk_to_csv(res, chunks, path)` → `result_wrapper_t<bool>` |
| `chunk_to_ndjson` | `chunk_to_ndjson(res, chunks, path)` → `result_wrapper_t<bool>` |
| `writable_columns.hpp` | the column validation shared by the writers: only named, flat scalar columns (plus NA) can be written |

The three file-format writers are the `COPY (...) TO 'path'` companion to the
input loaders. `db::S3Manager::upload` and `conn::file::FileManager::dump_file`
pick the writer by `FileFormat` enum (auto-detected from the location's
extension when `format=` is omitted).

### `operators/`

`IDataManager` / `OtterbrixDataManager` — interface injected into `db::OtterbrixManager`. Provides `execute_plan()` and `get_schema()` against a live `otterbrix::otterbrix_ptr`, plus the catalog-side primitives the external-table registration needs: `create_collection` (reads the planner-stamped pg_class oid off the create node), `describe_collection` (columns AND oid of an existing base relation: the columns through the schema probe, then the identity — oid and relkind — through the pg_catalog probes, so a VIEW under the name is `schema_error`), `insert_rows` / `delete_rows` (node_insert / node_delete-where-eq named on the node, the plan registering the table lookup that carries the constraints — outgoing for the insert, referencing for the delete — the value bound as a parameter — the manifest writes; `insert_data`, the file loaders' path, first probes the name with `schema_probe::make_table_probe` — a relation already under it is `table_already_exists`, with nothing created or appended, because the engine accepts a create over an existing table and the insert would append to it; a missing table or database lets the create go ahead — then creates the table and ends in `insert_rows`, which hands the rows to the engine cut to the chunk contract). The interface exists so tests can swap in `SimpleMockOtterbrixManager` (see `tests/mock/otterbrix.hpp`), whose engine holds no relation it could describe.

`execute_plan` walks the plan before the engine runs it: the engine evaluates a node that reads no column once per chunk (`execution_dag_t::run`), so an aggregate over constants would fold one row of each chunk, and `count(1)` over no rows crashes the process. `count(<non-NULL literal>)` is rewritten to `COUNT(*)` — a call's arguments cleared and its star flag set, an aggregate expression's parameters cleared — and any other aggregate over constants (a bound parameter, a constant scalar, arithmetic or a cast over those) is refused as `unimplemented_yet` `<fn>() over a constant argument is not supported`. The SELECT list carries an aggregate as a call to `count` / `sum` / `avg` / `min` / `max`, a HAVING-only aggregate and the Spark translator's as an aggregate expression; both are handled. `tests/system/test_constant_aggregates.cpp` pins it.

`schema_probe.hpp` — the probe protocol shared by `get_schema`, `describe_collection` and `KafkaManager::recover()`. Every answer comes out of the engine's REPLY, never off the plan the probe sent: a plan travels into the dispatcher by value and the engine keeps writing to its nodes while it runs. Three questions, one plan each: `make_table_probe` → `read_columns` (the column types, from the cursor of a `LIMIT 0` read — the same answer for an empty relation and a full one, and a VIEW comes back expanded because the engine splices the body into the probe's own aggregate); `make_namespace_probe` → `read_namespace_oid` and `make_relation_probe` → `read_relation` (pg_class oid + relkind, which is what tells a base relation from a VIEW). The pg_catalog lookups bind their literal as a plan parameter, so no name is ever spelled into SQL text.

### `config.hpp`

Re-exports the Otterbrix configuration type used when initialising the engine in `main.cpp` and in system tests.

## Adding a New Backend Translator

1. Create `translators/input/<backend>_to_chunk.{hpp,cpp}`
2. Follow `mysql_to_chunk` as the template — map each column type to the corresponding `components::types::logical_type`.
   Keep the translator's helpers (`value_translator_t`, the `set_*` converters, `to_local_translator`) in an
   anonymous namespace: the translators reuse those names inside `tsl`, and with external linkage the linker keeps
   one definition for all of them, so one translator's objects get destroyed with another translator's layout
3. Register in `otterbrix/CMakeLists.txt` under both `OTTERBRIX_HEADERS` and `OTTERBRIX_SOURCES`
4. Use the new translator in the relevant `integration/<backend>/connection_manager.cpp`
