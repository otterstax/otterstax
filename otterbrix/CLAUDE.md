# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`otterbrix/` is the glue layer between OtterStax and the external Otterbrix library: the parser, the SQL generator for remote backends, the data translators, the engine requests, and the federation provider the engine is opened with (`federation/`, see `federation/CLAUDE.md`).

## Subdirectories

### `parser/`

`IParser` / `GreenplumParser` — thin interface over the Otterbrix SQL parser: the engine's `raw_parser` (with the s3 / file / kafka extensions registered) and transformer over the statement text as written. `parse()` returns `ParsedQueryDataPtr` which carries:
- `otterbrix_params` (`OtterbrixStatementPtr`) — the logical plan (its root, the flattened sub-queries ahead of it, the catalog lookups the transformer registered, the shared parameter node); names ride on the plan nodes whole — a remote name keeps its uid slot, which the federation resolves by
- `binder` (`transform_result`) — parameter bindings for prepared statements
- `sql` — the text, which the query session parses again to run a statement a second time (a plan is single-use: a rediscovery retry, the describe of a prepared statement)
- `ddl_target` — a table DDL statement's target as written, every slot (the transformer keeps only database and table on the DDL node); the session runs remote DDL by it
- `dml_target` — an INSERT / UPDATE / DELETE target written with a uid slot (four parts), every slot (the transformer keeps no uid on a DML target); the session runs such a statement on its backend directly
- `extension_kind`, `awaiting_bind`

`parse_fragment(sql, shared_params)` parses a single SELECT into the caller's parameter node (the Spark translator's SQL leaves). `backend_type_t` (`otterbrix/backend_type.hpp`, included by `parser.hpp` and `federation/metadata.hpp`; `{MySQL, PostgreSQL, ClickHouse}`) is the dialect the generator writes and the only enumeration of a backend's kind (the federation, the catalog and the backends module use it too). `qualified_name.hpp` restores the ordering of `qualified_name_t` (the dependency maps).

`parser/grammar_extention/` — pluggable parser extensions registered with the
core parser registry. Two ship by default: `s3/` and `file/`, both adding the
`CREATE EXTERNAL TABLE` and `COPY (...) TO ...` syntax. Each extension is a flex
+ bison pair (`*_scan.l` / `*_gram.y`) plus a small `*_extension.cpp` that
implements the registry hook and produces an `otterstax::external::external_node_t`
the query session routes via
`query_session_t::load_external_table` / `copy_to`. See
[`parser/grammar_extention/CLAUDE.md`](parser/grammar_extention/CLAUDE.md) for the
extension recipe and `cmake/otterbrix_parser_extension.cmake` for the build helper.
#### `parser/grammar_extension/kafka/`

The Kafka DDL grammar extension (library `kafka_grammar`): a self-contained
flex+bison pair (`kafka_yy` prefix, isolated from the core parser) producing a
`kafka_node_t` for `CREATE/DROP SOURCE/STREAM` (DROP takes `[IF EXISTS] <name>
[CASCADE | RESTRICT]`, RESTRICT by default — `kafka_node_t::cascade()`), registered into `GreenplumParser`
via `make_kafka_extension()`. Also defines `kafka_write_target` (detects
`INSERT INTO kafka.<obj>`) and `kafka_stream_source` / `kafka_find_aggregate`
(stream plan helpers). Namespace `otterstax::kafka`. The Kafka **runtime** that
consumes these nodes lives in `integration/kafka/` — see its CLAUDE.md.

### `query_generation/`

`sql_gen::generate_query(node, params, backend, statement_target_t, resource, generation_options_t)` — walks an Otterbrix logical plan node (a query block's aggregate, a DML / DDL root) and produces a SQL string for a specific `backend_type_t`. `statement_target_t` names the table (`name`, as `federation::generator_name` spells it), the second table (`from_name`: the FROM / USING source, the INSERT ... SELECT source, the DROP INDEX index), how the engine's column names are spelled on the backend (`columns` / `from_columns`, each column with its whole engine type — a DECIMAL with its width and scale) and every column for `SELECT *` (`all_columns`); a query over a JOIN tree or a derived table (validated tree only) names its tables in `relations` instead (`relation_t`: the node that reads the table, its name, columns and declared column list). Such a query is written with every table under an alias of its own (`<table> AS tN`, a derived table `(SELECT … ) AS dN` whose outputs are named as the engine names them) and every column qualified by its relation's alias, a column found by its position (the validator numbers each column reference by the column's place in what the FROM — in an ON condition, what its side — outputs; HAVING / ORDER BY / DISTINCT ON of a query that groups or aggregates — the only one the validator leaves a group node — number the group's outputs: HAVING and DISTINCT ON are written as the output's expression, ORDER BY by the output's name, and every output of such a query over relations is named, `AS <name>` (a name two outputs share takes a `_<n>` suffix on the later one), since a bare column name is ambiguous on MySQL and the left table's column on ClickHouse), so two tables sharing a column name never mix; a JOIN node alone is the SELECT of every column it outputs. JOIN kinds: INNER / LEFT / RIGHT / CROSS on every dialect, FULL on PostgreSQL and ClickHouse; LATERAL / semi / anti, a JOIN tree on the right of a ClickHouse JOIN and a ClickHouse ON condition other than side-to-side equalities AND-ed with conditions on the side an outer join does not preserve are `unimplemented_yet`; a ClickHouse JOIN is written with its strictness (`ALL INNER JOIN`, `ALL LEFT JOIN`, …: the server's `join_default_strictness` decides otherwise), an outer one ends the statement with `SETTINGS join_use_nulls = 1`, and a statement reading an Array / Tuple / Map column of the side it pads (`source_column_t::padded_without_null`: no Nullable holds it, so it would read its default value where the engine has NULL) is `unimplemented_yet`. `generation_options_t::placeholders` writes every parameter as `?` (the pushdown rule's "can this be one statement" probe); `validated` marks the engine's validated tree (a numeric cast — the validator's and a user's are the same node — is written in the dialect's syntax where its CAST does to the value what the engine's does and refused otherwise, a lossless one that is a comparison's operand written as its operand; `*` expands to the declared columns; a two-table statement trusts the caller for its source). `generate_insert_rows` / `generate_update_row` / `generate_delete_row` are the federation sinks' statements (one VALUES list per chunk; one row named by its primary key — `WHERE k1 = v1 AND …`, no LIMIT, no row address — ClickHouse refused), `quote_identifier` the dialect's quoting (ClickHouse doubles a backslash too; its PostgreSQL form also spells the names of the engine's own bookkeeping SQL, `operators/engine_requests.cpp` and `catalog/remote_metadata.cpp`). Backend differences:
- MySQL: `database.collection` table reference
- PostgreSQL: `schema.collection` table reference
- ClickHouse: `database.collection` with backticks, like MySQL. ClickHouse 23.8 has no `UPDATE`
  statement, so `UPDATE` is generated as the `ALTER TABLE ... UPDATE` mutation, with
  `SETTINGS mutations_sync = 2` (a mutation runs in the background otherwise and the next read may see
  the old rows; 2 waits for every replica); both it and the lightweight `DELETE` (synchronous by
  default) require a `WHERE`, so a statement without a predicate gets `WHERE 1`

**Literals.** A bound value is written as a literal of the dialect: numbers (a FLOAT / DOUBLE with
`max_digits10` digits; NaN / ±infinity as PostgreSQL `'NaN'::float8` / ClickHouse `nan`, none on MySQL),
a DECIMAL as its exact text (ClickHouse `toDecimal128('…', scale)`: a bare decimal literal is Float64
there; a cast into DECIMAL of a bound integer or DECIMAL that the engine's cast leaves unchanged —
`price > 50` over DECIMAL(10, 2), a DECIMAL whose digits the target holds — is the lossless
`CAST(50 AS DECIMAL(10, 2))`; AVG over a DECIMAL(p, s) is the sum widened to keep s + 19 fraction digits
over the count — PostgreSQL `(CAST(SUM(x) AS numeric(p + 38, s + 19)) / COUNT(x))`, MySQL
`(CAST(SUM(x) AS DECIMAL(65, t)) / COUNT(x))` with t = min(30, 65 - (p - s + 19)), `unimplemented_yet`
where t < s + 19 — so that rounding the quotient again to s is the engine's single rounding for any
count (`federation/CLAUDE.md`), and `unimplemented_yet` on ClickHouse, whose avg of a Decimal is a
Float64), DATE / TIME / TIMESTAMP as typed literals (`DATE '…'`, ClickHouse `toDate('…')` /
`toDateTime64('…', 6, 'UTC')` — in UTC, as `ch_to_chunk` renders every DateTime it reads, where a literal
without a zone would be parsed in the server's time zone; no TIME on ClickHouse), TIMESTAMPTZ on PostgreSQL only, strings with the quote doubled everywhere and the
backslash doubled on MySQL / ClickHouse (a MySQL connection under `NO_BACKSLASH_ESCAPES` is refused at
discovery), ROW / ARRAY on PostgreSQL and ClickHouse. Anything else (INTERVAL, UUID, BLOB, …) is
`unimplemented_yet`, which the federation's operators answer with the engine's own plan.

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
  name on the backend), and always spells out where NULLs go, as the engine places them (default ASC
  NULLS LAST / DESC NULLS FIRST): PostgreSQL / ClickHouse `NULLS FIRST|LAST`, MySQL an `IS NULL` key in
  front of the column (over an aggregate's alias the key spells the aggregate: MySQL / MariaDB refuse the
  alias of a group function inside an expression);
- `/` over the validated tree whose result type is integral is integer division as the engine computes
  it (truncation toward zero): PostgreSQL `/`, MySQL `DIV`, ClickHouse `intDiv(a, b)`; over the parser's
  tree it is written as written;
- SQL LIKE arrives as a `regexp_like` function call carrying the LIKE pattern and its flags, so it is
  written as `[NOT] LIKE` / `[NOT] ILIKE` (MySQL, which has no ILIKE, gets `LOWER(...) LIKE LOWER(...)`
  for the case-insensitive form); a real regular expression is `unimplemented_yet`;
- `CAST` takes each dialect's own target-type spelling (MySQL `SIGNED` / `UNSIGNED` / `CHAR`, not a
  column type), and `TRY_CAST` is `unimplemented_yet`. Over the validated tree a numeric CAST is never
  dropped as such (a user's `CAST(a AS DOUBLE) / CAST(b AS DOUBLE)` must not become integer division):
  it is classified as the engine runs it — lossless (an integer into one that holds its range or into a
  DECIMAL with enough integer digits, a DECIMAL into a wider one, FLOAT into DOUBLE), converting (into
  DOUBLE, an integer into FLOAT) or narrowing (range-checked, a fraction rounded ties to even) — and
  written where the dialect's CAST does the same: PostgreSQL every numeric cast (`int2` / `int4` / `int8`
  / `float4` / `float8` / `numeric(p, s)`; an unsigned, 1-byte or 16-byte target only when lossless),
  MySQL / ClickHouse only the lossless and converting ones (`SIGNED` / `UNSIGNED` / `DECIMAL(p, s)` with
  s ≤ 30 — MySQL's limit — / `DOUBLE`, `Int64` / `Decimal(p, s)` / `Float64`, …). A narrowing cast there is `unimplemented_yet`
  (MySQL's CAST does not narrow and rounds ties away from zero, ClickHouse's wraps and truncates), and the
  expression stays in the engine (the rule's `placeholders` probe, which knows no parameter's type,
  leaves a CAST of a parameter to the operator's generation with the values). A lossless cast that is a
  comparison's operand is written as its
  operand (`CAST(a AS BIGINT) = 5` → `a = 5`): that is the validator's cast between two widths, and
  every dialect compares them exactly;
- unary minus over a negative literal is `(- -5)`: `--` opens a comment in every dialect;
- `UPDATE ... FROM` / `DELETE ... USING` are pushed down when the plan's source child is a plain
  table of the same server (the pushdown rule checks it) and `target.from_name` names it: PostgreSQL `UPDATE t SET … FROM s` /
  `DELETE FROM t USING s`, MySQL the multi-table `UPDATE t, s SET t.c = …` / `DELETE t FROM t, s`
  (the plan's join condition is the WHERE, so the comma forms are the faithful ones), with every
  column qualified by the table its key's side names. A source on another backend or a local one, a
  source that is not a plain table, the target table itself (`UPDATE t … FROM t` needs aliases on both
  backends), and ClickHouse (whose mutations name one table) are refused — never silently emitted as a
  single-table statement.

### `translators/input/`

Converts raw backend results or file bytes into Otterbrix `data_chunk_t`:

| File | Input type | Output |
|------|-----------|--------|
| `mysql_to_chunk` | `boost::mysql::results` of a SELECT (a DML's count is the backend's write) | `result_wrapper_t<data_chunk_t>` (`conversion_failure` on a MySQL column type without a mapping) |
| `mysql_to_struct` (same file) | `boost::mysql::results` | `result_wrapper_t<complex_logical_type>` (schema only). The STRUCT of the chunk's own types, so discovery and execute answer off ONE table: each arm of `to_local_translator` names both the engine type and the reader of its values. Every MySQL column type has an arm — DECIMAL(p, s) of at most 38 digits as the engine's DECIMAL(p, s) (p from the column definition's display length less the point and the sign, s its `decimals`; the digits read off the text the wire sends, a value past the precision `conversion_failure`), a wider DECIMAL (MySQL takes 65) as its exact text, the date/time family and JSON/ENUM/SET as the text MySQL prints (no wire frontend can encode a DATE/TIMESTAMP), BINARY/VARBINARY/BLOB/GEOMETRY as their bytes, BIT as the UBIGINT the wire sends — except `column_type::unknown`, which is `conversion_failure` in both paths rather than a guess |
| `pg_to_chunk` | `PGresult*` of a SELECT (a DML's count is the backend's write, never read through here) | `result_wrapper_t<data_chunk_t>` (`conversion_failure` on a value with no reading as its column's type). NUMERIC(p, s) of at most 38 digits (its typmod, `PQfmod`) is the engine's DECIMAL(p, s), NaN its DECIMAL NaN; every other NUMERIC — wider, a scale outside 0..p (PostgreSQL 15), and the unconstrained NUMERIC, whose values each keep their own scale and up to 131072 digits (also what PostgreSQL answers for SUM(bigint) / AVG) — is its exact text: no DECIMAL(38, s) holds it without rounding or overflowing, and DOUBLE would round past 15 digits |
| `decimal_text.hpp` | — | `decimal_type(p, s, alias)` (inline: the engine's DECIMAL(p, s) when it can declare one: 1 ≤ p ≤ 38, 0 ≤ s ≤ p), shared by the three translators. A backend's decimal text is read with the engine's own parse, `components::casts::kernels::detail::parse_decimal(text, p, s, &out)` (`<components/casts/kernels/string_cast.hpp>`: the unscaled integer at scale s, a longer fraction rounded half away from zero, a value past p digits refused), in `mysql_to_chunk`, `pg_to_chunk` and the federation's `conform_to_result` |
| `ch_to_chunk` | ClickHouse blocks of a SELECT + the declared types (`system.columns` type strings) by result position: the type of the table column each result column reads as it is (`ClickhouseManager::declared_types_for` over `federation::scan_request_t::columns`: a pushed statement's columns come from several tables under names of the statement's own) | `result_wrapper_t<data_chunk_t>`. A column's type is its override's (`Nullable(T)`, `LowCardinality(T)`, `SimpleAggregateFunction(f, T)` → `T`; `Bool` → BOOLEAN; `Array(T)` → a LIST of `T` whose rows keep their own length; `Tuple` → STRUCT; Decimal(P, S) / Decimal32 / 64 / 128 of at most 38 digits → DECIMAL(P, S) holding the wire's unscaled integer, a result column of another width under a Decimal column's name keeping the wire's; DateTime64, Enum, IPv4/6, Int128/UInt128, Time, Map, … → STRING) when the wire column is a representation of it — the same type up to Nullable / LowCardinality / SimpleAggregateFunction, Tuple field names and `Bool` over the wire's UInt8 — and the wire type's otherwise: a result column that only reads like a table column (`AVG(x) AS x` is Float64, `toString(id) AS id` String, `count() AS x` UInt64, `x + 1 AS x` Int64) is not that column. `ch_to_struct(res, block, named_type_overrides)` gives discovery the same answer for its probe (the table's columns under their own names, the overrides keyed by name). Every value is read as that type, never handed to the engine under another (rc-3's `set_value` aborts on one): `Bool` from the wire's UInt8, and under STRING the text of any wire scalar that has one — Date / Date32 `YYYY-MM-DD` (from the day number, `RawAt`), DateTime / DateTime64(p) `YYYY-MM-DD hh:mm:ss` with p fraction digits in UTC (the column's timezone is not applied), Time / Time64 `[-]hh:mm:ss[.f]`, Int128 / UInt128 decimal, Enum the item's name, IPv4 / IPv6 via `inet_ntop`, UUID, numbers. A value with no reading as its column's type — Map, the geo types, Nothing, an Enum value outside its items, a DateTime64 outside the years 0000–9999 — is `conversion_failure` naming the column, not a placeholder; a NULL is NULL whatever its wire type |
| `parquet_to_chunk` / `parquet_to_struct` | parquet file or buffer | `result_wrapper_t<data_chunk_t>` / `result_wrapper_t<complex_logical_type>` (via Arrow + snappy/brotli/zlib/lz4/zstd) |
| `csv_to_chunk` | csv file or buffer | `result_wrapper_t<data_chunk_t>` |
| `ndjson_to_chunk` | ndjson file or buffer | `result_wrapper_t<data_chunk_t>` (TableReader; LSan note: Arrow's `BackgroundGenerator` worker thread races during teardown — suppressed in `tsan.supp`) |
| `arrow_to_chunk` | `arrow::RecordBatch` | `result_wrapper_t<data_chunk_t>` (shared shim used by parquet/csv/ndjson). Two explicit switches — the column type and the value reading — the second keyed on the array's own `type_id()`, so a type the reader has no case for is `conversion_failure` naming the column, never another array's layout read through a wrong cast. `decimal128(p, 0)` is HUGEINT (the carrier our own `COPY ... TO` writes for it), `decimal128(p, s > 0)` a DECIMAL(p, s) holding the same unscaled integer; a scale outside `0 <= s <= p`, a precision past 38, a payload that does not fit its declared precision, and a non-finite decimal payload are refused rather than read as another number |
| `chunk_windows` | one translated `data_chunk_t` | `split_to_capacity(resource, chunk)` → `std::pmr::vector<data_chunk_t>` of chunks within `DEFAULT_VECTOR_CAPACITY` (the chunk contract below) |

**Chunk contract.** The engine takes at most `DEFAULT_VECTOR_CAPACITY` (1024) rows per
`data_chunk_t`; a wider result is a run of such chunks sharing one column shape, and that
is also what the cursor hands the frontends. Every translator above builds its whole input —
a backend result set, a file — as ONE chunk. A remote table's rows are handed to the engine in
windows of at most that many rows by the federation's operators (`federation/remote_storage.hpp`,
`rules/pushdown.hpp`); a file / s3 load is cut into that run by `tsl::split_to_capacity` in
`otterstax::engine::create_table`. A chunk within the bound is moved in unchanged. A column-less
chunk is never cut, whatever its cardinality: a zero-column window would be the engine pipeline's
drain sentinel. The translators of a backend answer read only a SELECT (a scan); a remote write's
count is the backend's `write` (`federation/CLAUDE.md`). The windows are
`data_chunk_t::partial_copy` slices of the source's reference-counted buffers, so no row is copied.
`tests/unit/translators/test_chunk_windows.cpp` pins `split_to_capacity` per source (2500 rows),
`tests/system/test_backend_managers.cpp` and `tests/federation/` the federation's windows through a
real engine's cursor.

Nothing in `translators/` throws: a file that cannot be opened is `io_error`, a
body Arrow cannot parse or a value without a mapping is `conversion_failure`, a
bad argument is `invalid_parameter`. `translators/error.hpp` holds the
`make_error` / `arrow_error` helpers that build those `core::error_t` values on
the caller's resource (`tsl::make_error` is also the one the federation, the
catalog's metadata plans and the engine requests build their errors with).

The file-format loaders all take a `std::pmr::memory_resource*` and route every
allocation through it. **Lifetime trap:** the returned `data_chunk_t` keeps that
resource alive — if you borrow the engine's resource (`engine_t::resource()`), the chunk
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

`engine_requests.{hpp,cpp}` (namespace `otterstax::engine`) — every request otterstax makes to the embedded engine, as free functions sent straight to the engine dispatcher (`runtime::engine_t::dispatcher_address()`, `services::dispatcher::manager_dispatcher_t`); there is no engine actor of otterstax's own. Each names the engine SESSION it runs on (a query session passes its connection's; the catalog and the file manager their own) and the coroutines take the memory resource first and must be awaited from an actor coroutine (or a test's drive loop). Nothing throws; an engine refusal is an error cursor or an error value with the engine's own code.

- `sql_plan` — one bookkeeping SQL statement through the engine's own parser and transformer.
- `send_plan` — one plan on a session. (A statement that failed before the engine is reported by the query session itself, `manager_dispatcher_t::refuse_statement`, so it fails an open transaction.)
- `execute_statement` — runs a parsed statement and holds the answer to the shape its root owes: a DML without RETURNING answers its count through `cursor->affected_rows()` (a write to a remote table too: the backend's count when the pushdown rule made it one remote statement, otherwise the rows the engine handed the storage's sink — for an UPDATE / DELETE equal to the sum the backend reported, each row statement having to change exactly one row; `federation/CLAUDE.md`), a row-producing statement without columns is `schema_error`.
- `describe` — the result schema without execution: the engine's plan-only EXPLAIN when every parameter is bound and the statement is not a set operation (a remote table's columns come from `federation.remote_columns`; no backend is asked), else the federated computation over one probe per relation the root reads — a `LIMIT 0` read (`schema_probe.hpp`) for a local one, `probe_declared_columns` (a plan-only EXPLAIN of a read of the full name) for a remote one, positional. The engine always answers a cursor (asserted).
- `delete_rows_plan` (the one builder of a `DELETE … WHERE c1 = v1 AND …` plan: node_delete over node_match named on the node, the referencing constraints looked up, every value bound as a parameter — the caller sends it, e.g. inside the startup's metadata transaction), and `create_table` — the file / s3 load: makes the database exist, refuses a relation already under the name as `table_already_exists` (the engine accepts a create over an existing table and the insert would append), creates the table from the chunk's columns (a create node named on its target) and inserts (node_insert named on the node, constraints looked up; rows cut to the chunk contract). The statement plan, the relation probe, the create and the insert are internal to `engine_requests.cpp`.

The engine evaluates aggregates over constants correctly, so no plan rewrite or guard stands in front of it (`tests/system/test_constant_aggregates.cpp` pins the engine's answers). `tests/system/test_engine_requests.cpp` and `tests/unit/schema/test_schema_probe.cpp` cover these over a real engine.

`schema_probe.hpp` — the probe protocol shared by `describe` / `create_table` and `KafkaManager::recover()`. Every answer comes out of the engine's REPLY, never off the plan the probe sent: a plan travels into the dispatcher by value and the engine keeps writing to its nodes while it runs. Three questions, one plan each: `make_table_probe` → `read_columns` (the column types, from the cursor of a `LIMIT 0` read — the same answer for an empty relation and a full one, and a VIEW comes back expanded because the engine splices the body into the probe's own aggregate); `make_namespace_probe` → `read_namespace_oid` and `make_relation_probe` → `read_relation` (pg_class oid + relkind, which is what tells a base relation from a VIEW). The pg_catalog lookups bind their literal as a plan parameter, so no name is ever spelled into SQL text.

## Adding a New Backend Translator

1. Create `translators/input/<backend>_to_chunk.{hpp,cpp}`
2. Follow `mysql_to_chunk` as the template — map each column type to the corresponding `components::types::logical_type`.
   Keep the translator's helpers (`value_translator_t`, the `set_*` converters, `to_local_translator`) in an
   anonymous namespace: the translators reuse those names inside `tsl`, and with external linkage the linker keeps
   one definition for all of them, so one translator's objects get destroyed with another translator's layout
3. Register in `otterbrix/CMakeLists.txt` under both `OTTERBRIX_HEADERS` and `OTTERBRIX_SOURCES`
4. Use the new translator in the relevant `integration/<backend>/connection_manager.cpp`
