# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`otterbrix/` is the glue layer between OtterStax and the external Otterbrix library: the parser, the SQL generator for remote backends, the data translators, the engine requests, and the federation's backend actors the engine is opened with (`federation/`, see `federation/CLAUDE.md`).

## Subdirectories

### `parser/`

`IParser` / `GreenplumParser` — thin interface over the Otterbrix SQL parser: the engine's `raw_parser` (with the s3 / file / kafka extensions registered) and transformer over the statement text as written. `parse()` returns `core::result_wrapper_t<ParsedQueryDataPtr>` (a refusal is the engine's error, never an exception) which carries:
- `plan` (`ExecutionPlanPtr`, `types/otterbrix.hpp`: the engine's `execution_plan_t` — its root `plan_root()`, the flattened sub-queries ahead of it, the catalog lookups the transformer registered, the parameter node); every name is the name the client wrote, 1–4 parts (`t`, `s.t`, `d.s.t` local, `c.d.s.t` a connection's table), resolved by the engine. For a statement still awaiting its Bind, the describe plan (`finalize_for_describe`: every `$n` unbound)
- `binder()` (`transform_result`) — binds the client's values and finalizes the statement (`query_session_t::parse_bound`)
- `sql` — the text, which the query session parses again to run a statement a second time (a plan is single-use: a prepared statement's executions, the registration retry)
- `tag`, `extension_kind`, `awaiting_bind`

`parse_fragment(sql, shared_params)` parses a single SELECT into the caller's parameter node (the Spark translator's SQL leaves). `backend_type_t` (`otterbrix/backend_type.hpp`, included by `parser.hpp` and `federation/metadata.hpp`; `{MySQL, PostgreSQL, ClickHouse}`) is the dialect the generator writes and the only enumeration of a backend's kind (the federation, the catalog and the backends module use it too).

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
via `make_kafka_extension()`. Also defines the Kafka names (`kafka_table_name`: `main.kafka.<obj>`;
`kafka_service_table_name` / `kafka_offsets_table_name`: `federation.kafka.*`), `kafka_write_target`
(an INSERT whose written target can name `main.kafka.<obj>`) with `kafka_target_named` (the target
completed by the session's place), and `kafka_stream_source` / `kafka_find_aggregate` (stream plan helpers). Namespace `otterstax::kafka`. The Kafka **runtime** that
consumes these nodes lives in `integration/kafka/` — see its CLAUDE.md.

### `query_generation/`

The SQL OtterStax sends to a backend, in its dialect (`sql_query_generator.hpp`, namespace `sql_gen`). The caller is the backend actor (`federation/CLAUDE.md`): it generates from the blocks otterbrix offers it (`storage_offer`: a detached copy of a validated sub-tree whose nodes name their table by `table_oid()` — every name and column reaches the generator through `statement_target_t` / `relation_t`); the discovery probe (`catalog/discovery.hpp`) generates a SELECT from a hand-built node. Entry points: `generate_query` (below), `generate_table_read` (a scan of one table: the projected columns, the WHERE conjuncts the actor took, a LIMIT), `generate_insert_rows` (one `INSERT … VALUES` per chunk, `RETURNING` on PostgreSQL only), `generate_create_table` / `generate_drop_table` / `generate_add_column` / `generate_drop_column` (a client's DDL of a remote table; ClickHouse tables are MergeTree ordered by `tuple()`), `quote_identifier` (the dialect's quoting; ClickHouse doubles a backslash too).

`sql_gen::generate_query(node, params, backend, statement_target_t, resource, generation_options_t)` — walks an Otterbrix logical plan node (a query block's aggregate, a JOIN, a DML / DDL root) and produces a SQL string for a specific `backend_type_t`. `statement_target_t` names the table (`name`, as `federation::generator_name` spells it), the second table (`from_name`: the FROM / USING source, the INSERT ... SELECT source, the DROP INDEX index), how the engine's column names are spelled on the backend (`columns` / `from_columns`, each column with its whole engine type — a DECIMAL with its width and scale) and every column for `SELECT *` (`all_columns`); a query over a JOIN tree or a derived table (validated tree only) names its tables in `relations` instead (`relation_t`: the node that reads the table, its name, columns and declared column list). Such a query is written with every table under an alias of its own (`<table> AS tN`, a derived table `(SELECT … ) AS dN` whose outputs are named as the engine names them) and every column qualified by its relation's alias, a column found by its position (the validator numbers each column reference by the column's place in what the FROM — in an ON condition, what its side — outputs; HAVING / ORDER BY / DISTINCT ON of a query that groups or aggregates — the only one the validator leaves a group node — number the group's outputs: HAVING and DISTINCT ON are written as the output's expression, ORDER BY by the output's name, and every output of such a query over relations is named, `AS <name>` (a name two outputs share takes a `_<n>` suffix on the later one), since a bare column name is ambiguous on MySQL and the left table's column on ClickHouse), so two tables sharing a column name never mix; a JOIN node alone is the SELECT of every column it outputs. JOIN kinds: INNER / LEFT / RIGHT / CROSS on every dialect, FULL on PostgreSQL and ClickHouse; LATERAL / semi / anti, a JOIN tree on the right of a ClickHouse JOIN and a ClickHouse ON condition other than side-to-side equalities AND-ed with conditions on the side an outer join does not preserve are `unimplemented_yet`; a ClickHouse JOIN is written with its strictness (`ALL INNER JOIN`, `ALL LEFT JOIN`, …: the server's `join_default_strictness` decides otherwise), an outer one ends the statement with `SETTINGS join_use_nulls = 1`, and a statement reading an Array / Tuple / Map column of the side it pads (`source_column_t::padded_without_null`: no Nullable holds it, so it would read its default value where the engine has NULL) is `unimplemented_yet`. `generation_options_t::placeholders` writes every parameter as `?` (the offer's "can this be one statement" probe; the text is never sent); `validated` (the default; only the discovery probe clears it) marks the engine's validated tree (a numeric cast — the validator's and a user's are the same node — is written in the dialect's syntax where its CAST does to the value what the engine's does and refused otherwise, a lossless one that is a comparison's operand written as its operand; `*` expands to the declared columns; a two-table statement trusts the caller for its source); `returning` appends ` RETURNING <every column>` to an UPDATE / DELETE (PostgreSQL only). Backend differences:
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
backslash doubled on MySQL / ClickHouse (a MySQL session under `NO_BACKSLASH_ESCAPES` is refused when
the agent opens it) and on PostgreSQL inside an escape string constant `E'…'` — only for a value that
holds a backslash, the plain `'…'` otherwise —, which the server reads the same under
`standard_conforming_strings` on and off, ROW / ARRAY on PostgreSQL and ClickHouse, an ENUM as its label
(a string literal: against a PostgreSQL ENUM column it takes the column's type, so it compares in the
declaration order as the engine does; a MySQL ENUM / ClickHouse Enum column is text in the catalog,
and the label the text the engine compares). Anything else (INTERVAL, UUID, BLOB, …) is
`unimplemented_yet`, so the backend actor does not take that block in its offer and the engine evaluates it:
the offer's probe (`placeholders`) writes a bound value as `?` but writes it once more into nothing first,
so it refuses exactly what the run with the value would (only a parameter of kind execution, unknown
there, is not checked).

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
- SQL LIKE arrives as a `like_match` call (subject, pattern, mode bits: glob, case-insensitive,
  negate), so it is written as `[NOT] LIKE` / `[NOT] ILIKE` (MySQL, which has no ILIKE, gets
  `LOWER(...) LIKE LOWER(...)` for the case-insensitive form); a mode without glob (a regular
  expression) is `unimplemented_yet`;
- `x = ANY(<list>)` / `x <> ALL(<list>)` is `x IN (…)` / `x NOT IN (…)` (an empty list FALSE / TRUE, a
  NULL list `IN (NULL)`);
- `CAST` takes each dialect's own target-type spelling (MySQL `SIGNED` / `UNSIGNED` / `CHAR`, not a
  column type), and `TRY_CAST` is `unimplemented_yet`. Over the validated tree a numeric CAST is never
  dropped as such (a user's `CAST(a AS DOUBLE) / CAST(b AS DOUBLE)` must not become integer division):
  it is classified as the engine runs it — lossless (an integer into one that holds its range or into a
  DECIMAL with enough integer digits, a DECIMAL into a wider one, FLOAT into DOUBLE), converting (into
  DOUBLE, an integer into FLOAT) or narrowing (range-checked, a fraction rounded ties to even) — and
  written where the dialect's CAST does the same: PostgreSQL every numeric cast (`int2` / `int4` / `int8`
  / `float4` / `float8` / `numeric(p, s)`; an unsigned, 1-byte or 16-byte target only when lossless),
  MySQL / ClickHouse only the lossless and converting ones (`SIGNED` / `UNSIGNED` / `DECIMAL(p, s)` with
  s ≤ 30 — MySQL's limit — / `DOUBLE`, `Nullable(Int64)` / `Nullable(Decimal(p, s))` / `Nullable(Float64)`, …:
  every ClickHouse CAST target is `Nullable(T)`, since a CAST into a bare `T` refuses a NULL, Code 349, and
  `isNull` over it folds to 0; a target is always a scalar, never an Array / Tuple / Map). A narrowing cast there is `unimplemented_yet`
  (MySQL's CAST does not narrow and rounds ties away from zero, ClickHouse's wraps and truncates), and the
  expression stays in the engine (the offer's `placeholders` probe, which knows no parameter's type,
  leaves a CAST of a parameter to the operator's generation with the values). A lossless cast that is a
  comparison's operand is written as its
  operand (`CAST(a AS BIGINT) = 5` → `a = 5`): that is the validator's cast between two widths, and
  every dialect compares them exactly;
- an UPDATE assignment's narrowing cast (`write_assignment_cast`): otterbrix wraps a SET value not of its
  column's type in a `cast_kind::cast` to that type (unmarked, so the topmost cast
  of a SET value to the column's own type is taken as it — a user's CAST there is the same cast for a
  number). Where `numeric_cast_target` has no spelling, the store into the column range-checks:
  MySQL — an integer into a narrower integer, a DOUBLE into FLOAT — gets the value uncast and
  `statement_needs_t::strict_store` (the agent runs it only on a strict `sql_mode`, whose store refuses
  an out-of-range value, ER 1264, and rounds a DOUBLE to the nearest FLOAT; a bound DOUBLE the engine
  takes needs no strict mode); ClickHouse an integer `if(isNull(x), NULL, accurateCast(x, 'Nullable(T)'))`
  (CANNOT_CONVERT_TYPE out of range; accurateCast alone also range-checks the value ClickHouse computes
  under a NULL from the column's default 0 — `i + 3000000000` over a NULL row — so a NULL is kept NULL
  before it, as the engine keeps it; `if` evaluates each branch only for its rows and is Nullable(T)), a
  DOUBLE into Float32 only a
  bound value (`CAST(v AS Float32)`; ClickHouse's conversion checks no range). A bound DOUBLE out of
  FLOAT's range is `numeric_value_out_of_range`, a fraction into an integer stays refused;
- unary minus over a negative literal is `(- -5)`: `--` opens a comment in every dialect;
- `UPDATE ... FROM` / `DELETE ... USING` are generated when the plan's source child is a plain
  table of the same server and `target.from_name` names it (the backend actor fills it from the
  block's source): PostgreSQL `UPDATE t SET … FROM s` /
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
| `mysql_to_chunk` | `boost::mysql::results` of a SELECT (a DML's count is the backend's write) | `result_wrapper_t<data_chunk_t>` (`unimplemented_yet` on a MySQL column type without a mapping; a value with no reading as its column's type `invalid_text_representation`, past its range `numeric_value_out_of_range`) |
| `mysql_to_struct` (same file) | `boost::mysql::results` | `result_wrapper_t<complex_logical_type>` (schema only). The STRUCT of the chunk's own types, so discovery and execute answer off ONE table: each arm of `to_local_translator` names both the engine type and the reader of its values. Every MySQL column type has an arm — DECIMAL(p, s) of at most 38 digits as the engine's DECIMAL(p, s) (p from the column definition's display length less the point and the sign, s its `decimals`; the digits read off the text the wire sends, a value past the precision `numeric_value_out_of_range`), a wider DECIMAL (MySQL takes 65) as its exact text, the date/time family and JSON/ENUM/SET as the text MySQL prints (no wire frontend can encode a DATE/TIMESTAMP), BINARY/VARBINARY/BLOB/GEOMETRY as their bytes, BIT as the UBIGINT the wire sends — except `column_type::unknown`, which is `unimplemented_yet` in both paths rather than a guess |
| `pg_to_chunk` | `PGresult*` of a SELECT (a DML's count is the backend's write, never read through here) | `result_wrapper_t<data_chunk_t>` (`invalid_text_representation` on a value with no reading as its column's type, `numeric_value_out_of_range` past its range). NUMERIC(p, s) of at most 38 digits (its typmod, `PQfmod`) is the engine's DECIMAL(p, s), NaN its DECIMAL NaN; every other NUMERIC — wider, a scale outside 0..p (PostgreSQL 15), and the unconstrained NUMERIC, whose values each keep their own scale and up to 131072 digits (also what PostgreSQL answers for SUM(bigint) / AVG) — is its exact text: no DECIMAL(38, s) holds it without rounding or overflowing, and DOUBLE would round past 15 digits |
| `decimal_text.hpp` | — | `decimal_type(p, s, alias)` (inline: the engine's DECIMAL(p, s) when it can declare one: 1 ≤ p ≤ 38, 0 ≤ s ≤ p), shared by the three translators. A backend's decimal text is read with the engine's own parse, `components::casts::kernels::detail::parse_decimal(text, p, s, &out)` (`<components/casts/kernels/string_cast.hpp>`: the unscaled integer at scale s, a longer fraction rounded half away from zero, a value past p digits refused), in `mysql_to_chunk`, `pg_to_chunk` and the federation's `conform_to_result` |
| `ch_to_chunk` | ClickHouse blocks of a SELECT + the declared types (`system.columns` type strings) by result position: the type of the table column each result column reads as it is (the agent passes the actor's `remote_type` of each column the statement reads: a pushed statement's columns come from several tables under names of the statement's own) | `result_wrapper_t<data_chunk_t>`. A column's type is its override's (`Nullable(T)`, `LowCardinality(T)`, `SimpleAggregateFunction(f, T)` → `T`; `Bool` → BOOLEAN; `Array(T)` → a LIST of `T` whose rows keep their own length; `Tuple` → STRUCT; Decimal(P, S) / Decimal32 / 64 / 128 of at most 38 digits → DECIMAL(P, S) holding the wire's unscaled integer, a result column of another width under a Decimal column's name keeping the wire's; DateTime64, Enum, IPv4/6, Int128/UInt128, Time, Map, … → STRING) when the wire column is a representation of it — the same type up to Nullable / LowCardinality / SimpleAggregateFunction, Tuple field names and `Bool` over the wire's UInt8 — and the wire type's otherwise: a result column that only reads like a table column (`AVG(x) AS x` is Float64, `toString(id) AS id` String, `count() AS x` UInt64, `x + 1 AS x` Int64) is not that column. `ch_to_struct(res, block, named_type_overrides)` gives discovery the same answer for its probe (the table's columns under their own names, the overrides keyed by name). Every value is read as that type, never handed to the engine under another (rc-3's `set_value` aborts on one): `Bool` from the wire's UInt8, and under STRING the text of any wire scalar that has one — Date / Date32 `YYYY-MM-DD` (from the day number, `RawAt`), DateTime / DateTime64(p) `YYYY-MM-DD hh:mm:ss` with p fraction digits in UTC (the column's timezone is not applied), Time / Time64 `[-]hh:mm:ss[.f]`, Int128 / UInt128 decimal, Enum the item's name, IPv4 / IPv6 via `inet_ntop`, UUID, numbers. A value with no reading as its column's type — Map, the geo types, Nothing, an Enum value outside its items, a DateTime64 outside the years 0000–9999 — is `datatype_mismatch` naming the column, not a placeholder; a NULL is NULL whatever its wire type |
| `parquet_to_chunk` / `parquet_to_struct` | parquet file, buffer or stream | `result_wrapper_t<std::pmr::vector<data_chunk_t>>` (a run, as `arrow_to_chunk` cuts it) / `result_wrapper_t<complex_logical_type>` (via Arrow + snappy/brotli/zlib/lz4/zstd) |
| `csv_to_chunk` | csv file, buffer or stream | `result_wrapper_t<std::pmr::vector<data_chunk_t>>` (a run) |
| `ndjson_to_chunk` | ndjson file, buffer or stream | `result_wrapper_t<std::pmr::vector<data_chunk_t>>` (a run; TableReader; LSan note: Arrow's `BackgroundGenerator` worker thread races during teardown — suppressed in `tsan.supp`) |
| `arrow_to_chunk` | `arrow::RecordBatch` | `result_wrapper_t<std::pmr::vector<data_chunk_t>>`: the batch as a run of chunks of at most `DEFAULT_VECTOR_CAPACITY` rows sharing one column shape (shared step of parquet/csv/ndjson, and FlightSQL's DoPut bind); a batch without rows is ONE chunk of its columns and no row, so the schema survives an empty file. Every Arrow type otterbrix's importer reads is read by it, through the Arrow C data interface (`ExportRecordBatch` → `schema_from_arrow` → `data_chunks_from_arrow`, which answers the pieces itself): the integers, floats, bool, utf8 / large_utf8 / string_view, binary (BLOB), date32/64, time, timestamp (any unit, ± tz), duration / interval, dictionary- and run-end-encoded columns, list, struct, map. The pieces alias the batch's buffers — strings as string_views into them (a fixed-size binary is copied) — and every column of every piece also holds a second, zero-copy export of the batch as its owner (`hold_batch_in_every_column`). Temporal values are moved from Arrow's 1970 epoch to the engine's 2000-01-01 by the importer. Two types keep this file's per-cell reader, filled into the rows of each piece: **decimal128** (the importer narrows a value to its storage width with an unchecked cast, and reads `decimal128(p, 0)` as DECIMAL where this shim reads the HUGEINT our writers carry under it — `decimal128(p, s > 0)` is DECIMAL(p, s), a scale outside `0 <= s <= p` or a precision past 38 `unimplemented_yet`, a payload past its declared precision `numeric_value_out_of_range`) and **null**. A type neither reads is the importer's refusal (`unimplemented_yet`) naming the column. `arrow_schema_to_struct` / `parquet_to_struct` (discovery) ask the same importer, so a discovered type is the loaded one. otterbrix refuses a dictionary-encoded column that holds nulls |

**Chunk contract.** The engine takes at most `DEFAULT_VECTOR_CAPACITY` (1024) rows per
`data_chunk_t`; a wider result is a run of such chunks sharing one column shape, and that
is also what the cursor hands the frontends. A file loader (`arrow_to_chunk` and the
parquet / csv / ndjson loaders over it) answers that run itself — otterbrix's importer
cuts the pieces — and `otterstax::engine::load_table` inserts it as it is; an empty file is
one chunk of its columns and no row (`create_table_plan` names the table's columns from the
run's first chunk). A backend translator builds its whole answer as ONE chunk, which the
federation's backend actor hands the engine page by page (`backend_actor_t::next_page`:
`data_chunk_t::partial_copy` windows of at most 1024 rows, slices of the source's
reference-counted buffers, so no row is copied). A column-less chunk is never cut: a
zero-column window would be the engine pipeline's drain sentinel. The translators of a
backend answer read only a SELECT (a scan, a RETURNING); a remote write's count is the
backend's (`federation/CLAUDE.md`). `tests/unit/translators/test_chunk_windows.cpp` pins the loaders'
runs and the backend pages per source (2500 rows), `tests/system/test_backend_managers.cpp`
and `tests/federation/` the federation's pages through a real engine's cursor.

Nothing in `translators/` throws: a file that cannot be opened is `io_error`, a
body Arrow cannot parse `invalid_text_representation`, a type without a reading
`unimplemented_yet`, a value past its type `numeric_value_out_of_range`, a
broken Arrow step `internal_error`, a bad argument `invalid_parameter`. `translators/error.hpp` holds the
`make_error` / `arrow_error` helpers that build those `core::error_t` values on
the caller's resource (`tsl::make_error` is also the one the federation's agents, the
engine requests and the s3 / file managers build their errors with).

The file-format loaders all take a `std::pmr::memory_resource*` and route every
allocation through it — Arrow's own buffers included: every reader, builder,
writer and `CombineChunksToBatch` call allocates from `tsl::arrow_pool(res)`
(`translators/arrow_memory_pool.hpp`), an `arrow::MemoryPool` over the
resource, interned per resource address for the life of the process (an Arrow
buffer frees through the raw pool pointer it was made with, possibly after the
call returned). Arrow allocates from its helper threads too (even the serial
CSV/JSON readers read ahead on a background thread); the pool enters the
resource one call at a time, but code that also uses the resource directly
while such a thread runs (S3 background part uploads) needs a thread-safe one —
the engine's `synchronized_pool_resource` is. Each loader and writer has a
stream overload (`std::shared_ptr<arrow::io::RandomAccessFile>` /
`InputStream` in, `OutputStream` out — Arrow's own handles, for the Arrow
calls) beside the path / buffer forms; the S3 path uses those. Between actors a
stream travels only in its one owner, `tsl::input_stream_t` / `output_stream_t`
(`translators/arrow_stream.hpp`: move-only; `arrow()` is the handle for an
Arrow call; an output stream let go unclosed is aborted, so only `close()`
publishes it). A writer's stream form leaves the stream open (its holder
publishes or aborts it: `FileManager::dump_file` closes it once the whole
result is written); its path form writes through `write_local_file`
(`output/file_sink.hpp`) into a temporary sibling (`temporary_path`: same
directory, hidden, timestamp + sequence number) that is renamed over the target
only when the write succeeded — a failed COPY never truncates or removes a file
already at the path, and readers see the old file or the whole new one. `chunk_to_parquet` writes
snappy-compressed row groups of up to `parquet_row_group_rows` (128 Ki) rows.

**Lifetime trap:** the returned chunks keep that
resource alive — if you borrow the engine's resource (`engine_t::resource()`), the chunk
must die *before* the engine does, otherwise `synchronized_pool_resource`
deallocate sees freed memory (`tests/system/test_file_ingestion.cpp`).

`translators/internal/doc_to_chunk.cpp` handles conversion from Otterbrix document cursor rows.

### `translators/output/`

| File | Output |
|------|--------|
| `chunk_to_arrow` | `to_arrow_schema(res, types)` / `to_arrow_schema(res, struct_t)` → `result_wrapper_t<shared_ptr<arrow::Schema>>` (`unimplemented_yet` on a type without an Arrow mapping; DATE → date32, TIME → time64[us], TIMESTAMP → timestamp[us], TIMESTAMP_TZ → timestamp[us, UTC], as otterbrix's exporter writes them). `chunk_to_record_batch(res, chunk)` (frontends, FlightSQL) and `chunk_to_file_batch(res, chunk)` (the csv / parquet writers): BOOLEAN, the integers, FLOAT, DOUBLE and the temporal types are written by otterbrix's `to_arrow_array` (temporal values moved to Arrow's epoch — no builder ever writes them) and imported with `arrow::ImportRecordBatch`; `chunk_to_file_batch` writes STRING through it too, as `large_utf8`, while `chunk_to_record_batch` keeps STRING on a utf8 builder because that is what the advertised FlightSQL schema says. DECIMAL / HUGEINT (sentinel and precision refusals, `to_arrow_decimal`) and NA always stay on builders, which allocate on `arrow_pool(res)`; the exporter's buffers are otterbrix's own `malloc`'d memory, not `res` |
| `chunk_to_parquet` | `chunk_to_parquet(res, chunks, path)` → `result_wrapper_t<bool>` (snappy/brotli/zlib/lz4/zstd via Arrow) |
| `chunk_to_csv` | `chunk_to_csv(res, chunks, path)` → `result_wrapper_t<bool>` |
| `chunk_to_ndjson` | `chunk_to_ndjson(res, chunks, path)` → `result_wrapper_t<bool>`; DATE / TIME / TIMESTAMP / TIMESTAMP_TZ as otterbrix's own text (`core::date::to_string`, a JSON string) |
| `writable_columns.hpp` | the column validation shared by the writers: only named, flat scalar columns (plus NA) can be written |

The three file-format writers are the `COPY (...) TO 'path'` companion to the
input loaders. `db::S3Manager::upload` and `conn::file::FileManager::dump_file`
pick the writer by `FileFormat` enum (auto-detected from the location's
extension when `format=` is omitted).

### `operators/`

`engine_requests.{hpp,cpp}` (namespace `otterstax::engine`) — every request otterstax makes to the embedded engine, as free functions sent straight to the engine dispatcher (`runtime::engine_t::dispatcher_address()`, `services::dispatcher::manager_dispatcher_t`); there is no engine actor of otterstax's own. Each names the engine SESSION it runs on (a query session passes its connection's; the catalog its own) and the coroutines take the memory resource first and must be awaited from an actor coroutine (or a test's drive loop). Nothing throws; an engine refusal is an error cursor or an error value with the engine's own code.

- `sql_plan` — one bookkeeping SQL statement through the engine's own parser and transformer.
- `send_plan` — one plan on a session. (A statement that failed before the engine is reported by the query session itself, `manager_dispatcher_t::refuse_statement`, so it fails an open transaction.)
- `execute_statement` — runs a parsed statement (`ExecutionPlanPtr`) and holds the answer to the shape its root owes: a DML without RETURNING answers its count through `cursor->affected_rows()` (a write to a remote table too: the backend's count for a statement the backend actor ran whole, otherwise the rows the engine handed the storage; `federation/CLAUDE.md`), a row-producing statement without columns is `internal_error`. Every cursor carries the engine's transaction status and window (`transaction_status()`, `portal_id()`, `has_more()`). A describe is the engine's own (`session/CLAUDE.md`).
- `create_table_plan` / `load_table` — the load behind CREATE EXTERNAL TABLE, on the session it is given (the query session passes its connection's): CREATE TABLE of the target as written (1–4 parts, its empty slots the session's place; the database is not created, a missing schema and an existing relation are the engine's refusals — `table_already_exists`), a column without a name or an empty run `invalid_parameter`; then the insert of the loader's run (each chunk within the chunk contract); `own_block` wraps both in BEGIN / COMMIT, ROLLBACK on a failure, the answer carrying the status the session is left in.

The engine evaluates aggregates over constants correctly, so no plan rewrite or guard stands in front of it (`tests/system/test_constant_aggregates.cpp` pins the engine's answers). `tests/system/test_engine_requests.cpp` and `tests/system/test_external_registration.cpp` cover these over a real engine.


## Adding a New Backend Translator

1. Create `translators/input/<backend>_to_chunk.{hpp,cpp}`
2. Follow `mysql_to_chunk` as the template — map each column type to the corresponding `components::types::logical_type`.
   Keep the translator's helpers (`value_translator_t`, the `set_*` converters, `to_local_translator`) in an
   anonymous namespace: the translators reuse those names inside `tsl`, and with external linkage the linker keeps
   one definition for all of them, so one translator's objects get destroyed with another translator's layout
3. Register in `otterbrix/CMakeLists.txt` under both `OTTERBRIX_HEADERS` and `OTTERBRIX_SOURCES`
4. Use the new translator in the backend agent's read (`otterbrix/federation/backend_agent.cpp`)
