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

A converter that cannot produce a chunk (`mysql_to_chunk` / `pg_to_chunk` / `ch_to_chunk` report
`conversion_failure`) records the error in its query's `conversion_errors` slot and answers with no chunk; the manager then returns that error,
code included, instead of the generic "returned no result chunk". The slots live in the batch scope,
declared before the batch's `QueryHandleWaiter`: when `execute` returns while queries are still in
flight (the first failure `wait()` reports, or an error during dispatch), the waiter's destructor
drains them and their converters still write to the slots, so the slots must be destroyed after it.
`tests/system/test_backend_managers.cpp` pins it with a converter that runs after an earlier slot failed.

The translated chunk holds the whole result set. Before it replaces its plan slot as `node_raw_data`
it is cut into chunks of at most `DEFAULT_VECTOR_CAPACITY` rows (`tsl::split_to_capacity`,
`otterbrix/translators/input/chunk_windows.hpp`) — the engine's chunk bound, which then also holds for
the cursor the frontends read. A column-less DML count carrier is substituted whole, after
`capture_remote_dml_count` has read it. `tests/system/test_backend_managers.cpp` pins both, the wide
slice read back through a real engine's cursor included.

## Schema discovery (`discover`)

Each remote manager is the **sole driver** of its `ConnectorManager`: besides `execute` it handles
`discover(qualified_name_t scope)`, the catalog's discovery request (`catalog/discovery.hpp` for the
reply type and the shared query builders). Empty `scope.collection` → every base table of the
database/schema (list query, then one probe per table); non-empty → that single table. The connector
futures are consumed on the actor (`.get()` under its mutex), so nothing outside it touches the
connector manager after startup.

The metadata a query later needs is discovered here and kept as actor-confined state, assigned from
the value the io-thread handler returns — never written by the handler itself:

- `PostgressManager::enums_` — ENUM oid → labels per uid, refreshed on every discovery of the uid; a
  failed `pg_enum` query fails the discovery. `execute` copies the map into its converter; a uid
  without an entry never went through discovery and is `do_not_exists`.
- `ClickhouseManager::named_types_` — column → type string per (uid, table) from `system.columns`
  (query built with `escape_sql_literal`), replaced on rediscovery; a failed or malformed answer fails
  the discovery. The probe's STRUCT is built with the same overrides, so the registered schema and the
  chunk `execute` decodes agree on named tuple columns. A table without an entry (a DDL target) has no
  overrides. An override is keyed by the base column's NAME, so `ch_to_chunk` applies it to a result
  column only when the wire column is a representation of the named type (the same type up to
  Nullable / LowCardinality / SimpleAggregateFunction, Tuple field names and Bool over UInt8); a result
  column that only shares the name (`AVG(x) AS x`, `toString(id) AS id`, `count() AS x`, `x + 1 AS x`)
  is its wire type. The catalog prepares an aliased aggregate by its function (MIN / MAX by their
  argument column), not by a column of the same name (`schema_utils::aggregate_filter_schema`), so a
  prepared `AVG(x) AS x` and its executed chunk are both DOUBLE — `tests/system/test_backend_managers.cpp`
  pins it through prepare, execute and `ChunkBatchReader`. The prepared types of other expressions read
  off the plan are not ClickHouse's result types — `count()` is BIGINT on the plan and UInt64 on the wire,
  arithmetic takes its first operand's type where ClickHouse widens `x + 1` to Int64, a non-aggregate
  function aliased as a column (`length(name) AS name`) has no type on the plan at all (NA) — which is
  why the prepared schema of a ClickHouse statement is the backend's answer, not the plan's:
  `ClickhouseManager::describe(id, data)`.

## Prepare probe (`describe`)

All three remote managers answer `describe`, the prepare-time counterpart of `execute`: for every
schema_node_t stub of the statement — the catalog's, made of an aggregate slot, and the parser's,
carrying the raw SQL of a lifted-out sub-query — the manager generates the statement `execute` would
run (the same `generate_slot_statement` call, so both see one text), wraps it with
`db::make_prepare_probe` (`integration/prepare_probe.hpp`) as
`SELECT * FROM (<statement>) [AS otx_prepare_probe] LIMIT 0` and runs it through `executeQuery`. The
alias is there for MySQL and PostgreSQL, which refuse a derived table without one; ClickHouse needs
none, so its probe text is unchanged. `LIMIT 0` is what keeps the probe at the statement's analysis
on all three — PostgreSQL's `ExecLimit` reports the empty result without pulling from the subplan,
ClickHouse's `LimitTransform` closes its input before the first block, MySQL/MariaDB short-circuit
the query block while optimizing it ("Zero limit") — where a `WHERE 1 != 1` wrap would be a filter
over the derived table's rows, which an aggregate under it has to produce first. The ClickHouse
path, which came first, is described in full below; MySQL and PostgreSQL differ only in how the
answer is read. ClickHouse answers the
header block of the wrapped statement (its result columns and wire types; the server sends the
pipeline header before any data) and the outer `LIMIT 0` finishes the pipeline before a source reads
a row (`LimitTransform` closes its input once `rows_read >= offset + limit`, which holds before the
first block), so the probe costs the statement's parse and analysis plus one round-trip, never its
data. A `WHERE 1 = 0` wrap would not do: a constant-false filter over a subquery is a filter on the
subquery's rows, and an aggregate under it runs to completion. The header goes through
`tsl::ch_to_struct` under the slot's table's named types, exactly as `execute`'s blocks go through
`ch_to_chunk` — `column_type` is one function — so the prepared column types are the executed chunk's
by construction (`tests/unit/translators/test_ch_to_chunk.cpp` pins the equality on a header and its
data blocks). The stub is replaced by one carrying that STRUCT, which `Worker::prepare_schema` reads
where it read the catalog's; nothing is cached — every prepare asks the backend.

What `describe` refuses, as a value: a statement with unbound parameters is `unimplemented_yet`
before the backend is asked — the binder (`transform_result`) takes every parameter out of the plan,
literals included, until `finalize`, so no SQL exists to probe with, and a type read off the plan
instead would be the guess the probe replaces; a probe the backend refuses or does not answer keeps
the connector's error and code (`io_error` on a silent backend, the classified server code
otherwise); an answer without a header block is `schema_error`; a slot whose uid has no connection
is `do_not_exists`. A raw-SQL subquery stub is described too: one `schema_node_t` serves both kinds
of stub and its schema field was always there — the raw constructor simply left it at the default
`NA` — so `describe` fills that field in place instead of replacing the node, since the raw text and
qualifiers `execute` generates the statement from have to survive. Until it did, a derived table
reached the client with NA column types and a JOIN with a subquery did not merely degrade: merging
the stub's NA failed the prepare outright with "OtterBrix collection is missing in catalog". A
statement without a stub — a DML, a JOIN root — is handed back unchanged with nothing probed. `tests/system/test_single_backend_prepare_schema.cpp` drives all of it
over a typed ClickHouse mock: classify → describe → execute, the described stub against the executed
chunk for `COUNT(*)`, `score + 1 AS score`, `length(name) AS name` and a named Tuple column, the probe
text against the executed statement, and every refusal.

**MySQL and PostgreSQL answer the same way.** `MySQLManager::describe` and
`PostgressManager::describe` are that handler in their own dialect, and each reads the answer with
the translator its own `execute` reads rows with, so the prepared types are the executed chunk's by
construction. PostgreSQL uses `tsl::pg_to_struct` under the connection's ENUM oids — the same
`to_local_translator` `pg_to_chunk` types tuples with. MySQL uses `tsl::mysql_to_chunk` over the
probe's zero-row result set (a SELECT's column definitions precede its rows, and the connection
reads them in `metadata_mode::full`). `tsl::mysql_to_struct` reads the same answer off the same
table — it IS the STRUCT of that chunk's types — so `discover` and `describe` and `execute` cannot
disagree about a column's type. They once could: a second copy of the map (`mysql_to_complex`) had
no BIGINT case, so discovery mirrored a real BIGINT column as `NA` while its rows arrived as BIGINT,
and the same for the date/time family and JSON. That copy is gone.
A column type the MySQL translator cannot represent is therefore the same `conversion_failure`
`execute` answers for it. Reading the prepared-statement metadata instead (COM_STMT_PREPARE, the
route MySQL documents for "the types the optimizer determined") is not available here: Boost.MySQL's
`statement` exposes only `id()` and `num_params()` and drops the column definitions it reads, and
the connector's query API takes text. The refusals are ClickHouse's, one message per backend:
unbound parameters are `unimplemented_yet` before the backend is asked, a refused probe keeps the
connector's error and code, an answer without columns (a bare OK packet on MySQL) is `schema_error`,
and a slot whose uid has no connection is `do_not_exists` — to which PostgreSQL adds `do_not_exists`
for a uid whose discovery left no ENUM metadata, as its `execute` does.

A `Mixed` statement is described by each participant in turn: the manager fills only the stubs its
`node_backend_types` entry names and hands the statement on, the same skip `execute` makes, so the
Worker sends it to MySQL, PostgreSQL and ClickHouse in the order `run_pipeline` executes them
(`scheduler/CLAUDE.md`). `tests/system/test_single_backend_prepare_schema.cpp` drives the MySQL path
over a typed MySQL mock the way it drives the ClickHouse one: `score + 1 AS score` and
`length(name) AS name` prepared as the BIGINT the backend answers while the plan reads INTEGER / NA,
the probe text against the executed statement, a raw-SQL stub filled in place, both stubs of a JOIN
probed, and every refusal.

The local-engine schema probe (`OtterbrixManager::get_schema`, `KafkaManager::recover()`) is a
different mechanism: `otterbrix/operators/schema_probe.hpp`. Its caller reads the types the engine
stamped onto the probe's plan after the engine replied. That is a deliberate exception to actor
isolation; why it is safe, and what the otterbrix rc-3 migration changes, is in `kafka/CLAUDE.md`
("Constraints / conventions").

## OtterbrixManager

`OtterbrixManager` accepts an `IDataManager` rather than a direct `otterbrix_ptr`. This decoupling is load-bearing for tests: `tests/mock/otterbrix.hpp` provides `SimpleMockOtterbrixManager`. Do not change the constructor to take `otterbrix_ptr` directly.

`execute` holds the engine's result to the shape the plan root owes, both ways (`types/otterbrix.hpp`):
a no-RETURNING DML (`dml_without_returning`) reports only its count, so columns the engine attached
to the carrier are dropped and the cardinality kept; a row-producing statement
(`row_producing_statement` — an aggregate / set-operation root, or a DML with RETURNING) whose result
has no columns is answered with `schema_error` (`SELECT returned a result without columns: ...`)
instead of the chunk, because every frontend reads a column-less payload as "N rows affected" and
would report the SELECT as a DML. No column is invented; the engine produces such a result for a
column it cannot type (a `hugeint` column is read back as UNKNOWN). A backend slice inlined as raw
data (`data_t` root) is outside the rule — its shape is the translator's contract. `execute` is the
single choke point, so the rule covers the Worker pipeline and `COPY (...) TO` (`FileManager::dump_file`,
`S3Manager::upload`) alike. `tests/system/test_row_producing_result_shape.cpp` pins it over the real
engine, `test_otterbrix_manager.cpp` over the mock.

The `get_schema` handler is called during the schema-resolution phase (before actual execution) to determine output column types from Otterbrix's local catalog. It returns a `(cursor_t_ptr, ParsedQueryDataPtr)` pair so the calling `Worker` can build the schema (via `Worker::prepare_schema` → `finish_schema_value`) without executing the query.

### External-table registration channel

The catalog mirrors remote tables into the engine through four handlers; the uid is
the engine database, the remaining qualifiers fold into the encoded collection name
`<db>:<schema>:<table>` (`encode_external_collection`, one-way by design).

- `register_external_database(uid)` — creates the uid's database, or reuses the one
  the engine restored from a previous run's data dir (`false`); the name is guarded
  against user DDL (`CatalogManager::check_database_ownership`), the one exception
  being a data dir written before that guard existed. Either way it makes the uid's
  **manifest** exist: the collection `<uid>.__otterstax_tables`, one `collection`
  STRING row per mirror. The manifest exists because on rc-2 the engine's `pg_class`
  is not readable through SQL (named projections fail, `SELECT *` answers a 1×1
  result), so it is the only record of which mirrors a previous run left behind.
- `register_external_table(name, columns)` — writes the manifest row FIRST (a crash
  between the two leaves a row without a collection, which `drop_stale_external_tables`
  resolves; the reverse would leave a mirror no run can find), then **probes** the
  engine (`IDataManager::describe_collection`): absent → created; present with the
  same column names and types → reused under its restored OID; present with another
  schema → dropped and recreated (logged at warn). The probe comes first because on
  rc-2 a repeated `CREATE TABLE` of an existing collection is not an error and stamps
  a fresh OID the catalog never holds — the create verdict cannot tell the two apart.
- `drop_external_table(name)` — the collection, then its manifest row (the catalog's
  undo when it cannot mirror a table).
- `drop_stale_external_tables(uid, live)` — every manifest row whose collection `live`
  does not name: the collection is dropped (a row whose collection is already gone is
  deleted alone) and the row removed; answers the number of rows removed. The catalog
  sends it on the uid's first registration in the process when the database was reused.

`tests/system/test_restart_reconciliation.cpp` pins the three outcomes over a real
engine restarted on the same data dir; `test_otterbrix_manager.cpp` the database
verdicts over the mock.

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

Every handler forwards the callee's `core::error_t` code unchanged (`do_not_exists` for an unknown alias,
`invalid_parameter`, `io_error`, an engine code from `dump_file`) and only prefixes the message, so the
Worker reports the real cause. Its own checks (null statement, unrecognised object-key extension) are
`invalid_parameter`. Covered by `tests/system/test_s3_errors.cpp`.

Its handlers (and `FileManager`'s) `co_await` the cross-actor sends rather than `.take_ready()`, so they
stay correct if those callees ever move off the synchronous busy-wait path. End-to-end coverage lives in
the python integration suite (`tests/test_{schema_}mysql_s3.py`,
`tests/test_mysql_join_sql_s3_to_s3.py`, `tests/test_mysql_join_otb_local_s3.py`, and
`tests/test_mysql_join_otb_local_s3_file.py` — 3-origin: s3 parquet ⋈ file csv ⋈
otterbrix-internal), driven by `docker-run-tests.sh`.
