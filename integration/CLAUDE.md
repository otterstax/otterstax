# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`integration/` sits between the `Scheduler`'s `Worker` pool and the connection pools
(`connectors/`) and the Otterbrix engine. The remote databases are reached through
**plain objects**, `db::backend_t<Traits>` (`backend.hpp`) — one template for MySQL,
PostgreSQL and ClickHouse, `Traits` holding what differs (`sql/mysql_backend.*`,
`postgresql/postgres_backend.*`, `clickhouse/clickhouse_backend.*`). The engine is reached
through the `db::OtterbrixManager` actor. Kafka has its own runtime (`kafka/`, own
CLAUDE.md).

## Backends

| Directory | Type | Over |
|-----------|------|------|
| `sql/` | `db::MySQLBackend` = `backend_t<mysql_traits>` | `registry.mysql` (`connection_pool_t`), `no_metadata_t` |
| `postgresql/` | `db::PostgresBackend` = `backend_t<postgres_traits>` | `registry.postgresql`, `pg_metadata_t` (ENUM oids per uid) |
| `clickhouse/` | `db::ClickhouseBackend` = `backend_t<clickhouse_traits>` | `registry.clickhouse`, `ch_metadata_t` (named types per uid and table) |
| `otterbrix/` | `db::OtterbrixManager` (actor) | `IDataManager` (wraps `otterbrix::otterbrix_ptr`) |
| `kafka/` | `otterstax::kafka::KafkaManager` (actor) | librdkafka consumer/producer threads + Kafka SOURCE/STREAM objects |

`backends.hpp` holds what they are built on. `db::backend_registry_t` — owned by
`ComponentManager` (or a test fixture) — has one `conn::connection_pool_t` per backend
type, the decode metadata discovery fills, and the S3 / local-file transfer pools
(`conn::transfer_pools_t`). `db::backend_set_t` is one backend of each type over a
registry. **Every Worker owns one set** (built with the Worker's waker, see below) and
**the catalog owns one** (`CatalogManager::set_backends`) for discovery. A backend's
coroutines run inside its owner's behavior: awaiting a statement suspends the owner instead of
blocking a scheduler thread, and two Workers' statements run on two pool connections at
once. Metadata is written by discovery (catalog) and read by every Worker's backend
under a `shared_mutex`; a statement carries a `shared_ptr<const …>` to it as
`statement_t::hints`.

**Parking the owner.** A backend is built with its owner's `conn::wait_owner_t`
(`connectors/common/wait.hpp`); a Worker passes its `actor_parking_t`
(`scheduler/parking.hpp`). `backend_t::send(uid, statement)` submits the statement
with the waker of a fresh ticket (`conn::watch`); `backend_t::answer` holds a
`conn::suspended_on_t` across the `co_await`, which reports the ticket right before it
suspends on the answer (`suspending_on`) and clears it once resumed or the coroutine
is destroyed (`resumed`) — the same two helpers `Worker::run_transfer` uses; `run` is
`answer(send(...))`, and `execute` sends every slot before it answers the first. While the ticket is unsettled the Worker's `resume` answers
`awaiting` and the Worker leaves the scheduler — actor-zeta would otherwise re-run
it until the answer arrives — and the ticket's waker enqueues it again. A waker that
finds the Worker not parked sends it the no-op `Worker::wake` instead (at most one
queued at a time): actor-zeta 1.2.0 leaves a cooperative actor blocked on its mailbox
when, after resuming one awaited
answer, its handler suspends on a second one (a JOIN's two slots, a Mixed statement's
second backend, a describe of two stubs) and no message is queued. The catalog's set
has no owner: `CatalogManager::enqueue_impl` polls its behavior until it finishes.

## Execution pattern (`backend_t::execute`)

1. Receive a `session_hash_t` + `ParsedQueryDataPtr`.
2. For every external slot the backend owns (`owns()`: all for a single-backend
   statement, the ones `node_backend_types` names for a Mixed one), check the uid has
   a connection (`do_not_exists` `no <Name> connection registered for '<uid>'`) and
   its metadata (`Traits::check` — PostgreSQL refuses a uid without discovered ENUMs),
   generate the slot's statement (`sql_gen::generate_query` through
   `generate_slot_statement`), and send it (`Traits::slot_statement` decides the
   result kind: a ClickHouse INSERT/UPDATE/DELETE is `affected_rows`, everything else
   `rows`; `is_read` decides whether the driver may repeat it on a stale connection —
   every slot that is not a read is sent `once()`, a MySQL / PostgreSQL DML too,
   though it is a `rows` statement).
3. Batches run back to front (innermost dependencies first); within a batch every
   statement is sent before the first answer is awaited, so they run side by side on
   the alias's pool. The first failed answer is the result, with the driver's code.
   The two halves are separate calls: `send_batch` (synchronous — checks and generates
   every owned slot of one batch, and sends nothing when one is refused) and
   `answer_batch` (awaits and splices). `execute` is the two per batch;
   `backend_set_t::execute_mixed` (`backends.hpp`) runs a Mixed statement batch by batch
   with ALL three backends' `send_batch` before any `answer_batch`, so the backends of a
   cross-backend JOIN run side by side. Batch order is what keeps dependencies right: an
   outer slot may read an inner one's rows (an INSERT whose SELECT ran on another backend
   is generated as `INSERT ... VALUES`), while slots of one batch never read each other.
4. Each answer becomes a chunk (`Traits::slot_chunk`: ClickHouse turns an INSERT's
   `written_rows` into the count carrier, a mutation into a 0 carrier; a rows answer
   without a chunk is `other_error` `… returned no result chunk`), its DML count is
   captured (`capture_remote_dml_count`), and it replaces its plan slot as
   `node_raw_data`.

The driver already decoded the answer on the pool thread (`mysql::decode`,
`pg::decode`, `ch::decode` over the translators in `otterbrix/translators/input/`), so
a conversion failure arrives as the answer's `conversion_failure`, code included. A
statement still in flight when `execute` returns an earlier slot's failure only writes
into its own promise state, which the pool keeps alive;
`tests/system/test_backend_managers.cpp` pins it with a decode that runs 500 ms after
the earlier slot failed, every freed block quarantined and checked.

The translated chunk holds the whole result set. Before it replaces its plan slot as `node_raw_data`
it is cut into chunks of at most `DEFAULT_VECTOR_CAPACITY` rows (`tsl::split_to_capacity`,
`otterbrix/translators/input/chunk_windows.hpp`) — the engine's chunk bound, which then also holds for
the cursor the frontends read. A column-less DML count carrier is substituted whole, after
`capture_remote_dml_count` has read it. `tests/system/test_backend_managers.cpp` pins both, the wide
slice read back through a real engine's cursor included.

What the Worker does with the returned plan depends on what is left in it. When a backend
answered the statement whole — the plan's statement node is the slice itself — those chunks
ARE the result: `Worker::take_backend_result` moves them into the payload and the engine is
not called. A remote DML (its count goes through `OtterbrixManager::execute`, see
`OtterbrixStatement::remote_affected_rows`) and a column-less slice keep the engine path,
as does every plan with something above the slice: a JOIN of two backends, a backend
JOINed with a local table.

## Schema discovery (`discover`)

Every backend answers `discover(qualified_name_t scope)`, the catalog's discovery request
(`catalog/discovery.hpp` for the reply type and the shared query builders): the catalog
runs `co_await backend->discover(scope)` on its own backend set. Empty `scope.collection`
→ every base table of the database/schema (list query, then one probe per table);
non-empty → that single table. A per-table failure fails the whole discovery.

The metadata a query later needs is discovered here and written into the shared metadata
the Workers' backends read:

- `pg_metadata_t` — ENUM oid → labels per uid, refreshed on every discovery of the uid; a
  failed `pg_enum` query fails the discovery. Every statement carries the uid's map as its decode
  hint; a uid without an entry never went through discovery and is `do_not_exists`.
- `ch_metadata_t` — column → type string per (uid, table) from `system.columns`
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
  `backend_t::describe(id, data)`.

## Prepare probe (`describe`)

All three backends answer `describe`, the prepare-time counterpart of `execute`: for every
schema_node_t stub of the statement — the catalog's, made of an aggregate slot, and the parser's,
carrying the raw SQL of a lifted-out sub-query — the backend generates the statement `execute` would
run (the same `generate_slot_statement` call, so both see one text), wraps it with
`db::make_prepare_probe` (`integration/prepare_probe.hpp`) as
`SELECT * FROM (<statement>) [AS otx_prepare_probe] LIMIT 0` and sends it as a `schema` statement. The
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
the driver's error and code (`io_error` on a silent backend, the classified server code
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

**MySQL and PostgreSQL answer the same way.** Their `describe` is that probe in their own dialect,
and each driver decodes the answer with
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
the driver's statement API takes text. The refusals are ClickHouse's, one message per backend:
unbound parameters are `unimplemented_yet` before the backend is asked, a refused probe keeps the
driver's error and code, an answer without columns (a bare OK packet on MySQL) is `schema_error`,
and a slot whose uid has no connection is `do_not_exists` — to which PostgreSQL adds `do_not_exists`
for a uid whose discovery left no ENUM metadata, as its `execute` does.

A `Mixed` statement is described by each participant in turn: the backend fills only the stubs its
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

Every handler runs to completion on the sender's thread: `enqueue_impl` drives a behavior of
its own until it is done. **Statements run concurrently** — `execute`, `get_schema` and
`create_table` take no lock, so the Workers' statements are in the engine at once, each on a
fresh engine session (statement-level autocommit; the Kafka runtime sends plans to the
engine's dispatcher the same way). The engine answers a lone statement slowly — its pipeline
hands a statement on by polling, `wait_for(100 µs)` per hop, which a VM with coarse timers
(Docker Desktop: any sleep under 1 ms takes ~1 ms) stretches to ~18 ms for `SELECT 1` — and
concurrent statements share those ticks, so serializing them here made N sessions cost N
times that. The four mirror-registration handlers (`register_external_database`,
`register_external_table`, `drop_external_table`, `drop_stale_external_tables`) are
multi-step manifest operations and stay serialized under `registration_mutex_`.
`tests/system/test_otterbrix_manager.cpp` pins both: four 300 ms statements overlap, four
registrations never do. An `IDataManager` must therefore be safe to call from several
threads at once.

`execute` holds the engine's result to the shape the plan root owes, both ways (`types/otterbrix.hpp`):
a no-RETURNING DML (`dml_without_returning`) reports only its count, so columns the engine attached
to the carrier are dropped and the cardinality kept; a row-producing statement
(`row_producing_statement` — an aggregate / set-operation root, or a DML with RETURNING) whose result
has no columns is answered with `schema_error` (`SELECT returned a result without columns: ...`)
instead of the chunk, because every frontend reads a column-less payload as "N rows affected" and
would report the SELECT as a DML. No column is invented; the engine produces such a result for a
column it cannot type (a `hugeint` column is read back as UNKNOWN). A backend slice inlined as raw
data (`data_t` root) is outside the rule — its shape is the translator's contract. `execute` is the
single choke point, so the rule covers the Worker pipeline and `COPY (...) TO` (`Worker::copy_to`,
local and S3 alike). `tests/system/test_row_producing_result_shape.cpp` pins it over the real
engine, `test_otterbrix_manager.cpp` over the mock.

The `get_schema` handler is called during the schema-resolution phase (before actual execution) to determine output column types from Otterbrix's local catalog. It returns a `(cursor_t_ptr, ParsedQueryDataPtr)` pair so the calling `Worker` can build the schema (via `Worker::prepare_schema` → `finish_schema_value`) without executing the query.

### External-table registration channel

The catalog mirrors remote tables into the engine through four handlers; the uid is
the engine database, the remaining qualifiers fold into the encoded collection name
`<db>:<schema>:<table>` (`encode_external_collection`, one-way by design).

- `register_external_database(uid)` — creates the uid's database together with its
  **manifest** (`create_manifest`): the collection `<uid>.__otterstax_tables`, one
  `collection` STRING row per mirror. A database the engine already holds is reused
  (`false`) only when it holds the manifest (`has_manifest`) — the mirror a previous
  run left on this data dir; one without it is a local database of the alias's name
  and is refused with `database_already_exists`. The manifest is written only into a
  database this call created, so a crash between the two creates leaves a database
  the next start refuses (after the engine bump the two go into one transaction). The manifest exists because on rc-2 the engine's `pg_class`
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

## External tables (`CREATE EXTERNAL TABLE`, `COPY ... TO`)

There is no integration actor for S3 or local files. `Worker::load_external_table` /
`Worker::copy_to` (`scheduler/worker.cpp`) send a transfer to the S3 alias's or the
local-file transfer pool (`registry.transfers`, `connectors/common/transfer.hpp`) and call
the engine themselves:

- `CREATE EXTERNAL TABLE` — `load_t{location, format}` on the pool, then
  `OtterbrixManager::create_table` with the chunk it answered. An S3 object streams
  straight into the loader; nothing is staged on local disk.
- `COPY (<select>) TO` — the inner statement is re-parsed and run through
  `OtterbrixManager::execute`, then `export_t{location, format, cursor}` writes the result
  on the pool. An S3 object appears only when the whole result was written: the driver
  `Close()`s the stream on success and `Abort()`s it on every other path, so a COPY that
  fails — the query, a value the writer refuses, the transfer — publishes no object. An
  unknown alias is refused before the query runs.

Both take the statement's `WITH (format = ...)` (empty → the location's extension), so an
object key without an extension can be loaded and written. Every error keeps the
transfer's or the engine's `core::error_t` code (`do_not_exists` for an unknown alias,
`invalid_parameter`, `io_error`, an engine code) and the Worker only prefixes the message
(`CREATE EXTERNAL TABLE failed: `, `COPY ... TO failed: `). Transfers of different
aliases, and of one alias up to its pool's `max_size`, run side by side. Covered by
`tests/system/test_s3_errors.cpp`, `tests/system/test_file_ingestion.cpp` and, against
RustFS, the python integration suite (`tests/test_{schema_}mysql_s3.py`,
`tests/test_mysql_s3_stream.py`, `tests/test_mysql_join_sql_s3_to_s3.py`,
`tests/test_mysql_join_otb_local_s3.py`, and `tests/test_mysql_join_otb_local_s3_file.py` —
3-origin: s3 parquet ⋈ file csv ⋈ otterbrix-internal), driven by `docker-run-tests.sh`.
