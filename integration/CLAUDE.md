# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`integration/` contains the layer between the engine / the query sessions and the raw
`ConnectorManager`s. The three backends (plain objects, not actors) are the federation's backends: the
engine reaches a remote table through them (`federation::backend_t`, `otterbrix/federation/CLAUDE.md`),
never through a session, and a session reaches them for rediscovery and remote DDL through the same
interface, found in the federation registry by uid. `remote_backends_t` (`backends/remote_backends.hpp`)
owns them and their connector managers;
`main.cpp` owns the S3 / file actors and Kafka (spawned after the engine, destroyed before it). The engine has no actor of otterstax's own: every engine
request is a free function of `otterbrix/operators/engine_requests.hpp` sent straight to the engine
dispatcher (`runtime::engine_t::dispatcher_address()`).

## Managers

| Directory | Class | What it wraps |
|-----------|-------------|---------------|
| `sql/` | `db::MySQLManager` | `mysql::ConnectorManager` |
| `postgresql/` | `db::PostgressManager` | `pg::ConnectorManager` |
| `clickhouse/` | `db::ClickhouseManager` | `ch::ConnectorManager` |
| `kafka/` | `otterstax::kafka::KafkaManager` | librdkafka consumer/producer threads + Kafka SOURCE/STREAM objects (NOT a remote-DB connector — own runtime; has its own CLAUDE.md) |

## The backends

Each implements `federation::backend_t` (`otterbrix/federation/provider.hpp`) and nothing else — one
entry, no actor. Two members run on the calling thread and answer a value:

- `discover(qualified_name_t scope)` — the connection's tables (`federation::discovered_tables_t`;
  `catalog/discovery.hpp` for the shared query builders): empty `scope.collection` → every base table of the database /
  schema (list query, then one probe per table); non-empty → that table. Each table comes with its
  primary key (`federation::discovered_table_t::primary_key`, what the federation's row-change sinks name a row by):
  MySQL reads it off the probe's column definitions (`PRI_KEY_FLAG`), PostgreSQL with one
  `pg_index.indisprimary` query per discovery (`make_primary_keys_query`, in `indkey` order), ClickHouse
  has none. A key with a binary column (MySQL `BINARY` / `VARBINARY` / `BLOB` / `GEOMETRY` by the probe's
  column type, PostgreSQL `bytea` by the probe's type oid) is dropped whole by the backend's
  `discover`: its raw bytes have no exact literal; so is a MySQL key with
  a DECIMAL the engine holds as its text (wider than 38 digits: MySQL compares a DECIMAL column with a
  string literal as DOUBLE). A MySQL
  connection whose `@@SESSION.sql_mode` holds `NO_BACKSLASH_ESCAPES` is refused (`unimplemented_yet`):
  the generated literals double a backslash (`tests/system/test_backend_discovery.cpp`: the connection
  left out at startup, the others serving). Called by `remote_backends_t::open` at startup and by the
  query session's rediscovery (`backend_t` from `federation::registry().find(uid)`). PostgreSQL and
  ClickHouse run one discovery at a time (a mutex held for the whole call): a discovery publishes the
  metadata it read (below) only after its queries, so two at once could leave the older reading in
  place of the newer. MySQL keeps no such state and takes no lock.
- `execute_ddl(uid, sql)` — a DDL statement the session generated for a remote table (DDL never
  reaches the engine's host hooks), run as it is; the connector's error, code kept. No lock.

Both block the caller on the connector (`executeQuery(...).get()`), each statement on a connection of
its own checked out of the uid's pool, so they are never called from an engine executor. The other two
are called by the engine's operators from any executor thread:

- `scan(resource, scan_request_t{uid, sql, columns}, rows)` — the statement (a whole-table read or a
  pushed-down query block) is posted as a job to the statements pool (`statement_executor.hpp`: the
  runtime's `backends` pool, `runtime_t::backends_scheduler()`, sized by `service.runtime.backends` and
  shared by the three backends — none has threads of its own), which calls
  `ConnectorManager::executeQuery(...).get()` — the connection checkout and the wait stay off the
  engine's executors — translates the answer
  (`tsl::mysql_to_chunk`, `tsl::pg_to_chunk` under the connection's ENUM oids, `tsl::ch_to_chunk` under
  the declared types by result position `declared_types_for(request)`: per result column, the
  `system.columns` type of the table column `request.columns` says it reads as it is — of whichever table
  of the statement, a JOIN's second table included) onto the backend's resource, append it to `rows` and
  fulfil the promise.
- `write(resource, write_request_t{uid, sql, kind})` — the same for an INSERT / UPDATE / DELETE; the
  answer is the rows the backend reports: MySQL `affected_rows()` for an INSERT / DELETE and, for an
  UPDATE, the rows it matched (`db::mysql_update_rows_matched`: the OK packet's info is ER_UPDATE_INFO,
  `Rows matched: N  Changed: M  Warnings: W` on MySQL and MariaDB alike, but translated by the session's
  `lc_messages`, so it is read by its numbers — exactly three, the first being N; Boost.MySQL never sets
  `CLIENT_FOUND_ROWS`, so `affected_rows()` counts only the rows whose value changed; any other info is
  `conversion_failure`), PostgreSQL `PQcmdTuples`, ClickHouse `written_rows` for an INSERT and 0 for a
  mutation (a mutation rewrites whole parts; whatever `written_rows` the server attributes to it is a part
  size).

A job's body runs under `db::answer_of`: whatever it throws (a broken connector future, an allocation)
becomes the `io_error` its promise carries, so the operator waiting on it always gets an answer; the
executor (`statement_executor_t`) contains anything that escapes past that — the job still counts as
finished and its promise, destroyed unfulfilled, settles its future failed — and `post()` counts a job
only once it is allocated, so a throwing `post()` leaves nothing to wait for.

`resource` is used for the promise only; the rows live on the backend's resource (the runtime's
thread-safe pool), which outlives every statement. The backend's destructor first calls its connector
manager's `shutdown()` — later statements refused at once with `io_error`, the pools closed (a statement
waiting for a free connection wakes up refused), the running statements aborted through
`IConnector::cancel` (`connectors/CLAUDE.md`, "Shutdown") — and then its `statement_executor_t` (its last
data member) waits for every job it posted, so a statement still queued finishes (with that error)
before the connector manager it uses goes, and a backend that never answers does not hold the shutdown
— the backends pool must keep running until the backends are gone (an actor-zeta scheduler drops queued
jobs at `stop()`); the federation registry is reset before any of
this (`remote_backends_t::~remote_backends_t`). A converter that cannot produce a
chunk reports its `conversion_failure`, code included, instead of the generic "returned no result
chunk"; a connector error keeps its code (`io_error` for an exception on the io thread,
`do_not_exists` for a uid nobody opened).

The metadata a read needs is discovered by `discover` and kept per backend, assigned from the value the
io-thread handler returns — never written by the handler itself:

- `PostgressManager` — ENUM oid → labels per uid, refreshed on every discovery of the uid under a
  `shared_mutex` (reads take a shared lock); a failed `pg_enum` query fails the discovery.
- `ClickhouseManager` — column → type string per (uid, database, table) from `system.columns` (query built with
  `escape_sql_literal`), replaced on rediscovery under a `shared_mutex`; a failed or malformed answer
  fails the discovery. The probe's STRUCT is built with the same overrides, so the declared columns and
  the chunks `scan` decodes agree on named tuple columns. `scan` looks the types up by the column each
  result column reads (`scan_request_t::columns`, by position: a pushed JOIN's columns come from several
  tables, under names of the statement's own), and `ch_to_chunk` applies one only when the wire column
  is a representation of it; a computed result column (`AVG(x) AS x`, `toString(id) AS id`, `count() AS
  x`, `x + 1 AS x`) has none and is its wire type — and the pushed-down answer is then held to the
  engine's type for that column by the federation (`conform_to_result`).

`tests/system/test_backend_managers.cpp` (the federation's backends, a wide read through a real engine's
cursor, NOT NULL over an INSERT ... SELECT from a backend, ClickHouse nested columns),
`tests/unit/connectors/test_pooled_clickhouse.cpp` (two scans of one ClickHouse alias never share a
connection),
`tests/system/test_clickhouse_dml_count.cpp` (the write counts), `tests/system/test_concurrent_sessions.cpp`
(reads racing re-discovery), `tests/system/test_backend_discovery.cpp` (the discovery's metadata
queries; the MySQL key off `PRI_KEY_FLAG` recorded as `key_position`, a VARBINARY key and a key over a DECIMAL read as text dropped, a row
change named by the key and counted off a German OK packet info),
`tests/system/test_restart_reconciliation.cpp` (a composite PostgreSQL key in `indkey` order),
`tests/unit/connectors/test_pooled_managers.cpp` (the executor's drain and a throwing job,
`answer_of`, `mysql_update_rows_matched` in several languages, the backend's destruction aborting a scan
the backend never answers and the one queued behind it).

## S3Manager (`s3/`) and the file path

`db::S3Manager` is **not** a query backend and does not follow the four-manager pattern above; it
orchestrates object I/O by bridging the raw S3 connector (`conn::s3::ConnectorManager`) and the
file-mapping connector (`conn::file::FileManager`):

- `download` — pulls an S3 object, then `FileManager::add_file` ingests it into a `database.table`.
- `upload` — runs a **pre-parsed `OtterbrixStatementPtr`** and writes its result to S3. It hands the
  statement to `FileManager::dump_file`, which executes it with `otterstax::engine::execute_statement` on
  the file manager's own engine session and dumps the resulting chunk, so `COPY (<select>) TO 's3://…'`
  can export an arbitrary query — not just a table.
- `ls` — lists objects for an alias.

Every handler forwards the callee's `core::error_t` code unchanged (`do_not_exists` for an unknown alias,
`invalid_parameter`, `io_error`, an engine code from `dump_file`) and only prefixes the message, so the
session reports the real cause. Its own checks (null statement, unrecognised object-key extension) are
`invalid_parameter`. Covered by `tests/system/test_s3_errors.cpp`.

Its handlers (and `FileManager`'s) `co_await` the cross-actor sends rather than `.take_ready()`, so they
stay correct if those callees ever move off the synchronous busy-wait path. End-to-end coverage lives in
the python integration suite (`tests/test_{schema_}mysql_s3.py`,
`tests/test_mysql_join_sql_s3_to_s3.py`, `tests/test_mysql_join_otb_local_s3.py`, and
`tests/test_mysql_join_otb_local_s3_file.py` — 3-origin: s3 parquet ⋈ file csv ⋈
otterbrix-internal), driven by `docker-run-tests.sh`.
