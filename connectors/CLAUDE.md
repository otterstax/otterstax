# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`connectors/` is everything that talks to a remote system: one **driver** per backend
type (MySQL, PostgreSQL, ClickHouse) and per transfer kind (S3, local files), and the
**pools** that run them. Nothing here is an actor and nothing here touches the engine.
The integration layer (`integration/backend.hpp`) and the Worker (`scheduler/worker.cpp`)
send requests into the pools and `co_await` the answers.

Connections are registered once at startup from `config.yaml` (top-level `CLAUDE.md`,
"Connection Config"): `ComponentManager::register_connections` builds a disconnected
driver per connection, hands it to the pool of its type, and has the catalog discover
the connection's tables. There is no runtime add/remove API.

## Structure

```text
connectors/
├── common/
│   ├── statement.hpp        — statement_t (sql, result_kind, decode hints, timeout), statement_result_t,
│   │                          statement_outcome_t = result_wrapper_t<statement_result_t>
│   ├── driver.hpp           — conn::IDriver: connect / execute / ping / close / clone_disconnected
│   ├── pool_config.hpp      — pool_config_t (min/max size, idle, check-after-idle, queue timeout,
│   │                          connect backoff), waker_t, alias_map_t (alias → pool)
│   ├── wait.hpp             — wait_ticket_t, wait_owner_t: how an awaiting actor parks on a request
│   ├── work_pool.hpp        — work_pool_t<Session>: one queue, min_size..max_size threads
│   ├── connection_pool.{hpp,cpp} — driver_session_t, alias_pool_t, connection_pool_t (alias → pool)
│   └── transfer.{hpp,cpp}   — load_t / export_t, ITransferDriver, transfer_pools_t
├── mysql/        driver.{hpp,cpp}  — Boost.MySQL, binary protocol, async ops under one deadline
│                 errors.hpp        — server/client error → core::error_code_t
├── postgresql/   driver.{hpp,cpp}  — libpq async API + poll() against a deadline
│                 types.hpp, errors.hpp
├── clickhouse/   driver.{hpp,cpp}  — clickhouse-cpp with bounded socket timeouts
│                 types.hpp, errors.hpp
├── s3/           driver.{hpp,cpp}  — one S3FileSystem per alias (Arrow)
│                 s3_subsystem.{hpp,cpp} — subsystem_finalizer_t (FinalizeS3)
└── file/         driver.{hpp,cpp}  — local files; load_chunk / write_chunks (the format translators)
                  types.hpp         — FileFormat, resolve_format
```

CMake: `connectors` (common + the three database drivers), `s3`, `file`.

## The driver contract (`common/driver.hpp`, `common/statement.hpp`)

A driver is **one connection, synchronous, bounded, and never throws**:

- `connect()` / `ping()` → `core::error_t`; `close()` is `noexcept`.
- `execute(statement)` → `statement_outcome_t`. The statement says what answer it
  wants (`result_kind`): `rows` (a `data_chunk_t`), `schema` (a STRUCT, from a
  prepare/discovery probe) or `affected_rows` (a count). The driver **decodes on the
  pool thread** with a free `decode(resource, raw, statement)` of its own
  (`ch::decode`, `mysql::decode`, `pg::decode`), so a test driver answering canned
  raw results decodes them exactly as the real one does. `statement.hints` carries
  the decode metadata discovery found (ClickHouse named types, PostgreSQL ENUM oids).
- `clone_disconnected()` — the pool keeps a disconnected prototype per alias and
  opens every connection as a clone of it.

What a failure does to the connection:

| Failure | Connection | Statement repeated? |
| --- | --- | --- |
| the server's verdict (syntax, missing table, constraint…) | kept | never |
| stale: a client/transport error on a socket that was dead before the statement | reset | a read, once; never a statement that is not `repeatable` |
| our deadline fired, or the call failed outright | reset; MySQL / PostgreSQL also cancel the statement on the server | no |

`statement_t::repeatable` — not the result kind — decides the repeat: MySQL and
PostgreSQL send a DML as a `rows` statement (their translators build the count carrier
from the OK packet / command tag), so the kind cannot tell a read from a write.
`db::backend_t` marks every slot that is not a read (`is_read`: aggregate, join, set
operations, sort/limit/group, a stub) `once()` — INSERT/UPDATE/DELETE and every
CREATE/DROP, on all three backends — since one whose answer was lost may already have
been applied. `tests/system/test_statement_repeat.cpp` pins the flag per statement and
backend, `test_mysql_connector.cpp` the driver's repeat on a stale socket.

A statement the backend rejects keeps the backend's verdict — `mysql/errors.hpp`
(boost.mysql `error_code` + `diagnostics::server_message`), `postgresql/errors.hpp`
(SQLSTATE), `clickhouse/errors.hpp` (`ServerException::GetCode`) map syntax, missing
table/column/database, duplicates, ambiguity and constraint failures onto the
engine's `error_code_t`; transport failures are `io_error`. Message formats:
`[Run query] Alias: X query [sql] failed: …` and `[Connector] Alias: X connect failed: …`.

## Pools (`common/work_pool.hpp`, `common/connection_pool.hpp`)

`work_pool_t<Session>` is one queue served by `min_size`..`max_size` threads, each
owning one `Session` (a `driver_session_t` = one connection, or a
`transfer_session_t` = a handle on a shared transfer driver). Whichever thread is
free takes the next request, so a request never waits behind a slow one while
another session sits idle (PgBouncer's statement pooling).

- `submit(request, waker)` never blocks: it answers through an
  `actor_zeta::promise` whose future the caller `co_await`s. The awaiting actor
  resumes on its own scheduler thread; no scheduler thread blocks on a backend.
- `min_size` sessions open on the caller's thread when the alias is added (any
  failure → no pool). The pool grows when more requests are queued than sessions
  are idle, below `max_size`, **one connect at a time**, on the new session's own
  thread, and not within `connect_backoff` of a failed connect.
- A session idle for `idle_timeout` closes while more than `min_size` are open.
- `driver_session_t` pings a connection that sat idle longer than
  `check_after_idle` before using it (PgBouncer's `server_check_delay`): the server
  or a balancer may have dropped it, and a DML sent on a dead socket cannot be
  repeated.
- A request that waited longer than `queue_timeout` is answered `io_error`
  (`pool '<name>' is busy: …`) without running — when its time is up, by the pool's
  expiry thread, even while every session is still busy (the check a session makes
  when it takes the request is the backstop).
- Destruction answers every queued request `io_error` (`is shutting down`), then
  joins the threads; a request already running finishes first. Wakers are not
  called on that path — their awaiters are gone by then.
- **The waker.** After a request's answer is settled and its promise released,
  the session thread calls the request's `waker_t` (may be empty). A promise's
  `set_value` schedules nobody, and actor-zeta re-runs an actor whose behavior
  awaits an unsettled future until it settles. So a Worker submits every request
  with the waker of a `wait_ticket_t` (`common/wait.hpp`, the `wait_owner_t`
  contract): the Worker parks while it is suspended on the ticket, and the waker
  enqueues it again — or, when it is not parked, sends it a no-op `Worker::wake` (at
  most one queued at a time), since actor-zeta 1.2.0 leaves a cooperative actor blocked on its mailbox when,
  after resuming one awaited answer, its handler suspends on the next
  (`scheduler/parking.hpp`). An awaiter that polls until the answer is there
  (`CatalogManager::enqueue_impl`, `otterstax::test::drive`) passes none.

`connection_pool_t` maps alias → `alias_pool_t`; it is filled on the startup thread
(`add`, `drop` on a failed registration) and read-only afterwards, so `execute` takes
no lock but the pool's own. An unknown alias gets an already-settled `do_not_exists`
(`no connection registered for '<alias>'`). `ComponentManager` owns one
`connection_pool_t` per backend type in `db::backend_registry_t`
(`integration/backends.hpp`).

`resource` must be thread-safe: promise states and error messages are allocated on
the session threads and released on the awaiting actor's.

## MySQL (`mysql/driver.{hpp,cpp}`)

Boost.MySQL's own sync functions take no deadline, and Asio's blocking reads ignore a
socket timeout, so the driver runs Boost.MySQL's **async** operations, each under
`asio::cancel_after` with the time left of the call's one deadline
(`connect_timeout`, `statement_timeout` from `config.yaml`), inside a coroutine on a
**private single-thread `io_context`** that only the calling pool thread runs
(`run_on`). With one thread the deadline timer's handler and the operation's run one
after the other, so the `timed_cancel_op` race of a multi-threaded io_context (two
handlers sharing a ref_count, the final handler destroyed without being invoked —
broken promise, `std::terminate`) cannot happen. A cancelled operation leaves the
`any_connection` unusable, so `reset_connection()` builds a fresh one.

A statement past its deadline keeps running on the server after the socket closes, so
the driver kills it: `KILL QUERY <id>` over a second connection with the alias's
parameters, `connect_timeout` for the connect and for the KILL, best-effort (a failure
is logged; the deadline's `io_error` stands). The id is the session's
`SELECT CONNECTION_ID()`, read once per connect (Boost.MySQL 1.88 does not expose the
one in the server greeting); a server that refuses the query leaves the connection
without the cancel. Behind a proxy that multiplexes client sessions onto backend
connections and forwards both statements as they are, that id would be a backend
connection's.

Every statement runs over the **binary protocol** — `async_prepare_statement`, then an
`execute` + `close_statement` pipeline — never `COM_QUERY` (only the driver's own
`SELECT CONNECTION_ID()` and `KILL QUERY` use it): the text protocol renders a
FLOAT column with FLT_DIG (six) significant digits, so rows read over it are not the
values the backend holds (the stored 64647.5390625 arrives as "64647.5") while `SUM()`
over the same column is exact. Generated statements inline every value, so the
statement binds with zero parameters; the close stage runs even when the execute stage
fails, so no server-side statement handle outlives a query.
`tests/test_{mysql,pg}_client_mysql_backend.py::test_float_column_reads_back_the_stored_value`
pin it. `close()` uses the non-throwing `async_close`: it also runs from the destructor,
and the `COM_QUIT` it writes fails with `broken_pipe` when the server has already reset
the socket. `tests/unit/utility/test_mysql_connector.cpp` pins connect/close in a loop,
a connect to a server that never greets (io_error at the deadline), close after a
reset, a stale socket repeating a read but not a `once()` statement, and a statement
the server never answers killed by the id its session read, against a fake MySQL
server on 127.0.0.1 (it answers `SELECT CONNECTION_ID()` and records `KILL QUERY`).

## PostgreSQL (`postgresql/driver.{hpp,cpp}`)

libpq's blocking `PQexec` has no deadline, so the driver uses the async API in
non-blocking mode: `PQsendQuery`, a `PQflush` loop, then `PQconsumeInput` /
`PQisBusy` / `PQgetResult` with `poll()` on the socket against the statement's
deadline. The conninfo (`pg::connect_params::connection_string`) single-quotes every
value and sets `connect_timeout`, TCP keepalives and `tcp_user_timeout`, so a peer
that vanished is noticed. `CONNECTION_BAD` after a failure is stale; a deadline is
broken, and before the connection closes the statement is cancelled on the server with
`PQcancel` — the protocol's CancelRequest under the connection's cancel key, which a
pooler such as PgBouncer routes to the right server connection (a
`pg_cancel_backend(PQbackendPID())` would name the pooler's made-up pid). libpq 15
connects the cancel socket without a timeout. Best-effort: a failure is logged.

## ClickHouse (`clickhouse/driver.{hpp,cpp}`)

clickhouse-cpp 2.6.1 opened with one set of `ClientOptions` (`client_options` in
`driver.cpp`): connect, recv and send timeouts of **10 s**, `send_retries = 1`, no
`ping_before_query`. On its own the driver bounds only the TCP connect (5 s) and
leaves `SO_RCVTIMEO`/`SO_SNDTIMEO` at 0, and every read on the wire is a blocking
`recv()` — the ServerHello of the handshake, the Pong of `Ping()`, every packet of a
query — so a backend that accepted the connection and never answered froze the
calling thread for good. With the bound the client throws `std::system_error`
(EAGAIN) at expiry; the driver answers `io_error` and does not repeat a timed-out
statement. `send_retries` is the number of back-to-back connect attempts the
`Client` constructor makes, with no delay between them — one, so the pool's
`connect_backoff` is the only retry ladder. A running query never idles 10 s: the
server sends a Progress packet every `interactive_delay` (100 ms by default). Without
`ping_before_query`, `Client::Execute` never enters the client's `RetryGuard` (ping,
sleep, reconnect, ping… forever against a backend that completes handshakes but never
answers a Ping); liveness is the pool's `check_after_idle` ping.
`tests/unit/utility/test_ch_connector.cpp` pins all of this against a fake ClickHouse
server on 127.0.0.1 that speaks the native handshake and Ping/Pong.

No ClickHouse block carries an affected-row count. `ch::make_statement` collects the
blocks and `written_rows`, the sum of every Progress packet's delta; `decode` answers
it for an `affected_rows` statement. It is an INSERT's count (materialized views add
their rows, `async_insert` reports none); a lightweight DELETE and `ALTER TABLE ...
UPDATE` are mutations that report nothing — `db::clickhouse_traits` turns the number
into the engine's count carrier for `insert_t` only.

## S3 and local files (`common/transfer.hpp`, `s3/driver.*`, `file/driver.*`)

A transfer moves bytes and translates formats; it never touches the engine — the
Worker calls `OtterbrixManager::create_table` / `execute` itself.

- `load_t{location, format}` → the chunk (`CREATE EXTERNAL TABLE`);
  `export_t{location, format, cursor}` → writes the engine's result (`COPY ... TO`).
  An empty `format` lets the location's extension decide (`file::resolve_format`).
- `transfer_pools_t` holds one `transfer_pool_t` per S3 alias (`add_s3`) and one for
  local files (`set_files`, sized by `service.file_transfers`). A transfer thread owns
  no connection: every thread of an alias calls the alias's one driver.
- `s3::driver_t` holds the alias's `arrow::fs::S3FileSystem` — one AWS client with its
  own HTTP connection pool — built at startup by `driver_t::make` (which calls
  `arrow::fs::EnsureS3Initialized()`), with `connect_timeout` from
  `connect_timeout_ms`. Load: `OpenInputFile` → `file::load_chunk` over the stream.
  Export: `OpenOutputStream` → `file::write_chunks` into it → `Close()`, which
  completes the upload; every other path `Abort()`s it (`upload_guard_t`), so a failed
  export publishes no object, and `allow_delayed_open` makes an export aborted before
  its first write cost no request. Nothing is staged on local disk. Errors:
  `invalid_parameter` (empty alias/keys, unknown format), `do_not_exists` (unknown
  alias, from `transfer_pools_t`), `io_error` (any Arrow S3 failure, prefixed by the
  step: `open_input`, `open_output`, `close`), the translators'
  `conversion_failure`. Arrow reports through `Status`/`Result`; nothing throws.
- `file::driver_t` — stateless; a local file written by an export is removed again if
  the write fails. `file::load_chunk` / `file::write_chunks` are the format dispatch
  both drivers share (`tsl::<fmt>_to_chunk`, `tsl::chunk_to_<fmt>`).
- Arrow's S3 buffers come from the resource through `tsl::arrow_pool`, which must be
  thread-safe (Arrow uploads parts from its own IO threads).
- Arrow requires one `FinalizeS3()` before exit, and finalization is terminal.
  `conn::s3::subsystem_finalizer_t` is the process-level guard: `main.cpp` declares one
  before `ComponentManager` (which owns the drivers), `tests/system/main.cpp` runs one
  when the test run ends.

Error paths: `tests/system/test_s3_errors.cpp`; the format round trips:
`tests/system/test_file_ingestion.cpp`.

## Adding a new backend

1. Write a driver (`IDriver`) with a free `decode` for its raw answer, following
   `clickhouse/driver.*`, and an `errors.hpp` classifier.
2. Add a descriptor + parsing section in `config/connections/` (with its `pool:`
   block) and register it in `ComponentManager::register_connections`.
3. Add a `connection_pool_t` and metadata to `db::backend_registry_t` and a traits
   struct for `db::backend_t` (`integration/`), then route it in the Worker and the
   catalog.
