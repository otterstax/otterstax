# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`connectors/` provides raw database connectivity. The SQL backends (`mysql/`, `postgresql/`, `clickhouse/`) are **not** actor-based — plain C++ classes used by the integration layer. The newer `file/` and `s3/` connectors **are** `actor_zeta` actors (synchronous busy-wait `enqueue_impl`), called directly from the `integration/` actors (`db::S3Manager`).

Connections are registered once at startup from the connection config file (see the top-level `CLAUDE.md` "Connection Config"): `remote_backends_t::open` (`backends/`) calls each manager's `addConnection` (and `removeConnection` for a connection whose discovery failed), `backends::register_s3_aliases` (`backends/backends.hpp`) the s3 actor's `add_credentials`. There is no runtime add/remove API.

## Structure

```
connectors/
├── mysql/         — Boost.MySQL async connector + ConnectorManager
├── postgresql/    — libpq connector + ConnectorManager
├── clickhouse/    — clickhouse-cpp connector + ConnectorManager
├── file/          — actor conn::file::FileManager: add_file (file → table) / dump_file (query result → file)
├── s3/            — actor conn::s3::ConnectorManager: S3 object I/O via Arrow S3FileSystem (list/download/upload/credentials)
├── common/        — connection_pool.hpp (per-uid pool), pooled_query.hpp (run_on_checked_out: the
│                    statement coroutine that returns its connection when it ends)
└── api_connections/
    └── {connection,pg_connection,ch_connection}_config.hpp  — `conn::api_server::*Params` structs
        consumed by the managers' `addConnection` overloads (populated by
        remote_backends_t::open from the parsed connection config)
```

## file/ and s3/ actor connectors

Unlike the SQL backends, these are `actor_zeta::actor_mixin` actors whose handlers run synchronously:

- **`conn::file::FileManager`** — `add_file(FileAddParams)` translates a CSV/NDJSON/Parquet file into a
  `data_chunk_t` and creates `database.table` via `otterstax::engine::create_table` (sent straight to the engine dispatcher
  on the file manager's own engine session; `FileManager(resource, engine_dispatcher)`). The chunk holds
  the whole file and travels as one; `otterstax::engine::create_table` hands its rows to the engine as
  chunks of at most `DEFAULT_VECTOR_CAPACITY` rows (`tsl::split_to_capacity`, see `otterbrix/CLAUDE.md`),
  and `db::S3Manager::download` loads through the same `add_file`. `dump_file(FileMetadata)`
  takes a **pre-parsed `OtterbrixStatementPtr`** (not a database/table), runs it through
  `otterstax::engine::execute_statement` on that same engine session, and writes the result chunk out in the requested format. Format
  translators live in `otterbrix/translators/{input,output}`.
- **`conn::s3::ConnectorManager`** — credential store + S3 object I/O (`list`/`download`/`upload`) backed by
  Arrow's `S3FileSystem`. Credentials are registered per alias at startup from the `s3:` section of the
  connection config file (via `add_credentials`); there is no remove path. Every handler reports a typed
  `core::error_t`: `invalid_parameter` (empty alias / keys), `do_not_exists` (unknown alias), `io_error`
  (any Arrow S3 or local-file failure). A `download` that fails mid-transfer removes the partial local
  file. Nothing here throws — Arrow reports through `Status`/`Result`. `db::S3Manager` (in
  `integration/s3`) bridges these two with the engine. Error paths: `tests/system/test_s3_errors.cpp`.
  The constructor calls `arrow::fs::EnsureS3Initialized()`; Arrow requires one matching `FinalizeS3()`
  before the process exits (otherwise it warns and may segfault during static teardown), and
  finalization is terminal — every S3 call fails afterwards — so it is not the manager's to do.
  `conn::s3::subsystem_finalizer_t` (`s3_subsystem.hpp`) is the process-level RAII guard: the server's
  `main.cpp` declares one before every module, `tests/system/main.cpp` runs one when the test run
  ends.

## ConnectorManager Pattern (same for all three backends)

Each backend has a `ConnectorManager` with identical shape:

- Owns a `thread_pool_manager` (Boost.Asio `io_context` + its threads): the io pool every statement of
  the manager runs on. Its size is the constructor's `io_threads` (no default): `remote_backends_t`
  passes `connector_io_threads(service.runtime.connector_io.threads, connections)` — a configured
  `threads: N` for each manager, or (`0`, the default) one thread per connection config.yaml opens to
  that backend kind (the sum of the entries' `pool_size`), at least one. A statement holds an io thread
  while it runs — libpq's `PQexec` and clickhouse-cpp's `Execute` block it for the whole statement,
  Boost.MySQL completes its async operations on it — and a connection runs one statement at a time, so
  fewer threads than connections would queue statements that already hold a connection, and more could
  never all be busy. The pools stay per manager rather than one shared `io_context`: a blocking
  PostgreSQL / ClickHouse statement must not hold a thread MySQL's completions need.
- Stores a `conn::connection_pool_t<IConnector>` per uuid
  (`std::unordered_map<std::string, unique_ptr<connection_pool>>`) — see "Connection pools" below; the
  three managers pool the same way (a `clickhouse::Client` is one TCP session running one query at a
  time too, and a JOIN of two tables of one server opens two scans at once).
- `addConnection(params, uuid)` — `[[nodiscard]] core::result_wrapper_t<std::string>`; requires a running
  pool, parses the config `port` string with `otterstax::parse_port` (`utility/parse_port.hpp`, 1..65535 →
  `invalid_parameter`) and opens the connector. The connection's tables are discovered afterwards by the
  integration backend's `discover` (`remote_backends_t::open`), which calls `removeConnection(uuid)` for a
  connection whose discovery failed. These two are the only writes to the registry, and they happen on the
  startup thread before the first statement reaches that backend; from then on the registry (the map)
  is read-only, which is what lets `executeQuery` look it up without a lock — each pool synchronizes its
  own checkouts.
- No per-connection metadata lives here: the PostgreSQL ENUM oid map and the ClickHouse named-type
  overrides a query needs are state of the integration backend (`db::PostgressManager::enums_`,
  `db::ClickhouseManager::named_types_`), filled by its `discover` from the value the io-thread
  query hands back.
- `executeQuery(uuid, query, handler)` — returns `otterstax::query_future_t<R>` (`utility/wait_barrier.hpp`);
  never throws. Refuses before spawning with a ready future when the pool is not `RUNNING` (`io_error`),
  the uuid is unknown (`do_not_exists`), no connection of the uid frees up within the checkout timeout
  (`io_error`), the connector is `Closed` or its reconnect fails (`io_error`) —
  a failed reconnect fails that query only and leaves the connection registered. Otherwise the query is
  `spawn_marshaled` on the io_context and its outcome, including any exception thrown by the driver or
  the handler on the io thread, comes back as a value. The handler is any callable over the driver's
  result (`const boost::mysql::results&`, `PGresult*`, `const ch::select_result_t&`) returning
  `std::unique_ptr<data_chunk_t>`, `int64_t` or `otterstax::asio_error_t`. It is taken by value and
  moved into the coroutine frame of `otterstax::run_with_owned_handler` (`utility/wait_barrier.hpp`),
  so the handler object lives as long as the query; what it captures by reference stays the caller's
  to keep alive until the future settles (every caller — discovery, the statements pool's `scan` /
  `write` jobs — waits on the future with `.get()` inside the scope it captured).

`IConnector` reports every failure as a `core::error_t` value: `connect()`/`tryReconnect()` return it,
and each `runQuery` overload resolves to the marshaled outcome of its handler
(`core::result_wrapper_t<R>`, or bare `core::error_t` for `asio_error_t` handlers). The overloads are
`runQuery(std::string_view query, otterstax::function_ref_t<R(<driver result>)> handler)`, one per `R`:
the handler arrives as a non-owning reference (`utility/function_ref.hpp`) — the callable must outlive
the returned awaitable, which `run_with_owned_handler` guarantees for `executeQuery`; a test that awaits
`runQuery` directly keeps a named callable in scope until its future is settled. A statement the
backend rejects keeps the backend's verdict — `mysql/errors.hpp` (boost.mysql `error_code` +
`diagnostics::server_message`), `postgresql/errors.hpp` (SQLSTATE), `clickhouse/errors.hpp`
(`ServerException::GetCode`) map syntax, missing table/column/database, duplicates, ambiguity and
constraint failures onto the engine's `error_code_t`; transport failures are `io_error`.

The MySQL connector runs every statement over the **binary protocol** — `async_prepare_statement`, then
an `execute` + `close_statement` pipeline — never `COM_QUERY`: the text protocol renders a FLOAT column
with FLT_DIG (six) significant digits, so rows read over it are not the values the backend holds
(the stored 64647.5390625 arrives as "64647.5") while `SUM()` over the same column is exact. Generated
statements inline every value, so the statement binds with zero parameters; the close stage runs even
when the execute stage fails, so no server-side statement handle outlives a query.
`tests/test_{mysql,pg}_client_mysql_backend.py::test_float_column_reads_back_the_stored_value` pin it.

The MySQL connector's `any_connection` is built on `asio::make_strand(io_ctx)`, not on the pool's bare
`io_context`. `connect()`/`tryReconnect()` run one `async_connect` under `asio::cancel_after(10 s)`
inside a `use_awaitable` coroutine (`asyncConnect_`) that `connectWithTimeout` spawns on that strand
through `otterstax::spawn_marshaled` and blocks on with `.get()`, so the verdict — success, the driver's
error, `operation_aborted` on timeout, `io_error` if the pool went away — is always a `core::error_t`
value. The strand is what makes `cancel_after` safe on a multi-threaded io_context: boost 1.88's
`timed_cancel_op` completes through two handlers (the operation's and the deadline timer's) sharing a
ref_count, and with `use_future` on the bare io_context the timer's handler could run on another pool
thread between the operation's `timer_.cancel()` and `release()`, which destroyed the final handler
without invoking it — broken promise, `std::terminate` at server start on roughly every fifth run.
A statement runs on that strand too: `runQuery_` (spawned on the io_context by `executeQuery`) awaits a
coroutine it `co_spawn`s on the connection's executor, so the cancel signal `cancel()` posts there is
serialized with the statement's operations (see "Shutdown" below).
`close()` uses the non-throwing `close(ec, diag)` — it also runs from the destructor, and the `COM_QUIT`
it writes fails with `broken_pipe` when the server has already reset the socket; the failure is logged
(`warn`) and the connector is `Closed` either way. `tests/unit/utility/test_mysql_connector.cpp` pins
both against a fake MySQL server on 127.0.0.1.

The ClickHouse connector opens the driver (clickhouse-cpp 2.6.1) with one set of `ClientOptions`, built
by a file-local helper in `clickhouse/connector.cpp` that `connect()` and `tryReconnect()` share:
connect, recv and send timeouts of **10 s** (the MySQL connector's per-attempt budget), `send_retries = 1`,
no `ping_before_query`. On its own the driver bounds only the TCP connect (5 s) and leaves
`SO_RCVTIMEO`/`SO_SNDTIMEO` at 0, and every read on the wire is a blocking `recv()` — the ServerHello the
`Client` constructor waits for in its handshake, the Pong `Ping()` waits for, every packet of a query — so
a backend that accepted the connection and never answered froze the calling thread for good: `connect()`
at startup (the startup thread inside `remote_backends_t::open`, so the wire-protocol ports never came up)
and `isConnected()` in front of a query. With the bound the driver throws `std::system_error` (EAGAIN) at
expiry, the same path as a refused connect, and the connector answers `io_error` by value: `connect()`
against a silent backend is one 10 s attempt plus `tryReconnect()`'s three (≈ 40 s, the MySQL shape),
`isConnected()` is one Ping (10 s). `send_retries` is the number of back-to-back connect attempts the
`Client` constructor makes, with no delay between them — one, so the connector's ladder is the only retry
ladder and a silent backend costs 10 s per rung, not a multiple. The recv bound also applies to query
reads; a running query never idles that long because the server sends a Progress packet every
`interactive_delay` (100 ms by default). The driver is opened **without** `ping_before_query`: with it,
`Client::Execute` pings first and on a socket failure enters the driver's `RetryGuard` — sleep
`retry_timeout`, reconnect, ping again — which it leaves only when the ping succeeds or the reconnect
fails on exactly its `send_retries`-th turn (`client.cpp:1117-1135` in 2.6.1), so a backend that
completes every handshake but never answers a Ping kept the connector pool's io thread reconnecting for
good (a new connection every 11 s, the query never returning). The liveness check belongs to the manager:
`ConnectorManager::executeQuery` runs `isConnected()` (one direct Ping, bounded) and `tryReconnect()` on
the connection it checked out, in front of every query, and every ClickHouse query — the data path
(`scan` / `write`), the discovery probe, the table listing and the `system.columns` read in
`integration/clickhouse/connection_manager.cpp` — enters through `executeQuery`, so the driver's own
ping was a second copy of that check. Without it `RetryGuard` (the only user of `retry_timeout`, which is
therefore not set) is never entered, and a query is `SendQuery` plus reads, each bounded: against a
backend that stopped answering it is one recv timeout and `io_error`.
`tests/unit/utility/test_ch_connector.cpp` pins all of this against a fake ClickHouse server on 127.0.0.1
that speaks the native handshake and Ping/Pong.

ClickHouse handlers take a `ch::select_result_t` — the blocks plus `written_rows`, the sum of every
Progress packet's delta (`ch::make_statement` wires both). No ClickHouse block carries an affected-row
count; `written_rows` is an INSERT's count (materialized views add their rows to it, `async_insert`
reports none), while a lightweight DELETE and `ALTER TABLE ... UPDATE` are mutations that report
nothing — `db::ClickhouseManager::write` answers the number for an INSERT only.

The `connector_factory` constructor parameter is a plain function pointer
`(std::pmr::memory_resource*, [io_context&,] connect_params, std::string alias)`; the manager passes its
own resource, which owns every error message the connector produces. Tests inject mocks through it
(see `tests/mock/`).

## Shutdown: aborting the statements in flight

A statement blocks for as long as its backend takes, and nothing bounds that on its own: libpq's `PQexec`
and clickhouse-cpp's `Execute` block an io thread, a Boost.MySQL operation waits for its answer, and the
integration backend's destructor waits for every statement it posted (`integration/statement_executor.hpp`).
So each manager has `shutdown()` (idempotent, any thread): a flag makes every later `executeQuery`
answer `io_error` "the connector manager is shutting down" at once, every pool is closed (a statement
waiting for a free connection wakes up refused, idle connections are closed), and every connection
checked out right now gets `IConnector::cancel()` (`connection_pool_t::for_each_in_use`, under the pool
lock — `cancel()` never blocks). The integration backend calls it first thing in its destructor.
`cancel()` (default: nothing, for test stubs) aborts the statement from another thread and leaves the
connector refusing every later statement ("was cancelled"):

- **PostgreSQL** — `shutdown(PQsocket(conn), SHUT_RDWR)` under the connector's mutex (which also guards
  every replacement of the `PGconn`, so the descriptor is never one being freed): the blocking `PQexec`
  wakes up to the EOF and answers its "server closed the connection" error whatever the server's state.
  Not `PQcancel`: on libpq 15 it opens a new connection to the server with no timeout of its own, which
  against an unreachable host blocks the shutdown for the TCP connect timeout. The server ends the
  statement when it next writes to the client (or at `client_connection_check_interval`). The connection
  string also asks for TCP keepalive (`keepalives=1 keepalives_idle=30 keepalives_interval=10
  keepalives_count=3`, `connect_params`): a backend host that vanished without a FIN / RST fails the
  running `PQexec` after about 60 s of silence instead of the kernel's two hours, while a live server
  running a long statement answers the probes and is not cut.
- **MySQL** — per-operation cancellation: every operation of a statement binds the slot of the
  connector's `asio::cancellation_signal`, the statement runs on the connection's strand, and `cancel()`
  posts a terminal emission there (the signal is only ever touched on the strand; a flag checked before
  each operation catches a cancel that lands between two of them). The operation completes with
  `operation_aborted` → `io_error`, on the client side, so it works against a server that is gone; a
  Boost.MySQL connection is unusable after a terminal cancellation, which is why the connector refuses
  what follows. Not `KILL QUERY`: it needs a second connection to the server and the session's
  connection id, which Boost.MySQL does not expose.
- **ClickHouse** — the driver is built over a socket factory of the connector's own
  (`NonSecureSocketFactory::doConnect` overridden to build a `clickhouse::Socket` subclass with the same
  connect / recv / send timeouts), which records the live socket's descriptor in a slot shared with the
  connector and clears it before the descriptor is closed; `cancel()` shuts it down, and the blocked
  `recv()` throws, which the statement answers as `io_error`. The driver has no cancel of its own that
  reaches a statement waiting for its first block (`OnDataCancelable` is asked only when a data block
  arrives); its 10 s recv timeout already bounds a backend that went silent.

`tests/unit/connectors/test_pooled_managers.cpp` (shutdown aborting a running statement and refusing
the next ones; the integration backend's destruction aborting a scan the backend never answers and the one
queued behind it), `tests/unit/connectors/test_connector_cancel.cpp` (the real `pg::Connector` and
`ch::Connector` against fake servers that complete the startup and never answer a query — each statement
ended by `cancel()` well inside any timeout, the next one refused; the keepalive parameters),
`tests/unit/utility/test_mysql_connector.cpp` (the real `mysql::Connector` against a fake server that
swallows every command). `tests/unit/utility/test_ch_connector.cpp` keeps exercising the ClickHouse
connect and timeouts, now through the tracked socket factory.

## Connection pools

**Pool per uid (all three backends).** `addConnection(params, uuid, pool_size = 1)` opens `pool_size`
connectors through the factory (all of them, before the schema is discovered; one that cannot be opened
fails the whole call and nothing is kept; `pool_size == 0` is `invalid_parameter`). `ConnectionParams` /
`PgConnectionParams` / `ChConnectionParams` carry `pool_size` (default 1), which `remote_backends_t::open`
fills from the config's `pool_size` (`connections.{mysql,postgresql,clickhouse}[*]`, validated >= 1).
`conn::connection_pool_t<C>` (`common/connection_pool.hpp`) is generic over the connection type and never
talks to the backend: `checkout_for(timeout)` (a zero timeout never blocks) hands out a move-only
`handle_t` (exclusive ownership; returned on destruction or `release()`, from any thread), `close()`
destroys idle connections at once and checked-out ones when they come back. Its state is shared with the
handles, so a handle returned after the pool is gone is safe.

**Statements.** `executeQuery(uuid, ...)` checks a connection out for the statement — waiting up to the
checkout timeout (`setCheckoutTimeout`, default `conn::default_checkout_timeout` = 60 s; then `io_error`
"no free connection of '<uid>' within N ms (pool_size K)") — runs the health check on the connection it
now owns (a `Closed` one is refused with `io_error`, one whose ping fails gets `tryReconnect()`), and
spawns `conn::run_on_checked_out` (`common/pooled_query.hpp`), whose by-value handle returns the
connection on the io thread when the statement completes. So statements on one uid never overlap on one
connection: with every connection busy the next one waits. Discovery, remote DDL and the statements
pool's `scan` / `write` jobs all take this path. The checkout blocks the caller (as `.get()` on the
result does); never call `executeQuery` from a connector io thread.

**No transaction affinity.** Every remote statement — inside an explicit `BEGIN` block or not — checks a
connection out for that one statement and runs in the backend's autocommit. A `BEGIN` over remote tables
covers the engine tables only (root `CLAUDE.md`, Known Constraints): a remote write inside the block is
committed by the backend at once and a ROLLBACK does not undo it, and remote reads inside a block are not
one snapshot of the backend. Pinning a connection to a session's transaction needs storage transaction
callbacks otterbrix does not offer yet (the engine telling the federation's remote storage that a
transaction began, commits or rolls back); it will be built on them. Tests: `tests/unit/connectors/`
(`test_unit_connectors`: the pool, the pooled MySQL / PostgreSQL / ClickHouse managers).

## Connection Params (`api_connections/`)

`ConnectionParams`, `PgConnectionParams`, `ChConnectionParams` (namespace `conn::api_server`) are the plain param structs each manager's `addConnection` overload accepts (each carries `pool_size`, default 1). `remote_backends_t::open` fills them from the parsed `config::ConnectionsConfig` at startup — there is no HTTP server anymore.

## Adding a New Backend

1. Implement `IConnector` (follow `mysql/connector.hpp` as the reference)
2. Write a `ConnectorManager` mirroring the MySQL/PG shape
3. Add a `*_connection_config.hpp` param struct (in `api_connections/`)
4. Add a descriptor + parsing section in `config/connections/` and open it in `remote_backends_t::open`
5. Add an `integration/` actor implementing `discover`, `execute_ddl` and `federation::backend_t` (`scan`, `write`), a `backend_type_t` value (`otterbrix/backend_type.hpp`) and its dialect in `sql_gen`, and wire it into `remote_backends_t` / `session_services_t`
