# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`connectors/` provides raw connectivity. The SQL backends (`mysql/`, `postgresql/`, `clickhouse/`) are
one connector class each — **one physical connection** — with no pool and no threads of their own: a
federation backend agent (`otterbrix/federation/backend_agent.hpp`, see `otterbrix/federation/CLAUDE.md`)
owns one connector and the `io_context` it runs on. `file/` and `s3/` are `actor_zeta` actors
(synchronous `enqueue_impl`), called from the query session and from `db::S3Manager` (`integration/s3`).

Connections come from `config.yaml` once at startup (root `CLAUDE.md`, "Connection Config"):
`backends::remote_backends_t::open` builds `pool_size` agents per remote connection, each through the
connector factory of its kind (`backends::connector_factories_t`: `mysql::make_mysql_connector`,
`pg::make_pg_connector`, `ch::make_ch_connector`; tests pass mocks), and
`backends::register_s3_aliases` (`backends/backends.hpp`) calls the s3 actor's `add_credentials`.

## Structure

```
connectors/
├── mysql/       — Boost.MySQL connector (connector.hpp/.cpp), errors.hpp
├── postgresql/  — libpq connector, errors.hpp, types.hpp
├── clickhouse/  — clickhouse-cpp connector (select_result_t, make_statement), errors.hpp, types.hpp
├── file/        — actor conn::file::FileManager: load_file (file / stream → chunks) / dump_file (result → file / stream)
└── s3/          — actor conn::s3::ConnectorManager: S3 object streams via Arrow S3FileSystem; s3_subsystem.hpp
```

## The SQL connectors

Each kind has an `IConnector` interface and a `Connector` (`connect`, `tryReconnect`, `isConnected`,
`close`, `cancel`, `runQuery`). Every failure is a `core::error_t` value: `connect()` / `tryReconnect()`
return it, each `runQuery` overload resolves to the marshaled outcome of its handler
(`core::result_wrapper_t<R>`, or bare `core::error_t` for `asio_error_t` handlers; nothing throws out).
`runQuery(std::string_view query, otterstax::function_ref_t<R(<driver result>)> handler)` takes the
handler as a non-owning reference (`utility/function_ref.hpp`): the callable must outlive the returned
awaitable, which `otterstax::run_with_owned_handler` (`utility/wait_barrier.hpp`) guarantees for the
agent; a test that awaits `runQuery` directly keeps a named callable in scope until its future settled.
The driver results are `const boost::mysql::results&`, `PGresult*`, `const ch::select_result_t&`.

A statement the backend rejects keeps the backend's verdict: `mysql/errors.hpp` (boost.mysql
`error_code` + `diagnostics::server_message`), `postgresql/errors.hpp` (SQLSTATE), `clickhouse/errors.hpp`
(`ServerException::GetCode`) map syntax, missing table / column / database, duplicates, ambiguity,
constraint failures and deadlocks onto the engine's `error_code_t` — the PostgreSQL code of the same
verdict (`undefined_column`, `unique_violation`, `write_conflict`, …; a server code without one is
`internal_error`); transport failures are `io_error`. `tests/unit/utility/test_connector_errors.cpp`.

The factory is a plain function pointer `(std::pmr::memory_resource*, [io_context&,] connect_params,
std::string alias)`; the resource owns every error message the connector produces.

**MySQL.** Every statement runs over the **binary protocol** — `async_prepare_statement`, then an
`execute` + `close_statement` pipeline — never `COM_QUERY`: the text protocol renders a FLOAT column with
FLT_DIG (six) significant digits (the stored 64647.5390625 arrives as "64647.5"). Generated statements
inline every value, so the statement binds no parameter; the close stage runs even when the execute
fails. `tests/test_{mysql,pg}_client_mysql_backend.py::test_float_column_reads_back_the_stored_value`.
The `any_connection` is built on a strand of the agent's `io_context`: `asio::cancel_after` (boost 1.88
`timed_cancel_op`) completes through two handlers sharing a ref_count, and only a strand as their
associated executor keeps them from destroying the final handler uninvoked (broken promise at
connect). `connectWithTimeout` spawns one `async_connect` under `cancel_after(10 s)` on the strand and
runs the `io_context` on the calling thread (`run_one_for`) until it completed; `tryReconnect` makes
three attempts 200 ms apart. A statement runs on the strand too (`runQuery_` → `runOnStrand_`), pinging
first. `close()` uses the non-throwing `close(ec, diag)` (also from the destructor; a reset socket's
`COM_QUIT` fails with `broken_pipe`, logged as `warn`). `tests/unit/utility/test_mysql_connector.cpp`
pins both against a fake server.

**ClickHouse.** One set of `ClientOptions` for `connect()` and `tryReconnect()`
(`clickhouse/connector.cpp`): connect, recv and send timeouts of **10 s**, `send_retries = 1`, no
`ping_before_query`. The driver alone bounds only the TCP connect (5 s); every wire read is a blocking
`recv()`, so without the bound a backend that accepted the connection and never answered froze the
thread for good. At expiry the driver throws `std::system_error` and the connector answers `io_error`:
`connect()` against a silent backend is one 10 s attempt plus `tryReconnect()`'s three. `send_retries = 1`
leaves the connector's ladder the only retry. A running query never idles 10 s (the server sends a
Progress packet every `interactive_delay`, 100 ms). Without `ping_before_query` the driver's
`RetryGuard` (which against a server that never answers a Ping reconnects for good) is never entered;
the liveness check is the agent's (`isConnected()`, one bounded Ping, then `tryReconnect()`).
`tests/unit/utility/test_ch_connector.cpp` (fake server speaking the native handshake and Ping/Pong).
Handlers take a `ch::select_result_t` — the blocks plus `written_rows`, the sum of the Progress deltas
(`ch::make_statement`). `written_rows` is an INSERT's count (materialized views add theirs,
`async_insert` reports none); a lightweight DELETE and `ALTER TABLE … UPDATE` are mutations that report
nothing, so the agent's `write` answers 0 for them.

**PostgreSQL.** `PQexec` blocks the calling thread for the whole statement. The connection string asks
for TCP keepalive (`keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3`,
`connect_params`): a host that vanished without FIN / RST fails the running statement after ~60 s of
silence; a live server running a long statement answers the probes. The connect is bounded (10 s); a
statement is not.

## cancel(): aborting the statement in flight

The shutdown cancels every agent (`backend_actor_t::cancel_agents`) before it stops the backends pool.
`cancel()` (any thread, never blocks; the default does nothing — test stubs) aborts the running
statement and leaves the connector refusing every later statement and connect ("was cancelled"):

- **PostgreSQL** — `shutdown(PQsocket(conn), SHUT_RDWR)` under the connector's mutex (which also guards
  every replacement of the `PGconn`): the blocking `PQexec` wakes to the EOF. Not `PQcancel`: on libpq
  15 it opens a new connection with no timeout of its own. The server ends the statement when it next
  writes to the client (or at `client_connection_check_interval`).
- **MySQL** — every operation (a statement's and the connect's: `cancel_after` forwards the slot) binds
  the connector's `asio::cancellation_signal`; `cancel()` sets a flag checked before each operation and
  posts a terminal emission to the strand. The operation completes with `operation_aborted` → `io_error`
  on the client side; a Boost.MySQL connection is unusable after a terminal cancellation. Not
  `KILL QUERY`: it needs a second connection and the connection id, which Boost.MySQL does not expose.
- **ClickHouse** — the driver is built over a socket factory of the connector's own
  (`NonSecureSocketFactory::doConnect` overridden) that records the live descriptor in a slot shared
  with the connector and clears it before the descriptor is closed; `cancel()` shuts it down and the
  blocked `recv()` throws. The driver's `OnDataCancelable` is asked only when a data block arrives.

Tests: `tests/unit/connectors/test_connector_cancel.cpp` (the real connectors against fake servers that
never answer: each statement — and the MySQL connect with its retries — ended by `cancel()`, the next
refused; the keepalive parameters), `tests/unit/utility/test_mysql_connector.cpp`.

## file/ and s3/ actor connectors

These are `actor_zeta::actor_mixin` actors whose handlers run synchronously:

- **`conn::file::FileManager`** — moves bytes only, no engine work (`FileManager(resource)`):
  `load_file(FileLoadParams)` translates a CSV/NDJSON/Parquet file — or `FileLoadParams::input`, the
  stream of an S3 object (then `path` only names the source) — into a run of chunks of at most
  `DEFAULT_VECTOR_CAPACITY` rows with named columns (the format from `format` or the path's
  extension; an empty file is one chunk of its columns and no row), which the query session loads
  on the connection's engine session (`otterstax::engine::load_table`, see `otterbrix/CLAUDE.md`);
  `db::S3Manager::download` reads through the same `load_file`. `dump_file(FileMetadata)` writes
  `rows` — the cursor of a COPY's inner query, which the query session ran on the connection's
  engine session — in the requested format: into `FileMetadata::output` when it is set (the stream's
  last holder: it publishes it, `close()`, only when the whole result was written, and a stream it
  lets go unclosed is aborted), otherwise to a file at `path`, committed only on success
  (`tsl::write_local_file`). Format translators live in `otterbrix/translators/{input,output}`.
- **`conn::s3::ConnectorManager`** — credential store + S3 object streams (`list` / `open_input` /
  `open_output`) backed by Arrow's `S3FileSystem`, one built per alias on first use and cached (an
  `add_credentials` update drops it). Nothing is staged on local disk. The streams travel as their one
  owner, `tsl::input_stream_t` / `tsl::output_stream_t` (`otterbrix/translators/arrow_stream.hpp`:
  move-only, Arrow's `std::shared_ptr` handle only for the Arrow calls). `open_output` streams are
  opened with `allow_delayed_open`, so nothing reaches the bucket before the first write; Arrow's S3
  stream completes the upload from its destructor, so `output_stream_t` aborts a stream its last holder
  lets go without `close()`. The filesystem's buffers come from the manager's resource through
  `tsl::arrow_pool`, which must therefore be thread-safe (Arrow uploads parts from its IO threads).
  Credentials are registered per alias at startup from the `s3:` section of `config.yaml`
  (`add_credentials`); there is no remove path. Every handler reports a typed `core::error_t`:
  `invalid_parameter` (empty alias / keys), `undefined_object` (unknown alias), `io_error` (any Arrow S3
  failure). Nothing here throws — Arrow reports through `Status`/`Result`. Error paths:
  `tests/system/test_s3_errors.cpp`.
  The constructor calls `arrow::fs::EnsureS3Initialized()`; Arrow requires one matching `FinalizeS3()`
  before the process exits (otherwise it warns and may segfault during static teardown), and
  finalization is terminal — every S3 call fails afterwards — so it is not the manager's to do.
  `conn::s3::subsystem_finalizer_t` (`s3_subsystem.hpp`) is the process-level RAII guard: the server's
  `main.cpp` declares one before every module, `tests/system/main.cpp` runs one when the test run
  ends.

## Adding a New Backend

1. Implement `IConnector` + a `make_*_connector` factory (follow `mysql/connector.hpp`) and an
   `errors.hpp` mapping the server's codes.
2. Add a descriptor + parsing section in `config/connections/` and a factory in
   `backends::connector_factories_t`; build its agents in `remote_backends_t::open`.
3. Teach `federation::backend_agent_t` the connector (read / write / transaction steps / discover), add
   a `backend_type_t` value (`otterbrix/backend_type.hpp`), its input translator
   (`otterbrix/translators/input/`) and its dialect in `sql_gen`.
