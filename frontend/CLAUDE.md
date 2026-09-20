# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`frontend/` implements the three wire-protocol servers. Each server accepts client connections, receives SQL, dispatches to the `Scheduler` actor, and streams results back.

## Structure

```
frontend/
├── common/               — shared CRTP base server + connection base
├── flight_sql/           — the in-house Flight SQL server (port 8815)
├── mysql_server/         — MySQL wire protocol (port 8816)
└── postgres_server/      — PostgreSQL wire protocol (port 8817)
```

## Common Layer (`frontend/common/`)

`frontend_server<DerivedConnection>` is a CRTP template that manages a connection pool (max 1000 slots), a `thread_pool_manager` (Boost.Asio), and async accept/reject logic. `DerivedConnection` must implement:
- `socket()` — returns the underlying TCP socket
- `start()` — begins protocol handshake
- `finish()` — signals clean shutdown
- `static build_too_many_connections_error()` — protocol-specific rejection packet

`frontend_connection` provides the base connection class with shared utilities (`packet_reader_base`, `packet_writer_base`, `resultset_utils`).

A connection announces its own close through the `connection_close_sink`
interface (`release_connection_slot(slot)`), which `frontend_server` implements
to free the pool slot — a plain virtual call, no type-erased callback.

**Connection lifetime (`frontend_connection`).** Each connection's socket runs
on its own asio strand (`make_strand(ctx)`), so every completion and every
touch of the connection's members after construction happens on that strand,
one at a time (`start()` posts `start_impl` there). Every asio operation the
connection starts — read, write, the idle timer, a posted lambda — goes
through `safe_callback`, which counts it in `pending_ops_` and decrements once
the handler has run. `finish()` is callable from any thread and idempotent
(`finish_requested_` gate); it posts the close onto the strand, where the
socket is closed and the timer cancelled (`closed_`), so every outstanding
operation completes with `operation_aborted` (handlers that see `closed()`
return quietly). The connection is destroyed exactly once, by the completion
that brings `pending_ops_` to zero after the close has run: it calls
`finish_impl()` and then `release_connection_slot(slot)`. Nothing captures
`this` in a handler that can outlive the object, and no `shared_ptr` is
involved. Buffers handed to `async_write` are members (`send_buffer_`, the
server's `reject_packet_`), never locals. The idle timer is a member
(`read_timer_`), armed by `arm_read_timeout(stage)` before each read and
cancelled by the read's completion; a generation counter ignores a timer
completion that belongs to an earlier arm. Its duration is
`frontend_server_config::read_timeout` (no default; `main.cpp` passes
`CONNECTION_TIMEOUT_SEC`). The PG `StartupMessage` body is read through
`ensure_read_buffer` (bounded by `MAX_BUFFER_SIZE`), never past the 4 KiB
initial buffer. `tests/system/test_frontend_connection_lifetime.cpp` pins all
of this over raw client sockets: `stop()` with attached clients, the idle
timeout, a client dropping the socket mid-read, an oversize startup message.

**Shutdown contract (`frontend_server::stop()`).** `stop()` sets `stopped_`,
closes the acceptor, then finishes every pooled connection under `pool_mutex_`
and joins the pool. `finish()` closes the socket on the connection's strand and
the completion of the last pending operation releases the slot — the only path
that destroys a pooled connection; the accept handler never releases a slot
directly (on an accept error it calls `finish()` on the pooled connection).
Once `stopped_` is set the accept handler leaves the pool alone and
`accept_connections()` does not re-arm: an accept re-armed on the closed
acceptor completes at once with an error, and re-arming from there spins the
pool threads forever (join never returns) while racing `stop()`'s finish loop
on the same slots (use-after-free: SIGSEGV or `bad_executor`). The slot write
and the `async_accept` that takes the connection's socket run under the same
lock. When building the connection for the next accept throws, the catch in
`accept_connections()` pauses the chain on the member `accept_retry_timer_`
for `frontend_server_config::accept_retry_delay` (no default; `main.cpp` passes
`ACCEPT_RETRY_DELAY_MS`, 100 ms) — there is one accept chain, re-armed only
from its own completions, so one timer serves it and nothing is heap-shared.
The same catch, under `pool_mutex_`, hands back the slot the iteration took
when its connection was never built (the slot is still empty): left taken,
`MAX_CONNECTIONS` failures would fill the pool and every later client would get
the too-many-connections refusal. A connection that was built before the throw
(`async_accept` itself threw) stays pooled until `stop()` finishes it. The pause is armed under `pool_mutex_` only while `stopped_` is
clear, and `stop()` cancels the timer under the same lock before joining the
pool: a pending retry completes with `operation_aborted` and does not re-arm,
and the join does not wait out the pause. `local_port()` answers the bound port
(tests bind port 0). `tests/system/test_frontend_shutdown.cpp` pins this with
start/stop cycles on both servers; `tests/system/test_frontend_accept_retry.cpp`
pins the retry (a connection whose constructor throws on chosen attempts: accept
resumes after the pause, `stop()` with the retry pending returns, a client is
served after more failures than the pool has slots).

`asio_future_bridge.hpp` is the universal sink between the Scheduler/Worker pool
(which hands back `actor_zeta::unique_future`s) and the per-connection asio
executor: `async_await_future` polls `take_ready()` from inside a coroutine and
co_returns the `core::result_wrapper_t<session_payload>` when ready, never
blocking the executor thread; `await_future_blocking` drives it on a private
`io_context` for a handler that has no coroutine of its own. Both take the
memory resource every message they create lives on. The bridge reports its own
two failures by code, never by message text: `AWAIT_TIMEOUT_CODE` (deadline
reached — the frontends map it to their protocol's "query cancelled" error)
and `AWAIT_FAILED_CODE` (the actor runtime closed the channel). Neither code is
ever produced by a Worker result, which is what lets a frontend branch on them.

## FlightSQL (`flight_sql/`)

The in-house Flight SQL server — **no Arrow Flight dependency**: gRPC handlers
written as asio-grpc coroutines (`rpc/flight_server.cpp`) over the vendored
`Flight.proto` / `FlightSql.proto` (`format/`), with a vendored Arrow IPC
implementation on flatbuffers (`ipc/`: writer + reader, dissociated mode —
schema first in `data_header`, batches after). The protocol core
(`core/`) owns the ticket registry, the prepared statements and the engine
itself — there is no engine interface: the server has exactly one engine, and
`FlightSqlCore` owns it (`scheduler_engine.*`, over the Scheduler/Worker
pool; every Scheduler call awaited through the common `asio_future_bridge`,
the same bridge the wire frontends use). The exchange shapes live in
`core/core.hpp` next to their only consumers; the metadata carries the
PROJECT's `table_info` (utility/table_info.hpp), not a protocol twin.

Layers: `flight_sql_proto` (generated protobuf + gRPC stubs), `flight_sql_flatbuf`
(flatc-generated Arrow format headers), `flight_sql_ipc` (IPC model + writer/
reader), `flight_sql_core` (commands, auth, ticket manager) and `flight_sql`
(the server: rpc handlers + `SchedulerEngine` + `chunk_to_ipc`).

- **GetFlightInfo / GetSchema materialize the result**: the engine
  runs the query, the result is cached under an opaque ticket
  (`otterstax-tickets/<n>-<hash>`), DoGet streams the cached batches. There is
  no two-phase prepare/ticket handshake. The ticket cache is bounded (1024, FIFO
  eviction): a ticket lives from GetFlightInfo to DoGet (seconds), but the
  server is long-lived.
- **`chunk_to_ipc`** converts `session_payload` (schema + chunks) to the IPC
  model: the schema is the contract (fields matched to chunk columns by name,
  n-th occurrence to n-th occurrence — a JOIN keeps both key columns), nested
  STRUCT/LIST columns and unnamed chunk columns are refused (`EngineError` →
  `INVALID_ARGUMENT`), DECIMAL/HUGEINT ride decimal128 (the scale in the field
  type, the unscaled int128 in 16-byte little-endian slots; HUGEINT at
  precision 38 scale 0; UHUGEINT refused — decimal128 is signed). Bool columns
  are bitmaps (one bit per value), not byte-per-bool.
- **Prepared statements** are the full cycle: `CreatePreparedStatement`
  (prepare through `Scheduler::prepare_schema`; the session hash rides the
  opaque `Prepared.engine_handle`), DoPut bind (an Arrow batch of parameters —
  read with the in-house IPC reader), execute. Worker statements are
  single-use, so every execution re-prepares under a fresh session id and
  closes the CreatePreparedStatement session; `ClosePreparedStatement` calls
  `SchedulerEngine::close_prepared` so a never-executed statement releases its Worker
  entry. The parameter schema handed out is int64 fields `$1..$N` (the model
  the reference drivers bind against); the adapter re-types whatever arrives
  the way a text protocol frontend types its literals (int64 → double → bool
  → text). executemany of an UPDATE runs one execution per bound row, the
  affected counts sum.
- **Metadata** (Catalogs / DbSchemas / Tables / TableTypes / SqlInfo) answers
  from one `CatalogManager::get_tables` pass (`get_tables_command_t` — the
  Flight SQL filters decoupled from any wire library).
- **Auth** is anonymous by default (`AuthService::anonymous`); the extension
  point for configured credentials is `AuthService::basic` (Handshake Basic →
  Bearer in initial metadata).
- `main.cpp` drives it: `flight::server::flight_sql_server` binds the port,
  runs the `GrpcContext` on N threads (one per Worker — `SchedulerEngine`
  blocks its calling thread for the length of a query), and a signal thread
  (SIGINT/SIGTERM) stops the grpc server and the context; `run()` returns and
  the common shutdown sequence follows. `ListFlights` / `DoExchange` are
  `UNIMPLEMENTED`; IPC compression (lz4/zstd) is not supported — clients do
  not send it by default.

Acceptance: `tests/unit/flightsql/test_chunk_to_ipc.cpp` (the converter),
`tests/unit/flightsql/test_ipc.cpp` (the IPC bytes), `tests/system/
test_flightsql_schema_contract.cpp` (the GetFlightInfo/DoGet contract through
a real Scheduler stack), and `tests/flightsql_e2e/` — the original Apache
drivers (pyarrow, ADBC, the Go `database/sql` driver in `tests/flightsql_go/`)
against the live server, plus the 8 `tests/test_*flightsql*.py` suites.

## MySQL / PostgreSQL Servers

Both follow the same pattern: `frontend_server<XConnection>` accepts TCP connections; each connection implements the respective handshake + query/response protocol, then calls `Scheduler::execute` (and friends), which returns an `actor_zeta::unique_future<core::result_wrapper_t<session_payload>>`. The connection awaits that future via `frontend/common/asio_future_bridge.hpp::await_future_blocking` (polls `take_ready()` on a private asio executor — no blocking get, no shared state). The FlightSQL `GetTables` path goes the same way: `CatalogManager::get_tables` returns `unique_future<result_wrapper_t<std::pmr::vector<table_info>>>`.

**Prepared statements are single-use on the Worker side.** `Worker::execute_prepared_statement` drops the statement after one execution, successful or not, and answers a second one with `invalid_parameter` ("prepared statement must be re-prepared"). Both frontends therefore keep the accepted SQL text in their statement metadata (`prepared_stmt_meta::sql`, `consumed`) and, for every `COM_STMT_EXECUTE` / `Execute` after the first and after any failed one, run `Scheduler::prepare_schema` again under a fresh session id before executing.

**Closing a statement releases the Worker entry.** MySQL `COM_STMT_CLOSE` and PG `Close(statement)` — and a PG `Parse` that replaces a statement of the same name, the unnamed one included (`do_close`) — send `Scheduler::close_statement` for the statement's current session id when the statement is not `consumed` (a consumed one the Worker has already dropped), then forget the frontend entry. The close is awaited through the bridge like every other Scheduler call; a failure is logged (`warn`), never turned into a protocol error — MySQL sends nothing (protocol), PG answers `CloseComplete` as before. A MySQL `COM_STMT_PREPARE` refused by the frontend after the Worker stored the statement (an unencodable column, below) closes it the same way before the error packet goes out.

**Ending a connection releases every statement left unexecuted.** `frontend_connection::finish()` is the single teardown point of a connection (client `COM_QUIT` / `Terminate`, socket drop, protocol error, read timeout, FATAL error, `frontend_server::stop()`); its posted lambda closes the socket, and the completion that brings the pending-operation count to zero calls the derived `finish_impl()`, then releases the pool slot. `mysql_connection::finish_impl` / `postgres_connection::finish_impl` send `Scheduler::close_statement` for every statement in `statement_id_map_` / `statement_name_map_` that is not `consumed` (PG through `do_close`, portals included), awaited through the bridge, failures logged only. The connection is whole while `finish_impl` runs and the Scheduler outlives the frontend servers (`main.cpp` stops both servers before `ComponentManager` is destroyed; `thread_pool_manager::stop()` drains every pending completion before joining), so no message is sent to a dead actor and no member is touched after the slot release. The release is not observable on the wire; `tests/test_pg_extended_protocol.py` and `tests/test_mysql_wire_protocol.py` pin that the path completes (no crash, no wedged slot) for Terminate / `COM_QUIT` and a bare socket drop.

**A payload's column count is what tells a resultset from an affected count.** Both connections send an OK packet / a `CommandComplete` without `RowDescription` for a payload with no columns (`column_count() == 0`) and a resultset otherwise; they never consult the statement kind. The invariant that makes this sound — a row-producing statement (SELECT, set operations, DML with RETURNING) never reaches a frontend as a column-less payload — is enforced upstream in `OtterbrixManager::execute` (`schema_error`, see `integration/CLAUDE.md`), so a SELECT the engine could not type is an error on the wire, not a `SELECT 0` / OK. `tests/test_pg_extended_protocol.py` and `tests/test_mysql_wire_protocol.py` pin it with `SELECT big FROM <hugeint table>`. The MySQL OK packet (`build_ok`, `mysql_server/packet/packet_utils.cpp`) carries `affected_rows` and `last_insert_id` as length-encoded integers — a single byte holds a count only up to 250, and 0xFB..0xFF in that position are the NULL / int-size markers (`tests/mysql-front/test_reader_writer.cpp`, "build_ok: affected rows are a length-encoded integer"; `test_remote_dml_row_counts_span_chunks` in `tests/test_mysql_client_mysql_backend.py` drives a 1000-row count over the wire).

**A result column the wire cannot encode is a protocol error, never a dropped connection.** `frontend::{mysql,postgres}::get_field_type` (`frontend/common/utils.cpp`) answers `std::optional<field_type>` — nullopt for a logical type the protocol has no type for (UHUGEINT, DATE, TIMESTAMP, UNKNOWN, …). `find_unsupported_column<frontend_type>` (`resultset_utils.hpp`) names the first column that `is_encodable` rejects under the requested result formats (Bind convention: none = text, one for all, one per column): the type must be mapped AND the encoder of that format must have a case for it — the binary encoder carries scalars only, so ENUM/STRUCT/ARRAY/LIST pass in text and are refused in binary.

**DECIMAL and HUGEINT are carried, and their refusal is per format rather than per type.** Both map to MySQL `NEWDECIMAL` (0xF6) and PostgreSQL `NUMERIC` (oid 1700) — neither wire has a 128-bit integer type, and the fixed-point type each one does have carries its value as a string of digits, so every value of the range arrives intact where a 64-bit type would have to round or refuse. A HUGEINT is `decimal_to_text` at scale 0: the stored 128-bit integer, all of its digits and nothing after the point. It has no `(width, scale)` extension and **no sentinels** — the extremes of its range are ordinary values a `hugeint` column may hold, so unlike a DECIMAL none of them is reinterpreted as `NaN`/`±Infinity`; its magnitude is taken in unsigned arithmetic, so the most negative int128 (which has no positive counterpart) renders exactly instead of overflowing. On the MySQL wire the column definition reports `decimals` 0 and a length of 40 (39 digits plus the sign). `uhugeint` is mapped by neither wire and keeps taking the refusal path. For a DECIMAL, `decimal_to_text` (`resultset_utils.cpp`) renders a cell by putting the point back into the stored unscaled integer where the scale of the type says it belongs — DECIMAL(18, 4) holding 12345 is `1.2345`, never `12345`, which is the same corruption the Arrow path removed. The integer is read at the width its precision needs (INT16 / INT32 / INT64 / INT128, `decimal_storage_for_width`) and sign-extended; the engine's non-finite sentinels (the extremes of that integer's range) come out as PostgreSQL spells them for NUMERIC — `NaN`, `Infinity`, `-Infinity` — never as the finite number the sentinel would otherwise read as. MySQL takes that rendering in **both** formats, because a NEWDECIMAL field of a binary resultset row is a length-encoded string exactly as in a text row (this is how boost.mysql reads it back: `deserialize_binary_field_string`). PostgreSQL takes it in **text only**: binary NUMERIC is a sequence of base-10000 digit groups behind a weight/sign/dscale header, which PostgreSQL documents as backend-internal rather than as part of the client protocol ("Values passed in binary format require knowledge of the internal representation expected by the backend" — libpq, `PQexecParams`), and a PG `DataRow` states each field's length before its bytes, so a guessed encoding would desynchronise the row rather than merely mis-state a number. `is_encodable<POSTGRES>(DECIMAL, BINARY)` is therefore false and the column is refused with the usual `ErrorResponse 0A000` naming the column, its type and the format, while `is_encodable<MYSQL>` accepts it in both. On the MySQL wire the column definition also answers for itself: `apply_decimal_metadata` (`resultset/column_definition_41.cpp`) sets `decimals` to the scale and the length to the digits plus sign and point, on `COM_QUERY`/`COM_STMT_EXECUTE` results and on the `COM_STMT_PREPARE` schema alike. On the PG wire the field's `type_modifier` stays -1 — what PostgreSQL itself sends for a numeric with no declared typmod — and the scale rides in the value's own trailing digits, as PG's text output does. Every connection runs it before building a resultset or a RowDescription: PG `handle_query` (text), `do_describe` (statement: text, portal: the Bind formats), `handle_execute` (the portal formats, on every Execute); MySQL `handle_query` (text), `handle_prepared_stmt` (the schema, binary — `COM_STMT_EXECUTE` is binary), `handle_execute_stmt` (the executed chunk, binary). The refusal is `ErrorResponse 0A000` (`FEATURE_NOT_SUPPORTED`) / `ERR 1235` (`ER_NOT_SUPPORTED_YET`, SQLSTATE 42000) with `unsupported_column_message` — `column 'big' has type HUGEINT (15), which the PostgreSQL wire cannot encode in text format` — and the connection stays usable. The encoders' `default:` branches are unreachable after that check and `assert` it; nothing in the frontends throws. `tests/mysql-front/test_wire_type_support.cpp` pins the mapping and the check.

**A NULL is a property of the cell, not of the column's type, and a binary resultset row carries it in the row's NULL bitmap alone.** `mysql_resultset::encode_row_binary` asks `chunk.data[i].is_null(row)` per cell — which also answers true for every row of a column whose type is NA, so that case needs no separate test. The bitmap is `(column_count + 7 + 2) / 8` bytes and the bit of column *i* sits at *i + 2*, because a ResultsetRow reserves the first two bit positions; this is exactly how boost.mysql — the client this server answers — reads it back (`binary_row_null_bitmap_offset = 2`, `null_bitmap_parser::byte_count`), and for a field whose bit is set it consumes **no bytes**. So a NULL cell sets its bit and writes nothing. Deciding the bitmap from the column's *type* instead (`type() == NA`) left the bit clear for every empty cell of a typed column and still wrote the untouched slot's bytes: the client could not tell `NULL` from `0`, and because those bytes stayed in the row every field after them was read at the wrong offset — a string's length prefix taken out of a stale integer. Text rows state a NULL per cell already (the `0xFB` marker). On the PostgreSQL side a `DataRow` writes the `-1` length and no bytes, and the length a **binary** field states in front of its bytes is computed where it is written (`postgres_resultset::add_row`) rather than from a list built in a parallel pass — such a list holds no entry for a NULL cell, so every binary field after one took the previous column's length and desynchronised the row. `estimate_{text,binary}_field_size` answer for an empty cell without reading its slot. `tests/mysql-front/test_resultset.cpp` pins the bitmap bytes (a NULL in the first and the last column, a row of all NULLs, and columns 6–7 crossing into the second bitmap byte) and that no value bytes follow a set bit. **A result column without a name** (`SELECT 1`) has no alias in the prepared schema, and the engine's `complex_logical_type::alias()` has no null guard for such a type (`has_alias()` has one): PG `do_describe` (`Describe` of a statement or a portal) and MySQL `handle_prepared_stmt` (`COM_STMT_PREPARE`) name it with the empty string, which is the name an executed chunk carries for it and what the simple `Query` / `COM_QUERY` send. A bare `alias()` there dereferenced null on the pool thread and killed the process. The executed result can carry such a column as well — `SELECT 1 UNION ALL SELECT 2` (a set operation's column has no alias at all, where an executed `SELECT 1` has an empty one), and on rc-2 a `SELECT` over a VIEW — so the resultset builders `mysql_resultset::add_chunk_columns` / `postgres_resultset::add_chunk_columns` (behind `COM_QUERY`, `COM_STMT_EXECUTE`, the simple `Query` and the `Execute` of a portal nobody described) name it with the empty string too. Every `alias()` in the frontends sits behind `has_alias()` (`chunk_to_ipc` refuses an unnamed chunk column instead). `tests/system/test_frontend_malformed_packet.cpp` pins all of them.

**PostgreSQL extended query specifics.** `Describe(statement)` answers `ParameterDescription` followed by `RowDescription` (text formats) or `NoData`. **A statement that answers rows is described, never `NoData`**: `NoData` states that the statement will not return rows, and the clients that live by the statement's description (Npgsql prepared, asyncpg, tokio-postgres, lib/pq, PgJDBC in its describe path) take it at its word. What the prepare knows goes out — for a parameterized statement that is the plan's own projection, the columns named and untyped (`Worker::prepare_schema`, `scheduler/CLAUDE.md`), so they are described as `text`. **`Describe(portal)` describes the portal by running it**: a portal is a BOUND statement, so a row-producing one — decided by the statement kind (`prepared_stmt_meta::tag`), never by the columns of a result nobody has yet — is executed and described from the chunk it actually answers, with the portal's own result format codes, which belong to the `Bind` that created the portal (`portal_meta::format`). The result stays in the portal, so the `Execute` behind it streams that result instead of running anything again: the description is exact and costs no extra round-trip. A DML portal is NOT run — describing one would write, and write twice for a client that binds it again — so it is described from the prepared schema: a `RETURNING` list is described by the columns it projects, resolved at prepare from a `LIMIT 0` probe of the target relation (`Worker::returning_schema`), and a statement that answers an affected count alone is `NoData`. The `Execute` is what writes, in both cases. **An `Execute` whose result is not the shape the client was given refuses instead of streaming it**: when the statement was described before Bind and the portal was not described since, the executed chunk is checked against that description column by column (`same_wire_shape`, `resultset/postgres_resultset.cpp`: the same count and the same type oid per column; names are not compared, since a `DataRow` is decoded by position and type and an executed column may legitimately carry a name the prepared schema had none for). A divergence is `ErrorResponse 0A000` carrying PostgreSQL's own wording, `cached plan must not change result type` (`plancache.c`, `RevalidateCachedQuery`). A parameterized statement's pre-Bind description carries no types, so this is what a client that decodes by it gets — a clear error — rather than one type's bytes read as another's. An `Execute` with a row limit sends that many rows and `PortalSuspended`; the portal keeps the result and the next `Execute` on it continues from where it stopped, `CommandComplete` counting the rows of that `Execute`. Text-format `Bind` parameters are converted by `parse_text_parameter` (`std::from_chars`, no exceptions); a bad literal ends the `Bind` with an `ErrorResponse` and no `BindComplete`. **Errors and `Sync`** follow the protocol's pipeline rule, kept in `pipeline_state` (`connection/pipeline_state.{hpp,cpp}`): every extended-query message (`Parse`, `Bind`, `Describe`, `Execute`, `Close`, `Flush`) begins or continues a pipeline; a non-fatal error inside it (`send_error_response`, whatever the handler) sends the `ErrorResponse` alone, `handle_packet` discards every following message until the `Sync` (`Terminate` still ends the connection), and that `Sync` sends the single `ReadyForQuery`. A simple `Query` is outside any pipeline — it ends one, since it answers `ReadyForQuery` itself — so its error keeps `ErrorResponse` followed by `ReadyForQuery` at once. **The `ReadyForQuery` status byte is the transaction block's, never the pipeline's**, as in PostgreSQL. `transaction_manager` keeps the block for a simple `Query` (`try_handle_transaction`). It covers BEGIN / COMMIT / ROLLBACK, which the engine runs itself and answers with a `T_TransactionStmt` payload. It also covers SAVEPOINT / ROLLBACK TO / RELEASE, which the engine refuses as "Unsupported node type" and the frontend emulates. Each gets its own `CommandComplete` tag. The extended route does not track them. The status is `I` outside a block — an error there fails nothing, so a failed pipeline's `Sync` answers `I` — and `T` inside one. It becomes `E` once a non-fatal error inside the block failed it: `send_error_response` calls `mark_failed`, which fails an open block only. `E` then stands on the `Sync` and the simple `Query` alike until `ROLLBACK` or `COMMIT` ends the block; `COMMIT` of a failed block answers `CommandComplete ROLLBACK` and `I`, as PostgreSQL does. Statements sent inside a failed block still run: nothing is refused with `25P02`, since the emulation has no atomicity to protect. `tests/mysql-front/test_pg_frontend_packets.cpp` pins `transaction_manager`, `tests/system/test_frontend_malformed_packet.cpp` and `tests/test_pg_extended_protocol.py` the wire. `tests/mysql-front/test_pg_frontend_packets.cpp` pins the state machine, `tests/test_pg_extended_protocol.py` the wire (`read_failed_pipeline` asserts exactly one `ReadyForQuery` and nothing after it).

Packet encoding/decoding lives in `{mysql,postgres}_server/packet/` and `{mysql,postgres}_server/{mysql,postgres}_defs/`. Result-set serialisation is in `{mysql,postgres}_server/resultset/`.

**A client packet shorter than its fields is a protocol error, never an exception.** `packet_reader_base` (`common/packet_reader_base.{hpp,cpp}`, the MySQL little-endian and PG big-endian readers derive from it) reports a read past the end of the packet through a sticky fault state — `ok()` / `fault()`, `packet_fault::underflow` for too few bytes (a NUL-terminated string without its terminator included), `packet_fault::invalid_marker` for a MySQL length-encoded integer starting with 0xFB/0xFF. A failed read moves the position by nothing, its returned value is not data, and every later read fails too, so a handler checks once after a group of reads and before acting on any value. The byte-merging helpers in `common/utils.hpp` index unchecked (`operator[]`) — their callers own the bounds (`check_bounds`, or a header read to its exact size). Every handler that decodes a packet checks the reader: MySQL `handle_auth`, `COM_STMT_EXECUTE` / `COM_STMT_CLOSE` in `handle_command`, `handle_execute_params` (after the types, after the values); PG `read_initial_message` (the protocol version), `handle_startup_message`, the `Query` / `Parse` / `Bind` / `Execute` / `Close` / `Describe` cases of `handle_packet`, `handle_parse`, `handle_bind`. On MySQL the answer is `send_malformed_packet_error` — `ERR 1835` (`ER_MALFORMED_PACKET`) then `finish()`. On PG it follows PostgreSQL: after startup a malformed message — a `remaining()` pre-check (`Truncated PARSE message ...`) or the reader's fault — is `ErrorResponse 08P01` (`PROTOCOL_VIOLATION`) with severity **ERROR**, and the connection stays usable; the error obeys the pipeline rule above (inside an extended-query pipeline the `ErrorResponse` goes alone, the messages up to the `Sync` are discarded and the `Sync` answers the one `ReadyForQuery` with the transaction status, `I` outside a block; a simple `Query` without its terminator gets `ErrorResponse` + `ReadyForQuery` at once). The next message is framed correctly because every PG message is length-prefixed and `read_packet_payload` reads exactly the declared length before `handle_packet` runs — a short body is a defect of that message alone.

**A body longer than its fields is the same `ERROR 08P01`**, message `invalid message format` (PostgreSQL's `pq_getmsgend`), and it follows the same pipeline rule. `Query`, `Parse` (`handle_parse`), `Bind` (`handle_bind`), `Execute`, `Describe` and `Close` check `remaining() == 0` after their last field, before acting on the message. `Sync` and `Flush` must have an empty body: `Sync` ends the pipeline first, so its error gets `ReadyForQuery` at once, while `Flush` is an extended-query message, so its error waits for the `Sync`. As in PostgreSQL, `Terminate` ends the connection whatever its body holds, and `CopyData` / `CopyDone` / `CopyFail` outside COPY are accepted and ignored.

**A message type the protocol does not define is `FATAL 08P01`** `invalid frontend message type <n>` (the byte in decimal), followed by the close. This covers `FunctionCall` (`F`), which the frontend does not implement, and a `PasswordMessage` after startup. `validate_payload_size` decides it from the header, before waiting for the body the length announces, since the message boundaries are presumed lost. The other FATAL errors are a malformed `StartupMessage` (no session yet: an unterminated parameter, no room for the protocol version) and the transport (`IO_ERROR`) and resource (`INSUFFICIENT_RESOURCES`) failures; each closes the connection. `tests/mysql-front/test_reader_writer.cpp` and `test_pg_frontend_packets.cpp` pin every primitive on a truncated buffer; `tests/system/test_frontend_malformed_packet.cpp` sends the truncated packets over raw sockets (`tests/system/raw_wire_client.hpp`) and asserts the error packet, the MySQL close / the PG `ReadyForQuery` followed by a served `SELECT 1` on the same connection, and that the server keeps serving; `tests/test_pg_extended_protocol.py` (`test_malformed_message_is_a_protocol_error_not_a_disconnect`) pins the PG contract on the live server.

## Port Assignments

| Server | Default port | CMake target |
|--------|-------------|--------------|
| FlightSQL | 8815 | `flight_sql` |
| MySQL | 8816 | `mysql_server` |
| PostgreSQL | 8817 | `postgres_server` |

Ports are hardcoded in `main.cpp` and in the `config.yaml` / `compose.yml` files.
