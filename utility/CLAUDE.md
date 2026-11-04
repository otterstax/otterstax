# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`utility/` is a header-only collection of shared primitives used across all layers. There are no `.cpp` files; each user includes what it needs (`lib_otterstax`, the root `CMakeLists.txt`'s INTERFACE target, lists a few of them).

## Key Types

### `session_payload` — lives in `session/session_payload.hpp`, not here

`session_payload` holds the output of a completed statement: `schema`
(`complex_logical_type`), `chunks` (`std::pmr::vector<data_chunk_t>` — the
engine caps each chunk at 1024 rows, so a result spans multiple chunks;
`size()`/`column_count()`/`empty()` accessors span the whole vector),
`parameter_count`, `tag`, `affected_rows`, the transaction command it was, a
prepare's `parameter_types`, a window's `portal` / `has_more` and the settings it
changed (`setting_changes`, `savepoint`); `reported_rows()` is `size()` for a
result with columns and `affected_rows` otherwise (see `session/CLAUDE.md`). Frontends receive it through a typed
future (`actor_zeta::unique_future<core::result_wrapper_t<session_payload>>`)
returned by a send to their connection's `query_session_t`. The poll-side bridge
lives at `frontend/common/asio_future_bridge.hpp`.

### `wait_barrier.hpp`

`otterstax::query_result_t` / `as_query_result` / `spawn_marshaled` /
`run_with_owned_handler` — the single point where a
connector result crosses from the io_context to its consumer. A live exception
must never cross it (boost.asio's `use_future` destroys the captured
`exception_ptr` on the io thread, racing the consumer's `get()`; TSAN race under
boost 1.88), so failures are marshalled as **values**: `core::error_t` for
error-only (`asio_error_t`, `asio_error.hpp`) handlers, `core::result_wrapper_t<R>`
for handlers that return data. The connector's `runQuery` already resolves to that
outcome type; `spawn_marshaled` forwards it through a `std::promise` held by
`detail::outcome_promise_t`, a by-value coroutine parameter whose destructor
settles the promise with `io_error` if the frame is destroyed before the body
ran (io_context torn down or never run) — so `future.get()` never throws
`broken_promise`. The try/catch around the `co_await` in
`detail::marshal_outcome` is the driver boundary: an exception raised on the io
thread becomes an `io_error` value there. `spawn_marshaled`'s first argument
is whatever `co_spawn` takes — an execution context or an executor: the backend
agent passes its own `io_context` (`otterbrix/federation/backend_agent.cpp`), the
MySQL connector's `connectWithTimeout` its connection's strand executor, so the
coroutine and every completion handler the awaited `async_connect` derives from it
(the `cancel_after` deadline timer's included) run serialized on that strand.
`run_with_owned_handler<R, Arg>(connector, query, handler)` is the awaitable the
agent spawns: the handler is a by-value coroutine parameter, and the connector's
virtual `runQuery` is called from that frame with a `function_ref_t` to the copy —
so the handler object lives exactly as long as the query, and a caller may pass a
temporary or a loop-local lambda. The futures `spawn_marshaled` answers do not block
on destruction (unlike `std::async`): a caller whose handler captures its frame by
reference waits on the future before that frame ends.

### `function_ref.hpp`

`otterstax::function_ref_t<R(Args...)>` — a non-owning, non-allocating reference to a
callable: its address plus a trampoline typed on the callable. It is how a handler
crosses a virtual boundary — `IConnector::runQuery` in the mysql / pg / ch connectors —
without `std::function`, which owns a heap copy and is excluded by the code rules. The
reference does not keep the callable alive: the callable must outlive every call made
through it. It binds only a named, non-const object, so a temporary does not compile.
Production binds it in one place, `run_with_owned_handler` (above), to the handler copy
in that coroutine's frame.

### `parse_port.hpp`

`otterstax::parse_port(text, resource)` — the config `port` string →
`core::result_wrapper_t<uint16_t>`; digits only, 1..65535, otherwise
`invalid_parameter`. Used by the config reader
(`config/connections/connection_config_reader.cpp`) and by
`remote_backends_t::open` when it builds the connectors' params.

### `session.hpp`

`session_hash_t` — the per-call id (`session::to_hash` of a query session's statement id) the
file / s3 managers and Kafka take. Nothing else lives there: an engine
session is `components::session::session_id_t`, included where it is used.

### `thread_pool_manager.hpp`

`thread_pool_manager` — a `boost::asio::io_context` + the `std::thread`s that run it, sized by its
owner (no default). The wire-protocol frontend servers' asio pools (`frontend/common/frontend_server.hpp`,
`service.runtime.{mysql,postgres}_io`); the connector tests run their connectors' io_context on one.

### `asio_error.hpp`

`otterstax::asio_error_t` — the return type of an error-only connector handler (the outcome IS the
error, no payload); `query_result_t` keys off it to marshal a bare `core::error_t`.

### `table_info.hpp`

`table_info` — a table's `qualified_name_t` and its STRUCT schema (`catalog/discovery.hpp`, the Flight SQL
core).

### `tracy_memory_resource.hpp`

A PMR interposer that reports every upstream slab to Tracy under a named pool (the parser's arena, the
MySQL / PostgreSQL packet processing). It must outlive every container built over it; the name must
be a string literal.

### `logger.hpp`

`log_t` and `initialize_all_loggers()` — spdlog wrappers. Log level is compile-time via `SPDLOG_ACTIVE_LEVEL` (default `ERROR`; override with `-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG`).

### `tsan_helper.hpp`

TSAN annotation helpers (`ANNOTATE_HAPPENS_BEFORE` / `ANNOTATE_HAPPENS_AFTER`). Used to suppress false positives when actor message delivery provides implicit synchronization that TSAN cannot observe.

### `tracy_profiler.hpp`

Wraps the Tracy macros so they compile to no-ops when Tracy is disabled (the
default). Provides `OTX_ZONE()`, `OTX_ZONE_N("Scope::function")`, `OTX_FRAME()`,
`OTX_PLOT(name, val)`, `OTX_MESSAGE(msg)`, and `OTX_LOCKABLE_N(type, var, name)`.
Every actor handler and expensive standalone function carries an `OTX_ZONE_N`
as its first statement — see the root `CLAUDE.md` Tracy section for the rule.

### `timer.hpp`

`Timer` — RAII helper that logs `"<scope> took N µs"` at destruction through
the provided `log_t`. Used by the `query_session_t` entry points
(`execute`, `execute_prepared_statement`, `prepare_schema`, `execute_plan`,
`prepare_plan`) to get free per-query timings in DEBUG builds.

