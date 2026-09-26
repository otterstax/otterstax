# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`utility/` is a header-only collection of shared primitives used across all layers. There are no `.cpp` files; headers are compiled directly into `lib_otterstax` via the root `CMakeLists.txt`.

## Key Types

### `session_payload` — lives in `scheduler/session_data.hpp`, not here

`session_payload` holds the output of a completed query: `schema`
(`complex_logical_type`), `chunks` (`std::pmr::vector<data_chunk_t>` — the
b1+ engine caps each chunk at 1024 rows, so a result spans multiple chunks;
`size()`/`column_count()`/`empty()` accessors span the whole vector),
`parameter_count`, and `NodeTag`. With the Scheduler→Worker pool, frontends
receive the payload
through a typed future
(`actor_zeta::unique_future<core::result_wrapper_t<session_payload>>`) returned
by `Scheduler::execute`. The poll-side bridge lives at
`frontend/common/asio_future_bridge.hpp`.

A byte-identical dead copy used to sit in `utility/session_payload.hpp`,
included by nothing and listed in no CMakeLists — two structs of the same name
in the global namespace, i.e. an ODR trap waiting for the first TU to include
both. It has been deleted; `scheduler/session_data.hpp` is the only definition.

### `wait_barrier.hpp`

`otterstax::query_result_t` / `as_query_result` / `spawn_marshaled` /
`make_failed_future` / `QueryHandleWaiter` — the single point where a connector
result crosses the io_context worker → consumer thread boundary. A live
exception must never cross it (boost.asio's `use_future` destroys the captured
`exception_ptr` on the io thread, racing the consumer's `get()`; TSAN race under
boost 1.88), so failures are marshalled as **values**: `core::error_t` for
error-only (`asio_error_t`) handlers, `core::result_wrapper_t<R>` for handlers
that return data. The connector's `runQuery` already resolves to that outcome
type; `spawn_marshaled` forwards it through a `std::promise` held by
`detail::outcome_promise_t`, a by-value coroutine parameter whose destructor
settles the promise with `io_error` if the frame is destroyed before the body
ran (io_context torn down or never run) — so `future.get()` never throws
`broken_promise`. The try/catch around the `co_await` in
`detail::marshal_outcome` is the driver boundary: an exception raised on the io
thread becomes an `io_error` value there. `spawn_marshaled`'s first argument
is whatever `co_spawn` takes — an execution context or an executor: the three
`ConnectorManager::executeQuery` pass the pool's `io_context`, the MySQL
connector's `connectWithTimeout` passes its connection's strand executor so the
coroutine, and every completion handler the awaited `async_connect` derives
from it (the `cancel_after` deadline timer's included), runs serialized on that
strand rather than on any pool thread. `run_with_owned_handler<R, Arg>(connector,
query, handler)` is the awaitable the three `executeQuery` spawn: the handler is a
by-value coroutine parameter, and the connector's virtual `runQuery` is called from
that frame, on the io thread, with a `function_ref_t` to the copy — so the handler
object lives exactly as long as the query, and a caller may pass a temporary or a
loop-local lambda. `QueryHandleWaiter` takes the
`memory_resource` its pmr `futures`/`results` live on; `wait()` is
`[[nodiscard]]`: it returns the first failure instead of throwing, and its
callers index `results` positionally. Its destructor drains unconsumed futures
(asio futures do not block on destruction, unlike `std::async`).

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
`invalid_parameter`. Used by the three `ConnectorManager::addConnection`
overloads.

### `settled_future.hpp`

`otterstax::take_settled_error(unique_future<core::error_t>, resource)` — picks
up the outcome of an actor whose `enqueue_impl` runs the handler to completion on
the sending thread (`CatalogManager`, `OtterbrixManager`): the future is settled
when `actor_zeta::send` returns, so a non-settled future is reported as
`io_error` rather than waited on. Used by `addConnection` to observe
`add_connection_schema`.

### `cv_wrapper.hpp` — deleted

`cv_wrapper_t<T>` / `shared_data<T>` (a `shared_ptr` of mutex + condvar, handed
to an actor through `actor_zeta::send` and blocked on for up to 90 s) are gone.
Their last caller, the FlightSQL `GetTables` path, now takes the same route as
every other frontend: the actor returns
`core::result_wrapper_t<T>` through the future and the caller awaits it on its
own asio executor via `frontend/common/asio_future_bridge.hpp`. The behaviours
the primitive guaranteed are re-asserted against the bridge in
`tests/unit/utility/test_asio_future_bridge.cpp`. There is no longer any
supported way to hand shared mutable state to an actor — return a value.

### `session.hpp`

`session_hash_t` — the type alias for query session IDs threaded through all actors.

### `thread_pool_manager.hpp`

`thread_pool_manager` — wraps a `boost::asio::io_context` + `std::vector<std::thread>`. Used by each `ConnectorManager` to host async DB operations.

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
the provided `log_t`. Used by the `Worker` entry points
(`Worker::execute`, `Worker::execute_statement`, `Worker::prepare_schema`) to
get free per-query timings in DEBUG builds.

