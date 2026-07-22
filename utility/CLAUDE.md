# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`utility/` is a header-only collection of shared primitives used across all layers. There are no `.cpp` files; headers are compiled directly into `lib_otterstax` via the root `CMakeLists.txt`.

## Key Types

### `session_payload.hpp`

`session_payload` holds the output of a completed query: `schema`
(`complex_logical_type`), `chunks` (`std::pmr::vector<data_chunk_t>` — the
b1+ engine caps each chunk at 1024 rows, so a result spans multiple chunks;
`size()`/`column_count()`/`empty()` accessors span the whole vector),
`parameter_count`, and `NodeTag`. With the Scheduler→Worker pool, frontends
receive the payload
through a typed future
(`actor_zeta::unique_future<core::result_wrapper_t<session_payload>>`) returned
by `Scheduler::execute` — *not* through the old `shared_session_payload`
push-CV interface. The poll-side bridge lives at
`frontend/common/asio_future_bridge.hpp`.

### `cv_wrapper.hpp` (legacy, off the hot path)

`cv_wrapper_t<T>` and `shared_data<T>` are still present for a handful of
legacy callers (notably the connector retry / reconnect path and a few
unit-test helpers). New code should not introduce them on the query pipeline:
`Worker` returns `core::result_wrapper_t<session_payload>` through the
actor-zeta future and the frontend awaits via `async_await_future`. If you
find yourself reaching for `create_cv_wrapper` to bridge an actor result to a
frontend, you're almost certainly fighting the new architecture — use the
future instead.

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

### `wait_barrier.hpp`

`query_outcome<T>` + `get_or_throw()` + `QueryHandleWaiter<T>` — marshal
connector results across the io_context worker → consumer thread boundary as
plain values. A live exception must never cross that boundary (boost.asio
`use_future` races the exception_ptr destruction against the consumer's
`future.get()`; TSAN data race surfaced by boost 1.88), so connectors capture
errors into `query_outcome::error` on the io thread and consumers rethrow via
`get_or_throw()`/`QueryHandleWaiter::wait()` on their own thread.
`QueryHandleWaiter`'s destructor drains unconsumed futures (asio futures do
not block on destruction, unlike `std::async`).
