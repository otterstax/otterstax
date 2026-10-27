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

### `wait_barrier.hpp`, `function_ref.hpp`, `asio_error.hpp` — deleted

They marshalled connector results off a Boost.Asio io_context
(`spawn_marshaled`, `QueryHandleWaiter`, `run_with_owned_handler`) and carried
handlers across the old `IConnector::runQuery` virtual boundary. The connectors
are now synchronous drivers run on pool threads that answer through an
`actor_zeta::promise` (`connectors/CLAUDE.md`), so nothing crosses an io_context
any more.

### `parse_port.hpp`

`otterstax::parse_port(text, resource)` — the config `port` string →
`core::result_wrapper_t<uint16_t>`; digits only, 1..65535, otherwise
`invalid_parameter`. Used by the config layer to refuse a bad port at parse time
and by `ComponentManager::register_connections` to read it.

### `settled_future.hpp`

`otterstax::take_settled_error(unique_future<core::error_t>, resource)` — picks
up the outcome of an actor whose `enqueue_impl` runs the handler to completion on
the sending thread (`CatalogManager`, `OtterbrixManager`): the future is settled
when `actor_zeta::send` returns, so a non-settled future is reported as
`io_error` rather than waited on. Used by `ComponentManager::register_connections`
(and the test fixture `register_backend`) to observe `add_connection_schema`.

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

`thread_pool_manager` — wraps a `boost::asio::io_context` + `std::vector<std::thread>`. Used by the wire-protocol frontends (`frontend/common/frontend_server.hpp`).

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

