# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`scheduler/` contains the routing actor (`Scheduler`), the per-session worker actor (`Worker`), the typed result POD (`session_payload` in `result.hpp`), and schema computation utilities (`schema_utils`). Frontends talk only to the `Scheduler`.

## Scheduler Actor

`Scheduler` is a thin `actor_zeta::basic_actor<Scheduler>` (no custom `enqueue_impl`, no internal mutex). Its only job is to pick a `Worker` by `session_hash` and forward the message; the per-session pipeline lives in the picked `Worker`.

Entry-point methods (all return `unique_future<result<session_payload>>` where `result<T> = result_t<pipeline_error, T>` from `utility/pipeline_error.hpp`):

| Method | Caller | Purpose |
|--------|--------|---------|
| `execute(id, sql)` | FlightSQL/MySQL/PG frontend | Full query: parse → schema → dispatch → translate |
| `execute_statement(id)` | Frontend (prepared stmt execute) | Execute a previously prepared statement |
| `execute_prepared_statement(id, params)` | Frontend | Execute with bound parameter values |
| `prepare_schema(id, sql)` | Frontend (prepare phase) | Parse + schema-only, no execution |

All four methods are **non-coroutine passthroughs**: they pick a worker, do `actor_zeta::send`, and return the resulting future. The `dispatch.hpp` future-of-future passthrough forwards the worker's terminal result back to the caller. No exceptions: every error flows through `result<>` / `pipeline_error`.

## Worker Pool

A `std::vector<unique_ptr<Worker, pmr::deleter_t>>` is constructed in `Scheduler`'s ctor (size = `std::thread::hardware_concurrency()`). Sharding: `worker_index = session_hash_t % worker_count`. One session lives on one worker — `metadata_map_` is per-worker and needs no lock. Each Worker method asserts the routing invariant on entry.

`Worker` is also `basic_actor<Worker>`. Its 4 coroutine methods mirror the Scheduler API (same names, same signatures). Inside each method the pipeline is sequential `co_await` over backend manager futures; terminals are `co_return result<session_payload>{...}` on success or `co_return pipeline_error{code, tag, message}` on failure. The legacy `register_session / complete_session / shared_session_payload / Status::Empty` side-channel is gone.

## sharing_scheduler ownership

A `std::unique_ptr<actor_zeta::scheduler::sharing_scheduler>` lives in `ComponentManager` (`component_manager/component_manager.cpp`). It is `start()`ed before any actor is spawned and `stop()`ed first thing in `~ComponentManager`. The `Scheduler` ctor takes a raw pointer to it and calls `sched_->enqueue(&worker)` when `actor_zeta::send`'s `needs_sched` flag is true (cooperative_actor invariant: true only when the worker is idle, see `actor-zeta/actor/cooperative_actor.hpp:124-148, 504-538`).

## Routing Logic

After `mysql::CatalogManager::update_backend_type` (or `Otterbrix` fast path) sets `backend_type` on `ParsedQueryData`, the worker routes:

- `Otterbrix` → `db::OtterbrixManager::execute`
- `MySQL` → `db::MySQLManager::execute`
- `PostgreSQL` → `db::PostgressManager::execute`
- `ClickHouse` → `db::ClickhouseManager::execute`
- `Mixed` → sequential dispatch to MySQL/PG/CH backends, results merged before `OtterbrixManager::execute`

Backend `execute` methods already return `unique_future<otterstax::result<ParsedQueryDataPtr>>`. The Worker propagates errors into `pipeline_error` with the appropriate `error_tag_t::{sql,pg,ch}_connection_manager` tag.

## Frontends and the asio bridge

Frontends never block on actor-zeta futures directly. They use `frontend/common/asio_future_bridge.hpp::await_az_future(fut, timeout)` — a `noexcept` `asio::awaitable<result<T>>` polling helper. Today every frontend drives it from a per-request `boost::asio::io_context local; local.run();` so the connection state machine is unchanged. Future async-ifying (truly non-blocking connection loop) is out of scope of the Scheduler refactor.

## `schema_utils` Namespace

Used exclusively during the schema-resolution phase (not at execute time):

- `schema_node_t` — placeholder node substituted for external nodes during schema computation in Otterbrix
- `compute_otterbrix_schema()` — runs an aggregate/projection over a known schema without touching remote data
- `compute_join_schema()` — merges schemas across a join node
- `aggregate_filter_schema()` — applies SELECT-list projections/renames to a schema

These exist because Otterbrix's `execute_plan` cannot compute the schema of a query that references external tables without the external data. `Worker::prepare_schema` uses them to construct a valid `cursor_t_ptr` returned to frontends.

## Gotchas

- The pipeline must NOT use `throw`/`try`/`catch` in new code. actor-zeta is `-fno-exceptions` internally; any escaped exception in an actor coroutine asserts. Stay on `result<>`.
- `Worker` does NOT hold a `sharing_scheduler*` — all its sends go to `actor_mixin` backend managers whose `enqueue_impl` returns `needs_sched=false`. If backends ever migrate to `basic_actor`, the Worker will need to be wired to the scheduler.
- The empty-result path (`cursor->size()==0`) returns a default-constructed `session_payload` with a zero-sized chunk; frontend consumers already treat that as a successful empty response (no separate Status::Empty signalling).
