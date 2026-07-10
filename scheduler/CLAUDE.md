# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`scheduler/` contains the front-door `Scheduler` actor, the `Worker` pool that
runs the actual query pipeline, and the `schema_utils` helpers. The Scheduler
is the only actor that frontends talk to directly.

## Architecture: Scheduler → Worker pool

`Scheduler` is a thin session-affinity router. It owns:

- A pool of N `Worker` actors spawned on an `actor_zeta::scheduler::sharing_scheduler`.
  Every session keyed by `session_hash_t` always routes to `workers_[id % N]`
  (sticky), so prepared-statement state never crosses workers.
- An event-loop thread (`loop_thread_` + lock-free `inbox_` + `pump_cv_`)
  modelled on otterbrix's `services::dispatcher::manager_dispatcher_t`.
  `enqueue_impl` (called from any sender thread) only pushes into the inbox and
  signals the CV; *all* coroutine creation, suspension, and resumption happens
  on `loop_thread_`. This decouples asio frontend threads from query work and
  eliminates the inline-pump yield-spin we used before.

Every handler is a coroutine that forwards to the routed `Worker` and
co_returns the worker's result. This is a future-of-future passthrough — no
state is mutated in `Scheduler` between the send and the await.

## Scheduler entry points

All return `unique_future<core::result_wrapper_t<session_payload>>`:

| Handler | Caller | Purpose |
|---------|--------|---------|
| `execute(id, sql)` | FlightSQL/MySQL/PG frontend | Full query: parse → schema → dispatch → translate |
| `execute_statement(id)` | Frontend (prepared stmt execute) | Execute a previously prepared statement |
| `execute_prepared_statement(id, params)` | Frontend | Bind parameters then execute |
| `prepare_schema(id, sql)` | Frontend (prepare phase) | Parse + schema-only, no execution |

Frontends await the returned future via `frontend/common/asio_future_bridge.hpp`
(`async_await_future` polls `take_ready()`); there is no `cv_wrapper` / push
interface in the pipeline anymore.

## Worker pipeline

Each `Worker` (`worker.{hpp,cpp}`) is a `basic_actor` that owns its own parser
instance (no shared parser between actors — codex rule 10) and a per-worker
`metadata_map_` (`session_hash → metadata_t`). The map is unguarded because the
sticky routing guarantees a single Worker handles all messages for a session.

Each entry point is wrapped in try/catch: parser and otterbrix can throw, but
an exception must never escape an actor (codex rule 9) — caught exceptions
become `core::error_t` returned through the future.

### Backend routing inside `Worker`

After `CatalogManager::get_catalog_schema` sets `backend_type` on
`ParsedQueryData`:

- `Otterbrix` → `OtterbrixManager::execute`
- `MySQL` → `MySQLManager::execute`
- `PostgreSQL` → `PostgressManager::execute`
- `ClickHouse` → `ClickHouseManager::execute`
- `Mixed` → MySQL → PostgreSQL → ClickHouse → Otterbrix in sequence, each
  inlining its slice as `node_raw_data`

A query that mixes **one registered backend with one or more
otterbrix-internal tables** is classified as that **single backend**, not
Mixed — the parser strips local tables from `external_nodes`
(`otterbrix/parser/parser.cpp:212`), so the catalog only sees the backend.
The backend manager fetches its slice, inlines it as `node_raw_data`, and
hands the mutated plan to `OtterbrixManager`; the engine resolves the
remaining symbolic local-table nodes via their stamped `table_oid` and runs
the JOIN in-process. `examples/demo/sql/step_4.sql` exercises this every
demo run.

### External-table statements (s3/file grammar extensions)

`CREATE EXTERNAL TABLE … WITH (location=…)` and `COPY (<select>) TO '<location>'`
parse (via the registered s3/file parser extensions) into an
`otterstax::external::external_node_t` (tagged `unused`). `Worker::execute`,
`Worker::execute_statement`, and `Worker::prepare_schema` `dynamic_cast` the
root node and, when it matches, route through `Worker::handle_external_statement`
**before** the backend/schema logic (which cannot execute this node and would
otherwise mis-`static_cast` an `unused` root to a `schema_node_t`):

- create → `db::S3Manager::download` (s3://) or `conn::file::FileManager::add_file` (local) — loads into `db.table`
- copy → parse `inner_sql`, then `db::S3Manager::upload` / `conn::file::FileManager::dump_file`

The `s3_manager` (`db::S3Manager`) and `file_manager` (`conn::file::FileManager`)
addresses are passed through the `Scheduler` constructor and forwarded to every
`Worker` at spawn time. Successful DDL/COPY returns an empty `session_payload`
through the future; `prepare_schema` returns an empty schema and the work runs
in `execute`/`execute_statement` (DoGet).

## `schema_utils` Namespace

Used exclusively during the schema-resolution phase (not at execute time):

- `schema_node_t` — placeholder node substituted for external nodes during schema computation in Otterbrix
- `compute_otterbrix_schema()` — runs an aggregate/projection over a known schema without touching remote data
- `compute_join_schema()` — merges schemas across a join node
- `aggregate_filter_schema()` — applies SELECT-list projections/renames to a schema

These utilities exist because Otterbrix's `execute_plan` cannot compute the
schema of a query that references external tables without the external data.
The `Worker` uses them (via `prepare_schema` → `OtterbrixManager::get_schema`)
to construct a valid `cursor_t_ptr` to pass back to frontends for
`prepare_schema` requests.
