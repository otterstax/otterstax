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
| `close_statement(id)` | Frontend (statement close / refused prepare) | Drop a prepared statement that will not be executed; idempotent |

Frontends await the returned future via `frontend/common/asio_future_bridge.hpp`
(`async_await_future` polls `take_ready()`); no result is ever pushed to a
frontend through a condvar. The Scheduler's own `pump_cv_` is infrastructure:
it only wakes `loop_thread_` when the inbox is non-empty.

## Worker pipeline

Each `Worker` (`worker.{hpp,cpp}`) is a `basic_actor` that owns its own parser
instance (no shared parser between actors — codex rule 10) and a per-worker
`metadata_map_` (`session_hash → metadata_t`). The map is unguarded because the
sticky routing guarantees a single Worker handles all messages for a session.

Errors travel as `core::error_t` inside the future; nothing in a handler
throws and no handler wraps its body in try/catch. The one call that may
throw is `IParser::parse` (the engine's raw parser sits behind it, and test
parsers throw on purpose): `Worker::parse_sql` fences exactly that call and
converts any exception — std or not — into `sql_parse_error` on the spot.
Callee errors keep their code; the Worker only prefixes the text.

### Prepared-statement lifecycle

`prepare_schema` stores the parsed statement in `metadata_map_[id]`. The entry
is single-use: the first `execute_statement` / `execute_prepared_statement`
moves the plan out, runs it, and erases the entry on **every** exit path
(success, backend error, engine error, bind error). Any later execute on that
session — and an execute on a session that was never prepared — answers
`core::error_code_t::invalid_parameter` with the text
`prepared statement must be re-prepared`; the frontend prepares again.
`close_statement` erases the entry of a statement that will never be executed
(MySQL `COM_STMT_CLOSE`, PG `Close`/re-`Parse` under the same name, a FlightSQL
`GetFlightInfoStatement` refused after the prepare, a MySQL `COM_STMT_PREPARE`
refused for an unencodable column) and answers an empty `session_payload`; it
is idempotent — a consumed, already closed or never prepared session closes
with success. A wire connection that ends (quit/terminate, socket drop,
protocol error, timeout, server stop) closes every statement it left
unexecuted from `frontend_connection::finish()` (`finish_impl`), so an entry
outlives its connection only if that close message failed (logged by the
frontend).

### Database-level DDL guard

`CREATE DATABASE` / `DROP DATABASE` are engine-local by grammar (no alias can
be attached), but the engine also hosts one database per registered connection
uid — the mirrored remote schema — and the kafka object database. `DROP
DATABASE` is CASCADE, so such a statement would tear down the mirror while the
catalog kept resolving against it. `Worker::guard_database_ddl` runs on all
three entry points (`execute`, `execute_statement`, `prepare_schema`): it reads
the target name from the plan (`node_create_database_t::dbname()`, or the
`catalog_resolve` sibling of a `drop_t(database)` inside the wrapping
sequence) and asks `CatalogManager::check_database_ownership`, which answers
`invalid_parameter` (`database '<name>' is owned by connection '<uid>'`) for a
uid or `kafka`, matched case-insensitively. The refusal happens before the
statement reaches the engine, so the mirror and the catalog store stay intact.
A `DROP DATABASE` root that names no database is refused the same way.

### Tracy zones in coroutines

Tracy zones are per-thread and strictly nested. A Worker coroutine resumes
after `co_await` on whichever `sharing_scheduler` thread picks the actor up, and
the Scheduler's loop thread interleaves its coroutines, so a zone that spans a
suspension point ends on the wrong thread or out of order and corrupts the
trace. Every handler therefore opens `OTX_ZONE_N` in a block that closes before
its first `co_await` (the Scheduler handlers zone their synchronous `route()`
call); synchronous helpers — `parse_sql`, `finish_schema_value`,
`take_payload`, `handle_external_statement` — carry their own zones as the
first statement, and so does `Worker::close_statement`, which never suspends.
The awaited actors measure their own work. This is the coroutine exception to
the root `CLAUDE.md` "first statement" rule: only the `assert`, the `Timer`, the
`erase_on_exit_t` and the variables the block fills precede the block.

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
`Worker::execute_statement`, and `Worker::prepare_schema` recognise the root
through `extension_root()` in `worker.cpp` — `ParsedQueryData::extension_kind`
(stamped by `GreenplumParser::parse` from the `ExtensionNode` envelope's
`extension_id`: s3/file → `external`, kafka → `kafka`) says which extension
claimed the statement, which is what separates it from a `schema_node_t` stub
sharing the same `unused` node type — and route it through
`Worker::handle_external_statement` **before** the backend/schema logic (which
cannot execute this node and would otherwise mis-`static_cast` an `unused` root
to a `schema_node_t`):

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

A SELECT over **one** remote table needs no engine round-trip: the catalog
rewrites its aggregate into the `schema_node_t` carrying the projected schema,
and `Worker::prepare_schema` reads that stub directly. The transformer wraps a
table-referencing statement in a `node_sequence_t` whose data-producing node
is the LAST child, so the stub is looked for both as the root and as that last
child (`tests/system/test_single_backend_prepare_schema.cpp`). The projection
is resolved whatever the statement's parameters — `$1` in a WHERE does not
change the columns — so a parameterized remote SELECT carries both its STRUCT
and its `parameter_count`. The engine-local branch answers a parameterized
statement through `OtterbrixManager::get_schema` as well, but by the federated
computation rather than the engine's plan validation: the engine validates a
plan only with every parameter bound — its plan-only pass refuses one that
still carries `$n` with "unbound parameter in expression" — so the relations the
plan reads are probed for their columns instead (`plan_dependencies` in
`worker.cpp` numbers them, one `SELECT * FROM db.rel LIMIT 0` per relation
answers them) and the plan's projection is resolved against that answer.
`SELECT *`, which the transformer leaves a passthrough naming no column, is
expanded into the table's own columns that way — what PostgreSQL does out of its
catalog at parse time — and a named column carries the type it will arrive
under. Two things the computation still cannot answer are left to the frontend's
check on the executed result: the type of a function call (NA — the engine's
kernels are not consulted, while an arithmetic expression takes its first
operand's type) and the width of a JOIN (merged by name, while the engine
answers both key columns). A root the computation cannot read (a set
operation) still answers an unresolved schema. The price is one engine probe per
relation per prepare, and a statement naming a relation the engine does not hold
now fails at prepare rather than at execute — which is where PostgreSQL fails it
too (`42P01`).

**A remote statement's prepared schema comes from the backend.**
Right after `CatalogManager::get_catalog_schema`, and before the stub is read,
`prepare_schema` sends a statement classified `ClickHouse` to
`ClickhouseManager::describe`, which probes each ClickHouse slot with
`SELECT * FROM (<the statement execute would run>) LIMIT 0` and writes the STRUCT
of the header block the backend answers into the stub, decoded the way `execute`
decodes its data blocks. It fills the schema field in place rather than rebuilding
the node: a raw-SQL subquery stub carries the user's text and its qualifiers, and
that is what `execute` generates the statement from. Both kinds of stub are
described — the raw one carried `NA` until this, which left a derived table's
column untyped and made a JOIN with a subquery fail its prepare outright. ClickHouse owns the result type of every
expression it evaluates — `count()` is UInt64, `x + 1` over an Int32 column is
Int64 — so a plan-derived type would name something the executed chunk does not
carry. Only the single-backend classification is described: a `Mixed` root is
the engine's JOIN and has no stub to probe with. A parameterized statement is
outside that scope: the binder holds every parameter — the literals included —
until `finalize`, so no SQL exists to probe before Bind, and such a statement
keeps the catalog's stub schema, which names the projection and types it from
the catalog rather than from the backend. What a frontend does with a schema no
probe confirmed is the frontend's own contract: the PG one describes it before
Bind, describes a portal from the result that portal actually answers, and
refuses an `Execute` whose result does not match what was described
(`frontend/CLAUDE.md`, "PostgreSQL extended query specifics").
Acceptance: `tests/system/test_single_backend_prepare_schema.cpp`, tag
`[ch-describe-routing]`.
