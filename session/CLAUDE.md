# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`session/` (CMake target `session`) is the query pipeline of the server:
`otterstax::session::query_session_t`, ONE actor per client connection (a wire
connection, a Flight SQL RPC or prepared statement, a Spark session), and the
`session_factory_t` the frontends open them from. Every statement a client sends
goes parse → ownership guard → its route: CREATE EXTERNAL TABLE / COPY ... TO,
Kafka DDL and writes, else the engine on the connection's engine session. A
remote table is an ordinary table of otterbrix's catalog (a table of a
connection, `uid.ns.t`); otterbrix plans every statement over it and reaches its
rows through the connection's backend actor (`otterbrix/federation/CLAUDE.md`).
A statement that misses a table created on the backend after startup registers
it and runs once more. There is no router and no shared pipeline actor in front
of it.

## Files

| File | What |
|------|------|
| `query_session.{hpp,cpp}` | The actor, `session_services_t` (everything it sends to) and `make_session_services` (builds it, refusing an empty address), `parameter_row`, `session_ptr` / `session_closer_t`; `command_payload` (a statement that ran without rows — CREATE EXTERNAL TABLE / COPY ... TO, Kafka — under its tag) and `rows_payload` (an engine cursor) anonymous in the `.cpp` |
| `session_factory.{hpp,cpp}` | `session_factory_t`: opens sessions and holds the ones that exist (`add` / `forget` / `reap` private, `forget` called by the session as it frees itself); its destructor is the sessions' shutdown (drain, `runtime_t::stop_sessions`, reap) |
| `plan_inspection.{hpp,cpp}` | Pure plan readers: extension root, `transaction_command`, `place_namespace_ddl` (a client's CREATE / DROP DATABASE / SCHEMA into local, or refused), `write_target` (the relation or schema a DDL / DML changes, as written), `setting_changes`, `savepoint_name`, `is_query` (what COPY exports), `statement_names` (every table name as written, for the registration of a missed table) |
| `session_payload.hpp` | `transaction_command_t`; `session_payload`: schema, chunks, `parameter_count` / `parameter_types`, tag, `affected_rows`, `transaction`, `portal` / `has_more`, `setting_changes`, `savepoint`; `reported_rows()` |
| `session_ids.hpp` | `statement_id_t` (a strong id), `to_hash` |

## The actor

A cooperative actor (`basic_actor`) on the runtime's sessions pool
(`runtime_t::sessions_scheduler()`). In actor-zeta 2.0 a future can be
`co_await`ed only inside an actor coroutine, hence an actor and not a plain
connection object.

- **Self-scheduling.** `enqueue_impl` shadows `basic_actor::enqueue_impl`: when a
  delivery unblocks the idle mailbox it hands the actor to its own scheduler and
  always answers `needs_sched == false`. A frontend only calls
  `actor_zeta::send(session->address(), &query_session_t::execute, statement_id_t{...}, sql)`
  and awaits the future through `frontend/common/asio_future_bridge.hpp`
  (Spark: `spark_connect_server/await_future.hpp`). Only an allocation failure
  of the pool's job node can make the enqueue throw; `schedule()` is
  `noexcept`, so that terminates. `enqueue_impl` stays `[[nodiscard]]`.
- **One statement at a time, in arrival order.** A cooperative actor holds ONE
  behavior and does not pop the next message while it is suspended on a
  `co_await`. The mailbox is the statement queue; there is no second queue.
  `tests/session/test_session_lifetime.cpp` ("one statement at a time across real
  suspensions") pins it with every statement sent before any answer is read.
- **Waiting is a poll.** actor-zeta 2.0.0 settles a future by
  flags alone and wakes nobody: while the session is suspended on the engine's
  future, `basic_actor::resume` answers `resume` and the pool requeues the job at
  once, so a statement that waits costs CPU of the sessions pool (and the
  engine's executor spins the same way on its exec pool). The session keeps the
  plain `co_await` until actor-zeta / otterbrix wake an actor when the future it
  awaits settles. The hidden `[.engine-defect-otb-notify]` case in
  `test_session_lifetime.cpp` asserts the idle wait.

### Entry points

All answer `unique_future<core::result_wrapper_t<session_payload>>`:

| Handler | What |
|---------|------|
| `execute(statement_id_t, std::string sql)` | parse and run |
| `prepare_schema(statement_id_t, std::string sql)` | parse and describe (the engine's describe: columns and the type of every `$n`); keeps the text and the described schema under the id |
| `execute_statement(statement_id_t)` | run the prepared statement without values |
| `execute_prepared_statement(statement_id_t, data_chunk_t, row)` | bind row `row` of the chunk (column i is `$<i+1>`, `parameter_row` builds one) and run |
| `execute_window(statement_id_t, data_chunk_t, row, window)` | as above, answering at most `window` rows; while more may follow the payload names the engine's portal (`portal`, `has_more`) |
| `fetch_window(portal, count)` / `close_window(portal)` | the next rows of a window (`fetch_all`: every row left) / close a window the client abandoned |
| `describe_bound(statement_id_t, data_chunk_t, row)` | describe the prepared statement with the values bound (a PostgreSQL Describe of a portal); keeps nothing |
| `close_statement(statement_id_t)` | forget a prepared statement; idempotent |
| `execute_plan(statement_id_t, ParsedQueryDataPtr, rebuild_fn)` | run a pre-built plan (Spark) |
| `prepare_plan(statement_id_t, ParsedQueryDataPtr, rebuild_fn)` | describe a pre-built plan without running it; keeps nothing |
| `fail_transaction(core::error_code_t, std::string)` | a failure the frontend found itself (a protocol error, a value it cannot bind, a statement it stopped waiting for): reported to the engine on the connection's session, so it fails an open block; answers the error |

`transaction_status()` — a plain member, not a message — answers where the engine
session stands after the last statement that ended (`idle` / `in_transaction` /
`failed`, PostgreSQL's ReadyForQuery `I` / `T` / `E`); a frontend reads it after
awaiting a statement (the result is published before the future is ready).

`statement_id_t` is fresh per statement: a frontend takes each one from the
session itself (`next_statement()`, a public atomic counter from 1, callable from
any thread); the connection is the session itself. The S3 / file / Kafka
actors get the statement id as their per-call `session_hash_t` (`to_hash`).

### Lifetime: the close protocol

The owner holds a `session_ptr` (`std::unique_ptr<query_session_t, session_closer_t>`);
dropping it calls `close()` exactly once. A session has exactly one owner and
no shared ownership: a MySQL / PostgreSQL connection, a Flight SQL RPC running
a statement by text, a Flight SQL prepared statement's entry in `FlightSqlCore`,
the Spark session registry (`idle_session_registry_t`). Where the owner is not
the code that sends (a Spark RPC, an execution of a Flight SQL prepared
statement), every step reaches the session through a method of the owner —
`idle_session_registry_t::open_or_use` / `use` / `begin` with the Spark session id,
`FlightSqlCore::use_prepared` with the statement's handle: under
the owner's lock it finds the session and only queues the statement
(`actor_zeta::send`), then the lock is released and the future awaited — the
future's state is not the actor's memory, so it outlives a session freed
meanwhile. A close under the same lock drops the `session_ptr`; a statement
the session took runs to its end and its future is answered (below), while a
later step of the same RPC finds no session and is refused as closed (Spark
`INVALID_HANDLE.SESSION_CLOSED`, Flight SQL `NOT_FOUND`). A Spark step whose
answer the RPC awaits (`begin` … `finish`) keeps the Spark session from
expiring until the wait ends; a release still closes it at once. A connection
may go away in the middle of a statement:

- `close()` first sends `backend_actor_t::end_session` to the backend actor of
  every connection (not awaited): the connection's statements waiting for a
  backend connection leave the actor's queue (`connection_closed`) and its
  transaction rolls back there on the agent it holds, which is free again at
  once (`otterbrix/federation/CLAUDE.md`, "A closed connection"). Not left to
  the engine's ROLLBACK: that is a statement for one of the engine's four
  executors, each running one statement at a time, and every one of them may
  be running a statement of another block that waits for this very agent.
  A statement of the session still running on a backend
  is answered `connection_closed` at its next request there.
- Then the engine's `manager_dispatcher_t::close_session` for the connection's
  engine session (not awaited): the engine takes it after the statements the
  session sent before it, rolls back an open block (failed or not) and drops
  the session's settings, prepared statements and windows.
- `life_` packs a closed bit (`kClosed`) and the number of jobs handed to the
  pool and not yet ended (units of `kJob`). `schedule()` counts a job before
  `scheduler_->enqueue(this)`; `resume()` ends the job on any verdict other than
  `resume`; `close()` sets the bit.
- Whoever ends the last job of a closed session — `close()` itself when the
  session is idle, otherwise the pool thread finishing the statement in flight —
  calls `destroy()`: `factory_->forget(this)`, then the memory goes back to the
  resource. Nothing touches the actor after that.
- After close, a delivery is refused (`queue_closed`; its future fails), and a
  statement already queued behind the one in flight answers
  `connection_closed` "the connection is closed" without running.
- A closed session whose job a stopped pool dropped cannot free itself:
  `session_factory_t`'s destructor `reap()`s it, which is valid only once the
  sessions pool has stopped.

Shutdown order (`main.cpp`, the tests' `session_stack_owner`): the servers,
declared after the factory, are destroyed first — each stops in its destructor
and their connections close their sessions. Then `~session_factory_t`:
`wait_closed(30 s)` for the sessions to drain (a warning names the sessions
still running and how many are inside a block) → `runtime_t::stop_sessions()`
(idempotent; stopping the pool a second time directly would hang) → `reap()`.
`stop_sessions` must come before `reap`: a pool thread could otherwise resume a
session `reap` frees. Only then the Kafka / S3 / file actors, the engine and the
backends, declared before the factory, which those statements still talk to.

`tests/session/test_session_lifetime.cpp` covers an idle close (freed at once),
a close with a statement in flight (it finishes with its result, the one queued
behind it answers "the connection is closed", the session is freed after), a
block closed while more blocks wait for its backend connection than the engine
has executors (each gets it in turn at once), a statement waiting for a backend
connection answered "the connection is closed" with its close, and
`tests/system/test_frontend_transactions.cpp` a client dropping its socket
mid-statement on both wire servers.

### `session_services_t`

A plain aggregate of what a session sends to: `engine_dispatcher` (the address
every request of `otterbrix/operators/engine_requests.hpp` names, called by the
session directly), `s3` (`db::S3Manager`), `file` (`conn::file::FileManager`),
`kafka` (`otterstax::kafka::KafkaManager`) with `kafka_objects` (its object names,
read under their lock — an INSERT tells a Kafka write by them), and the remote
connections: `connections` (every configured connection with its backend actor,
`remote_backends_t::connections()` — what the registration of a missed table
needs) and `aliases` (the names the ownership guard keeps). A query over remote
tables reaches the backends through the engine, never through the session. No
address may be empty (an actor-zeta send to an empty address dereferences
null): `make_session_services(resource, engine_dispatcher, s3, file, kafka,
kafka_objects, connections)` builds it once (`main.cpp`) and answers
`invalid_parameter` naming the first empty address or a connection without its
actor; the `query_session_t` constructor asserts each. Every session gets a copy.

### `session_factory_t`

`session_factory_t(runtime, parser_factory, services)` (the runtime's resource and
sessions pool; the runtime's `stop_sessions()` in the destructor, above);
`open()` builds a session with its own parser (`parser_factory`), its own engine
session and a copy of the services, registers it and answers the `session_ptr`.
`live_sessions()`, `sessions_in_block()` (the live sessions inside a transaction
block, open or failed — what the factory's shutdown warning reports) and
`wait_closed(timeout)` read the sessions the factory holds. `main.cpp` owns one
and hands it to every server (`frontend_server_config::sessions`, the Flight SQL
`SessionEngine`, the Spark service).

## The engine session and transactions

Each query session owns `engine_session_` (`session_id_t::generate_uid()`).
Every statement of the connection runs on it — its transaction, its settings,
its prepared statements and windows — so BEGIN / COMMIT / ROLLBACK and
SAVEPOINT / RELEASE / ROLLBACK TO work with the engine's PostgreSQL semantics on
every protocol, over local and remote tables alike (what a block means on a
backend: `otterbrix/federation/CLAUDE.md`, "Transactions"):

- **The block is the engine's.** The engine stamps where the session stands on
  every answer, an error included (`cursor_t::transaction_status()`); `follow()`
  keeps the last one in `status_`, which `transaction_status()` answers. Every
  frontend reads the block from there: the PG ReadyForQuery status, the MySQL
  `SERVER_STATUS_IN_TRANS` flag, the Flight SQL BEGIN refusal, the Spark timeout.
- **settle.** Every entry ends in `settle()`. A statement that fails BEFORE it
  reaches the engine (parse, bind, the ownership guard, an S3 / file / Kafka
  failure, the frontend's own) is reported on the connection's engine session
  (`manager_dispatcher_t::refuse_statement`), so it fails an open block exactly
  as an execution error does, and the answer of that report is the status. Every
  prepare failure — a text that does not parse included — fails an open block,
  as PostgreSQL's own Parse error does.
- **COMMIT of a failed block.** The engine rolls it back and answers success;
  `settle` tags the payload `transaction_command_t::rollback` (PostgreSQL 18
  answers it `ROLLBACK`) — the PG frontend's `CommandComplete ROLLBACK`, the
  MySQL frontend's OK packet. An executed transaction statement's payload names
  it (`session_payload::transaction`, the CommandComplete tag).
- **A prepare is not outside the transaction.** The describe runs on
  `engine_session_`: it sees what the connection's transaction sees (a table the
  transaction created), and a refused describe fails the block. A describe in a
  failed block is refused (`25P02` on the PG wire).
- **What does not run on the engine is refused in a failed block.**
  CREATE EXTERNAL TABLE / COPY ... TO and Kafka statements do not reach the
  engine before their work starts (a file read, a topic written):
  `refuse_in_failed_block()` answers the engine's own `transaction_finalized`
  text for them in a failed block.

### Settings

A `SET` / `RESET` / `RESET ALL` / `SET LOCAL` is an ordinary statement of the
engine, which keeps settings per session (`timezone`, `autocommit`,
`search_path`; a parameter it does not have is its refusal). An executed one answers what it assigned (`setting_changes`, each
value canonicalised as the engine does, `plan::setting_changes`) and a
SAVEPOINT / RELEASE / ROLLBACK TO the savepoint it named (`savepoint`), which
the PostgreSQL frontend follows for its ParameterStatus (`frontend/CLAUDE.md`).
A setting of the whole server — `ALTER SYSTEM`, `ALTER DATABASE … SET`,
`ALTER ROLE … SET` — is refused by the parser before the engine
(`unimplemented_yet` "server-wide settings are not changed by a client",
`otterbrix/parser/parser.cpp`); MySQL's own forms (`SET GLOBAL`, `PERSIST`, ...)
are the MySQL frontend's to refuse, and its translation of a MySQL SET reaches
the session as one plan through `execute_plan` (`frontend/CLAUDE.md`, "MySQL
protocol statements").

## Pipeline

### Prepared statements

Prepared statements live in the session's own `prepared_` map (statement id →
`prepared_t`: the text and the result schema the engine described, which the
rows are answered under); a direct `execute` / `execute_plan` keeps no entry.
An entry runs any number of times until `close_statement`: every execute parses
the text again and binds its own values (`parse_bound`), a failed execute keeps
the entry. An id that was never prepared (or was closed) is `invalid_parameter`
"prepared statement does not exist"; closing an unknown or closed entry is
success. A direct `execute` of a statement with placeholders is refused
(`invalid_parameter`, "… no values bound; prepare and bind it"), and an execute
that leaves a `$n` without a value is `sql_parse_error` "Argument binding failed:
Not all parameters were bound: $n". The map is the connection's: a statement prepared on one
connection is unknown on another. The engine's SQL `PREPARE` / `EXECUTE` /
`DEALLOCATE` are statements like any other, kept by the engine session.

### Running a statement

`dispatch` routes every parsed statement (SQL and pre-built plans alike):

- the ownership guard (below);
- CREATE EXTERNAL TABLE / COPY ... TO, Kafka DDL and Kafka writes (below);
- everything else runs on the engine (`run_on_engine` → `on_engine`), the one
  road of a statement to the engine for every entry — a run, a window, a
  describe, a pre-built plan, COPY's inner query. Engine errors come back under
  the prefix `Otterbrix execution failed: ` (a backend's own error too: it
  reaches the client through the engine, its text whole).

**Registration of a missed table, one more run.** When the engine refuses a
statement over a table of a connection it does not have (created on the backend
after startup), `register_missed` looks for it among the statement's names as
written (`plan::statement_names`): `catalog::registration_target` names a
three-part name of a connection (`uid.ns.t`), and a short `t` / `s.t` is completed with the
session's place, whose `current_connection()` names that connection (`uid.ns.t`). For
each target the connection's
backend actor discovers the table (a message the session awaits — its thread
does not wait for the backend) and `catalog::registration_plans` records it on
the actor's registration session (`catalog/CLAUDE.md`), whatever the client's
block: the registration does not touch it. The statement is then built again
(`rebuild_fn`: the text parsed again with its values bound again, or the
DataFrame translated again) and run once more only when its failure left the
session idle — inside a block the failure has failed it. An empty `rebuild_fn`
registers the table and answers the original error. A connection configured
with one `table:` is a target for that table alone (`registration_target`): a
miss of another table of it is answered as it is, no discovery sent. A
discovery that fails at the connection (`io_error` / `connection_closed`: the
connection lost or never opened, marked unreachable, no free connection) is the
answer when nothing was registered — the client learns that the connection is
down, not that the table does not exist (`registration_outcome_t::unreachable`,
the cursor keeping the engine's transaction status); a backend that answered
without the table leaves the statement's own miss.

### Result shape

`rows_payload` builds the payload from the engine cursor: its chunks (at least
one, so `front()` is defined), for a write cursor `affected_rows`, and the
window's portal. `session_payload::reported_rows()` is the row count of a result
with columns and `affected_rows` of a column-less one — what the frontends put
in CommandComplete / the OK packet. A row-producing statement whose result has
no columns is a `schema_error` naming the statement (`engine::execute_statement`),
never a column-less payload a frontend would report as a DML.

### Pre-built plans (`execute_plan` / `prepare_plan`)

The Spark Connect frontend translates a DataFrame into a `ParsedQueryData`
itself (and the MySQL frontend a SET), so these two skip `parse_sql` and join
the SQL path at `dispatch` / `describe`. The `rebuild_fn` is the frontend's
translation of the same DataFrame, for the run after a registration. Both
refuse a missing plan and a grammar-extension root (`extension_kind` set) with
`invalid_parameter`.

### Ownership guard

`guard_ownership` runs on every entry that runs or describes a plan and answers
`invalid_parameter` before the statement reaches the engine
(`catalog/ownership.hpp`, `catalog/CLAUDE.md`): a table name of four parts, any
DDL or DML of a relation of `federation` (`check_write_ownership` over
`plan::write_target`), a DROP of the schema `local.kafka`, and DDL of a table of a
connection configured with one `table:` naming another table
(`catalog::check_table_scope` over `services_.connections`, the table DDL
`write_target` marks `table_ddl`; `load_external_table` checks its connection-table
target the same way before the file is read). No CREATE / DROP DATABASE
reaches it: the parse already made it a schema of `local` or refused it
(`plan::place_namespace_ddl`), so a client never makes or drops an engine
database, a connection's or `federation` included.

### External-table and Kafka statements

`CREATE EXTERNAL TABLE … WITH (location=…)` and `COPY (<query>) TO '<location>'`
parse into an `otterstax::external::external_node_t`; `plan::extension_root`
reads `ParsedQueryData::extension_kind` and the node's op routes it to
`load_external_table` or `copy_to`. Both run on the connection's engine
session — its place, its snapshot and its transaction; the S3 / file managers
only move the bytes:

- load → the target's ownership checked; the file read into chunks of at most
  1024 rows by `db::S3Manager::download` (s3://) or
  `conn::file::FileManager::load_file` (local); then `engine::create_table_plan`
  of the target as written (up to three parts, completed by the place as any CREATE TABLE:
  the database is not created, a missing schema is the engine's refusal, an
  existing table `table_already_exists`) and `engine::load_table`: outside a block
  BEGIN / CREATE / INSERT / COMMIT (the table with its rows or nothing), inside a
  block two more statements of it; a connection's table (`uid.ns.t`, or a short
  name in a place that is a connection's database) is created and filled in
  autocommit — two statements, not atomic — and inside a block as otterbrix's
  rules for a remote write in a block decide;
- copy → the inner SQL parsed on the session's own parser; only a query
  (`plan::is_query`: SELECT, VALUES, a set operation) and one without unbound
  parameters is accepted; it runs through `on_engine` (a missed table registered
  as for any statement) and its cursor goes to `db::S3Manager::upload` /
  `conn::file::FileManager::dump_file`. A write that fails after the query ran
  is reported to the engine as a failed statement (`settle`), so it fails an
  open block. The file or s3 object itself is not transactional.

Success is an empty payload under the statement's tag (`command_payload`);
`prepare_schema` answers an empty schema and the work runs at execute. Kafka DDL
(`kafka_node_t`) goes to `KafkaManager::execute` and cannot be prepared. An
INSERT is a Kafka write when its target (`kafka_write_target`) names a registered
object and, completed with the session's place (asked only then), is
`local.kafka.<obj>` (`names_kafka_object`, `integration/kafka/CLAUDE.md`,
"Routing"); an `INSERT … SELECT` into one goes to the Kafka manager as text.

### Describe (prepare)

`describe` sends the statement to the engine as a describe (`engine_call_t::describe`,
on `engine_session_`, through `on_engine`): the engine analyzes it as a run
would and answers right after — nothing planned for execution, no backend
reached (a remote table's columns are its catalog's). The payload is the result
columns as a STRUCT (NA for a statement without rows), the engine's type of every
`$n` (`parameter_types`) and the parameter count; the result types are the
engine's (count(*) UBIGINT, an integer sum BIGINT, avg DOUBLE), the types an
execution answers. RETURNING, sub-queries and set operations are the engine's
describe too. `describe_bound` describes with the client's values bound (a `$n`
keeps its value's type). A CREATE EXTERNAL TABLE / COPY is described as an empty
schema; Kafka DDL cannot be prepared.

### Tracy zones in coroutines

Tracy zones are per-thread and strictly nested. A session coroutine resumes after
`co_await` on whichever sessions-pool thread picks the actor up, so a zone that
spans a suspension point ends on the wrong thread or out of order and corrupts
the trace. Every handler therefore opens `OTX_ZONE_N` in a block that closes
before its first `co_await`; synchronous helpers — `parse_sql`, `parse_bound`,
`close_statement` (which never suspends), `close()`, `plan::*` — carry their own
zone as the first statement. The awaited actors measure their own work. This is
the coroutine exception to the root `CLAUDE.md` "first statement" rule: only
`assert`s, objects that must live for the whole coroutine (`Timer`) and the
variables the block fills (the future to await, held in a `std::optional` when
it has no empty state, the parsed query data, a refusal) precede the block. Work
after a `co_await` opens its own zone in a block of its own (`settle::tag`,
`prepare_schema::keep`, `register_missed::plans`, `run_on_engine::payload`), never
at function scope.

## Logger

`logger_tag::SESSION` ("Session").

## Tests

`tests/session/` (`test_session`) over the stack of `tests/system/session_stack.hpp`
(runtime → remote backends over mock connectors (`mock_connectors()`), the
configured connections registered → real engine in a temp dir opened with the
backend actors' map → the S3 / file / Kafka actors → session factory):
`test_session_pipeline.cpp` (a remote SELECT through the backend and the engine,
failure texts, N parallel connections, in-order completion, the parser boundary,
the ownership guard), `test_session_prepared.cpp` (reusable entries, a failed
execute keeping the entry, values per execute, close, per-connection isolation,
RETURNING, `describe_bound`, windows), `test_session_registration.cpp` (a missed
table registered on every route — execute, prepare, a DataFrame plan, COPY's inner
query, a short name in a connection's place — and inside a block registered
without the run once more; COPY's inner statement must be a query),
`test_session_settings.cpp` (server-wide settings refused on every SQL route, an
executed SET naming what it assigned), `test_session_external.cpp`,
`test_session_kafka.cpp`, `test_session_plans.cpp` (`execute_plan` /
`prepare_plan`), `test_session_subquery.cpp`, and `test_session_lifetime.cpp`:
`make_session_services` setting every address and refusing each empty one, the
close protocol, ordering across real suspensions, local transactions (ROLLBACK /
COMMIT, abort until ROLLBACK, two connections isolated), the engine's status on
every answer, SAVEPOINT / RELEASE / ROLLBACK TO, a SET of one session changing no
other, a SET of an unknown parameter being the engine's error, a SET inside a
block keeping it open and refused in a failed one, a session closed inside a
block rolling it back, and a statement over a connection's table refused in a
failed block.
