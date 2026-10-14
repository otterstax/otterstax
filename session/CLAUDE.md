# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`session/` (CMake target `session`) is the query pipeline of the server:
`otterstax::session::query_session_t`, ONE actor per client connection (a wire
connection, a Flight SQL RPC or prepared statement, a Spark session), and the
`session_factory_t` the frontends open them from. Every statement a client sends
goes parse → database-DDL guard → (remote table DDL: its backend) → engine on its
connection's session, a remote table resolved by the engine through the
federation; a statement that misses a remote table (created on the backend after
it was discovered, or whose columns changed) rediscovers it and runs once more.
There is no router and no shared pipeline actor in front of it.

## Files

| File | What |
|------|------|
| `query_session.{hpp,cpp}` | The actor, `session_services_t` (everything it sends to) and `make_session_services` (builds it, refusing an empty address), `session_ptr` / `session_closer_t`; `schema_payload` (a prepare's answer) and `command_payload` (a statement that ran without rows — remote DDL, CREATE EXTERNAL TABLE / COPY ... TO, Kafka — under its tag), both anonymous in the `.cpp`; an executed result is built in `run_on_engine` |
| `session_factory.{hpp,cpp}` | `session_factory_t`: opens sessions and holds the ones that exist (`add` / `forget` / `reap` private, `forget` called by the session as it frees itself); its destructor is the sessions' shutdown (drain, `runtime_t::stop_sessions`, reap) |
| `plan_inspection.{hpp,cpp}` | Pure plan readers: extension root, database DDL target, table DDL (`table_ddl`, by the parser's written `ddl_target`), `statement_names` (every name as written, for a rediscovery), `plan_dependencies` (root only), RETURNING |
| `session_payload.hpp` | `session_payload`: schema, chunks, `parameter_count`, tag, `affected_rows`; `reported_rows()` |
| `session_ids.hpp` | `statement_id_t` (a strong id), `to_hash` |
| `statement_kind.hpp` | The statements told by their raw parse tree before the engine sees them, each parsed only when its first keyword (`first_keyword` / `keyword_is`: past blanks, `;` and comments, without case — a lexical look, no parse) can begin it, so any other statement is parsed once, by `parse_sql`: `is_setting_statement` (SET / RESET — answered by the session without the engine), `deallocate_statement` (DEALLOCATE — the PostgreSQL frontend's, never sent to a session) and `is_transaction_exit_statement` (COMMIT / ROLLBACK — what the PostgreSQL frontend lets a Parse / Bind take in an aborted block) |
| `transaction_state.hpp` | `transaction_state_t` (idle / in_block / failed), `transaction_command_t` (BEGIN / COMMIT / ROLLBACK), `next_transaction_state` — the engine dispatcher's block rules, as a pure function |

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
  `co_await`. The mailbox is the statement queue; there is no second queue. This
  is also what keeps the engine dispatcher from running two statements of one
  session in parallel inside an explicit transaction.
  `tests/session/test_session_lifetime.cpp` ("one statement at a time across real
  suspensions") pins it with every statement sent before any answer is read, the
  engine on its own pools and a slow backend on the connector pool.
- **Waiting is a poll (OTB-notify).** actor-zeta 2.0.0 settles a future by
  flags alone and wakes nobody: while the session is suspended on the engine's
  future, `basic_actor::resume` answers `resume` and the pool requeues the job at
  once (`resume_job_later` → `enqueue` → `notify_one`), about 210 000 times a
  second. A statement that runs for a second costs a core of the sessions pool
  besides the engine's own work (the ClickHouse JOINs of the benchmark are
  +21–23% slower than before the migration for that reason alone). Nothing the
  session owns can be woken without polling, so the session keeps the plain
  `co_await` and the cost stays until actor-zeta / otterbrix wake an actor when
  the future it awaits settles (`docs/proposals/actor-zeta.md`, AZ-waker / AZ-park;
  the engine's side in `docs/proposals/otterbrix-embedding.md` §4.13).
  The hidden `[.engine-defect-otb-notify]` case in `test_session_lifetime.cpp`
  asserts the idle wait.

### Entry points

All answer `unique_future<core::result_wrapper_t<session_payload>>`:

| Handler | What |
|---------|------|
| `execute(statement_id_t, std::string sql)` | parse and run |
| `execute_setting()` | a SET in a frontend's own grammar (MySQL `SET @@session.x = v`): answered like every SET, without effect |
| `prepare_schema(statement_id_t, std::string sql)` | parse and describe; the entry stays for execute / close |
| `execute_statement(statement_id_t)` | run the prepared entry (consumes it) |
| `execute_prepared_statement(statement_id_t, std::pmr::vector<logical_value_t>)` | bind, then run the entry (consumes it) |
| `close_statement(statement_id_t)` | drop an entry that will never run; idempotent |
| `execute_plan(statement_id_t, ParsedQueryDataPtr)` | run a pre-built plan (Spark) |
| `prepare_plan(statement_id_t, ParsedQueryDataPtr)` | describe a pre-built plan without running it |
| `fail_transaction(core::error_code_t, std::string)` | a failure the frontend found itself (a protocol error, a value it cannot bind, a statement it stopped waiting for): reported to the engine on the connection's session, so it fails an open block; answers the error |

Besides those, `end_session()` is queued by `close()` itself (below), and
`transaction_state()` — a plain member, not a message — answers the block the
connection is in after the last statement that ended; a frontend reads it after
awaiting a statement (the result is published before the future is ready).
`ended_failed_block(error)` reads the same block: the statement just awaited
was a COMMIT of a failed block (`transaction_finalized` and the block idle
afterwards) — what the MySQL frontend answers with an OK packet and the
PostgreSQL frontend with `CommandComplete ROLLBACK`.

`statement_id_t` is fresh per statement: a frontend takes each one from the
session itself (`next_statement()`, a public atomic counter from 1, callable from
any thread); the connection is the session itself. The integration
actors still get the statement id as their per-call `session_hash_t` (`to_hash`).

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
- **The open block ends with the session.** otterbrix has no close-session
  call, so an engine session left inside BEGIN keeps its transaction for good.
  `close()` therefore queues `end_session` — before it sets the closed bit,
  behind everything already sent — whenever the session may end inside a block:
  a block is open, or a job is counted (the statement in flight may be a BEGIN).
  `end_session` runs once that statement ended and sends ROLLBACK on the
  connection's engine session if the block is not idle; the engine's ROLLBACK
  clears everything it keeps for the session (the transaction, the failed mark,
  the session's statement order). An idle session with no job is freed at once,
  as before. A session whose job a stopped pool dropped at shutdown (freed by
  `reap()` without running `end_session`) is not rolled back.
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
session `reap` frees. Only then the Kafka / S3 / file actors and the engine,
declared before the factory, which those statements still talk to.

`tests/session/test_session_lifetime.cpp` covers an idle close (freed at once),
a close with a statement in flight (it finishes with its result, the one queued
behind it answers "the connection is closed", the session is freed after), and
`tests/system/test_frontend_transactions.cpp` a client dropping its socket
mid-statement on both wire servers.

### `session_services_t`

A plain aggregate of the addresses a session sends to: `engine_dispatcher` (the
address every request of `otterbrix/operators/engine_requests.hpp` names —
`execute_statement`, `describe`, `send_plan` of a bookkeeping
plan, `probe_declared_columns` of a remote relation — called by the session
directly), `s3` (`db::S3Manager`), `file` (`conn::file::FileManager`) and `kafka`
(`otterstax::kafka::KafkaManager`). The remote backends
are not among them: rediscovery and remote DDL call the `federation::backend_t` the federation
registry holds for the uid (`federation::registry().find(uid)` → `discover` / `execute_ddl`, run on the
session's thread), and the engine reaches the same objects for every remote read and write. No
address may be empty (an actor-zeta send to an empty address dereferences null):
`make_session_services(resource, engine_dispatcher, s3, file, kafka)` builds it
once (`main.cpp`, from the actors it owns) and answers `invalid_parameter`
naming the first empty address; the `query_session_t` constructor asserts each.
Every session gets a copy.

### `session_factory_t`

`session_factory_t(runtime, parser_factory, services)` (the runtime's resource and
sessions pool; the runtime's `stop_sessions()` in the destructor, above);
`open()` builds a session with its own parser (`parser_factory`), its own engine
session and a copy of the services, registers it and answers the `session_ptr`.
`live_sessions()`, `sessions_in_block()` (the live sessions inside a transaction block, open or failed —
what the factory's shutdown warning reports, and what a test reads to prove a block was ended rather than
merely hidden by MVCC) and `wait_closed(timeout)` read the sessions the factory holds. `main.cpp`
owns one and hands it to every server (`frontend_server_config::sessions`,
the Flight SQL `SessionEngine`, the Spark service).

## The engine session and transactions

Each query session owns `engine_session_` (`session_id_t::generate_uid()`).
Every statement of the connection runs on it, so BEGIN / COMMIT / ROLLBACK over
engine tables work, with the engine's PostgreSQL semantics on every protocol:

- after an error inside an explicit transaction the block is aborted: every
  statement but ROLLBACK is refused (`transaction_finalized`, "the transaction
  failed; only ROLLBACK is accepted until it ends");
- a COMMIT of the failed block runs the rollback and answers
  `transaction_finalized` "the transaction failed and was rolled back; nothing
  was committed". The PG frontend answers it with the `ROLLBACK` tag and
  ReadyForQuery `I`; the MySQL frontend with an OK packet. Both tell it from a
  refusal by the code and the session's block afterwards (idle), never by text.
- **settle.** A statement that fails BEFORE it reaches the engine (parse, bind,
  the ownership guard, remote DDL) is reported on the connection's engine session
  (`settle` sends `manager_dispatcher_t::refuse_statement` to the engine dispatcher), so it
  fails an open transaction exactly as an execution error does. Every entry ends
  in `settle()`; an error that came from the engine is reported a second time
  harmlessly. Every prepare failure — a text that does not parse included —
  fails an open block, as PostgreSQL's own Parse error does. (A prepare never
  sees a MySQL `?`: the MySQL frontend writes the placeholders as `$n` before
  its first prepare, so nothing is retried under another spelling.)
- **A prepare is not outside the transaction.** The describe of a statement
  (`describe_by_engine`, the RETURNING probe of a local target) runs on
  `engine_session_`, not on a session of its own: a describe must see what the
  connection's transaction sees — a table the transaction created is in the
  catalog only for its own transaction (catalog rows are MVCC rows; otterbrix
  `agent_disk.cpp`: "{0,0} can't see this txn's own uncommitted catalog row").
  So the engine refusing a describe has failed the block already, and settle
  only follows it. A describe in a failed block is refused (`25P02` on the PG
  wire).
- **The block the session follows.** The engine keeps the block and answers no
  question about it (otterbrix has no "transaction status of a session" call:
  proposal item). The session follows it in `transaction_` from what every
  statement was (`command_`, read off the plan root in `run_parsed`:
  `plan::transaction_command`) and how it ended, in `settle()`:
  `next_transaction_state` (`transaction_state.hpp`) mirrors the dispatcher —
  BEGIN opens (a stray one keeps), COMMIT / ROLLBACK end (COMMIT of a failed
  block too, and a COMMIT that fails on its own: the commit operator drains the
  transaction — `txn_commit_drain_msg` commits or aborts it — before any step
  that can fail, so the engine session is idle after it and the session follows,
  never reporting a failed block the engine no longer has), an error fails an
  open block, a `transaction_finalized` refusal is a failed block whatever was
  believed. A success of a prepare changes nothing.
  An executed BEGIN / COMMIT / ROLLBACK's payload names it (`session_payload::transaction`,
  the CommandComplete tag). Every frontend reads the block from here: the PG
  ReadyForQuery status on the simple and the extended route alike, the MySQL
  failed-COMMIT answer, the Flight SQL BEGIN refusal, the Spark timeout.
- **SET never reaches the engine.** otterbrix applies a `SET` to the settings
  every session reads (`manager_dispatcher_t::default_settings_`,
  `dispatcher.cpp`) and persists it as a `pg_settings` row
  (`operator_set_setting_t`): one connection's `SET autocommit TO off` would put
  every other connection out of autocommit, across restarts. So a `SET` /
  `RESET` / `SET … TO DEFAULT` of any parameter is answered here without effect
  (`answer_setting`: no rows, no count, tag `T_VariableSetStmt`) on every entry
  point — `execute`, `execute_setting` (a SET a frontend recognised in its own
  protocol's grammar, which the engine's cannot read: the MySQL frontend's
  `SET @@session.autocommit = OFF`, `SET GLOBAL x = 1`, `SET a = 1, b = 2`;
  the frontend sends no text), `prepare_schema` + `execute_statement` /
  `execute_prepared_statement` (the entry is marked `setting`, no plan), and a
  plan whose root is `set_setting_t` in `run_parsed` / `describe_parsed`. The
  statement is told by its raw parse tree (`is_setting_statement`,
  `statement_kind.hpp`: one `VariableSetStmt`), not by the text and not only by
  the plan, because the engine's transformer refuses most forms before any plan
  exists (a numeric value, `RESET`, an unknown parameter). Only a text whose
  first keyword is SET or RESET is raw-parsed for it; every other statement
  goes straight to `parse_sql`, parsed once. An open block stays
  open; in a failed block it is refused as the engine refuses the rest. Passed
  on to the engine once otterbrix keeps settings per session
  (OTB-session-settings). `SHOW` is not intercepted (the engine has no SHOW).
- **What does not run on the engine is refused in a failed block.** Remote DDL,
  a four-part DML (`run_parsed`), CREATE EXTERNAL TABLE / COPY ... TO and Kafka
  statements (`execute_body`, `execute_statement_body`) do not run on
  `engine_session_`, so the engine cannot refuse them: `refuse_in_failed_block()`
  answers the engine's own `transaction_finalized` text for them in a failed
  block. Inside an open block they still run outside it (a backend's DDL / DML
  commits on its own; the file manager has its own engine session).
- The RETURNING probe of a remote
  target and the federation metadata a rediscovery writes (`apply_metadata`:
  BEGIN / plans / COMMIT) run on a second engine session (`probe_session_`), so
  none of them touches the connection's transaction. A metadata write that
  fails is rolled back (`end_probe_transaction`); a probe session whose
  ROLLBACK fails is abandoned for a fresh one, so a stuck block there never
  refuses the later rediscoveries.

## Pipeline

### Prepared-statement lifecycle

Prepared statements live in the session's own `prepared_` map (statement id →
`prepared_t`: the described schema the rows are answered under, the parsed
data, a SET mark); a direct `execute` / `execute_plan` keeps no entry. A prepared statement is
single-use: the first `execute_statement` / `execute_prepared_statement`
consumes it — the entry is erased on every exit path, success or failure — and
any later execute answers `invalid_parameter` "prepared statement must be
re-prepared" (the frontends re-prepare under the hood). `close_statement` erases
an entry the frontend will never execute; closing an unknown, consumed or closed
entry is success. A direct `execute` of a statement with placeholders is refused
(`invalid_parameter`, "no values bound"); an execute without Bind of a
parameterized entry is refused before the engine ("prepared statement executed
without binding its N parameter(s)"). The map is the connection's: a statement
prepared on one connection is unknown on another.

### Running a statement

`run_parsed` takes every parsed statement (SQL and pre-built plans alike):

- the database-DDL guard (below);
- a table DDL statement — `CREATE TABLE`, `CREATE INDEX`, `DROP TABLE`, `DROP INDEX` — whose target
  (`plan::table_ddl`, the parser's written `ddl_target`) resolves to a connection
  (`federation::resolve_written_name`) is remote DDL: DDL never reaches the engine's host hooks, so the
  session generates it in the backend's dialect (`sql_gen::generate_query` over
  `federation::generator_name`) and runs it through the server's `backend_t::execute_ddl` (the
  registry's server for the resolved uid), then
  the metadata follows — a created table is rediscovered, a dropped one's rows are removed. The answer is
  an empty payload under the statement's tag (`command_payload`: the PG CommandComplete reads
  `CREATE TABLE` / `DROP` / `CREATE INDEX` off it). Waits for otterbrix (`CLAUDE.md`, Known Constraints);
- an INSERT / UPDATE / DELETE whose target is written in four parts (`uid.db.schema.table`, the
  parser's `dml_target`) runs on its backend directly (`run_remote_dml`): the engine's transformer
  keeps no uid slot on a DML target, so the statement is generated in the backend's dialect from the
  parsed plan (bound values included) and sent through the server's `backend_t::write`; the answer is
  the backend's count. A source on another server or a local one is refused by the generator. A
  RETURNING list is refused (`unimplemented_yet`, at execute and at prepare): the backend's write
  answers a count, and the rows are never read back rather than dropped silently. Waits for otterbrix
  (a uid slot on a DML target); the three-part form runs on the engine;
- everything else runs on the engine (`run_on_engine` → `engine::execute_statement` on the
  connection's engine session). A remote table is resolved and read or written by the engine through
  the federation (`otterbrix/federation/CLAUDE.md`): a subtree over the tables of ONE server that its
  dialect can say — a query block over one table, a JOIN of its tables with the block above it, a
  derived table — and an INSERT / UPDATE / DELETE of one table run on the backend as one statement; a
  JOIN of tables of several servers, or of remote and local tables, runs in the engine, each server's
  own subtree still pushed whole and a bare remote table read through its storage's scan. Engine errors come back
  under the prefix `Otterbrix execution failed: ` (a backend's own error too: it reaches the client
  through the engine, its text whole).

**Rediscovery, one retry.** When the engine refuses a statement because a remote name is missing from
the federation metadata, or a remote table answered other columns than declared,
`catalog::rediscovery_targets` names the targets among the statement's names
(`plan::statement_names`). For each, the session claims the key (`rediscovery_claim_t`: a second session
missing the same table at once skips the discovery — no second copy of its rows is written — and its
retry sees the rows if the first session has written them by then, else answers its original error), reads
the backend's spelling of a known table (`make_remote_table_read`), calls `discover` on the uid's
backend (`federation::registry().find(uid)`; no server answers the reply `invalid_parameter` "no
backend"), turns the reply into upsert plans and applies them on `probe_session_`; then the statement is
parsed again (`rebuild`: a plan is single-use; for a prepared statement the bound values are bound
again) and run once more. A retry refused as `transaction_finalized` (the first failure aborted the
block) answers the original error; anything that fails on the way answers the original error. A
pre-built plan (Spark) has no text to parse again and is not retried.

### Result shape

`run_on_engine` builds the payload from the engine cursor: chunks
(at least one, so `front()` is defined) and, for a write cursor
(`cursor->is_write()`), `affected_rows = cursor->affected_rows()`.
`session_payload::reported_rows()` is the row count of a result with columns and
`affected_rows` of a column-less one — what the frontends put in CommandComplete
/ the OK packet. A row-producing statement whose result has no columns is a
`schema_error` naming the statement (`engine::execute_statement`), never a
column-less payload a frontend would report as a DML.

### Pre-built plans (`execute_plan` / `prepare_plan`)

The Spark Connect frontend translates a DataFrame into a `ParsedQueryData`
itself, so these two skip `parse_sql` and join the SQL entry points where
parsing ends: `execute` and `execute_plan` share `run_parsed` (above);
`prepare_schema` and `prepare_plan` share `describe_parsed`. `prepare_plan`
stores its entry until an execute or `close_statement` (the Spark `AnalyzePlan`
closes it at once). Both refuse a missing plan and a grammar-extension root
(`extension_kind` set) with `invalid_parameter`.

### Database-level DDL guard

`CREATE DATABASE` / `DROP DATABASE` are engine-local by grammar, but three kinds
of name belong to OtterStax: a connection uid (an engine database of that name
would capture the connection's remote names), the kafka object database and the
federation metadata database; `DROP DATABASE` is CASCADE. `guard_database_ddl`
runs on every entry point that runs or describes a plan, reads the target name
off the plan and asks `catalog::check_database_ownership`, which answers
`invalid_parameter` (`database '<name>' is owned by connection '<owner>'`)
before the statement reaches the engine.

### External-table and Kafka statements

`CREATE EXTERNAL TABLE … WITH (location=…)` and `COPY (<select>) TO '<location>'`
parse into an `otterstax::external::external_node_t`; `extension_root()` reads
`ParsedQueryData::extension_kind` and the node's op routes it — to
`load_external_table` or `copy_to` — before the backend / schema logic:

- create → `db::S3Manager::download` (s3://) or `conn::file::FileManager::add_file` (local);
- copy → the inner SQL re-parsed on the session's own parser and handed to
  `db::S3Manager::upload` / `conn::file::FileManager::dump_file`, which execute
  it on the file manager's OWN engine session (so outside the connection's
  transaction, as the load of `CREATE EXTERNAL TABLE` is).

Success is an empty payload under the statement's tag (`command_payload`, as
every statement that answers no rows: remote DDL, Kafka); `prepare_schema`
answers an empty schema and the work runs at execute. Kafka DDL (`kafka_node_t`) and `INSERT INTO kafka.<obj>`
go to `KafkaManager`; Kafka DDL cannot be prepared.

### Describe (prepare)

- **Every parameter bound, not a set operation:** the engine's own plan
  validation (plan-only EXPLAIN) through `engine::describe` — no execution, no
  backend round-trip: a remote table's columns come from
  `federation.remote_columns`, and the result types are the engine's (count(*)
  UBIGINT, an integer sum BIGINT, avg DOUBLE, the engine's arithmetic and
  function types), the types the execution holds a pushed-down answer to. The
  planned plan is rewritten in place (the optimizer rules, the pushdown among
  them), so the session describes a second parse of the statement's text and
  keeps the first for execute.
- **Otherwise (parameters, set operations):** the federated computation of
  `otterbrix/schema/schema_utils.hpp`: one probe per relation the statement's
  ROOT reads (`plan::plan_dependencies`, positional) — a `LIMIT 0` read of a
  local relation, a plan-only EXPLAIN of a read of a remote one
  (`otterstax::engine::probe_declared_columns`) — and the projection resolved against that
  answer. `SELECT *` is expanded into the table's columns; a relation the engine
  does not hold fails at prepare (`42P01` territory).
- **RETURNING** of a DML is described from the target relation's probe (a
  remote target written in three parts through `probe_declared`); on a
  four-part target it is refused (`unimplemented_yet`), as at execute.
- **Remote DDL** is described as an empty schema; the statement runs at execute.
- **Scalar sub-queries.** A parameterized statement whose SELECT list holds a
  scalar sub-query (`SELECT id, (SELECT max(id) FROM t2) AS top ... WHERE id = $1`)
  describes that column as the root's computation answers it: NA (the column
  reads the parameter the sub-query's result is bound into when it runs). The
  engine types it once it describes a statement with `$n`
  (`docs/proposals/otterbrix-embedding.md`, OTB-scalar-subquery-type). A
  relation only a sub-query reads is not probed, so one missing from the engine
  does not fail the prepare.

### Tracy zones in coroutines

Tracy zones are per-thread and strictly nested. A session coroutine resumes after
`co_await` on whichever sessions-pool thread picks the actor up, so a zone that
spans a suspension point ends on the wrong thread or out of order and corrupts
the trace. Every handler therefore opens `OTX_ZONE_N` in a block that closes
before its first `co_await`; synchronous helpers — `parse_sql`,
`finish_schema_value`, `close_statement` (which never suspends), `plan::*` —
carry their own zone as the first statement. The awaited actors measure their own work. This is
the coroutine exception to the root `CLAUDE.md` "first statement" rule: only
`assert`s, objects that must live for the whole coroutine (`Timer`,
`erase_on_exit_t`) and the variables the block fills (the future to await, held
in a `std::optional` when it has no empty state, the parsed query data) precede
the block. Work after a `co_await` opens its own zone in a block of its own
(`settle` → `settle::follow`, `describe_parsed::returning`,
`run_on_engine::payload`), never at function scope.

## Logger

`logger_tag::SESSION` ("Session").

## Tests

`tests/session/` (`test_session`) over the stack of `tests/system/session_stack.hpp`
(runtime → remote backends with mock connectors, the configured connections
discovered → real engine in a temp dir opened with the federation's primitives →
the S3 / file / Kafka actors, the federation metadata published → session factory): the pipeline over the mock MySQL connector and its failure texts, N
parallel connections, in-order completion of back-to-back statements, the
prepared lifecycle and its per-connection isolation, sub-queries (the describe
reading the root only), external tables and COPY, Spark DataFrame plans,
`prepare_plan` / `execute_plan` over a recording PostgreSQL mock, and
`test_session_lifetime.cpp`: `make_session_services` setting every
address and refusing each empty one, the close protocol, ordering across real suspensions, and local
transactions (ROLLBACK / COMMIT, abort until ROLLBACK after an engine error and
after a parse error, COMMIT of a failed block, two connections isolated), and the
block the session follows (`next_transaction_state`; `transaction_state()`
through BEGIN, a failure — engine, refused prepare, `fail_transaction` — the
refusals and COMMIT, a COMMIT that fails on its own leaving the session idle; a
prepare that does not parse failing an open block; a MySQL-form SET through
`execute_setting` answered like any SET; SAVEPOINT refused; a
session closed inside a block rolling it back — proven by a COMMIT sent on its
engine session afterwards publishing nothing, since MVCC hides the rows of a block
left open just as well; a SET of one session, in any form, answered without effect
and changing no other session (another session's INSERT stays autocommitted); a
SET inside a block keeping it open, refused in a failed one; remote DDL
and a four-part DML refused in a failed block; RETURNING on a four-part target
refused).
