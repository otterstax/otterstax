# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`otterbrix/federation/` (library `otterstax_federation`) is the core of the federation on otterbrix
61aa43d6 (`docs/embedding-host-api.md` in the otterbrix repo). A remote table is an ordinary row of
otterbrix's catalog in a database of a connection (`pg_database.datconnection`), named
`connection.database.schema.table` (`c.d.s.t`, every part lower case); otterbrix resolves every
name, plans every statement and reaches the table's rows only through the eighteen messages of its
storage contract (`services/disk/storage_contract.hpp`), sent to the address the map of
`open_engine` gives the connection. OtterStax answers them with one actor per connection of
config.yaml, `backend_actor_t`, which turns each message into SQL of the connection's dialect
(`otterbrix/query_generation/`) and hands it to one of the connection's agents, `backend_agent_t`
(`pool_size` of them, one physical connection each, over the connectors of `connectors/`).

## Files

| File | What |
|------|------|
| `backend_actor.hpp/.cpp` | `backend_actor_t`: the loop (manager_disk_t's shape: `actor_mixin`, `services::loop_thread_t`, an inbox under a mutex, `co_await actor_zeta::dispatch`), the eighteen handlers plus `discover` and `end_session`, the agents' checkout (`acquire` / `release`, the waiters' queue), the host's side (`registration_session`, `set_names_table`, `load_tables`, `stage_table` / `stage_column` / `unstage_*`, `cancel_agents`), `backend_address()` (its entry of the map `open_engine` takes), `max_rows_per_write` (PostgreSQL / MySQL 1000, ClickHouse 10 000) |
| `backend_agent.hpp/.cpp` | `backend_agent_t`: one physical connection (agent_disk_t's shape: a `basic_actor` on the runtime's backends pool); messages `read` / `write` / `step` / `discover`, `connect` / `discover_now` (startup), `check_session` (every session it opens) / `session_refused`, `cancel`; the transaction statements of each dialect (`transaction_statement`), `mysql_update_rows_matched`, `pg_commit_rolled_back` |
| `backend_actor_offer.cpp` | `storage_offer`'s probe and the statement of a take (`take_sql`) |
| `reachability.hpp` | `reachability_t`: the mark a failed connect of a connection leaves for 10 s, read by its agents before they connect |
| `conform.hpp/.cpp` | `conform_to_result`: a backend's answer held to the columns otterbrix planned, by otterbrix's own explicit casts |
| `metadata.hpp` | `federation.public.remote_names` (the host table of the backends' spelling), `fold_identifier`, `remote_table_name_t`, `generator_name`, `discovered_table_t` / `discovered_tables_t` |

## The actor

- **Loop.** `enqueue_impl` pushes the message into the inbox and wakes the loop; the loop starts each
  message's behavior and resumes the coroutines whose awaited future is ready. An agent's answer
  settles on a backends-pool thread, and an actor-zeta future wakes nobody (AZ-waker), so the agent
  wakes the loop itself after it settled (`backend_agent_t::set_owner`); idle, the loop sleeps (a
  1 s bound while something is in flight). Every handler's Tracy zone closes before its first
  `co_await` (the coroutines interleave on the loop thread).
- **Agents.** Each agent owns its connector and the connector's `io_context`, which it runs on the
  pool thread for the length of one statement (the connector's deadlines and `cancel()` work as on
  any io_context); the actor sends an agent one statement at a time and schedules it on the backends
  pool (`runtime_t::backends_scheduler()`, sized in `config/CLAUDE.md`). A statement takes its
  transaction's agent, else a free one; with none free it waits in the actor's queue (no thread
  held) up to the checkout timeout (60 s, `set_checkout_timeout`; precision ~1 s, the loop's bound),
  then `io_error` "no free connection of … within N ms (pool_size K)". A statement waiting for its own
  transaction's agent has no timeout (an end of the transaction given up would leave the agent
  pinned). An agent pings before a statement outside a transaction and reconnects a connection that
  failed it; a pinned connection is not reconnected (it would lose the transaction).
- **Every session checked.** A session an agent opens — `connect()` at startup, every reconnect
  (a lost connection, `reset_session`) — is checked before it serves (`check_session`): MySQL
  `SELECT @@SESSION.sql_mode`, a mode with `NO_BACKSLASH_ESCAPES` refused (`unimplemented_yet`: the
  generated literals double a backslash, which such a session would read as two characters); whether
  the mode is strict (`STRICT_TRANS_TABLES` / `STRICT_ALL_TABLES`) is kept: a write whose statement
  leaves an UPDATE's narrowing to the store into the column (`sql_gen::statement_needs_t::strict_store`,
  carried by `storage_run_write` to the agent's `write`) is refused on a session that is not
  (`unimplemented_yet`, nothing sent; `otterbrix/CLAUDE.md`, "an UPDATE assignment's narrowing cast"). A
  refused session, or one whose check failed, is closed and the agent connects again for the next
  statement, which asks again: a server replaced or reconfigured while OtterStax runs refuses its
  statements with that error, none of their SQL sent, until it serves under another mode.
  `session_refused()` tells startup that the refusal is the server's answer (`unusable`), not a
  failure to reach it. PostgreSQL needs no check: a string literal holding a backslash is an escape
  string constant `E'…'` (`otterbrix/CLAUDE.md`, "Literals"), read the same under either
  `standard_conforming_strings`; ClickHouse has no such mode.
- **Discovery.** At startup (`remote_backends_t::open`) the first agent connects and discovers on the
  calling thread (`discover_now`) before any agent serves; afterwards `discover` — a message of the
  actor after the contract's eighteen, run by a free agent — answers the session's registration of a
  table a statement missed (`catalog/CLAUDE.md`).
- **Names.** otterbrix keeps lower case; the backend's spelling of every table and column is the
  actor's, keyed by the table's oid and the columns' attoids: `registered_table_t`. It is kept in
  `federation.public.remote_names(table_oid, attoid, remote_name, remote_container, remote_type)` — a
  table row `(oid, 0, table, database-or-schema, NULL)`, a column row `(oid, attoid, column, NULL,
  type)` — `type` the backend's own where the dialect reads by it (ClickHouse's `system.columns`
  type, `ch_to_chunk`'s declared types), else NULL — which every DDL
  reply writes as `host_rows_t` in the DDL's own transaction, and which the startup hands back
  (`load_tables`).
- **Reads.** `storage_fetch_next_batch` (a table offered nothing was taken of): `SELECT <projected
  columns> FROM <table> [LIMIT n]` (`sql_gen::generate_table_read`), the answer conformed, pages of at
  most 1024 rows of the cursor (cursor id never 0, an empty page ends it), `storage_close_cursor`
  forgets it. `storage_total_rows` answers an estimate without a query (1000; OTB-total-rows).
- **Offer.** An UPDATE / DELETE block is one statement or the offer is refused (`unimplemented_yet`:
  OtterStax numbers no remote row, `storage_update` / `storage_delete_rows` refuse); RETURNING only on
  PostgreSQL (every column of the table, `generation_options_t::returning`); ClickHouse inside a block
  refused. Read blocks, top down: a relation (a SELECT block, a JOIN tree of the connection) the
  dialect says whole is taken (its answer the node's `output_types`); else, for a SELECT block of one
  table, its WHERE is taken with the conjuncts the dialect says, the others left as `residual` (the
  answer is the block's `projected_cols`); else the children are tried. A take keeps the block
  (`blocks_`) until its `storage_release_block`; `storage_run_block` generates the statement again with
  the statement's parameters (a rescan runs it again; on ClickHouse an answer column of a relation taken
  whole that carries a table column's name and type is declared with that column's `system.columns` type,
  so a Tuple's field names, which the wire does not carry, are the registered ones — a STRUCT has no
  cast in otterbrix, and the page must be the declared type), `storage_run_write` runs a taken write (its
  count the backend's; with RETURNING the rows of the PostgreSQL statement, read as a scan).
- **Writes.** `storage_append`: one `INSERT … VALUES` per chunk (at most `max_rows_per_write` rows a
  message), `awaits_publish = false`. ClickHouse: a write inside a transaction block
  (`ctx.txn.command_id != 0`) is `unimplemented_yet`; a mutation (UPDATE / DELETE) that failed on a
  part is killed by the agent (`kill_failed_mutation`: `KILL MUTATION WHERE database / table /
  mutation_id` of the one mutation the error names, `ch_failed_mutations`; the table is the one
  `storage_run_write` hands the agent's `write`), then its error answered — a KILL that failed adds its
  own; several mutations named together are left, the error says so.
- **Transactions** (PostgreSQL / MySQL). A request of a transaction block, or of a transaction that
  already wrote, runs on the agent pinned to `ctx.txn.transaction_id` from its first request to its
  end; an autocommit read takes any agent. A block therefore holds one agent while it is open: more
  open blocks on a connection than its `pool_size` (default 4) make the next one wait up to the
  checkout timeout.
- **A closed connection** (`end_session(session)`, sent by `query_session_t::close` to every actor,
  not awaited). otterbrix's own ROLLBACK of a closed session is a statement for one of its executors,
  each of which runs one statement at a time (OTB-executor-wait), and every executor may be running a
  statement of another block waiting in this actor's queue for the agent the closed block holds: the
  ROLLBACK would wait for them until their checkout timeout. So the actor ends the session itself: its
  statements waiting for an agent are answered `connection_closed` at once and leave the queue; its
  transaction (any not preparing or prepared: a COMMIT that runs or ran is otterbrix's to end) gets
  ROLLBACK (or unpin) on the agent it holds — after the statement that agent runs, never in the queue
  of a free agent — then the compensations, and stays as a closed mark: every later request of it is
  refused an agent (`connection_closed`; an agent taken now would be pinned with nobody to end it),
  its COMMIT refused, and otterbrix's abort / release of it only forgets it. Each statement and each
  waiter carries its engine session (`ctx.session`, the scan's `session`). BEGIN before the first statement of a block (a read too: the block is one backend
  transaction, one REPEATABLE READ snapshot) and before an autocommit write; `SAVEPOINT s<sub_id>`
  before the first statement of a savepoint level (a read too: a failed PostgreSQL statement aborts
  the backend transaction until a ROLLBACK TO a savepoint set before it); `storage_prepare` = (the
  deferred DROPs, then) COMMIT — refused, the transaction is left for the abort that follows, which
  rolls it back and frees the agent; `abort_transaction(0)` = ROLLBACK (or unpin for an agent
  pinned without BEGIN) plus the MySQL / ClickHouse compensations, the last first — each recorded
  only once its DDL ran; `abort_transaction(k)` = `ROLLBACK TO` the oldest backend savepoint of a
  level ≥ k, the later ones forgotten; `storage_release_prepared` without a prepare = ROLLBACK (a
  block that only read) or unpin. The agent marks its transaction broken by a failed statement
  (PostgreSQL: any; MySQL: `write_conflict` — a deadlock / lock wait timeout, rolled back whole — or
  a lost connection): only ROLLBACK (and on PostgreSQL the ROLLBACK TO a savepoint set before the
  failure) runs on it, its COMMIT is a ROLLBACK and refused, and a PostgreSQL COMMIT answered with
  the tag ROLLBACK is refused too.
- **An unreachable connection.** A connect an agent tried before a statement or a discovery (its
  connection lost or never opened) that failed marks the connection unreachable for 10 s
  (`reachability_t`, owned by the actor, `set_unreachable_interval` for tests; no config key, no
  timer): until then every agent that would have to connect answers at once `io_error` `connection
  "<alias>" is unreachable …` without trying; the first one after it tries again, and a connect that
  succeeds clears the mark. A connection that is connected is not affected.
- **Columns.** A client's ADD COLUMN / DROP COLUMN changes the actor's columns of the table only for
  its own transaction until it ended (`registered_column_t::added_by` / `dropped_by`,
  `visible_columns`): `projected_cols` are positions among the columns of the scan's snapshot, which
  in otterbrix keeps the dropped column and lacks the added one for every other transaction until the
  COMMIT. Applied at `storage_release_prepared` after a prepare, undone otherwise; a table a
  transaction created is forgotten again if it did not commit.
- **DDL.** On the registration session (`registration_session()`, catalog/registration.hpp) a
  CREATE / DROP / ADD COLUMN / DROP COLUMN only records the table (the spelling staged by the
  registration). On a client's session: PostgreSQL in the backend transaction (COMMIT at prepare);
  MySQL / ClickHouse at once with a compensating DROP at abort; a DROP TABLE / DROP COLUMN waits for the
  prepare. Every reply is `ddl_reply_t{false, remote_names rows}`. CREATE / DROP INDEX of a remote
  table never reaches the actor: otterbrix refuses it (`unimplemented_yet`, OTB-storage-index).

## Contracts

- The actors and their agents live in `backends::remote_backends_t`: `open()` builds `pool_size`
  agents and one actor per configured connection — an unreachable one included (its agents connect
  on their first statement, its statements fail at the connection), one whose server cannot be
  written to left out (a MySQL session under `NO_BACKSLASH_ESCAPES`, an agent's
  `check_session`: no actor, no address, a statement naming it refused by the engine); `remote_backends_t::primitives()` is
  the map (one address per connection). Shutdown, after the engine: `cancel_agents()` on every actor
  (the running statements aborted through the connector's `cancel()`, `connectors/CLAUDE.md`; later
  ones refused) → `runtime_t::stop_backends()` → the actors and agents destroyed. A MySQL reconnect in
  progress is ended by `cancel()` too.
- The requests of one transaction run on its agent in the order they were started; the loop never
  blocks on an agent.
- Nothing throws; a refusal is a `core::error_t` with the backend's code and text.

## Tests

`tests/federation/` (`test_federation`): a real otterbrix (`otterbrix::base_otterbrix_t`) opened with
one actor per connection over agents on fake connectors (`federation_fixture.hpp`,
`tests/mock/fake_backend.hpp`: canned tables, a log of every statement and transaction step, answers
settled on the pool's threads on demand): registration
without SQL, restarts (no DDL, the spelling kept, a dropped table, a changed column, an unreachable
connection), registration after a miss, ownership; scans (2500 rows in chunks of ≤ 1024, projection
order, an empty table, a refusal, a JOIN with a local table, two sessions over asynchronous answers,
the loop asleep while a statement runs, the checkout queue and its timeout, an unreachable
connection);
the offer per dialect (a SELECT block, GROUP BY, a JOIN of one connection, a residual WHERE,
UPDATE / DELETE whole, a refused UPDATE, RETURNING, an UPDATE narrowing its value to the column on a
strict and a non-strict MySQL session and on ClickHouse); transactions (autocommit, a block, ROLLBACK,
ROLLBACK TO, a reader's unpin, a refused write, ClickHouse, a client's DDL and its refusals, a broken
PostgreSQL / MySQL transaction); `conform_to_result`. `tests/unit/connectors/test_backend_agent.cpp`
(`test_unit_connectors`): the agent's transaction statements per dialect, a failed COMMIT, cancel, a
write relying on a strict `sql_mode` refused on a session without one, and
the actor's checkout (a transaction's agent, the queue, `pool_size` open blocks, the shutdown, a closed
connection's block ended on its agent and its waiting statement out of the queue). Session level
(`tests/session/test_session_lifetime.cpp`): a closed block's agent serving the blocks that wait for
it while every engine executor runs one of them, a waiting statement leaving the queue with its
connection's close.
