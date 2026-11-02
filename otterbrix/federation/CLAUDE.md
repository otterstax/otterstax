# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`otterbrix/federation/` (library `otterstax_federation`) is the core of the federation on otterbrix
61aa43d6 (`docs/embedding-host-api.md` in the otterbrix repo). A remote table is an ordinary row of
otterbrix's catalog in a database of a connection (`pg_database.datconnection`), named
`connection.database.schema.table` (`c.d.s.t`, every part lower case); otterbrix resolves every
name, plans every statement and reaches the table's rows only through the eighteen messages of its
storage contract (`services/disk/storage_contract.hpp`), sent to the address the map of
`open_engine` gives the connection. OtterStax answers them with ONE actor for every connection of
config.yaml, `backend_actor_t`, which runs SQL of the connection's dialect through the connection's
`backend_t` (the integration backends, `integration/`).

## Files

| File | What |
|------|------|
| `backend_actor.hpp/.cpp` | `backend_actor_t`: the loop (manager_disk_t's shape: `actor_mixin`, `services::loop_thread_t`, an inbox under a mutex, `co_await actor_zeta::dispatch`), the eighteen handlers, the host's side (`registration_session`, `set_names_table`, `load_tables`, `stage_table`, `stage_column`), `backend_addresses()` (the map `open_engine` takes), `max_rows_per_write` (PostgreSQL / MySQL 1000, ClickHouse 10 000) |
| `backend_actor_offer.cpp` | `storage_offer`'s probe and the statement of a take (`take_sql`) |
| `reachability.hpp` | `reachability_t`: the mark a failed connect of a connection leaves for 10 s, read by its agents before they connect |
| `conform.hpp/.cpp` | `conform_to_result`: a backend's answer held to the columns otterbrix planned, by otterbrix's own explicit casts |
| `provider.hpp` | `backend_t` (scan / write / discover / execute_ddl / transaction_step), `scan_request_t` / `write_request_t` (with the engine transaction the request runs on), `transaction_step_t`, `discovered_table_t` |
| `metadata.hpp` | `federation.public.remote_names` (the host table of the backends' spelling), `fold_identifier`, `remote_table_name_t`, `generator_name` |

## The actor

- **Loop.** `enqueue_impl` pushes the message into the inbox and wakes the loop; the loop starts each
  message's behavior and resumes the coroutines whose awaited future is ready. A `backend_t` future
  settles on a backends-pool thread that wakes nobody (actor-zeta has no waker: AZ-waker), so while a
  coroutine waits nothing polls: each backend's completion hook (`backend_t::set_completion_hook`,
  `statement_executor_t`) wakes the loop after a job settled its future; idle, the loop sleeps
  (a 1 s safety bound while something is in flight). Every handler's Tracy zone closes before
  its first `co_await` (the coroutines interleave on the loop thread).
- **Names.** otterbrix keeps lower case; the backend's spelling of every table and column is the
  actor's, keyed by the table's oid and the columns' attoids: `registered_table_t`. It is kept in
  `federation.public.remote_names(table_oid, attoid, remote_name, remote_container, remote_type)` — a
  table row `(oid, 0, table, database-or-schema, NULL)`, a column row `(oid, attoid, column, NULL,
  NULL)`; `remote_type` is for a column type a dialect needs (none yet) — which every DDL
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
  the statement's parameters (a rescan runs it again), `storage_run_write` runs a taken write (its
  count the backend's; with RETURNING the rows of the PostgreSQL statement, read as a scan).
- **Writes.** `storage_append`: one `INSERT … VALUES` per chunk (at most `max_rows_per_write` rows a
  message), `awaits_publish = false`. ClickHouse: a write inside a transaction block
  (`ctx.txn.command_id != 0`) is `unimplemented_yet`.
- **Transactions** (PostgreSQL / MySQL). A request of a transaction block, or of a transaction that
  already wrote, runs on the connection pinned to `ctx.txn.transaction_id`; an autocommit read takes
  any connection. BEGIN before the first statement of a block (a read too: the block is one backend
  transaction, one REPEATABLE READ snapshot) and before an autocommit write; `SAVEPOINT s<sub_id>`
  before the first statement of a savepoint level (a read too: a failed PostgreSQL statement aborts
  the backend transaction until a ROLLBACK TO a savepoint set before it); `storage_prepare` = (the
  deferred DROPs, then) COMMIT — refused, the transaction is left for the abort that follows, which
  rolls it back and frees the agent; `abort_transaction(0)` = ROLLBACK (or unpin for a connection
  pinned without BEGIN) plus the MySQL / ClickHouse compensations, the last first — each recorded
  only once its DDL ran; `abort_transaction(k)` = `ROLLBACK TO` the oldest backend savepoint of a
  level ≥ k, the later ones forgotten; `storage_release_prepared` without a prepare = ROLLBACK (a
  block that only read) or unpin. The agent marks its transaction broken by a failed statement
  (PostgreSQL: any; MySQL: `write_conflict` — a deadlock / lock wait timeout, rolled back whole — or
  a lost connection): only ROLLBACK (and on PostgreSQL the ROLLBACK TO a savepoint set before the
  failure) runs on it, its COMMIT is a ROLLBACK and refused, and a PostgreSQL COMMIT answered with
  the tag ROLLBACK is refused too. A statement waiting for its own transaction's agent has no
  checkout timeout (an end of the transaction given up would leave the agent pinned).
- **An unreachable connection.** A connect an agent tried before a statement or a discovery (its
  connection lost or never opened) that failed marks the connection unreachable for 10 s
  (`reachability.hpp`, owned by the actor, `set_unreachable_interval` for tests; no config key, no
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
  prepare. Every reply is `ddl_reply_t{false, remote_names rows}`.

## Contracts

- The actor lives in `backends::remote_backends_t`, spawned by `open()` over every configured
  connection (an unreachable one included: its statements fail at the connection), destroyed first
  in `~remote_backends_t` (after the engine). `remote_backends_t::primitives()` is the map.
- `backend_t` requests of one transaction must run on its pinned connection in the order they were
  started (B's integration backends); `scan` / `write` / `transaction_step` never block the loop.
- Nothing throws; a refusal is a `core::error_t` with the backend's code and text.

## Tests

`tests/federation/` (`test_federation`): a real otterbrix (`otterbrix::base_otterbrix_t`) opened with
the actor over fake backends (`federation_fixture.hpp`: canned tables, a log of every statement and
transaction step, pins counted, answers settled on the backend's threads on demand): registration
without SQL, restarts (no DDL, the spelling kept, a dropped table, a changed column, an unreachable
connection), registration after a miss, ownership; scans (2500 rows in chunks of ≤ 1024, projection
order, an empty table, a refusal, a JOIN with a local table, two sessions over asynchronous answers);
the offer per dialect (a SELECT block, GROUP BY, a JOIN of one connection, a residual WHERE,
UPDATE / DELETE whole, a refused UPDATE, RETURNING); transactions (autocommit, a block, ROLLBACK,
ROLLBACK TO twice, a reader's unpin, a refused write, ClickHouse, a client's DDL); `conform_to_result`.
