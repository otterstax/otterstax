# Embedding otterbrix in OtterStax: what the host still needs from the engine

> **Status:** proposal for the otterbrix maintainers (dispatcher, executor, planner, transformer, disk). Requests to
> actor-zeta are in [`actor-zeta.md`](actor-zeta.md).
>
> **Base:** branch `feat/otterstax-requests` @ `61aa43d6` (PR #850 "OtterStax requests and one storage road") and its
> host guide `docs/embedding-host-api.md`. OtterStax runs on it as the local package
> `otterbrix/1.0.0b2-embedding.af99cccf` (`61aa43d6` plus the clone / `count(*)` fix `39ca5e96` and the `count(NULL)` fix `af99cccf`); the port is otterstax/otterstax#55 (closing sessions: otterstax/otterstax#53),
> not merged yet.

References:

- `ob:<path>:<line>` — otterbrix `61aa43d6`; `ob-doc:<line>` — `docs/embedding-host-api.md` at the same commit;
- `otx:<path>[:<line>]` — OtterStax, the port branch (`e4ef465`), for illustration only;
- `#N` — `otterbrix/otterbrix`; `otterstax#N` — `otterstax/otterstax`.

Each open request has: **Request**, **Why** (the OtterStax scenario), **Evidence**, **OtterStax workaround** ("none":
the client gets the engine's refusal), **Blocks the port**:

- `yes` — the port is merged with a known breakage of an ordinary statement, listed in OtterStax's Known Constraints;
- `regression` — on `61aa43d6` OtterStax loses a feature or speed it had on `46fba865`;
- `no`.

Priorities: **P0** — blocks the port (`yes`) or clients cannot connect; **P1** — a regression, a wrong answer, or a
workaround that costs host code; **P2** — nice to have.

---

## 1. Summary

| ID | P | What | Blocks the port | Issue |
|---|---|---|---|---|
| OTB-tag | P0 | a release tag with #850 on the conan remote | no (CI, release) | — |
| OTB-append-omitted | P0 | `storage_append` carries only the written columns; otherwise backend DEFAULT / serial is lost | yes | — |
| OTB-driver-settings | P0 | the `SET` / `SHOW` / `BEGIN …` drivers send on connect | no | — |
| OTB-txn-storage | P1 | rest of #663: in-doubt window; connection DDL inside a block; one load as one statement | regression | #663 |
| OTB-ctx-block | P1 | an "inside a block" flag in `execution_context_t` | no | — |
| OTB-ctx-scan | P1 | `execution_context_t` in `storage_fetch_next_batch` / `storage_close_cursor` | no | — |
| OTB-insert-block | P1 | INSERT / INSERT … SELECT within one backend offered as a block | regression | — |
| OTB-no-row-ids | P1 | a "no row numbers" flag in `backend_address_t` | no | — |
| OTB-storage-index | P1 | CREATE / DROP INDEX on a connection's table through the contract | regression | — |
| OTB-abort-refused | P1 | a backend's refused rollback is an error, not a log line | no | — |
| OTB-error-abi | P1 | the layout of `core::error_t` depends on `NDEBUG` | no | — |
| OTB-show | P1 | `SHOW` / `SHOW ALL` / `current_setting` / `set_config` | no | — |
| OTB-discard | P1 | `DISCARD ALL` / `PLANS` / `SEQUENCES` / `TEMP` (session reset by connection pools) | no | — |
| OTB-idle-timeout | P1 | idle-in-transaction timeout, `statement_timeout` | no | — |
| OTB-cancel | P1 | cancel a running statement | no | — |
| OTB-qualifier-text | P1 | a column qualifier is compared as written text | no | — |
| OTB-describe-untyped | P1 | `$n` in UNION / CTE / derived table answers 42P18 | regression | — |
| CTAS | P1 | `CREATE TABLE … AS SELECT` | no | #675 |
| OTB-sink-root | P1 | COPY / S3 / file / Kafka as storages behind the contract | no | — |
| OTB-create-options | P1 | CREATE TABLE WITH options reach `create_storage_disk` | no | — |
| OTB-numeric-literal | P1 | `SELECT 0x41` is a column `x41` with value 0 | no | — |
| OTB-assignment-cast | P1 | an UPDATE assignment's cast to the column type marked as such (`cast_kind`) | no | — |
| OTB-notify | P1 | loops without timer polling; JOIN build sides at once | no | #693 |
| OTB-executor-wait | P1 | a statement waiting on a storage does not hold an executor from a transaction's end | no | — |
| OTB-register-table | P2 | register / forget a connection's table without backend DDL | no | — |
| OTB-error-object | P2 | parts of a missing name and the blocking object's oid as error fields | no | — |
| OTB-total-rows | P2 | `storage_total_rows`: with txn, sent at once, not on EXPLAIN | no | — |
| small (§8) | P2 | OTB-contract-comments, OTB-host-rows-many, OTB-sqlstate, OTB-name-case, OTB-flight-levels, OTB-sched-load, OTB-cascade-mixed, OTB-backend-map-check, OTB-connection-name-reserved, OTB-reply-width, OTB-awaits-publish, OTB-set-code, OTB-settings-read, OTB-privileges, OTB-count-type, OTB-not-bool, OTB-concat, OTB-current-connection, OTB-schema-qualified, OTB-like-escape, OTB-uncorrelated-in, OTB-alter-if-not-exists, OTB-implicit-cast, OTB-bytes-literal, OTB-nul-text, OTB-sanitizer-package | no | — |

OTB-session-settings is partly done in #850; its rest is OTB-show, OTB-driver-settings, OTB-idle-timeout and
OTB-settings-read. Done in #850: §9. Not accepted or obsolete: §10.

---

## 2. Context

OtterStax is a federated SQL server: MySQL, PostgreSQL, Arrow Flight SQL and Spark Connect clients send SQL that mixes
tables of remote databases (MySQL / MariaDB, PostgreSQL, ClickHouse), local tables and external sources (S3, files,
Kafka). Every statement runs on the embedded otterbrix, one engine session per client connection.

In the #850 model a remote database is a `pg_database` row with `datconnection`; `c.d.s.t` is a table of database `d`
of connection `c` (`ob-doc:3-13`, `:73`). The catalog resolves every name and the engine plans every statement. At
`open_engine` the host passes only the map "connection → backend actor address"
(`ob:components/planner/host_hooks.hpp:16-26`). Operators reach a table's storage through the same contract messages
(`ob:services/disk/storage_contract.hpp:90-201`): otterbrix's disk for its own tables, the backend actor for a
connection's table. Pushdown is a block offer to the owner (`storage_offer`); transactions are `storage_prepare` /
`storage_release_prepared` / `abort_transaction`. The host keeps its table parameters (remote spelling, server type)
in its own otterbrix tables keyed by oid (`ob-doc:181-184`).

OtterStax has one backend actor per configured connection, with agents on its `backends` pool, one physical connection
each (`otx:otterbrix/federation/backend_actor.hpp`, `backend_agent.hpp`); table parameters live in
`federation.public.remote_names` (`otx:otterbrix/federation/metadata.hpp`).

---

## 3. P0

### OTB-tag — release tag (P0)

- **Request.** Merge #850 and publish a `1.0.0b2-*` tag on `conan.otterbrix.com` that requires `actor-zeta/2.0.0`
  (already on the remote, `actor-zeta.md`, AZ-release).
- **Why.** OtterStax consumes otterbrix only by a tag pinned by recipe revision. Docker images now build otterbrix from
  an archived local recipe (`otx:scripts/conan/export_local_packages.sh`); CI cannot build it from a clean checkout.
- **Evidence.** `46fba865` is not an ancestor of `61aa43d6` (common base `b18b898d`); neither branch has a tag.
- **OtterStax workaround.** A local package plus the recipe archive in the Docker context, until the tag.
- **Blocks the port:** no — it blocks CI and the OtterStax release.

### OTB-append-omitted — INSERT loses the backend's DEFAULT (P0)

- **Request.** `storage_append` to a connection's table carries only the columns the statement wrote (`attoids` =
  theirs); the backend fills the rest. If not possible, allow a declared DEFAULT on a connection's table, as on a
  PostgreSQL foreign table, so the host can mirror the backend's DEFAULT.
- **Why.** `INSERT INTO pg.shop.public.orders (customer, amount) VALUES (…)` into a table with `id serial` /
  `AUTO_INCREMENT` / `DEFAULT now()`: the backend gets `id = NULL` — a `not_null_violation`, or a row with NULL instead
  of the default. On `46fba865` OtterStax sent the backend an INSERT of the written columns only, and it worked.
- **Evidence.**
  - The INSERT plan takes the attoid of every column (`ob:components/physical_plan_generator/impl/create_plan_insert.cpp:19-26`);
    the operator fills omitted columns with the default or NULL "so storage_append/WAL/constraints see one full-width
    row" (`ob:components/physical_plan/operators/operator_insert.cpp:97-116`).
  - "NULL in an omitted column (a connection's table declares no defaults). `storage_append` gets full rows"
    (`ob-doc:211-213`, test `an_omitted_column_is_null`, `ob:integration/cpp/test/test_host_name_resolution.cpp:726`);
    DEFAULT on a connection's table is refused (`ob-doc:122-123`).
  - The contract comment says the opposite: "`attoids` name the chunk's columns: the ones the statement wrote, the
    others the storage fills itself" (`ob:services/disk/storage_contract.hpp:107`) — see OTB-contract-comments.
- **OtterStax workaround:** none — an INSERT that omits a column with a backend DEFAULT / serial fails or writes NULL
  (Known Constraints); clients must list every column.
- **Blocks the port:** yes.

### OTB-driver-settings — what drivers send on connect (P0)

- **Request**, by priority:
  - **P0 — the client cannot connect without it.** `SHOW` of any engine parameter (OTB-show); `version()` /
    `pg_catalog.version()`; `DISCARD ALL` (OTB-discard); transaction modes — `BEGIN [TRANSACTION] ISOLATION LEVEL …`,
    `BEGIN READ WRITE | READ ONLY`, `START TRANSACTION READ ONLY`, `SET [SESSION] TRANSACTION …` (accept the level the
    engine provides or the nearest stricter one; enforce READ ONLY); the session parameters MySQL drivers set —
    `sql_mode`, `sql_select_limit`, `sql_auto_is_null`, `wait_timeout`, `net_read_timeout`, `net_write_timeout`
    (accept the values the engine honours, refuse others by value, not by name).
  - **P1.** Time zones with DST (IANA database) and POSIX sign for `UTC+5`, as PostgreSQL does.
  - **P2 (pg_dump, mysqldump).** `statement_timeout = 0`, `lock_timeout`, `idle_in_transaction_session_timeout`,
    `row_security`, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`,
    `SELECT set_config('search_path', '', false)`; `SQL_MODE`, `information_schema_stats_expiry`,
    `FOREIGN_KEY_CHECKS`, `UNIQUE_CHECKS`, `SQL_NOTES`.
- **Why.** The OtterStax frontends own the protocol parameters and never send them to the engine: PG
  `application_name`, `client_encoding`, `extra_float_digits` (the client's value), `DateStyle`, `IntervalStyle`,
  `standard_conforming_strings` (fixed: the same value is OK, another is 22023;
  `otx:frontend/postgres_server/connection/protocol_parameters.hpp:16-27`); MySQL `SET NAMES` / `CHARACTER SET` /
  `character_set_*` (utf-8) and `@@…` reads. So psql, pgJDBC, the mysql CLI, PyMySQL and mysql-connector-python connect.
  Everything else is an engine session setting, and drivers send:
  - SQLAlchemy (psycopg2): `show transaction isolation level`, `show standard_conforming_strings`,
    `select pg_catalog.version()` on first connect (the frontend answers `SHOW` of its client parameters only,
    `otx:frontend/postgres_server/connection/protocol_parameters.hpp:219-223`; the rest goes to the engine);
  - psqlODBC: `SHOW DateStyle`; Npgsql: `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` on `BeginTransaction()`,
    `DISCARD ALL` on return to its pool; lib/pq: `BEGIN READ WRITE` on `BeginTx` with options;
  - pgJDBC: the JVM `TimeZone` in the StartupMessage — a zone without DST gives the wrong offset in summer, an unknown
    one is FATAL;
  - pg_dump: `SET statement_timeout = 0` and others, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`,
    `set_config('search_path', '', false)`;
  - MySQL (the frontend turns a MySQL SET into one engine SET, applied whole or not at all):
    `SET SESSION TRANSACTION ISOLATION LEVEL …` / `READ ONLY` (Connector/J; Django on every connect),
    `START TRANSACTION READ ONLY` (go-sql-driver), `set autocommit=1, sql_mode=CONCAT(@@sql_mode, ',STRICT_TRANS_TABLES')`
    (MariaDB C/J — `sql_mode` makes the whole SET fail), `SET SQL_SELECT_LIMIT` (Connector/J `setMaxRows`),
    `SET SQL_AUTO_IS_NULL = 0` (Django), `@@SESSION.wait_timeout`, `sql_auto_is_null` (Rails), mysqldump.
- **Evidence.**
  - Three session parameters: `timezone`, `autocommit`, `search_path` (`ob:components/catalog/settings.cpp:21-23`); any
    other is `sql_parse_error` "SET <name> is not supported" (`ob:components/sql/transformer/impl/transform_set_setting.cpp:20-23`).
  - Transaction modes are `unimplemented_yet` "transaction modes are not supported"
    (`ob:components/sql/transformer/impl/transform_transaction.cpp:21-24`); `SET TRANSACTION …` becomes a parameter named
    `TRANSACTION` (`ob:components/sql/parser/gram.y:2088-2094`) and is refused like an unknown one.
  - `SHOW` and `DISCARD` parse (`gram.y:2392-2425`, `:2465-2471`); the transformer answers "Unsupported node type"
    (`ob:components/sql/transformer/transformer.cpp:247-250`); there is no `version`, `current_setting` or `set_config`
    (`ob:components/compute/function.hpp:408-428`).
  - Time zones are fixed offsets without DST ("Standard (non-DST) offsets", `ob:core/date/timezones.cpp:14`;
    `america/new_york` = −18000, `:176`); `utc±HH` is read with the ISO sign (`:887`), PostgreSQL reads `UTC+5` as POSIX.
- **OtterStax workaround:** none — the rule is "protocol parameters belong to the frontend, session settings to the
  engine"; the drivers above get the engine's refusal and do not connect.
- **Blocks the port:** no.

---

## 4. Storage contract and transactions

### OTB-txn-storage — rest of #663 (P1)

Done in #850: transaction participants, `storage_prepare` / `storage_release_prepared` / `abort_transaction`, ROLLBACK
TO, one write place (`ob:services/disk/storage_contract.hpp:146-155`, `ob-doc:265-294`; COMMIT —
`ob:components/physical_plan/operators/operator_commit_transaction.cpp:203-240`).

- **Request.** (a) Close the in-doubt window — the backend prepared and committed its part, then the WAL marker failed:
  a two-phase outcome (`PREPARE TRANSACTION` / `XA PREPARE` over the same messages, recovery after a crash); with it,
  writes to several connections in one transaction. (b) DDL on a connection's table inside a block and with
  `autocommit = off`: the backend as a participant, `ddl_reply_t` in the same transaction.
- **Why.** (a) The client gets a COMMIT error while the rows stay on the backend; a write to a second connection in a
  block is refused. (b) On `46fba865` OtterStax ran remote DDL inside `BEGIN`; now it is refused. Without (b), loading
  `CREATE EXTERNAL TABLE c.d.s.t …` is two autocommit statements (CREATE, INSERT): a failed INSERT leaves an empty
  table, and COPY TO cannot be one statement (OTB-sink-root).
- **Evidence.** "The window of #663 … the backend's outcome is in doubt until #663" (`ob-doc:282-283`;
  `operator_commit_transaction.cpp:203-204`, marker refusal `:237-239`); "One write place … A write to another
  connection … is refused" (`ob-doc:289-294`). DDL: "only supports DDL in autocommit"
  (`ob:services/collection/executor.cpp:558-564`, second copy `:1867-1872`; `ob-doc:260-261`). Another window:
  the backend ran the DDL, then writing `host_rows_t` or the WAL marker failed — the catalog rolls back, the
  backend object stays.
- **OtterStax workaround.** A 4-part load target runs CREATE and INSERT in autocommit; inside a block it is refused.
- **Blocks the port:** regression (DDL inside `BEGIN`).

### OTB-ctx-block — "inside a block" flag (P1)

- **Request.** A flag in `execution_context_t`: "the transaction ends with this statement" (autocommit) or "a block is
  open".
- **Why.** An autocommit statement is a transaction too (`ob-doc:272-273`) and the backend cannot tell it from a block:
  for every statement it takes a connection, opens a remote transaction and waits for `storage_release_prepared`,
  where a single statement could run in the backend's autocommit — one or two round trips fewer.
- **Evidence.** `execution_context_t` = `session`, `txn`, `session_tz`, `table_oid`, `database_oid`
  (`ob:components/context/execution_context.hpp:23-29`); no block flag.
- **OtterStax workaround.** Infer the block from `ctx.txn.command_id` (0 outside a block).
- **Blocks the port:** no.

### OTB-ctx-scan — context in scan messages (P1)

- **Request.** `storage_fetch_next_batch` and `storage_close_cursor` take `execution_context_t`, like the other
  messages.
- **Why.** A backend keys its transaction state (open cursors, undo) by `ctx.txn.transaction_id` (`ob-doc:138-140`), but
  a cursor close arrives without a transaction; a scan also needs the session time zone (`session_tz`) to convert the
  backend's `TIMESTAMP` / `DATETIME`.
- **Evidence.** `storage_fetch_next_batch(session, oid, cursor_id, limit, projected_cols, txn)`,
  `storage_close_cursor(session, oid, cursor_id)` (`ob:services/disk/storage_contract.hpp:95-103`).
- **Blocks the port:** no.

### OTB-insert-block — INSERT within one backend as a block (P1)

- **Request.** Offer INSERT … VALUES and INSERT … SELECT to the owner when the source reads only tables of
  the same backend; a taken one runs as `storage_run_write`.
- **Why.** `INSERT INTO pg.db.public.a SELECT … FROM pg.db.public.b` was one remote statement on `46fba865`; now the
  rows travel to otterbrix and back in `storage_append` batches.
- **Evidence.** A block holds aggregate, group, match, sort, limit, having, select, join, update, delete
  (`ob:components/logical_plan/detached_block.cpp:243-259`); a block root is aggregate / join / update / delete
  (`ob:services/collection/executor.cpp:758-761`). Pinned by `an_insert_flushes_on_the_storage_threshold`
  (`ob:integration/cpp/test/test_host_name_resolution.cpp:696-707`). Not in the #850 follow-ups (#849).
- **Blocks the port:** regression (performance).

### OTB-no-row-ids — backend without row numbers (P1)

- **Request.** A flag in `backend_address_t` (or per table): "no row numbers". Then a scan does not number rows, and an
  UPDATE / DELETE the backend did not take as a block is refused at planning with a clear error.
- **Why.** ClickHouse and tables without a primary key cannot change a row by number. Without the flag a backend must
  remember "number → key" for every scan in case of a write (`ob-doc:201-203`), and the refusal comes after the scan.
- **Evidence.** The refusal exists only on the write message: `a_storage_without_row_numbers_refuses_the_batch_path`
  (`ob:integration/cpp/test/test_host_name_resolution.cpp:915`, `ob-doc:217-218`); `backend_address_t` has only
  `connection`, `address`, `max_rows_per_write` (`ob:components/planner/host_hooks.hpp:16-20`).
- **OtterStax workaround.** No backend gives row numbers: an UPDATE / DELETE not taken as a block is refused
  (`unimplemented_yet`) on the write message, after the scan.
- **Blocks the port:** no.

### OTB-storage-index — indexes of a connection's table (P1)

- **Request.** `CREATE INDEX` / `DROP INDEX` on a connection's table as contract messages (like `add_storage_column`),
  with index rows in the catalog and no data.
- **Why.** On `46fba865` OtterStax ran `CREATE` / `DROP INDEX` of a remote table on the backend; now it is refused.
- **Evidence.** `case create_index_t` → `unimplemented_yet` "indexes and constraints are not kept for a table of
  connection" (`ob:services/collection/executor.cpp:1818-1822`; `ob-doc:122-123`).
- **OtterStax workaround:** none (Known Constraints).
- **Blocks the port:** regression.

### OTB-abort-refused — a backend refuses the rollback (P1)

- **Request.** A participant's refusal in `abort_transaction`: for ROLLBACK TO an error that fails the
  block; for ROLLBACK an error to the client; for `close_session` an answer to the host. A "refuse abort" switch in the
  test backend.
- **Why.** `BEGIN; SAVEPOINT s; INSERT INTO pg.d.s.t …; ROLLBACK TO s;` — the backend refuses, the client sees success,
  and COMMIT commits on the backend the rows the user rolled back.
- **Evidence.** `revert_transaction` returns `void` and the refusal goes to the log
  (`ob:components/physical_plan/operators/transaction_teardown.cpp:32-57`); "A refused rollback reaches only the log"
  (`ob-doc:431-432`); the test backend always succeeds (`ob:integration/cpp/test/test_host_actor.hpp:791-808`).
- **OtterStax workaround.** After a refused ROLLBACK TO the agent marks the backend transaction broken, so COMMIT is
  refused (`otx:otterbrix/federation/backend_agent.cpp:293-317`, `:465-489`); the client still sees ROLLBACK TO succeed.
- **Blocks the port:** no.

### OTB-error-abi — `core::error_t` layout depends on `NDEBUG` (P1)

- **Request.** One layout of `error_t` in every build: keep `std::source_location error_origin` always (or move it out
  of the struct), as was done for `core/pmr.hpp`.
- **Why.** `error_t` is a public header type. A Debug host against a Release package (or the reverse) sees another size
  and other offsets: an `error_t` from the engine is read shifted — an ODR violation with no diagnostic. A garbage error
  code seen in OtterStax under ASAN is the likely result.
- **Evidence.** `#if not defined(NDEBUG) std::source_location error_origin{};`
  (`ob:core/result_wrapper.hpp:101-102`; constructors and assignments `:110`, `:122`, `:136`, `:151`, `:160`, `:188`).
- **OtterStax workaround.** Build with the same `NDEBUG` as the package.
- **Blocks the port:** no.

### OTB-register-table — register / forget without backend DDL (P2)

- **Request.** Catalog plans "register a connection's table", "forget a table", "add / drop a column in the
  catalog" with no message to the backend, like `make_node_create_schema` for schemas (`ob-doc:103-108`).
- **Why.** A table created or dropped on the server outside OtterStax, or a column added there. Today such a
  registration differs from a user's `CREATE TABLE` / `DROP TABLE` only by the session that sends it.
- **Evidence.** "otterbrix has no registration of its own. A table gone from the backend is taken away by an ordinary
  `DROP TABLE`" (`ob-doc:258-259`).
- **OtterStax workaround.** Registration runs `CREATE DATABASE` / `SCHEMA` / `TABLE` (and `DROP` / `ALTER` on restart)
  on a service session the actor recognises by `ctx.session` and never sends to the backend
  (`otx:catalog/registration.cpp`, `otx:otterbrix/federation/backend_actor.hpp`).
- **Blocks the port:** no.

---

## 5. Session, settings, errors

### OTB-show — reading settings (P1)

Done in #850 (OTB-session-settings): per-session `SET` / `RESET` / `SET LOCAL` of `timezone`, `autocommit`,
`search_path`, rolled back with the transaction; `ALTER SYSTEM` for defaults (`ob-doc:80-85`, `:558-580`;
`ob:components/catalog/settings.cpp:21-23`).

- **Request.** `SHOW name`, `SHOW ALL`, `current_setting(name [, missing_ok])`, `set_config(name, value, is_local)`.
- **Why.** Drivers and ORMs read settings on connect (OTB-driver-settings); inside a block a failed `SHOW` fails the
  block.
- **Evidence.** The grammar builds `VariableShowStmt` (`ob:components/sql/parser/gram.y:2392-2425`), the transformer
  answers "Unsupported node type" (`ob:components/sql/transformer/transformer.cpp:247-250`); no such functions
  (`ob:components/compute/function.hpp:408-428`).
- **OtterStax workaround.** The PG frontend answers `SHOW` of its client parameters; MySQL answers `@@…` from the last
  SET the engine accepted (blind to a SET undone by ROLLBACK).
- **Blocks the port:** no.

### OTB-discard — session reset (P1)

- **Request.** `DISCARD ALL | PLANS | SEQUENCES | TEMP[ORARY]` on the engine session with PostgreSQL semantics: `ALL`
  outside a block = `CLOSE ALL` (session cursors) + `RESET ALL` + `DEALLOCATE ALL` (engine PREPARE) + `DISCARD PLANS` +
  `DISCARD SEQUENCES` (`currval` / `lastval` cache) + `DISCARD TEMP`; `ALL` inside a block is refused with 25001.
- **Why.** pgbouncer (`server_reset_query = DISCARD ALL` by default), pgpool and application pools reset a connection
  before handing it to the next client; otherwise the next client sees the previous one's cursors, settings and
  `currval`.
- **Evidence.** The grammar builds `DiscardStmt` (`ob:components/sql/parser/nodes/parsenodes.h:3180-3183`), the
  transformer answers "Unsupported node type" (`ob:components/sql/transformer/transformer.cpp:247-250`); cursors and
  sequences live in the engine (`ob:components/sql/transformer/transformer.cpp:179-180` — `CREATE SEQUENCE`).
- **OtterStax workaround:** none — the engine's refusal (25001 inside a block). For pgbouncer:
  `server_reset_query = 'RESET ALL; DEALLOCATE ALL'` (cursors and `currval` are not reset).
- **Blocks the port:** no.

### OTB-idle-timeout — session timeouts (P1)

- **Request.** `idle_in_transaction_session_timeout`: the dispatcher rolls back (with `abort_transaction` to the
  participants) a block idle past the limit, with its own code (PostgreSQL 25P03); `statement_timeout` (with
  OTB-cancel).
- **Why.** An abandoned block holds the MVCC horizon and backend connections; a fetch window in autocommit holds its
  transaction until the next statement.
- **Evidence.** No `statement_timeout`, `idle_in_transaction_session_timeout` or `lock_timeout` in `components/`,
  `services/` or `core/`; three session parameters (`ob:components/catalog/settings.cpp:21-23`).
- **OtterStax workaround.** Spark closes a session after `session_timeout_s` without RPCs; MySQL / PG use the
  connection's read timeout.
- **Blocks the port:** no.

### OTB-cancel — cancel a statement (P1)

- **Request.** `cancel_statement(session)`: the executor stops, closes its operators (and backend cursors), the
  statement answers a cancel error (57014); inside a block the block fails.
- **Why.** PG `CancelRequest`, Spark `Interrupt` and a frontend wait timeout must stop the statement, not just stop
  waiting for it; otherwise it holds an executor and a backend connection.
- **Evidence.** No API (`cancel` / `interrupt` / `kill` appear only in comments on actor-zeta message cancellation);
  `begin_shutdown` waits for the running statement (`ob:services/dispatcher/dispatcher.cpp:396-412`, `:403`) and refuses
  new ones (`:546-555`).
- **OtterStax workaround.** A timed-out statement runs to its end; inside a block the frontend fails the block.
- **Blocks the port:** no.

### OTB-qualifier-text — qualifier compared as written text (P1)

- **Request.** Match a column qualifier against the completed name of the FROM item (session place,
  connection), not against the text.
- **Why.** `SELECT public.orders.id FROM orders` and `SELECT main.public.orders.id FROM orders` fail but work in
  PostgreSQL; a self-reference `REFERENCES public.t` in `CREATE TABLE t` fails with "referenced relation does not exist".
  Clients and BI tools write fully qualified columns.
- **Evidence.** `answers()` compares the slots as written (`ob:components/sql/transformer/utils.cpp:53-64`, refusal
  `:106-108`); a test pins it (`ob:integration/cpp/test/test_name_resolution.cpp:381`); self-reference:
  `ob:components/sql/transformer/impl/transform_table.cpp:135`.
- **Blocks the port:** no.

### OTB-error-object — structured error parts (P2)

- **Request.** Error fields: the parts of a missing name (connection, database, schema, table, column) and the oid /
  name of a blocking object (`dependent_objects_still_exist`), like the `s`, `t`, `c`, `n` fields of a PostgreSQL
  ErrorResponse.
- **Why.** The PG frontend would fill the ErrorResponse fields, and the host would know the miss is on a connection of
  its map without parsing text. A table of an unreachable connection now answers `database "down.db" does not exist`:
  connection and database are merged into one name.
- **Evidence.** `error_t` = `type`, `position`, `what` (`ob:core/result_wrapper.hpp:96-103`); the database is written as
  `qualified_name_t{connection, database}.to_string()` (`ob:services/dispatcher/validate_logical_plan.cpp:1502-1505`).
- **Blocks the port:** no.

---

## 6. SQL and external storages

### OTB-describe-untyped — untyped `$n` (P1)

- **Request.** Type `$n` in a UNION branch, a CTE, a derived table, `INSERT … SELECT $n`, `= ANY($n)`, as PostgreSQL 18.
- **Why.** PG Parse / MySQL `COM_STMT_PREPARE` of such statements is described by `describe_sql` and answers 42P18; on
  `46fba865` OtterStax described them itself.
- **Evidence.** "Not typed yet … a `$n` in a UNION branch, a CTE or a derived table … otterbrix answers 42P18"
  (`ob-doc:681-696`).
- **OtterStax workaround:** none.
- **Blocks the port:** regression.

### CTAS — `CREATE TABLE … AS SELECT` (#675) (P1)

- **Request.** CTAS: columns from the SELECT schema, create and insert in one statement; the target is a local table or
  a connection's table, the source anything.
- **Why.** Clients write CTAS. OtterStax's `CREATE EXTERNAL TABLE` is a CTAS of a local table from an S3 / file scan;
  `COPY (…) TO` is a CTAS of a connection's table on S3 / file (OTB-sink-root).
- **Evidence.** "CREATE TABLE AS is not supported yet (#675)" (`ob:components/sql/transformer/transformer.cpp:191-199`);
  `CREATE TABLE … AS EXECUTE` likewise (`ob-doc:485`).
- **OtterStax workaround.** `CREATE EXTERNAL TABLE` runs two plans (create, load) on the client's session; a client's
  CTAS gets the engine's refusal.
- **Blocks the port:** no.

### OTB-sink-root — COPY / S3 / file / Kafka behind the contract (P1)

- **Request.** Accept the model "S3 / file / Kafka are connections": a host actor implements the storage contract; a
  table of such a connection is an object (file, prefix, topic) with its parameters as CREATE TABLE WITH options
  (OTB-create-options). Then `CREATE EXTERNAL TABLE` = CTAS of a local table from a connection's table;
  `COPY (q) TO 's3://…'` = CTAS of a connection's table (or `INSERT … SELECT` into it); a Kafka SOURCE = a scan of a
  connection's table. Needed from the engine: CTAS (#675), WITH options, connection DDL in a transaction
  (OTB-txn-storage, b).
- **Why.** `CREATE EXTERNAL TABLE` and `COPY … TO` are parsed by OtterStax's own grammar, and its actors read and write
  the file outside the transaction (a COPY rolled back leaves its file written); Kafka DDL is direct calls from the
  session. Behind the contract they get the client's transaction, pushdown and EXPLAIN.
- **Evidence.** There is no host extension node any more: `node_extension_t` was removed in `829b233d` and is not in
  `node_type` (`ob:components/logical_plan/forward.hpp:7-92`); an unknown host plan node hits the validator's `default`
  (`ob:services/dispatcher/validate_logical_plan.cpp:3702-3706`); the transformer has no COPY.
- **OtterStax workaround.** Its own grammars and actors; rows go in as plans on the client's session.
- **Blocks the port:** no.

### OTB-create-options — WITH options reach the backend (P1)

- **Request.** `CREATE TABLE … WITH (k = v, …)` of a connection's table: the options reach `create_storage_disk` (a
  key / value list); the host decides what to accept.
- **Why.** File location and format, `s3_alias`, Kafka topic, ClickHouse table engine (`ENGINE`).
- **Evidence.** `storage` is "option has been removed", the others `unimplemented_yet` "… is not implemented; no table
  was created" (`ob:components/sql/transformer/impl/transform_table.cpp:65-86`); `create_storage_disk` takes no options
  (`ob:services/disk/storage_contract.hpp:126-132`).
- **Blocks the port:** no.

### OTB-numeric-literal — a number glued to letters (P1)

- **Request.** At least a parse error when letters or `_` directly follow a number (PostgreSQL 15); better, `0x` /
  `0o` / `0b` and `_` between digits (PostgreSQL 16).
- **Why.** A silently wrong answer: `SELECT 0x41` → column `x41` = 0, `SELECT 12abc` → `abc` = 12. PG, Flight SQL and
  Spark texts reach the engine as written.
- **Evidence.** The scanner knows only decimal `integer` / `decimal` / `real` (`ob:components/sql/parser/scan.l:369-374`,
  `{integer}` at `:941`); no trailing-junk rules.
- **OtterStax workaround.** The MySQL frontend rewrites `0x..` / `0b..` as string literals; other protocols are exposed.
- **Blocks the port:** no.

### OTB-assignment-cast — an UPDATE assignment's cast is not marked (P1)

- **Request.** Mark the cast the validator wraps around an UPDATE assignment whose value is not of the column's type:
  a `cast_kind::assignment` (or a flag on the cast node) instead of the `cast_kind::cast` a user's `CAST` is.
- **Why.** The cast is resolved as an assignment cast (`cast_type::assignment`), and a literal is BIGINT / DOUBLE, so
  `SET i = i + 1` over an INTEGER column and `SET f = 2.5` over a FLOAT one narrow (BIGINT → INTEGER, DOUBLE → FLOAT,
  range-checked). A backend whose dialect has no range-checked narrowing CAST (MySQL: CAST to SIGNED neither narrows
  nor checks; ClickHouse: the conversion wraps) can still run the statement whole — the store into the column is the
  range check (MySQL under a strict `sql_mode`) — but only if it knows the cast is the assignment's: a user's
  `CAST(x AS INTEGER)` elsewhere in the statement must stay a cast. Today the host recognises it by its shape (the
  topmost node of a SET value, cast to the column's own type), which the user's `SET i = CAST(x AS <column type>)`
  shares. Related: OTB-implicit-cast (the validator's casts in general).
- **Evidence.** `cast_registry->resolve(value_type, target_type, cast_type::assignment)` then
  `make_cast_expression(…, *cast, components::casts::cast_kind::cast)` with `conversion->key() = expr->key()`
  (`ob:services/dispatcher/validate_logical_plan.cpp:3415-3437`); `cast_kind { cast, try_cast }`
  (`ob:components/casts/cast_function.hpp:12-16`). An INSERT's assignment casts are column bindings the engine applies
  itself (`:3211-3229`), not expressions of the offered tree.
- **OtterStax workaround.** `sql_gen` takes the topmost cast of a SET value to the column's type as the assignment's
  (`otx:otterbrix/query_generation/sql_query_generator.cpp`, `write_assignment_cast`): an integer narrowed to an
  integer is left to MySQL's store (the agent runs such a statement only on a session under `STRICT_TRANS_TABLES` /
  `STRICT_ALL_TABLES`) and written `accurateCast(x, 'Nullable(T)')` on ClickHouse; a DOUBLE into FLOAT is MySQL's
  store, or on ClickHouse only a bound value the engine's cast takes.
- **Blocks the port:** no.

---

## 7. Performance

### OTB-notify — no timer polling; JOIN build sides at once (P1)

- **Request.** (1) Once actor-zeta has a waker (`actor-zeta.md`, AZ-waker): wake the dispatcher, disk, index and WAL
  loops with it instead of sleeping `pump.in_flight`; `wait_ready` waits on a semaphore (#693, stage 2). (2) Start every
  JOIN build side first, then wait for all; this does not depend on actor-zeta.
- **Why.** A JOIN of K connection tables materializes its build sides one after another. The cost of polling is
  measured in `actor-zeta.md`, AZ-park (≈ 3.1 CPU-seconds per second a statement waits).
- **Evidence.**
  - The loops poll `is_awaited_ready()` and sleep `pump.in_flight` (5 µs) / `pump.idle` (10 ms)
    (`ob:components/configuration/configuration.hpp:50-57`): dispatcher `ob:services/dispatcher/dispatcher.cpp:186`,
    `:219`; disk `ob:services/disk/manager_disk.cpp:418`, `:443`; index `ob:services/index/manager_index.cpp:324`,
    `:356`; WAL `ob:services/wal/manager_wal_replicate.cpp:210`, `:242`. `wait_ready` sleeps 100 µs
    (`ob:core/executor.hpp:54-62`).
  - `materialize_build_sides_` waits for the sides one by one (`ob:services/collection/executor.cpp:4537-4551`,
    `:4544`), while sources already open at once (`open_sources_`, `:4586-4597`; `ob-doc:194-196`).
- **OtterStax workaround.** None for the session and the executor (hidden test `[.engine-defect-otb-notify]`).
- **Blocks the port:** no.

### OTB-executor-wait — a waiting statement holds its executor (P1)

- **Request.** A statement suspended on a storage's answer does not keep the end of another session's transaction
  (COMMIT, ROLLBACK, the ROLLBACK of `close_session`) from running: run the statements of an executor interleaved
  (its loop resumes whichever coroutine's future is ready, as the disk / dispatcher loops do), or run the end of a
  transaction off the executor pool.
- **Why.** A backend connection of OtterStax serves one transaction at a time: a block holds one of the connection's
  `pool_size` agents from its first statement to its end, and a statement of another block waits for a free one (up to
  a checkout timeout, 60 s). With every agent held by open blocks and four statements of other blocks waiting, every
  executor runs a waiting statement; the COMMIT / ROLLBACK that would free an agent is queued behind them on an
  executor, so nothing moves until the waiting statements time out, and every other statement of every session (a
  `SET`) waits too. Seen on the live stack: 16 MySQL clients with autocommit off (each opens a block with its first
  SELECT and closes the connection without COMMIT) over a connection of `pool_size` 4 stalled for minutes.
- **Evidence.** `executor_t` is a `basic_actor` (`ob:services/collection/executor.hpp:279-280`): one behavior at a
  time, `execute_plan_full` awaited whole (`ob:services/collection/executor.cpp:321-325`); the dispatcher picks the
  executor round-robin for every statement, the ROLLBACK of `close_session` included
  (`ob:services/dispatcher/dispatcher.cpp:382-385`, `:833`, `:943`); `executor_pool_size` 4
  (`ob:components/configuration/configuration.hpp:66`).
- **OtterStax workaround.** A closed connection ends its transaction on every backend actor itself, not through the
  engine (`otx:otterbrix/federation/backend_actor.hpp`, `end_session`), and its statements waiting for an agent leave
  the queue. A client's own COMMIT / ROLLBACK under such a load still waits for an executor.
- **Blocks the port:** no.

### OTB-total-rows — table size estimate (P2)

- **Request.** `storage_total_rows` with `execution_context_t` (the transaction); send it to every table
  at once, like block offers; do not send it on EXPLAIN without ANALYZE.
- **Why.** Before a hash join every connection table is a separate round trip to its backend, one at a time, outside
  the statement's transaction — even on EXPLAIN, which "runs nothing".
- **Evidence.** A `co_await` loop over the tables (`ob:services/collection/executor.cpp:3734-3751`); the message has no
  transaction (`ob:services/disk/storage_contract.hpp:105-106`); `ob-doc:634-635`.
- **OtterStax workaround.** The actor answers a constant 1000 without a query
  (`otx:otterbrix/federation/backend_actor.cpp:31-33`), so hash-join sides are not ordered by size.
- **Blocks the port:** no.

---

## 8. Small requests (P2)

None blocks the port. An OtterStax workaround, where there is one, is in the "Why" column.

| ID | Request | Why | Evidence |
|---|---|---|---|
| OTB-contract-comments | fix two comments of `storage_contract.hpp` to match the code (or the code to match `storage_contract.hpp:107`, OTB-append-omitted) | a backend written from the header fails: a full-width page is refused with "answered a page of other columns" on every projected scan | `ob:services/disk/storage_contract.hpp:94` says "a column outside the set is a buffer-less stub", but the engine expects only the projected columns (`ob:components/physical_plan/operators/scan/guard_chunk.cpp:28`, `ob-doc:196-197`); `storage_contract.hpp:107` says "the others the storage fills itself", but the plan sends every column |
| OTB-cascade-mixed | stamp the address of every table in a DROP closure; local ones go to the disk | a local materialized view over `c.d.s.t` → `DROP TABLE c.d.s.t CASCADE` is `internal_error` | `ob:components/physical_plan/operators/operator_dynamic_cascade_delete.cpp:366-382`, filter `:844`; precondition test `ob:integration/cpp/test/test_host_name_resolution.cpp:311`. Related (not ours): an open cursor under `DROP … CASCADE` — extend #887 |
| OTB-backend-map-check | at `open_engine`, refuse an empty address and an empty or repeated connection name | an empty address is `std::abort()` on the first scan; with a repeat the first entry wins and the second's `max_rows_per_write` is silently lost | `ob:services/engine/engine.cpp:165-201` (checks only the pools and the log); map copy `ob:services/collection/executor.cpp:314-318`; abort `ob:services/disk/storage_contract.hpp:227-230` |
| OTB-connection-name-reserved | at `open_engine`, refuse a connection named like a local database — above all the default `main` (session place `main.public`); refuse `CREATE DATABASE` of a local database named like a mapped connection | `main.public.t` (local) and `main.db.public.t` (connection `main`) differ only in the number of parts; the host cannot protect a local database created before the connection. Workaround: aliases `main` and `federation` are reserved (`otx:config/connections/connection_config_reader.hpp:32-39`); the session refuses `CREATE DATABASE <alias>` (`otx:catalog/ownership.cpp`) | `ob:services/collection/executor.cpp:569-584` (`storage_of_connection_` compares the connection name only); `ob:services/engine/engine.cpp:165-201` (`refuse_host_arguments` does not check the map) |
| OTB-reply-width | check backend answers (RETURNING, `host_rows_t`) by column count and type, like scan pages; compare types with parameters (DECIMAL scale, LIST, STRUCT) | a backend answering fewer columns makes the engine read past the chunk; a DECIMAL of another scale passes. Workaround: the actor casts answers to the declared types (`otx:otterbrix/federation/conform.hpp`) | `check_chunk_width` checks only the row count (`ob:components/physical_plan/operators/scan/guard_chunk.cpp:41-52`); pages are compared by logical type only (`:28-31`); `host_rows_t` keys are checked by column number only (`ob:services/collection/executor.cpp:705-723`) |
| OTB-awaits-publish | read `awaits_publish` only from the disk address, and say so in the contract | a backend's flag writes a WAL delete with a connection table's oid and sends the disk undo / publish of a foreign oid. Workaround: the actor always answers `false` | `ob:services/disk/storage_contract.hpp:48-71`; `ob:components/physical_plan/operators/operator_delete.cpp:376`, `operator_insert.cpp:199`, `operator_update.cpp:678`, `operator_create_collection.cpp:49-58` |
| OTB-set-code | an unknown `SET` parameter is `undefined_object` (42704) "unrecognized configuration parameter", as in PostgreSQL | a valid `SET` with a syntax-error code confuses clients; the MySQL frontend answers it `ER_SYNTAX_ERROR` | `sql_parse_error` "SET <name> is not supported" (`ob:components/sql/transformer/impl/transform_set_setting.cpp:20-23`) |
| OTB-settings-read | a public `session_settings(session)` (or `SHOW`, OTB-show): the host reads a session's settings | the PG frontend sends ParameterStatus `TimeZone` / `search_path`; it now replays the engine's rules from the session's answers and reads the defaults from `pg_settings` on every connect | `session_settings()` is private in `manager_dispatcher_t` (`ob:services/dispatcher/dispatcher.hpp:171`, `:374`) |
| OTB-privileges | the code `insufficient_privilege` (42501) and, later, role privileges | OtterStax refuses `ALTER SYSTEM` / `ALTER DATABASE … SET` / MySQL `SET GLOBAL` from a client with `unimplemented_yet` (0A000): there is no right code | no 42501 in `error_code_t` (`ob:core/result_wrapper.hpp:14-87`) |
| OTB-count-type | `count` returns BIGINT (signed), as PostgreSQL and MySQL do | `count(*)` arrives as UBIGINT: the MySQL frontend marks it UNSIGNED and Connector/J `getObject` returns `BigInteger`; Spark retypes it as signed | `output_type::fixed(logical_type::UBIGINT)` (`ob:components/compute/kernels/aggregate.cpp:808`) |
| OTB-not-bool | `NOT <boolean column>` (any boolean expression) in WHERE | `WHERE NOT active` fails with "Unsupported expression"; it works in PostgreSQL | `AEXPR_NOT` takes only A_Expr / A_Indirection / FuncCall / SubLink (`ob:components/sql/transformer/impl/transfrom_common.cpp:991-1005`) |
| OTB-concat | the `\|\|` operator and `concat` / `concat_ws` for strings | everyday client and BI SQL; the MySQL frontend cannot announce `PIPES_AS_CONCAT` (`@@sql_mode` is `STRICT_TRANS_TABLES`) | no `\|\|` among the operators (`ob:components/sql/transformer/utils.hpp:348-371`), no `concat` among the functions (`ob:components/compute/function.hpp:408-428`) |
| OTB-current-connection | `current_connection()` (or the connection in `current_database()`) for a session placed in a connection's database | after `SET search_path = 'c.d.s'` `current_database()` answers `d`; Spark `currentDatabase()` and Flight SQL lose the connection | the value is `place.database` without `place.connection` (`ob:services/collection/executor.cpp:2323`; `ob:components/catalog/place.hpp:15-18`) |
| OTB-schema-qualified | `CREATE SCHEMA d.s` / `DROP SCHEMA d.s` — a schema in a database other than the session place's | the host creates schemas such as `main.kafka` and `federation.kafka` only through a `make_node_create_schema` plan | `CREATE SCHEMA ColId` (`ob:components/sql/parser/gram.y:1984`); the transform builds `schema_t{schema}` without a database (`ob:components/sql/transformer/impl/transform_database.cpp:30-48`) |
| OTB-like-escape | `LIKE … ESCAPE '…'` | Flight SQL `GetTables` filters and clients escape `_` / `%`; now refused | the grammar builds a `like_escape` call (`ob:components/sql/parser/gram.y:13775`, `:13784`, `:13793`, `:13802`); there is no such function (`ob:components/compute/function.hpp:408-428`) |
| OTB-uncorrelated-in | an uncorrelated `x IN (SELECT …)` is also a semi-join (or an offered block) when both sides are on one backend | `SELECT … FROM c.d.s.a WHERE x IN (SELECT y FROM c.d.s.b)` is two remote statements: the sub-query is read whole and its values sent as an IN list, instead of one | the pull-up takes only correlated sub-queries: `if (!split \|\| split->moved.empty()) return false;` (`ob:components/sql/transformer/impl/pull_up_sublinks.cpp:1060-1062`) |
| OTB-host-rows-many | `ddl_reply_t::rows` as a list of `host_rows_t`, one per host table | table and column parameters naturally live in separate tables that CREATE TABLE writes in its transaction. Workaround: one wide table `federation.public.remote_names` (`table_oid`, `attoid`, `remote_name`, `remote_container`, `remote_type`) | `ddl_reply_t{awaits_publish, rows}` holds one `host_rows_t` (`ob:services/disk/storage_contract.hpp:70-73`) with one `table` (`ob:components/context/host_rows.hpp:14-21`); the executor already writes a list (`write_host_rows_`, `ob:services/collection/executor.cpp:635-638`) |
| OTB-sqlstate | a public `sqlstate(error_code_t)` → five characters (also in the C ABI); MySQL errnos stay with OtterStax | the frontends send SQLSTATE and copy the table from the comments (`otx:frontend/postgres_server/connection/packet_processing.cpp`) | SQLSTATE exists only in comments of `error_code_t` (`ob:core/result_wrapper.hpp:14-87`); the C ABI returns the number (`ob:integration/c/otterbrix.h:105`); only tests check the mapping (`ob:integration/cpp/test/test_sqlstate_codes.cpp`) |
| OTB-name-case | read quotes in `search_path` by identifier rules; check connection names at open (OTB-backend-map-check) | schema `"Sales"` cannot be the place; connection `Sales` is not found by `Sales.x.y.z` without quotes. Workaround: aliases are `[a-z_][a-z0-9_]*` only (`otx:config/connections/connection_config_reader.hpp:48`); names are registered in lower case, the real spelling kept in `federation.public.remote_names` | `search_path` is lower-cased whole (`ob:components/catalog/place.cpp:66-69`); the connection name is compared byte by byte (`ob:services/collection/executor.cpp:574`) |
| OTB-flight-levels | a local name `d.s.t` for a connection's table that can be read and written (a synonym or an auto-updatable view) | Flight SQL `GetTables` has three levels and a connection's table four: OtterStax reports catalog `c.d` (`otx:frontend/flight_sql/core/core.hpp:68`), and a BI tool that builds `"c.d".s.t` cannot read it | name levels `ob-doc:73`; a view over a connection's table can live in a local schema (`ob-doc:119-120`); writes through it are untested |
| OTB-sched-load | pick an executor by load, if the OtterStax benchmark shows a long statement delaying short ones | — | round-robin `next_executor_++ % executors_.size()` (`ob:services/dispatcher/dispatcher.cpp:381-385`); a session with portals is tied to the portal's executor (`:752-787`, `:833`) |
| OTB-alter-if-not-exists | `ALTER TABLE … ADD COLUMN IF NOT EXISTS` | migrating the host's parameter tables on upgrade without recreating them | the grammar has only `ADD columnDef` / `ADD COLUMN columnDef` (`ob:components/sql/parser/gram.y:2682`, `:2690`); `AT_AddColumn` ignores `missing_ok` (`ob:components/sql/transformer/impl/transform_alter_table.cpp:351-381`), DROP COLUMN reads it (`:394`) |
| OTB-implicit-cast | mark a cast as "inserted by the validator" vs "written in the query" (`cast_kind::implicit` or a flag) | the backend turns an offered block into remote SQL: the engine's casts are not needed there, the user's must stay | `cast_kind { cast, try_cast }` (`ob:components/casts/cast_function.hpp:12-16`); the validator inserts `cast_kind::cast` (`ob:services/dispatcher/validation/resolve_expression.cpp:40-49`), a user CAST is the same kind (`ob:components/sql/transformer/impl/transfrom_common.cpp:413`) |
| OTB-bytes-literal | a physical BLOB type; `X'..'` / `'\x..'::bytea`; casting a string to BLOB | binary values from clients (`X'..'`, `bytea`, BINARY(16) UUIDs) and binary backend columns | `XCONST` / `BCONST` become bit strings (`ob:components/sql/parser/gram.y:15755-15766`) → "unsupported constant kind" (`ob:components/sql/transformer/utils.cpp:1316-1322`); `'abc'::blob` is `unimplemented_yet` (`:1188-1192`); no BLOB casts in `components/casts`; no physical type (`ob:components/types/types.cpp:16-48`) |
| OTB-nul-text | `raw_parser` with a text length: a 0 byte is a parse error, and so is a 0 byte from an escape in a literal, as in PostgreSQL | the engine runs a different text: `DELETE FROM t<0> WHERE …` = `DELETE FROM t`; `SELECT E'a\000b'` → `a`. Workaround: the MySQL frontend refuses a text with NUL | `raw_parser(const char*)` (`ob:components/sql/parser/parser.h:15-18`), `strlen` (`ob:components/sql/parser/scan.l:1159`); the `pg_verifymbstr` error is commented out (`ob:components/sql/parser/pg_functions.cpp:213`, `:222`), its result unread (`scan.l:548-550`); the escape stores a 0 byte (`:655-669`) |
| OTB-sanitizer-package | a recipe option (`sanitizer=thread\|address`): an engine package built with a sanitizer, as otterbrix CI builds it | an OtterStax TSAN run reports a race on memory the uninstrumented engine reuses through its pool; ASAN misses memory errors inside the engine. Workaround: OtterStax's own `tsan-deps.profile` sets `cxxflags` for `otterbrix/*`, and `asan-deps.profile` turns the engine's own CMake `ENABLE_ASAN` on through `tools.cmake.cmaketoolchain:extra_variables` (needed there anyway: `libotterbrix` embeds and exports abseil, whose swisstable layout changes under ASAN) | the recipe's only options are `shared`, `build_python` (`ob:conanfile.py:15-18`); TSAN exists only as the CI profile `ob:profiles/tsan` and the workflow `ubuntu-22-04-thread-sanitizer.yaml` |

---

## 9. Done in #850

| ID | Commit |
|---|---|
| OTB-storage-ddl | `df5cfe07`, `829b233d` |
| OTB-dml-uid | `df5cfe07` |
| OTB-resolve-uid (G1) | `df5cfe07` |
| OTB-session | `8f4b2441` |
| OTB-txn-status | `1d2d2dad` |
| OTB-describe | `27e2a8fa`, `0def481a` |
| OTB-reply | `0039f9fc` |
| OTB-sink-count | `f47ff750` |
| OTB-null-operand | `d6f20b6e` |
| OTB-select-param | `27e2a8fa` |
| OTB-hash-join-cast | `0f3dedb8` |
| OTB-order-qualifier | `66139d58`, `3cf0348a` |
| OTB-order-alias | `66139d58`, `3cf0348a` |
| OTB-savepoint | `08e06c6d` (#739), `829b233d` |
| OTB-struct-eq | `7526639f` |
| OTB-block-uninit | `ae5fdb34` |
| OTB-eviction-queue | `fedca435` |
| OTB-scalar-subquery-type | `27e2a8fa` |
| OTB-pushdown-offers (#695) | `829b233d`, `f33e0725` |
| OTB-sink-flush | `9d9c4b00`, `829b233d` |
| PREPARE (#682) | `4005a23d` |
| streaming cursor (#683) | `77289cc3` |
| OTB-embed-pools, items 3–5 | `a755cb25`, `ab08bf1d`, `8b59ee7c` (#686) |
| OTB-count-null (`count(NULL)` counted every row) | `af99cccf` |

---

## 10. Not accepted, obsolete

- **OTB-embed-pools, items 1–2** (host-managed pools, one pool for every role) — not accepted: the engine starts and
  stops its pools itself and refuses one pool for two roles (`ob:services/engine/engine.cpp:906-908`, `:962-964`,
  `:191-198`); OtterStax keeps separate engine pools.
- **OTB-host-ctx** — obsolete: done as `execution_context_t` in the contract messages.
- **OTB-rule-params** — obsolete: no host optimizer rules; statement parameters come with `storage_offer`.
- **OTB-decimal-cast** — obsolete: `logical_value_t` is gone; the rest is OTB-reply-width.
- **STREAM / SOURCE (#684)** — obsolete: Kafka objects go through OTB-sink-root.
- **OTB-txn-mode** — withdrawn by OtterStax: PostgreSQL transaction semantics on every protocol.

---

## 11. Open questions

1. **OTB-tag.** When is #850 merged, and under which tag?
2. **OTB-append-omitted.** Send only the written columns, or allow DEFAULT on a connection's table (as on a PostgreSQL
   foreign table)? The first is simpler for the host: backend DEFAULTs, serials and triggers work without copying.
3. **OTB-txn-storage.** Is a two-phase outcome over the same three messages (prepare = `PREPARE TRANSACTION` /
   `XA PREPARE`) with recovery from the otterbrix journal acceptable? When does connection DDL join the transaction?
4. **OTB-sink-root.** Do you accept "S3 / file / Kafka are host connections" and CTAS as the form of COPY TO?
5. **OTB-driver-settings.** For parameters the engine does not honour (`sql_mode`, `wait_timeout`, isolation levels
   weaker than its own): accept the value equal to the engine's behaviour and refuse the others, or store them without
   effect?
6. **OTB-no-row-ids.** A flag per connection or per table (a MySQL table without a primary key)?
7. **OTB-ctx-block / OTB-ctx-scan.** One extension of `execution_context_t` for every message, or fields where needed?
