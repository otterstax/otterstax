---
name: otterstax-sql
description: Run SQL against a running OtterStax federated-SQL server over the PostgreSQL wire (psql), and a reference matrix of which SQL constructs OtterStax supports. Use when the user wants to query OtterStax, write federated JOINs across registered backends (MySQL/PostgreSQL/ClickHouse), or check whether a SQL feature is supported.
version: 1.0.0
---

# OtterStax SQL

OtterStax is a federated SQL server. Clients connect over the **PostgreSQL wire
(default port 8817)** and issue SQL; queries are either executed locally by the
engine or dispatched to registered remote backends (MySQL/MariaDB, PostgreSQL,
ClickHouse). This skill drives it with `psql` and documents the SQL surface.

## Where to run it

`.claude/skills/otterstax-sql/psql.sh` talks to a running OtterStax. For one with data to join, bring up the quick
start stack — two PostgreSQL servers, MariaDB, ClickHouse, RustFS (S3) and Kafka behind
one OtterStax server that publishes the PostgreSQL wire on `localhost:8817`:

```bash
docker compose -f examples/quick_start/compose.yml up --wait
```

The command waits for both seed jobs and the server health check. Run the
`SELECT 1` check below. It is meant for a **first run or an intentional
re-seed**: on an existing stack the seed jobs overwrite the three S3 source
objects, delete `exports/` and recreate both demo topics, while a reused
OtterStax container keeps its local tables and Kafka read offsets (*Persistence*
in `examples/quick_start/README.md`). To bring a stopped stack back without
re-seeding, use the pause/resume commands from its *Teardown* section. For
startup failures and fixture checks, see `examples/quick_start/README.md`.
The walkthrough's connections are `pgshop`,
`pgcat`, `sales`, `web` and `qs_s3` (`examples/quick_start/config.yaml`), and the
examples below use its tables. It shares port 8817 with `examples/demo/`: do not run
both at once.

## Connecting: `.claude/skills/otterstax-sql/psql.sh`

Use the bundled wrapper for every interaction:

```bash
.claude/skills/otterstax-sql/psql.sh -c "SELECT 1"        # one statement
.claude/skills/otterstax-sql/psql.sh -f query.sql         # a .sql file
echo "SELECT 1" | .claude/skills/otterstax-sql/psql.sh    # SQL from stdin
```

For a script with multiple statements, add `-v ON_ERROR_STOP=1` if you want
`psql` to stop at the first SQL error and exit nonzero. Statements already
committed remain in effect.

It calls the host `psql` against the OtterStax PG wire. **The wire does not
authenticate** — any user/password is accepted. **The database sets the initial
place** (one of `uid`, `uid.ns`, `local`
(`local.public`) or `local.ns` — `uid` a connection alias, a bare `uid` its
default namespace), and a name that is neither a connection nor `local` is
refused at connect (`FATAL 3D000 database "<db>" does not exist`, hint
`A database is a connection, connection.namespace, local or local.namespace.`).
An existing connection or `local` with a missing namespace gives `FATAL 3F000`.
Change the place on an existing connection with `SET search_path = 'local.ns'`
using an existing namespace.
The defaults below work out of the box against
a local engine. Override `HOST`/`PORT` only to reach a remote one:

| Var | Default | Notes |
|---|---|---|
| `OTTERSTAX_HOST` | `localhost` | |
| `OTTERSTAX_PORT` | `8817` | |
| `OTTERSTAX_USER` | `otterstax` | not checked by the wire |
| `OTTERSTAX_DB` | `local` | must be a place: `local` (the engine database of your own tables), `local.ns`, a connection alias, or `alias.ns` |
| `OTTERSTAX_PASSWORD` | *(empty)* | not checked by the wire |

`psql` must be on PATH. On macOS: `brew install libpq` then add
`/opt/homebrew/opt/libpq/bin` to PATH.

If `psql` exits with code 2, read its error and try a new connection with `SELECT 1`.
Check the configured host and port for a refused connection. If the connection was
lost during a query, check the server logs for the cause.

## Federated query syntax

A connection alias is the outermost qualifier. Backends are declared under
`connections:` in the server's `config.yaml` and registered once at startup —
there is no runtime add/remove API; to change them, edit the file and restart the
server. Reference their tables in **three parts**:

```sql
SELECT * FROM <alias>.<ns>.<tbl> JOIN <alias2>.<ns2>.<tbl2> ON ...
```

`<ns>` is a namespace of the connection (lower case): for PostgreSQL a schema of
its configured `database` (the configured `schema:`, or, with none configured, any
user schema); for MySQL / ClickHouse one of the server's databases.
Example, from the quick start stack: `sales.ops.orders` (MariaDB, database `ops`),
`pgshop.shop.customers` (PostgreSQL, config `schema: shop`), `web.analytics.pageviews`
(ClickHouse). Find quick start names in `examples/quick_start/README.md`. For
another deployment, get the connection aliases and namespaces from its config.
`federation.public.remote_names` records original remote spellings, including
table entries with `attoid = 0`. On rc-7, querying `information_schema.tables`
on this stack returns a missing-relation error.

An unresolved two- or three-part relation can return `42601` on the PostgreSQL
wire with a message such as `relation "nope.x" does not exist`. An invalid
database at connection startup returns `3D000`. Use `local.<ns>.<tbl>` for
tables created in the local engine.

```sql
-- against the quick start stack: 1000 rows, every order has its customer
SELECT count(*) FROM sales.ops.orders o
JOIN pgshop.shop.customers c ON c.customer_id = o.customer_id;
```

## SQL support matrix

What OtterStax accepts over the wire, schema-independent. Generic placeholders:
`<tbl> <col> <struct> <field> <type> <enum> <alias> <db>`. Substitute whatever
your registered backends expose. **Status legend:**
`✅ works · ⚠️ path-dependent (read the rule) · ❌ known-broken (use the workaround)`.

Otterbrix plans the query. It can push a block over **one** remote backend down
whole when that dialect can express it with the required semantics. Reads may
also combine remote scans with engine execution. See *Rules* below for examples.

### Table identifiers

| Form | Means |
|---|---|
| 3 parts `<uid>.<ns>.<tbl>` | a table of the connection `<uid>` (PostgreSQL: a schema of its configured database; MySQL / ClickHouse: a database), or `local.<ns>.<tbl>` for a local one |
| 2 parts `<ns>.<tbl>` | uses the connection from the session's place and the namespace from the query: `uid.<ns>.<tbl>` or `local.<ns>.<tbl>` |
| 1 part `<tbl>` | completed by the session's place the same way (`local.public` from the place `local`) |

`<uid>` = a connection declared under `connections:` in `config.yaml` (`local`,
`main` and `federation` are reserved and cannot be aliases). **Four-part names are
refused** (`a table is named connection.namespace.table`). Short names use the
current place. The PostgreSQL StartupMessage `database` sets the initial place
and `SET search_path = 'local.ns'` can change it in the same session. Without a
place short names fail.

Discovery on first use is limited to the connection's configured scope. An
explicit `table:` excludes other tables. A statement is retried automatically
only if registration succeeds, the statement can be rebuilt and the session is
outside a transaction block. Inside a block a miss may register the table and
still return an error. Run `ROLLBACK` before retrying the statement.

### SELECT / read

| Feature | Syntax | Status |
|---|---|---|
| Star / explicit projection | `SELECT *` / `SELECT <col> AS <a>` | ✅ |
| Table & column aliases | `FROM <tbl> <a>` / `<col> AS <a>` | ✅ |
| `INNER JOIN` | `... INNER JOIN <tbl> ON <a>.<c> = <b>.<c>` | ✅ |
| `LEFT JOIN` | `... LEFT JOIN ...` | ✅ |
| Self-join | join a table to itself | ✅ |
| 3-way+ cross-backend join | mix `<alias>`es in one query | ✅ |
| Compound `ON` predicate | `ON <a>.<c> = <b>.<c> AND <pred>` | ✅ |
| Derived table / subquery in `FROM` | `FROM ( SELECT ... ) <a>` | ✅ |
| Comparisons | `= <> < > <= >=` | ✅ |
| Membership / pattern | `IN (...)`, `LIKE`, `NOT LIKE` | ✅ |
| Null tests | `IS NULL`, `IS NOT NULL` | ✅ |
| Boolean combinators | `AND`, `OR` | ✅ |
| Date/time literal compare | `<ts_col> >= '<date>'` (in a pushed-down `WHERE`) | ✅ |
| Aggregates | `COUNT(*)`, `COUNT(DISTINCT <c>)`, `SUM`, `AVG`, `MIN`, `MAX` | ✅ |
| `GROUP BY` | `GROUP BY <col>` | ✅ |
| `HAVING` (incl. by output alias) | `HAVING <agg_alias> > <n>` | ✅ |
| `ORDER BY` | `ORDER BY <col> [ASC|DESC]` | ✅ |
| `DISTINCT` | `SELECT DISTINCT ...` | ✅ |
| Arithmetic expression | `<col> * <n>`, etc. | ✅ |
| `LIMIT` | `LIMIT <n>` (pushed down on a single-backend query) | ✅ |
| `CASE` inside an aggregate | `SUM(CASE WHEN <pred> THEN <e> ELSE 0 END)` | ⚠️ engine path only |
| Return a date/time column | selecting a `date` / `time` / `timestamp` / `datetime` column, local or remote | ✅ the engine's temporal types, sent as PostgreSQL's own |
| Return an `uhugeint` column | selecting a column of that type | ❌ refused `column '<col>' has type UHUGEINT (…), which the PostgreSQL wire cannot encode in text format`; leave the column out (Rule 2) |

### Types: ENUM / STRUCT / composite

| Feature | Syntax | Status |
|---|---|---|
| ENUM cast in predicate | `WHERE <col> = '<value>'::<enum>` | ✅ |
| Struct field access — local table | `(<struct>).<field>` | ✅ |
| Nested struct access — local | `((<struct>).<sub>).<field>` | ✅ |
| Struct field in `WHERE` / JOIN key | `WHERE (<struct>).<field> = ...` | ✅ |
| `IS NULL` on a struct field | `(<struct>).<field> IS NULL` | ✅ |
| Struct access on a **remote** column | `(<struct>).<field>` over a remote scan | ⚠️ engine path + known shape only |
| Multi-dimensional ARRAY | `INT[][]`, ClickHouse `Array(Array(Int32))` | ⚠️ local create/insert and ClickHouse reads work on rc-7; PG wire returns the nested array as text |

### DDL

| Feature | Syntax | Status |
|---|---|---|
| Create database | `CREATE DATABASE <db>` — refused on the PostgreSQL wire (a database is a connection; connections come only from `config.yaml`) | ❌ use `CREATE SCHEMA` for a local namespace |
| Create schema (local) | `CREATE SCHEMA [IF NOT EXISTS] <s>` — lands in the local database (`local.<s>`) wherever the session stands; needed before `CREATE TABLE <s>.<tbl>` / `CREATE EXTERNAL TABLE <s>.<tbl>` | ✅ |
| Create ENUM type | `CREATE TYPE <name> AS ENUM(...)` | ✅ |
| Create composite/STRUCT type | `CREATE TYPE <name> AS (<field> <type>, ...)` | ✅ |
| Create table (scalar + custom types) | `CREATE TABLE <tbl> (<col> <type>, ...)` | ✅ |
| Create / drop a remote table | `CREATE TABLE <alias>.<ns>.<tbl> (...)` / `DROP TABLE <alias>.<ns>.<tbl>` | ⚠️ runs on the backend in autocommit. If the connection has `table:` set, use its configured target |
| Create index | `CREATE INDEX <ix> ON <tbl> (<col>)` — on a local table | ✅ |
| Create index on a **remote** table | `CREATE INDEX <ix> ON <alias>.<ns>.<tbl> (<col>)` | ❌ refused `unimplemented_yet` (`indexes and constraints are not kept`): the engine keeps no index of a connection's table |
| Drop a local index | `DROP INDEX local.<schema>.<table>.<index>` | ✅ qualify the index with its table. The short PostgreSQL forms are rejected |
| Drop a local table or schema | `DROP TABLE …`, `DROP SCHEMA …` | ✅ |
| Drop database | `DROP DATABASE <db>` | ❌ refused on the PostgreSQL wire. For a local namespace use `DROP SCHEMA <s> RESTRICT` to require an empty schema, or `DROP SCHEMA <s> CASCADE` to drop its contents too |

### DML

| Feature | Syntax | Status |
|---|---|---|
| Insert single / multi-row | `INSERT INTO <tbl> (<cols>) VALUES (...),(...)` | ✅ |
| Insert composite values | `... VALUES (ROW(...), NULL, ...)` | ✅ |
| Insert from select | `INSERT INTO <tbl> (<col>) SELECT ...` | ✅ |
| Update with expression | `UPDATE <tbl> SET <col> = <expr> WHERE <pred>` | ✅ |
| Delete | `DELETE FROM <tbl> WHERE <pred>` | ✅ |
| Update / delete a **remote** table | the write runs on its backend as one statement. Direct `FROM` / `USING` sources must be on the same connection. An uncorrelated `WHERE id IN (SELECT …)` can read a local table or a different backend. An untranslatable write returns `unimplemented_yet` (`UPDATE of "<t>" cannot run as one statement of connection "<alias>": ...`) | ⚠️ A failed ClickHouse mutation can leave already rewritten parts changed, even if the agent kills the mutation. Check the affected rows before retrying. |
| `RETURNING` on a remote write | `INSERT` / `UPDATE` / `DELETE … RETURNING <cols>` | ⚠️ PostgreSQL answers every write; MariaDB answers `INSERT` / `DELETE … RETURNING` and — from 13.0 — `UPDATE … RETURNING`, anything else is the server's own refusal; ClickHouse refuses it (`unimplemented_yet`) |

### Transactions and settings

| Feature | Syntax | Status |
|---|---|---|
| Blocks over local tables | `BEGIN` / `COMMIT` / `ROLLBACK` | ✅ |
| Blocks over PostgreSQL / MySQL backends | the block is one backend transaction; `ROLLBACK` undoes the backend writes too | ✅ |
| A write to a **second** connection inside a block | `BEGIN`, a write to one connection's table, a write to another connection's | ❌ the second write is refused `unimplemented_yet` and the block fails until `ROLLBACK`: a block writes to one connection |
| Savepoints | `SAVEPOINT <s>` / `RELEASE SAVEPOINT <s>` / `ROLLBACK TO SAVEPOINT <s>` (inside a block; outside one `25P01`) | ✅ |
| ClickHouse write inside a block | an INSERT / UPDATE / DELETE of a ClickHouse table between `BEGIN` and `COMMIT` | ❌ refused `unimplemented_yet` (ClickHouse has no transactions; autocommit writes work) |
| Engine session settings | `SET` / `RESET` / `SHOW` of `timezone`, `autocommit`, `search_path` | ✅ |
| Client parameters | `SET` / `RESET` / `SHOW` of `application_name`, `extra_float_digits`, `idle_session_timeout` — kept by the PostgreSQL frontend | ✅ |
| Client encoding | `SET` / `RESET` / `SHOW` of `client_encoding` | ⚠️ Accepts `UTF8` and `SQL_ASCII` (including aliases such as `UNICODE` for UTF8), with no transcoding. Other encodings such as `LATIN1` return `22023`, including at connect. |
| Fixed protocol parameters | `SET` / `RESET` / `SHOW` of `DateStyle` (`ISO, MDY`), `IntervalStyle` (`postgres`), `standard_conforming_strings` (`on`) | ⚠️ the frontend returns the fixed value for `SHOW` and refuses attempts to set a different value |
| Server facts | `SHOW server_version`, `SHOW TRANSACTION ISOLATION LEVEL`, `SHOW max_identifier_length` | ✅ the frontend returns its advertised values |
| Any other `SET` | `SET <other> = ...` | ❌ refused `SET <name> is not supported` (inside a block this fails the block) |

### Rules for generating valid OtterStax SQL

Apply these to turn a `⚠️`/`❌` into a `✅`:

1. **Qualify every remote table in three parts**, `<alias>.<ns>.<tbl>` (`<ns>` a
   schema of the connection's database on PostgreSQL, a database on MySQL /
   ClickHouse). Short names use the session's current place, which you can set
   at connect or change with `SET search_path`. Four parts are refused.
2. **Date/time columns come back as dates and times.** A `date` / `time` /
   `timestamp` column, local or from a backend, is the engine's own temporal type and
   goes out as PostgreSQL's (`date`, `time`, `timestamp`, `timestamptz`) — compare it
   with a string literal (`<ts_col> >= '<date>'`) and let the client parse it. A column
   the wire cannot encode is refused, the connection staying usable:
   `column '<col>' has type UHUGEINT (…), which the PostgreSQL wire cannot encode in text format`
   — an `uhugeint` column in any result format (a `hugeint` one is carried as NUMERIC).
   Leave such a column out of the result set.
3. **Reads can fall back to the engine.** A query block over one backend is
   pushed down whole when the backend dialect can express it with the required
   semantics. An expression the dialect cannot express, such as a struct field
   access, can run over a scan when the engine supports that expression.
   If `ROUND` fails with `unrecognized function 'round'`, round in the client
   as described in *Troubleshooting* in `examples/quick_start/README.md`.
   Writes do not fall back (DML rows above).
4. **Prefer `WHERE`-side struct access on remote columns**; for projecting remote
   struct fields, mirror a known-good shape rather than improvising.
5. **Where a `CAST` runs depends on the backend.** On rc-7, casting the quick start
   MariaDB `orders.amount` from DOUBLE to BIGINT reads the filtered raw column
   and performs the cast in the engine (the dialect writes no cast that must
   round and range-check). A PostgreSQL connection takes the whole block as one
   statement, `CAST` included: the same cast leaves as
   `CAST("amount" AS int8)` and matches native PostgreSQL, ties included
   (`2.5 → 2` on both sides).
6. **Send one statement per query.** Use separate `psql -c` arguments for
   multiple statements.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `psql: command not found` | client not installed | `brew install libpq`; add its `bin` to PATH |
| port 8817 `Connection refused` | no OtterStax server on that host/port | start the server or set `OTTERSTAX_HOST`/`OTTERSTAX_PORT` |
| `FATAL 3D000 database "<db>" does not exist` at connect | the client named something that is neither a connection nor `local` | set `OTTERSTAX_DB=local` (a fresh engine's own database) or to a connection alias |
| `FATAL 3F000` at connect | the namespace in the requested place does not exist, including the default namespace of a bare alias | choose an existing namespace with `OTTERSTAX_DB=alias.ns` or `OTTERSTAX_DB=local.ns` |
| `... does not exist` for a one- or two-part name | the current place is unset or resolves the name to a different namespace | set `search_path` in the session, set `OTTERSTAX_DB` for the next connection, or qualify the name in full `<alias>.<ns>.<tbl>` |
| `... does not exist` for `<alias>.<ns>.<tbl>` | alias not declared in the server's config, or a wrong `<ns>` | add the alias under `connections:` in `config.yaml` and restart the server; check the configured `database` / `schema` |
| `a table is named connection.namespace.table` | a four-part name | write the table in three parts, `<alias>.<ns>.<tbl>` |
| `UPDATE of "<t>" cannot run as one statement of connection "<alias>"` | a write that cannot be rendered as one statement on the target backend | keep direct `FROM` / `USING` sources on that connection or use a supported `WHERE` subquery |
| `SET <name> is not supported` | a setting the engine does not have: it has `timezone`, `autocommit`, `search_path` (the frontend keeps `application_name`, `client_encoding`, `extra_float_digits`, `idle_session_timeout` itself) | drop the `SET` |
| `column '<col>' has type UHUGEINT (…), which the PostgreSQL wire cannot encode in text format` | an `uhugeint` column in the result set | leave the column out (Rule 2) |
| `server closed the connection unexpectedly` / `psql exit 2` | the client could not connect or lost its connection | check host and port, try a new connection with `SELECT 1`, and inspect the server logs |
| the server exits 139 or repeatedly dies during startup | the process crashed; check the server log for the cause | check the logs and try a normal restart first — see *If the server crashed or will not start* below |

### If the server crashed or will not start

1. **Check the server log and try a normal restart.** Logs are under `log_dir`
   or, by default, `data_dir`. The engine replays WAL segments stored at
   `data_dir/<database_oid>/wal_*` to recover committed local work. An open
   transaction does not survive. Engine tests cover two rc-3 replay defects fixed
   in rc-4: a crash before the first checkpoint and a large committed insert
   spanning WAL records. For quick start, run these commands from the repository
   root. If you launched the stack with a custom Compose project name, reuse it.

   ```bash
   docker compose -f examples/quick_start/compose.yml logs --tail 100 qs-otterstax
   docker compose -f examples/quick_start/compose.yml restart qs-otterstax
   ```

2. **Use a fresh engine if the logs confirm repeated replay failures and you
   can rebuild its local state.** Save the log and a copy of the old data
   directory first. The quick start mounts its config at `/otterstax/config.yaml`.
   That config sets `service.engine.data_dir` to `/tmp/test_collection_sql/base`,
   which is in the container's own filesystem. From the repository root, stop
   just OtterStax so WAL and checkpoint writes cannot overlap the copy:

   ```bash
   docker compose -f examples/quick_start/compose.yml stop -t 60 qs-otterstax
   docker compose -f examples/quick_start/compose.yml ps -a qs-otterstax
   ```

   Confirm the container shows `Exited` before copying. Save the Compose logs
   and copy the entire directory to a new backup location. Choose fresh names
   if either output path exists. Check that the backup is readable before
   recreating the container:

   ```bash
   docker compose -f examples/quick_start/compose.yml logs --no-color qs-otterstax \
     > otterstax-recovery.log 2>&1
   docker compose -f examples/quick_start/compose.yml cp \
     qs-otterstax:/tmp/test_collection_sql/base ./otterstax-recovery-backup
   ```

   With the backends already running, recreate just OtterStax. `--wait` returns
   only once the new container passes its health check — the PG wire accepting
   connections, which startup reaches after opening the engine, registering the
   backends and recovering Kafka — and fails when it never does; on that failure
   go back to the logs in step 1:

   ```bash
   docker compose -f examples/quick_start/compose.yml up --wait --no-deps --force-recreate qs-otterstax
   ```

   Then confirm the wire answers before rebuilding any state:

   ```bash
   .claude/skills/otterstax-sql/psql.sh -c "SELECT 1"
   ```

   For a native process, stop the server before copying the old directory.
   Keep the old directory and configure an empty `data_dir` for the fresh start.

3. **Rebuild the local state.** A fresh engine loses the local catalog and its
   data, including schemas, tables, views, Kafka `SOURCE` / `STREAM` definitions
   and locally stored read offsets. Re-run the local DDL and reload the rows
   from their sources, including files loaded by `CREATE EXTERNAL TABLE`.
   Recreate Kafka definitions only after checking where consumption should resume
   and how to handle reprocessed messages. A `TRANSACTIONAL = true` source keeps
   its read offset in OtterStax's local state: after the input topic has been
   recreated (a re-seed), compare that offset with the topic's actual state
   (*Verify the Kafka input* in the README) before re-running K1 — the
   materialized row count alone can show stale rows. Recreating only OtterStax
   preserves consumer-group offsets on the running Kafka broker. K1 and K4 in
   `examples/quick_start/README.md` show how to recreate its source and stream. Rows in remote
   databases stay on their backends when only OtterStax is recreated. OtterStax
   discovers the reachable connections from `config.yaml` on startup.
