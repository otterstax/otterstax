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

## Connecting: `psql.sh`

Use the bundled wrapper for every interaction:

```bash
.claude/skills/otterstax-sql/psql.sh -c "SELECT 1"        # one statement
.claude/skills/otterstax-sql/psql.sh -f query.sql         # a .sql file
echo "SELECT 1" | .claude/skills/otterstax-sql/psql.sh    # SQL from stdin
```

It calls the host `psql` against the OtterStax PG wire. **The wire ignores
authentication** — any user/db/password is accepted (it's a federation router,
not a real PG server), so the defaults below work out of the box against a local
engine. Override `HOST`/`PORT` only to reach a remote one:

| Var | Default | Notes |
|---|---|---|
| `OTTERSTAX_HOST` | `localhost` | |
| `OTTERSTAX_PORT` | `8817` | |
| `OTTERSTAX_USER` | `otterstax` | ignored by the wire |
| `OTTERSTAX_DB` | `otterstax` | ignored by the wire |
| `OTTERSTAX_PASSWORD` | *(empty)* | ignored by the wire |

`psql` must be on PATH. On macOS: `brew install libpq` then add
`/opt/homebrew/opt/libpq/bin` to PATH.

If a call fails with `psql exit 2` / `server closed the connection`, the engine is
down or a query crashed it — see **Engine crashes** below; the wrapper prints a
recovery hint on that exit code.

## Federated query syntax

A connection alias is the outermost qualifier. Register backends via the HTTP
connection API (default port 8085), then reference their tables as:

```sql
SELECT * FROM <alias>.<db>.<tbl> JOIN <alias2>.<db>.<tbl2> ON ...
```

A query to an unregistered alias fails with `database does not exist`.

## SQL support matrix

What OtterStax accepts over the wire, schema-independent. Generic placeholders:
`<tbl> <col> <struct> <field> <type> <enum> <alias> <db>`. Substitute whatever
your registered backends expose. **Status legend:**
`✅ works · ⚠️ path-dependent (read the rule) · ❌ known-broken (use the workaround)`.

The **decisive factor** is the execution path (see *Rules* below): a query hitting
**one** remote backend is pushed down to that backend's native SQL wholesale;
a **federated** (multi-backend) or **local-table** query is planned by the
otterbrix engine. Some features only survive the engine path.

### Table identifiers

| Form | Pattern |
|---|---|
| 3-part | `<alias>.<db>.<tbl>` |
| 4-part | `<alias>.<db>.<schema>.<tbl>` |

`<alias>` = a connection registered via the HTTP API. The parser promotes 3-part
to its 4-part shape, then emits backend-native qualifiers (`db.tbl` for MySQL/CH,
`schema.tbl` for PG).

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
| `LIMIT` | `LIMIT <n>` | ⚠️ ignored on single-backend pushdown |
| `CASE` inside an aggregate | `SUM(CASE WHEN <pred> THEN <e> ELSE 0 END)` | ⚠️ engine path only |
| Return a timestamp column | selecting a `timestamp`/`datetime` col | ❌ wrap-subquery workaround |

### Types: ENUM / STRUCT / composite

| Feature | Syntax | Status |
|---|---|---|
| ENUM cast in predicate | `WHERE <col> = '<value>'::<enum>` | ✅ |
| Struct field access — local table | `(<struct>).<field>` | ✅ |
| Nested struct access — local | `((<struct>).<sub>).<field>` | ✅ |
| Struct field in `WHERE` / JOIN key | `WHERE (<struct>).<field> = ...` | ✅ |
| `IS NULL` on a struct field | `(<struct>).<field> IS NULL` | ✅ |
| Struct access on a **remote** column | `(<struct>).<field>` over a remote scan | ⚠️ engine path + known shape only |
| Multi-dimensional ARRAY | `<type>[][]` | ❌ 1-D only |

### DDL

| Feature | Syntax | Status |
|---|---|---|
| Create database | `CREATE DATABASE <db>` | ✅ |
| Create ENUM type | `CREATE TYPE <name> AS ENUM(...)` | ✅ |
| Create composite/STRUCT type | `CREATE TYPE <name> AS (<field> <type>, ...)` | ✅ |
| Create table (scalar + custom types) | `CREATE TABLE <tbl> (<col> <type>, ...)` | ✅ |
| Create index | `CREATE INDEX <ix> ON <tbl> (<col>)` | ✅ |
| Drop index / table / database | `DROP INDEX|TABLE|DATABASE ...` | ✅ |

### DML

| Feature | Syntax | Status |
|---|---|---|
| Insert single / multi-row | `INSERT INTO <tbl> (<cols>) VALUES (...),(...)` | ✅ |
| Insert composite values | `... VALUES (ROW(...), NULL, ...)` | ✅ |
| Insert from select | `INSERT INTO <tbl> (<col>) SELECT ...` | ✅ |
| Update with expression | `UPDATE <tbl> SET <col> = <expr> WHERE <pred>` | ✅ |
| Delete | `DELETE FROM <tbl> WHERE <pred>` | ✅ |

### Rules for generating valid OtterStax SQL

Apply these to turn a `⚠️`/`❌` into a `✅`:

1. **Qualify every table** as `<alias>.<db>.<tbl>` (or 4-part). Bare names don't
   resolve to a backend.
2. **Never put a `timestamp`/`datetime` column in the result set.** Wrap the remote
   scan in a derived table that projects only the non-temporal columns you need,
   and keep the time filter *inside* that subquery — it pushes down to the backend
   where the type is fine. This is the single most common failure.
3. **Route engine-only features through the engine.** `CASE`-in-aggregate and
   struct/composite field access need the otterbrix plan. If the query touches only
   one remote backend and one of these errors (`sum requires single argument`,
   `<struct> / <field>`, `path '<field>' not found`), add a join to a second source
   or a local table so the engine plans it.
4. **Prefer `WHERE`-side struct access on remote columns**; for projecting remote
   struct fields, mirror a known-good shape rather than improvising.
5. **Don't rely on `LIMIT` for correctness on a single-backend query** — it can be
   dropped in pushdown. Filter with `WHERE` instead, or add a join.
6. **Avoid `CAST` on pushed-down scans** (results can be garbled); project the raw
   column.
7. **One query per connection at a time.** Cross-backend `ARRAY` and exotic struct
   shapes can crash the engine — keep array columns 1-D and out of cross-backend
   joins.
8. **Keep federated aggregations small.** A federated query with **several
   `CASE`-in-aggregate expressions at once** has been observed to SIGSEGV the
   engine (see below). One or two are fine; if you need many, split them into
   separate queries.

## Engine crashes

Some queries crash the OtterStax process outright (`psql exit 2` /
`server closed the connection unexpectedly`; the container exits **139** =
SIGSEGV). Confirmed triggers in this codebase:

- A **federated** query stacking **multiple `CASE WHEN ... THEN ... END` aggregates**
  in one `SELECT` (e.g. four `SUM(CASE ...)` over a cross-backend join).
- Cross-backend `ARRAY` queries and some exotic struct shapes (per project TODOs).

**Recovery is not a plain restart.** The crash can leave the local WAL
(`/tmp/.../base/.wal_*`) in a state that makes the process **crash-loop on replay**,
so `restart` keeps dying. Instead:

1. Recreate the engine **fresh** (new process / `docker compose up -d
   --force-recreate <engine>`), which starts from a clean WAL.
2. **Re-register every backend connection** via the HTTP API — registrations live
   in memory and are gone after a restart.
3. Then re-run your query in a **safer shape** (fewer aggregates per statement).

To avoid it: prefer one aggregate per federated query, and validate a new query
shape on a small `LIMIT`/`WHERE` slice before running it broadly.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `psql: command not found` | client not installed | `brew install libpq`; add its `bin` to PATH |
| port 8817 `Connection refused` | no OtterStax server on that host/port | start the server or set `OTTERSTAX_HOST`/`OTTERSTAX_PORT` |
| `database does not exist` | alias not registered | register the backend via the HTTP connection API |
| `Cant find to_local_type translator for type: timestamp` | timestamp in result set | apply Rule 2 (wrap-subquery) |
| `sum requires single argument` / `<struct> / <field>` / `path '<field>' not found` | engine feature pushed to a single backend | apply Rule 3 (route through the engine) |
| `server closed the connection unexpectedly` / `psql exit 2` | engine crashed mid-query (SIGSEGV) | see **Engine crashes** — recreate fresh (not plain restart) + re-register connections |