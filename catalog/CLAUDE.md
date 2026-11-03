# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`catalog/` (CMake target `catalog`) holds the registration of the connections' tables in otterbrix's
catalog, the discovery queries, and the database names user DDL must leave alone. otterbrix's catalog
is the only catalog: a remote table is rows of `pg_database` (`datconnection`), `pg_namespace`,
`pg_class`, `pg_attribute`; the backend's spelling is the backend actor's
(`otterbrix/federation/CLAUDE.md`).

| File | What |
|------|------|
| `discovery.hpp` | The handwritten discovery queries the backend agents run (`otterbrix/federation/backend_agent.hpp`), the schema probe, `make_discovery_error`, `escape_sql_literal` (the quote doubled; the backslash doubled on MySQL / ClickHouse, and on PostgreSQL inside `E'…'` for a value that holds one) |
| `registration.hpp` | `register_connections` (startup), `registration_target` / `registration_plans` (a table a statement missed), `check_table_scope` (DDL of a one-table connection) |
| `ownership.hpp` | `check_database_ownership(resource, name, aliases)`, `check_write_ownership(resource, target, drops_schema)` |

Nothing here throws; every failure is a `core::result_wrapper_t` / `core::error_t` value.

## Registration (`registration.hpp`)

Every statement runs on the backend actor's registration session, so no SQL reaches a backend.

- **Startup** (`register_connections`, through `backends::register_remote_tables`): `CREATE DATABASE IF
  NOT EXISTS federation` and `federation.public.remote_names`; the catalog's tables of every connection
  read from pg_catalog with their live columns (types decoded from `atttypid` / `atttypspec`) and their
  spelling from `remote_names`, handed to the actor (`load_tables`); then each connection whose
  discovery answered brought in line: a new table → `CREATE DATABASE` / `SCHEMA` / `TABLE IF NOT EXISTS`
  of it (its spelling staged in the actor), a table the backend lost → `DROP TABLE`, a column gone or
  of another type → `ALTER TABLE … DROP COLUMN`, a new or retyped one → `ADD COLUMN`. A change the
  catalog cannot follow (a local view depends on the table or column: the DROP is RESTRICT) leaves the
  table as the catalog has it, with a warning; startup goes on. A connection the backend did not
  answer keeps its rows; one config.yaml no longer names keeps its databases (a warning in the
  BACKENDS log). The startup discovery is scoped by the connection's `table:` (empty: every table).
  An engine refusal of anything else aborts startup.
- **Names.** Lower case: `c.d.s.t` with `d` the configured database, `s` = `public` for MySQL /
  ClickHouse, the discovered schema for PostgreSQL (config `schema: X` → only X; none → every user
  schema). Two columns folding to one name leave the table out.
- **A miss** (`registration_target`): a statement's `table_not_exists` / `schema_not_exists` /
  `database_not_exists` over a four-part name whose first part is a configured connection gives the
  discovery scope (the connection and database as configured, the written schema and table; a
  connection configured with one `table:` is that table only — another name of it is no target, the
  statement's own miss answers and no discovery reaches the backend, as the startup's discovery and
  its reconciliation keep that table alone —, and the scope names it as configured; a table spelled
  in another case than the backend's is found only when written — or configured — in the backend's
  spelling); a discovery that fails at the connection (`io_error` / `connection_closed`: the
  connection lost or never opened, marked unreachable, no free connection) is the statement's answer
  instead of the miss, one the backend answered without the table leaves the miss; the
  session discovers it (the backend actor's `discover`), runs `registration_plans` on the registration
  session — inside a block too — and runs the statement once more only when its failure left the
  session outside a block. Every route a statement takes to the engine does so (a run, a describe —
  every prepare —, a Spark DataFrame plan with the frontend's translation as its rebuild, COPY's
  inner query: `query_session_t::on_engine`). `t` / `s.t` are completed with the session's place
  (`current_database()` / `current_schema()`): the engine does not name the place's connection, so the
  name is completed for every connection whose database the place's is, and the backend that has the
  table registers it.
- **DDL of a one-table connection** (`check_table_scope`): a connection configured with one `table:`
  is that table for DDL too. A CREATE / ALTER / DROP TABLE, a constraint or a CREATE INDEX
  (`session::plan::write_target`'s `table_ddl`) — and a CREATE EXTERNAL TABLE — whose four-part
  target is of such a connection and names another table (any case) is refused by the session before
  the engine (`invalid_parameter` `connection "<c>" is configured with table "<X>": DDL of its table
  "<Y>" is refused`; a prepare too): nothing reaches the backend, and no table appears that the next
  startup's discovery, scoped by `table:`, would drop from the catalog. The configured table's own
  DDL and every DDL of a connection without `table:` go on as before. A short name in a place that is
  the connection's database is not seen (the engine does not name the place's connection, and a
  local database may share the name).

## Owned database names (`ownership.hpp`)

SQL's `CREATE DATABASE` / `DROP DATABASE` are local; refused (`invalid_parameter` `database '<name>' is
owned by connection '<owner>'`, ASCII case-insensitively): a connection alias of config.yaml and
`federation` (the Kafka objects are the schema `main.kafka`, guarded below). The session passes
`remote_backends_t::aliases()`.
Inside them (`check_write_ownership`, `invalid_parameter` `... is owned by OtterStax ...`): any DDL or DML
of a relation written in `federation` (`session::plan::write_target`; CREATE EXTERNAL TABLE too), and
DROP SCHEMA of `main.kafka` — a simple name, completed by the session with its place (a place it cannot
read counts as main). A relation name the place completes into `federation` is not seen.

## Tests

`tests/federation/test_federation_registration.cpp` over a real otterbrix and fake backends;
`tests/system/test_restart_reconciliation.cpp`, `test_catalog.cpp`, `test_backend_discovery.cpp`,
`test_database_ownership_guard.cpp`, `test_registration_scope.cpp` over the session stack.
