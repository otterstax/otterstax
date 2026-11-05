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
| `ownership.hpp` | `check_write_ownership(resource, target, drops_schema)` |

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
  BACKENDS log). The startup discovery is scoped by the connection's `table:` (empty: every table;
  a MySQL / ClickHouse connection without `table:` discovers every user database of its server). An
  engine refusal of anything else aborts startup. Before any of it the startup creates the local
  database `local`, leaves fresh sessions without a place (`ALTER SYSTEM SET search_path = ''`) and
  refuses an alias that is the name of an existing local database (the engine would read the first
  part of a three-part name as both) — with connections or none.
- **Names.** Lower case, one engine database per connection: `c.d.ns.t` with `d` the configured
  database for PostgreSQL and `def_database` (`def`) for MySQL / ClickHouse; `ns` the discovered
  schema for PostgreSQL (config `schema: X` → only X; none → every user schema) and the backend's
  database for MySQL / ClickHouse. A client names the table `c.ns.t` (the engine reads a first part
  that is a connection as its database). Two columns folding to one name leave the table out.
- **A miss** (`registration_target`): a statement's `table_not_exists` / `schema_not_exists` /
  `database_not_exists` over a name `c.ns.t` whose first part is a configured connection gives the
  discovery scope (the connection and its database — PostgreSQL's configured one, the namespace for
  MySQL / ClickHouse —, PostgreSQL's schema and the table; a
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
  inner query: `query_session_t::on_engine`). `t` / `s.t` are completed with the session's place: in a
  connection (its full place, `search_path` = `c.d.ns`, names it) they are `c.ns.t` / `c.s.t`.
- **DDL of a one-table connection** (`check_table_scope`): a connection configured with one `table:`
  is that table for DDL too. A CREATE / ALTER / DROP TABLE, a constraint or a CREATE INDEX
  (`session::plan::write_target`'s `table_ddl`) — and a CREATE EXTERNAL TABLE — whose target names
  such a connection (`c.ns.t`) and names another table (any case) is refused by the session before
  the engine (`invalid_parameter` `connection "<c>" is configured with table "<X>": DDL of its table
  "<Y>" is refused`; a prepare too): nothing reaches the backend, and no table appears that the next
  startup's discovery, scoped by `table:`, would drop from the catalog. The configured table's own
  DDL and every DDL of a connection without `table:` go on as before. A short name in a place that is
  the connection's database is not seen (the engine does not name the place's connection).

## Owned database names (`ownership.hpp`)

A client's `CREATE DATABASE` / `DROP DATABASE` is a schema of `local` or refused before it gets here
(`session::plan::place_namespace_ddl`); what still names an engine database is refused
(`invalid_parameter` `database '<name>' is owned by connection '<owner>'`, ASCII case-insensitively): a
connection alias of config.yaml, `federation` and `local`. The session passes
`remote_backends_t::aliases()`.
Inside them (`check_write_ownership`, `invalid_parameter` `... is owned by OtterStax ...`): any DDL or DML
of a relation written in `federation` (`session::plan::write_target`; CREATE EXTERNAL TABLE too), and
DROP SCHEMA of `local.kafka`, the Kafka objects (a client's DROP SCHEMA names a schema of local). A
relation name the place completes into `federation` is not seen.

## Tests

`tests/federation/test_federation_registration.cpp` over a real otterbrix and fake backends;
`tests/system/test_restart_reconciliation.cpp`, `test_catalog.cpp`, `test_backend_discovery.cpp`,
`test_database_ownership_guard.cpp`, `test_registration_scope.cpp` over the session stack.
