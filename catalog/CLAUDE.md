# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`catalog/` (CMake target `catalog`) holds the catalog **building blocks** — pure functions and small
types, no actor and no I/O. The engine's own catalog is the only catalog: remote servers, tables and
columns are rows of ordinary engine tables in the database `federation` (layout:
`otterbrix/federation/metadata.hpp`), and a remote name is answered by the federation's `need` /
`decide` hooks (`otterbrix/federation/CLAUDE.md`). What lives here builds the plans that write those
rows, decides when a statement's failure is a rediscovery, names the database names user DDL must
leave alone, and turns the catalog rows into a table listing.

| File | What |
|------|------|
| `discovery.hpp` | The handwritten discovery queries (the reply of a backend's `discover` is `federation::discovered_table_t` / `discovered_tables_t`, `otterbrix/federation/provider.hpp`) and `escape_sql_literal`; a backend's kind is `backend_type_t` (`otterbrix/backend_type.hpp`) everywhere |
| `remote_metadata.hpp` | Plans that write the federation metadata (bootstrap, the rows of another backend kind, a discovery reply), the read of one remote table's spelling, the leftover-mirror probe / drop |
| `remote_rediscovery.hpp` | The session's "does not exist / stale → rediscover → upsert → retry once" flow |
| `ownership.hpp` | `check_database_ownership`: the names a CREATE / DROP DATABASE must leave alone |
| `introspection.hpp` | The Flight SQL GetTables listing (`list_tables`) and the reads it is computed from |

Nothing here throws; every failure is a `core::result_wrapper_t` / `core::error_t` value.

## Discovery (`discovery.hpp`)

A backend (`integration/{sql,postgresql,clickhouse}`, `federation::backend_t`) answers `discover(scope)`:
empty `scope.collection` → every base table of the database (MySQL / ClickHouse) or schema
(PostgreSQL), a list query then one probe per table; non-empty → that table. The metadata queries
(PostgreSQL `pg_enum` and the primary keys of `make_primary_keys_query`, ClickHouse `system.columns`)
are part of the discovery: their failure fails it. `federation::discovered_table_t::primary_key` holds the table's
primary key columns (backend spelling, key order): MySQL reads them off the probe's column definitions
(`PRI_KEY_FLAG`), PostgreSQL from `pg_index.indisprimary` (one query per discovery, in `indkey` order, a
partitioned table's key being its parent's), ClickHouse has none (a sorting key names no row).
Each backend's `discover` empties a key that has a column no literal the federation's row-change sink
writes names exactly: a binary column (PostgreSQL `bytea`, MySQL
`BINARY` / `VARBINARY` / `BLOB` / `GEOMETRY`; each actor names them from the probe's wire types — the
engine holds such a value as a STRING_LITERAL of raw bytes) and a MySQL DECIMAL the engine holds as its
text (wider than 38 digits, from the probe's column type and the discovered type: MySQL compares a
DECIMAL column with a string literal as DOUBLE; PostgreSQL resolves the untyped literal to its NUMERIC
and keeps such a key). A part of a key could name several rows — so the table has no key, and its rows
are not changed one by one.
`escape_sql_literal(resource, value, backend)` quotes the values embedded in the list queries
(`information_schema.tables` / `system.tables`) and in `make_named_types_query`: the apostrophe is
doubled everywhere, the backslash for MySQL and ClickHouse too. Every per-table probe goes through
`sql_gen::generate_query` (`make_schema_probe_query`, `WHERE 1 = 0`).

Startup: `remote_backends_t::open` (`backends/`) runs the discovery of every configured connection and
keeps a connection only when its reply can be recorded (`make_discovery_upsert` validates it); a
connection whose discovery fails is removed from its connector manager and left out (logged).
`backends::publish_federation_metadata` (`backends/backends.hpp`) writes the replies once the engine is open.

## Federation metadata (`remote_metadata.hpp`)

Functions that BUILD execution plans; the caller runs them on one engine session, inside
`BEGIN … COMMIT`, in order (plans are single-use):

| Function | Plans |
|----------|-------|
| `make_metadata_bootstrap(resource)` | `CREATE DATABASE IF NOT EXISTS federation`, `CREATE TABLE IF NOT EXISTS` × 2 — idempotent, run at every start |
| `make_other_kind_removal(resource, uid, kind)` | the `remote_tables` / `remote_columns` rows of the uid (folded to lower case, the key they are written under) written for another backend kind than `kind` removed — an alias that now names a server of another kind: those rows are keyed by database where the new kind keys by schema, or the reverse, so no discovery would replace them; run for every configured connection at startup |
| `make_discovery_upsert(resource, kind, scope, tables)` | a `discover` reply → rows. Empty `scope.collection`: every row under the scope's database (MySQL / ClickHouse) or schema (PostgreSQL) is replaced, so a table the backend dropped disappears; named collection: that table's rows replaced (a reply without tables removes them). Columns: `column_name` lower case, `remote_column` the backend's spelling, `column_type` otterbrix's type spec (`components::catalog::encode_type_spec`; a built-in scalar by its pg_type name), `ordinal` 1-based, `key_position` the column's 1-based position in the primary key (0: not a key column). `schema_error` for a nameless column, two columns folding to one name or a primary key column that is not a column of the table, `conversion_failure` for a type the spec cannot carry; tables folding to one key keep the first |
| `make_remote_table_read(resource, key)` | the rows of one remote table, its backend spelling among them (`remote_database`, `remote_schema`, `remote_table`; read by `with_remote_spelling`, `remote_rediscovery.hpp`) — what a rediscovery of a known table asks the backend for |
| `make_legacy_mirror_probe(resource, uid)` / `make_legacy_mirror_drop(resource, uid)` | a database an older OtterStax kept per connection uid (its mirrors and the manifest `__otterstax_tables`): the probe answers rows when the manifest is there, the drop is `DROP DATABASE` of the uid (CASCADE). `publish_federation_metadata` runs them for every configured uid before writing the metadata, because such a database would capture the connection's remote names (G1) |

The metadata database name `federation` is reserved: `config.yaml` refuses a connection alias
`federation` in any case (`config/CLAUDE.md`), and `check_database_ownership` refuses user DDL on it.

## Rediscovery (`remote_rediscovery.hpp`)

The query session's recovery (`query_session_t::run_on_engine`): a statement fails →
`rediscovery_targets(error, names, registry)` answers the targets
(`rediscovery_target_t{type, key, discovery_scope}`) when the error is a missing relation
(`table_not_exists`, `database_not_exists`, or the engine's "schema … does not exist"
`invalid_parameter`) or a stale remote schema (the storage's `schema_error` "… changed since it was
discovered": a column of another type or another number of columns, of the scan or a pushed block) and a statement name resolves to a uid; the session
retries a statement once, whatever the retry answers. `discovery_scope` is what the backend's
`discover` takes: the uid in its config spelling, the database / schema / table as the statement wrote them (a quoted identifier keeps its
case). For a table the metadata already holds, `with_remote_spelling(target, rows)` replaces the
written spelling with the backend's (the rows `make_remote_table_read` read): a case-sensitive backend finds
`Orders`, not `orders`. `make_rediscovery_plans(resource, target, reply)` turns the reply into upsert
plans; an error reply keeps its code, an empty reply is `table_not_exists` — the session then reports
the statement's original error.

`rediscovery_claim_t::try_claim(key)` — a process-wide set of the keys being rediscovered: two
sessions that miss the same table at once rediscover it once instead of each writing a copy of its
rows (the second skips the discovery; its retry sees the rows if the first has written them by then). `decide` also reads a column recorded twice
once (`otterbrix/federation/name_resolution.cpp`), so a row duplicated anyway never doubles a column.

## Owned database names (`ownership.hpp`)

`CREATE DATABASE` / `DROP DATABASE` are engine-local by grammar; three kinds of name belong to
OtterStax: a connection uid of the federation registry (an engine database of that name would capture
the connection's remote names — the engine resolves its own catalog first), `kafka` (the Kafka object
database) and `federation` (the metadata). `check_database_ownership(resource, name)` answers
`invalid_parameter` `database '<name>' is owned by connection '<owner>'` for them, ASCII
case-insensitively (an unquoted identifier reaches the engine lower-cased), the owner in its own
spelling. The query session checks every CREATE / DROP DATABASE before it reaches the engine,
on every entry point that runs or describes a plan.

## Introspection (`introspection.hpp`)

`list_tables(resource, command, namespaces, classes, attributes, remote_tables, remote_columns)` — the
Flight SQL GetTables answer from five reads the frontend runs through a query session
(`namespaces_sql`, `classes_sql`, `attributes_sql`, `remote_tables_sql`, `remote_columns_sql`): the
engine's own tables (pg_catalog, named (database, "", table)) and the remote tables the federation
knows (named (remote database, remote schema, table), schema empty for MySQL / ClickHouse — the
spelling the old catalog listed). Hidden: system rows and namespaces, the federation metadata tables
(`federation.*`) and the Kafka bookkeeping tables (`kafka.__sources`, `kafka.<source>__offsets`,
`is_internal_table`). `get_tables_command_t`: with `include_schema` the columns ride as a STRUCT named
by the engine's column names; the GetTables filters (catalog, the LIKE patterns, table types) are the
Flight SQL frontend's, applied over the listing (`frontend/flight_sql/core/commands.cpp`). Every table
is listed under `table_type_name` (`"TABLE"`). The Spark catalog
(`frontend/spark_connect_server/catalog_rows.hpp`) reads the same rows its own way.

## Gotcha: DAY/SECOND macro clash

A file that includes the Otterbrix parser before Arrow must `#undef DAY` and `#undef SECOND` in
between: the parser defines them as macros that collide with Arrow's symbol names.

## Tests

`tests/system/test_catalog.cpp` (`test_system`): literal escaping, the GetTables listing over a real
stack (remote and local tables, `include_schema`), the startup discovery error paths (a connection
left out, nothing recorded), the owned names, remote DDL. `tests/system/test_restart_reconciliation.cpp`:
the metadata over restarts (rows replaced, a changed schema read with its new columns, a dropped table
gone, a leftover mirror database dropped) and the rediscovery of a table created or changed while the
server runs, under the backend's spelling; a composite PostgreSQL key recorded in `indkey` order as
`key_position` and naming the row a change writes. `tests/system/test_backend_discovery.cpp`: the MySQL
key off `PRI_KEY_FLAG`, a VARBINARY key and a key over a DECIMAL read as text dropped. `tests/system/test_database_ownership_guard.cpp`: the
guard through a session. `tests/federation/` (`test_federation`): the metadata plans (an alias that changed
backend kind), the claim, the rediscovery flow over a real engine
with fake backends (a stale schema of the scan and of a pushed block, a missing relation).
