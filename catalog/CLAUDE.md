# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`catalog/` contains a single actor: `mysql::CatalogManager`. It serves two purposes:

1. **Schema resolution** — given a parsed query, resolves every external (remote-backend) table to the schema discovered at registration time, stamps the engine OID onto the plan node, and classifies the statement's `backend_type` so the Worker can route it.
2. **External-table registration** — discovers table schemas on the remote backends and mirrors them into the engine `pg_catalog` (through `OtterbrixManager`) so the planner can resolve them by OID.

Despite the `mysql::` namespace the class handles all three backend types.

The catalog reaches a backend only through its integration actor: schema discovery is a `discover` message to `db::MySQLManager` / `db::PostgressManager` / `db::ClickhouseManager` (addresses set once through `CatalogManager::set_backend_managers`, which `ComponentManager` calls), and the catalog `co_await`s the reply — it never holds a connector manager and never blocks on a connector future.

`catalog/discovery.hpp` holds what the catalog and those actors share: `catalog_ext::ConnectionType`, `discovered_table_t` / `discovered_tables_t` (the reply of `discover`), and the handwritten query builders. `otterstax::catalog::escape_sql_literal(resource, value, backend)` quotes the values embedded in the discovery list queries (`information_schema.tables` / `system.tables`) and in the ClickHouse named-types query (`make_named_types_query`, `system.columns`). It is dialect-aware: the apostrophe is doubled everywhere, and the backslash is doubled for MySQL and ClickHouse, where it is an escape character inside a literal. Every per-table probe goes through `sql_gen::generate_query` instead (`make_schema_probe_query`).

## Actor Handlers (`dispatch_traits`)

| Method | Called by | Purpose |
|--------|-----------|---------|
| `update_backend_type` | `Worker::execute` (statements with external nodes) | Lazily registers unknown external tables, stamps OIDs, sets `backend_type`; classifying a statement that already carries a backend type is an `invalid_parameter` error |
| `get_catalog_schema` | `Worker::prepare_schema` (extended-protocol prepare) | Same pre-pass as `update_backend_type`, then rewrites each external aggregate into a `schema_node_t` carrying the projected output schema |
| `add_connection_schema(name, type)` | The three `ConnectorManager::addConnection` (eager, at startup; the message names the backend type, which the manager knows) and the catalog itself (lazily, from the pre-pass above, with the type from `connection_registry_` — a uid the registry does not know is `do_not_exists`) | Send `discover(name)` to the backend actor of `type`, register the discovered table(s) in the engine, mirror them in `store_`. A uid already registered under another type is `invalid_parameter`; a type whose backend actor address is empty is `do_not_exists`. See "Registration on a persisted data dir" below |
| `get_tables` | FlightSQL frontend (`DoGetTables`) | List mirrored tables for the `GetTables` RPC. `catalog` is an exact match on the database part; `db_schema_filter_pattern` / `table_name_filter_pattern` are SQL LIKE patterns (`%`, `_`); a non-empty `table_types` lists anything only if it names `catalog_ext::table_type_name` (`"TABLE"`) |
| `check_database_ownership` | `Worker::guard_database_ddl` (every `CREATE DATABASE` / `DROP DATABASE`, on all three Worker entry points) | Answers `invalid_parameter` (`database '<name>' is owned by connection '<uid>'`) when the name matches a uid in `connection_registry_` or `KAFKA_DATABASE_NAME`, case-insensitively — the engine database mirroring a connection must not be created or dropped by user DDL. Any other name passes with `no_error` |

There is no removal handler: connections are read once from `config.yaml` at startup and live for the process lifetime.

## State

- `otterstax::catalog::schema_store_t store_` — actor-confined mirror of external table schemas (qualified name + STRUCT, keyed by engine pg_class oid); the tables themselves are registered in the engine pg_catalog via `OtterbrixManager`, one engine database per connection uid
- `connection_registry_` (`pmr::unordered_map<uid, ConnectionType>`) — private to the actor; a uid is entered only after `add_connection_schema` mirrored its tables, so a registry miss is what makes a registration the uid's first in this process (the one that runs `register_external_database` and the stale-mirror drop; a failed attempt repeats it). It drives the two catalog-internal decisions that need a backend type before the store is consulted: dropping the schema qualifier for non-PostgreSQL targets during the pre-pass, and `backend_type` classification. Nothing outside the catalog reads it — the Worker routes on the `backend_type` / `node_backend_types` the catalog writes onto `ParsedQueryData`
- Three backend actor addresses (`set_backend_managers`, called by `ComponentManager` once the integration actors exist — the connector managers were built with the catalog's address first, hence the setter). The catalog holds no connector manager: the per-connection metadata a query needs (PostgreSQL ENUM oids, ClickHouse named types) is state of the actor that discovered it
- No locking besides `mutex_` in `enqueue_impl`: every handler body runs to completion on the sender's thread under that mutex, so the registry and the store never see concurrent access. The `discover` reply is settled by the time `send` returns (the backend actors run their handlers to completion the same way), so the `co_await` on it never suspends across another catalog message

## Backend-type classification (`update_backend_type_impl`)

`update_backend_type_impl` iterates `data->otterbrix_params->external_nodes`
and sets `data->backend_type` from what it finds:

- ≥ 2 distinct registered backend connections present → `Mixed`.
- Exactly one registered backend present → that backend (`MySQL`,
  `PostgreSQL`, or `ClickHouse`).
- No registered backend, but the plan root (`otterbrix_params->node`) is
  `join_t` / `intersect_t` / `union_t` → `Otterbrix`.
- Otherwise → `schema_error`.

DDL targets are exempt from OID stamping and from lazy registration: `CREATE`
targets a table that does not exist yet, `DROP TABLE` / `DROP INDEX` remove one.
Those two are the only drop kinds the parser lets into `external_nodes`
(`carries_table_reference`); any other drop kind there is an
`invalid_parameter` error, not a target to skip. Subquery stubs
(`schema_node_t`, `node_type::unused`) are exempt too: they name no relation to
register, so there is nothing to stamp an OID onto. Their schema is the
backend's: the `describe` handler of the backend that owns the slot — MySQL,
PostgreSQL or ClickHouse — writes it into the stub at prepare
(`integration/CLAUDE.md`); a stub nobody described carries `NA`.

**Backend ⋈ otterbrix-internal isn't `Mixed`.** The parser drops local tables
from `external_nodes` (`otterbrix/parser/parser.cpp` — empty
`unique_identifier` skipped) so the catalog only sees the registered backend
side. The query is classified as that single backend type. The backend
manager fetches its slice, inlines it as `node_raw_data`, and the
`OtterbrixManager` resolves the still-symbolic local-table nodes via their
stamped `table_oid` (set on `target.oid` here) before running the JOIN.
`examples/demo/sql/step_4.sql` is the live demo of this.

## Registration on a persisted data dir

The engine restores its catalog from disk, so on a restart the uid's mirror
database and its collections already exist when `add_connection_schema` runs.
`register_tables` reconciles instead of refusing, on the uid's first
registration in the process (registry miss):

1. `OtterbrixManager::register_external_database(uid)` — creates the database,
   or answers `false` when the engine already holds it: `check_database_ownership`
   keeps user DDL off the name, so an existing database of the uid's name is
   the previous run's mirror (the only exception is a data dir written before
   the guard existed, where a user database of that name is taken for the
   mirror).
2. When it was reused, `OtterbrixManager::drop_stale_external_tables(uid, live)`
   with the discovered names: every mirror the uid's manifest lists that the
   discovery did not return is dropped.
3. Per discovered table, `register_external_table` (unchanged call): the engine
   side creates an absent collection, reuses a present one with the same column
   names and types under its restored OID, and drops + recreates one whose
   schema changed. The OID it answers — created or restored — is what `store_`
   holds, exactly as for a fresh registration.

The manifest, the engine-side probe and the encoding live in
`integration/otterbrix/otterbrix_manager.*` (see `integration/CLAUDE.md`).
`tests/system/test_restart_reconciliation.cpp` drives all three outcomes over
a real engine restarted on the same data dir.

## Discovery contract (`discover` on the backend actors)

`discover_connection_schemas` picks the backend actor by `ConnectionType` and
sends it `discover(name)`. Empty `name.collection` → every base table of the
database/schema the name carries (list query, then one probe per table);
non-empty → that single table. The name is the only input: the eager path
builds it from the connect params in `addConnection`, the lazy path takes it
from the statement. PostgreSQL needs a non-empty schema, MySQL and ClickHouse
a non-empty database (`missing_field`) — there is no default. The metadata
queries (PostgreSQL `pg_enum`, ClickHouse `system.columns`) are part of the
discovery: their failure fails it. A ClickHouse probe that returns no header
block, or a block without columns, is a `schema_error`; a column-less table is
never registered. A discovered column without a name (a STRUCT field whose
type has no alias) is a `schema_error` for the whole registration, checked in
`register_tables` before the engine database is touched: a mirrored column is
defined by its name, and the engine's `alias()` has no null guard. PostgreSQL
and MySQL never answer one for a table; the check keeps a backend that does from
killing the process during the startup registration. Any per-table failure fails
the whole discovery.

## Gotcha: DAY/SECOND macro clash

`catalog_manager.hpp` does `#undef DAY` and `#undef SECOND` after including the Otterbrix parser but before including Arrow. This is required because the Otterbrix parser defines these as macros that collide with Arrow's symbol names. Do not remove these undefs.

## Tests

`tests/system/test_catalog_manager.cpp` (`test_system`) drives the actor
directly over mocked connectors: literal escaping, `GetTables` filters, the
DDL/drop-kind exemptions, repeated classification, and the discovery error
paths. `tests/system/test_scheduler.cpp` covers `get_catalog_schema` through
the sequence-root unwrap. `tests/system/test_database_ownership_guard.cpp`
drives `check_database_ownership` through a real Scheduler stack with a
registered mock uid (refused DDL on every entry point, mirror intact).
`tests/system/test_restart_reconciliation.cpp` registers a PostgreSQL mock
connection, restarts the engine on the same data dir and registers again: the
mirror database is reused and OIDs survive, a table whose backend schema
changed is recreated with the new columns, a table the backend dropped is
dropped from the engine and the manifest.
