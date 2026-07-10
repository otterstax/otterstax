# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`catalog/` contains a single actor: `mysql::CatalogManager`. It serves two purposes:

1. **Schema resolution** — given a parsed query, fetches column schemas from the relevant remote backends so the Scheduler can compute the output schema without executing the query.
2. **Connection registry** — maps connection UUIDs to `catalog_ext::ConnectionType` (`MySQL`, `PostgreSQL`, `ClickHouse`); used by `Scheduler` to route mixed-backend queries.

Despite the `mysql::` namespace the class handles all three backend types.

## Actor Handlers (`dispatch_traits`)

| Method | Called by | Purpose |
|--------|-----------|---------|
| `get_catalog_schema` | Scheduler | Fetch schemas for all external nodes in a `ParsedQueryData`; sets `backend_type` |
| `update_backend_type` | Scheduler | Re-classify backend after schema is known |
| `add_connection_schema` | ComponentManager (at connection registration) | Discover and store schema for a new connection |
| `remove_connection_schema` | ComponentManager (at connection removal) | Drop cached schema |
| `get_tables` | FlightSQL frontend | List tables for `GetTables` RPC |

## State

- `otterstax::catalog::schema_store_t store_` — actor-confined mirror of external table schemas (qualified name + STRUCT, keyed by engine pg_class oid); the tables themselves are registered in the engine pg_catalog via `OtterbrixManager`, one engine database per connection uid (tracked in `registered_dbs_`)
- `connection_registry_` (`unordered_map<uuid, ConnectionInfo>`) — guarded by `connection_registry_mtx_`; the sole source of truth for which UUID maps to which backend type
- Three `ConnectorManager` shared_ptrs set via setters after construction (circular reference avoided — catalog is constructed first, managers pass catalog's address to their own constructors)

## Backend-type classification (`update_backend_type_impl`)

`update_backend_type_impl` iterates `data->otterbrix_params->external_nodes`
and sets `data->backend_type` from what it finds:

- ≥ 2 distinct registered backend connections present → `Mixed`.
- Exactly one registered backend present → that backend (`MySQL`,
  `PostgreSQL`, or `ClickHouse`).
- No registered backend, but the plan root is `join_t` / `intersect_t` /
  `union_t` (the `has_otterbrix_nodes` term) → `Otterbrix`.
- Otherwise → unknown / error.

**Backend ⋈ otterbrix-internal isn't `Mixed`.** The parser drops local tables
from `external_nodes` (`otterbrix/parser/parser.cpp:212` — empty
`unique_identifier` skipped) so the catalog only sees the registered backend
side. The query is classified as that single backend type. The backend
manager fetches its slice, inlines it as `node_raw_data`, and the
`OtterbrixManager` resolves the still-symbolic local-table nodes via their
stamped `table_oid` (set on `target.oid` here) before running the JOIN.
`examples/demo/sql/step_4.sql` is the live demo of this — see also
`FIX_JOIN.md`.

## Gotcha: DAY/SECOND macro clash

`catalog_manager.hpp` does `#undef DAY` and `#undef SECOND` after including the Otterbrix parser but before including Arrow. This is required because the Otterbrix parser defines these as macros that collide with Arrow's symbol names. Do not remove these undefs.
