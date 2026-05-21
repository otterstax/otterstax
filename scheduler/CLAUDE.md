# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`scheduler/` contains the central routing actor (`Scheduler`) and schema computation utilities (`schema_utils`). The Scheduler is the only actor that frontends talk to directly.

## Scheduler Actor

Entry-point handlers (all return `unique_future<void>`, signal completion via `shared_session_payload`):

| Handler | Caller | Purpose |
|---------|--------|---------|
| `execute(id, sdata, sql)` | FlightSQL/MySQL/PG frontend | Full query: parse → schema → dispatch → translate |
| `execute_statement(id, sdata)` | Frontend (prepared stmt execute) | Execute a previously prepared statement |
| `execute_prepared_statement(id, params, sdata)` | Frontend | Execute with bound parameters |
| `prepare_schema(id, sdata, sql)` | Frontend (prepare phase) | Parse + schema-only, no execution |

## Internal State

- `shared_data_map_` — maps `session_hash_t → shared_session_payload`; guarded by `data_map_mtx_`
- `metadata_map_` — maps `session_hash_t → metadata_t` (schema, `ParsedQueryDataPtr`, `NodeTag`, `backend_type_t`); guards removed after `complete_session()`

Session lifecycle: `register_session` → (zero or more `update_metadata`) → `complete_session` or `complete_session_on_error`. Always call one of the `complete_session` variants to unblock the waiting frontend.

## Routing Logic

After `CatalogManager::get_catalog_schema` sets `backend_type` on `ParsedQueryData`:

- `Otterbrix` → `OtterbrixManager::execute`
- `MySQL` → `MySQLManager::execute`
- `PostgreSQL` → `PostgressManager::execute`
- `ClickHouse` → `ClickHouseManager::execute`
- `Mixed` → iterate `node_backend_types`, dispatch each external node to its manager, merge results

## `schema_utils` Namespace

Used exclusively during the schema-resolution phase (not at execute time):

- `schema_node_t` — placeholder node substituted for external nodes during schema computation in Otterbrix
- `compute_otterbrix_schema()` — runs an aggregate/projection over a known schema without touching remote data
- `compute_join_schema()` — merges schemas across a join node
- `aggregate_filter_schema()` — applies SELECT-list projections/renames to a schema

These utilities exist because Otterbrix's `execute_plan` cannot compute the schema of a query that references external tables without the external data. The Scheduler uses them to construct a valid `cursor_t_ptr` to pass back to frontends for `prepare_schema` requests.
