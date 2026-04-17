# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`frontend/` implements the three wire-protocol servers. Each server accepts client connections, receives SQL, dispatches to the `Scheduler` actor, and streams results back.

## Structure

```
frontend/
├── common/               — shared CRTP base server + connection base
├── flight_sql_server/    — Apache Arrow FlightSQL (port 8815)
├── mysql_server/         — MySQL wire protocol (port 8816)
└── postgres_server/      — PostgreSQL wire protocol (port 8817)
```

## Common Layer (`frontend/common/`)

`frontend_server<DerivedConnection>` is a CRTP template that manages a connection pool (max 1000 slots), a `thread_pool_manager` (Boost.Asio), and async accept/reject logic. `DerivedConnection` must implement:
- `socket()` — returns the underlying TCP socket
- `start()` — begins protocol handshake
- `finish()` — signals clean shutdown
- `static build_too_many_connections_error()` — protocol-specific rejection packet

`frontend_connection` provides the base connection class with shared utilities (`packet_reader_base`, `packet_writer_base`, `resultset_utils`).

`asio_future_bridge.hpp` is the universal sink between the Scheduler/Worker pool
(which hands back `actor_zeta::unique_future`s) and the per-connection asio
executor: `async_await_future` polls `take_ready()` from inside a coroutine and
co_returns the `core::result_wrapper_t<session_payload>` when ready, never
blocking the executor thread.

## FlightSQL (`flight_sql_server/`)

`SimpleFlightSQLServer` extends `arrow::flight::sql::FlightSqlServerBase`. Key overrides:
- `GetFlightInfoStatement` — parse SQL, register with Scheduler via `Scheduler::prepare_schema`, return a `FlightInfo` with a ticket
- `DoGetStatement` — execute the ticketed query via `Scheduler::execute`, wrap the result chunk in a `BatchReader` stream
- `GetFlightInfoTables` / `DoGetTables` — delegate to `CatalogManager::get_tables`
- `DoPutCommandStatementUpdate` — DML (INSERT/UPDATE/DELETE) via `Scheduler::execute`

FlightSQL does its own session management because the Arrow Flight protocol separates `GetFlightInfo` (schema + ticket) from `DoGet` (data retrieval). The `TicketData` struct carries the `session_hash_t` between the two RPC calls.

## MySQL / PostgreSQL Servers

Both follow the same pattern: `frontend_server<XConnection>` accepts TCP connections; each connection implements the respective handshake + query/response protocol, then calls `Scheduler::execute` (and friends), which returns an `actor_zeta::unique_future<core::result_wrapper_t<session_payload>>`. The connection awaits that future via `frontend/common/asio_future_bridge.hpp::async_await_future` (polls `take_ready()` on the asio executor — no blocking get, no `cv_wrapper`).

Packet encoding/decoding lives in `{mysql,postgres}_server/packet/` and `{mysql,postgres}_server/{mysql,postgres}_defs/`. Result-set serialisation is in `{mysql,postgres}_server/resultset/`.

## Port Assignments

| Server | Default port | CMake target |
|--------|-------------|-------------|
| FlightSQL | 8815 | `flight_sql_server` |
| MySQL | 8816 | `mysql_server` |
| PostgreSQL | 8817 | `postgres_server` |

Ports are hardcoded in `main.cpp` and in the `config.yml` / `compose.yml` files.
