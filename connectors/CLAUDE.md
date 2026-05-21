# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`connectors/` provides raw database connectivity and the HTTP API for registering/removing connections at runtime. It is **not** actor-based — these are plain C++ classes used by the integration layer.

## Structure

```
connectors/
├── mysql/         — Boost.MySQL async connector + ConnectorManager
├── postgresql/    — libpq connector + ConnectorManager
├── clickhouse/    — clickhouse-cpp connector + ConnectorManager
└── api_connections/
    ├── connection_server.{hpp,cpp}  — Boost.Beast HTTP server (port 8085)
    └── *_connection_config.hpp      — JSON-deserialisable param structs
```

## ConnectorManager Pattern (same for all three backends)

Each backend has a `ConnectorManager` with identical shape:

- Owns a `thread_pool_manager` (Boost.Asio `io_context` + thread pool, size = `hardware_concurrency()`)
- Stores connectors in `std::unordered_map<std::string /*uuid*/, unique_ptr<IConnector>>`
- `addConnection(params, uuid)` — registers and opens a connector; **not thread-safe**
- `removeConnection(uuid)` — closes and erases; **not thread-safe**
- `executeQuery(uuid, query, handler)` — `co_spawn`s the query on the io_context, returns `std::future<ResultT>`; auto-reconnects once on failure then notifies `CatalogManager` if reconnect fails

The `connector_factory` constructor parameter allows injection of mock connectors in tests (see `tests/mock/`).

## HTTP Connection API (`api_connections/`)

`http_server::Server` (Boost.Beast) accepts JSON POST requests to `/add_connection` and `/remove_connection` and delegates directly to the three `ConnectorManager` instances injected at construction. `ConnectionParams`, `PgConnectionParams`, `ChConnectionParams` map to the JSON payloads.

## Adding a New Backend

1. Implement `IConnector` (follow `mysql/connector.hpp` as the reference)
2. Write a `ConnectorManager` mirroring the MySQL/PG shape
3. Add a `*_connection_config.hpp` for JSON parameter deserialization
4. Inject into `http_server::Session` and `http_server::Server` alongside the existing three managers
5. Wire into `CatalogManager`, `Scheduler`, and the relevant `integration/` manager
