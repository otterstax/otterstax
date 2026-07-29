# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`connectors/` provides raw database connectivity. The SQL backends (`mysql/`, `postgresql/`, `clickhouse/`) are **not** actor-based — plain C++ classes used by the integration layer. The newer `file/` and `s3/` connectors **are** `actor_zeta` actors (synchronous busy-wait `enqueue_impl`), called directly from the `integration/` actors (`db::S3Manager`).

Connections are registered once at startup from the connection config file (see the top-level `CLAUDE.md` "Connection Config"): `ComponentManager::register_connections` calls each manager's `addConnection` / the s3 actor's `add_credentials`. There is no runtime add/remove API.

## Structure

```
connectors/
├── mysql/         — Boost.MySQL async connector + ConnectorManager
├── postgresql/    — libpq connector + ConnectorManager
├── clickhouse/    — clickhouse-cpp connector + ConnectorManager
├── file/          — actor conn::file::FileManager: add_file (file → table) / dump_file (query result → file)
├── s3/            — actor conn::s3::ConnectorManager: S3 object I/O via Arrow S3FileSystem (list/download/upload/credentials)
└── api_connections/
    └── {connection,pg_connection,ch_connection}_config.hpp  — `conn::api_server::*Params` structs
        consumed by the managers' `addConnection` overloads (populated by
        ComponentManager from the parsed connection config)
```

## file/ and s3/ actor connectors

Unlike the SQL backends, these are `actor_zeta::actor_mixin` actors whose handlers run synchronously:

- **`conn::file::FileManager`** — `add_file(FileAddParams)` translates a CSV/NDJSON/Parquet file into a
  `data_chunk_t` and creates `database.table` via `db::OtterbrixManager::create_table`. `dump_file(FileMetadata)`
  takes a **pre-parsed `OtterbrixStatementPtr`** (not a database/table), runs it through
  `db::OtterbrixManager::execute`, and writes the result chunk out in the requested format. Format
  translators live in `otterbrix/translators/{input,output}`.
- **`conn::s3::ConnectorManager`** — credential store + S3 object I/O (`list`/`download`/`upload`) backed by
  Arrow's `S3FileSystem`. Credentials are registered per alias at startup from the `s3:` section of the
  connection config file (via `add_credentials`). `db::S3Manager` (in `integration/s3`) bridges these two with the engine.

## ConnectorManager Pattern (same for all three backends)

Each backend has a `ConnectorManager` with identical shape:

- Owns a `thread_pool_manager` (Boost.Asio `io_context` + thread pool, size = `hardware_concurrency()`)
- Stores connectors in `std::unordered_map<std::string /*uuid*/, unique_ptr<IConnector>>`
- `addConnection(params, uuid)` — registers and opens a connector; **not thread-safe**
- `removeConnection(uuid)` — closes and erases; **not thread-safe**
- `executeQuery(uuid, query, handler)` — `co_spawn`s the query on the io_context, returns `std::future<ResultT>`; auto-reconnects once on failure then notifies `CatalogManager` if reconnect fails

The `connector_factory` constructor parameter allows injection of mock connectors in tests (see `tests/mock/`).

## Connection Params (`api_connections/`)

`ConnectionParams`, `PgConnectionParams`, `ChConnectionParams` (namespace `conn::api_server`) are the plain param structs each manager's `addConnection` overload accepts. `ComponentManager::register_connections` fills them from the parsed `config::ConnectionsConfig` at startup — there is no HTTP server anymore.

## Adding a New Backend

1. Implement `IConnector` (follow `mysql/connector.hpp` as the reference)
2. Write a `ConnectorManager` mirroring the MySQL/PG shape
3. Add a `*_connection_config.hpp` param struct (in `api_connections/`)
4. Add a descriptor + parsing section in `config/connections/` and register it in `ComponentManager::register_connections`
5. Wire into `CatalogManager`, `Scheduler`, and the relevant `integration/` manager
