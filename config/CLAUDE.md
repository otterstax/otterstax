# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`config/` is the configuration layer. There is **one config file** (`config.yaml`)
that carries both the wire-server settings **and**, under a `connections:` key,
every remote backend and s3 alias. It is parsed by `ConfigReader` (built into the
`config` target, `config::config`), which depends only on `yaml-cpp` (plus
`spdlog`/otterbrix logging) — **no connector dependencies**.

The `connections:` section is the **single source of truth for connections**;
there is no runtime add/remove API. To change connections, edit `config.yaml` and
restart the server.

Actual registration of connections lives one layer up in
`ComponentManager::register_connections` (it needs the connector managers), which
keeps `config/` free of connector/actor dependencies.

## Structure

```
config/
├── config.hpp / config.cpp             — ServiceConfig + ConfigReader (reads the whole config.yaml)
├── yaml_scalar.hpp                     — yaml_scalar<T>(node, field, resource): the one place a yaml-cpp
│                                         conversion exception becomes a core::error_t
└── connections/
    ├── connection_config.hpp           — descriptor structs + ConnectionsConfig aggregate
    └── connection_config_reader.{hpp,cpp} — parse_connections(YAML::Node, resource) → result_wrapper_t<ConnectionsConfig>
```

## Error contract

Nothing in `config/` throws. Every entry point returns a
`core::result_wrapper_t<...>` whose error is a `core::error_t`:

| Failure | Code | Message names |
|---------|------|---------------|
| file is not YAML (`YAML::LoadFile` refuses it) | `conversion_failure` | the path + yaml-cpp's own message |
| a `service.*` value that cannot be read as its type (`port: abc`) | `invalid_parameter` | the field (`service.mysql.port`) |
| a connection field that is not a scalar (`host: [a, b]`) | `invalid_parameter` | the section + entry position + field |
| an incomplete connection entry, or a malformed backend port | `invalid_parameter` | the section + `alias='…'` + the missing field / bad port |

The yaml-cpp calls (`YAML::LoadFile`, `Node::as<T>()`) are the only throwing
sites; each is wrapped in a try/catch exactly where it is made and converted to
the value above (`yaml_scalar.hpp` for `as<T>()`, `ConfigReader::load` for
`LoadFile`). Error messages live on the `std::pmr::memory_resource*` passed to
`ConfigReader` / `parse_connections` — there is no default resource.

## Types

- `config::ServiceConfig` — `{ FlightSqlConfig flight_sql; MysqlConfig mysql;
  PostgresConfig postgres; ConnectionRetryConfig connection_retry;
  ConnectionsConfig connections; }`. The `flight_sql`/`mysql`/`postgres`/
  `connection_retry` fields come from the top-level `service:` node; `connections`
  from the `connections:` node of the same file.
- `config::ConnectionRetryConfig` — `{ int max_attempts = 1; int delay_ms = 1000; }`
  (plain data, lives in `connections/connection_config.hpp`). Startup retry policy
  for opening backend connections. `max_attempts <= 1` = single attempt.
- `config::ConnectionsConfig` — `{ vector<MysqlConnectionDesc> mysql;
  vector<PgConnectionDesc> postgresql; vector<ChConnectionDesc> clickhouse;
  vector<S3CredentialDesc> s3; }`. The descriptor structs are plain data
  (`alias/host/port/username/password/database[/schema]/table` for backends;
  `alias/access_key/secret_key/region/session_token/endpoint` for s3).

## Parsing split

- `ConfigReader(resource)` / `ConfigReader::load(path)` →
  `core::result_wrapper_t<ServiceConfig>`. Reads the whole file: parses the
  `service:` node for `flight_sql`/`mysql`/`postgres` ports and
  `connection_retry` (each scalar through `yaml_scalar<T>`), then delegates the
  `connections:` node to `parse_connections`. A missing file is the default
  `ServiceConfig` (a warning, not an error). Every failure is logged once under
  the `Config` logger and returned.
- `parse_connections(const YAML::Node&, std::pmr::memory_resource*)` (a free
  function in `connections/connection_config_reader.cpp`) →
  `core::result_wrapper_t<ConnectionsConfig>`. A null/missing node → empty
  config; each section (mysql/postgresql/clickhouse/s3) is optional. It is a
  pure function (no logging, no file IO) so it is trivially unit-testable on a
  parsed node — and it **validates every entry: the first incomplete one (or
  non-scalar field, or malformed port) is returned as `invalid_parameter`**
  (fail-fast: a broken connection must not start the server).
- `validation_error(const <Desc>&)` (also in `connection_config_reader.cpp`) —
  pure per-descriptor checks returning the first missing required field (or the
  malformed port), or `std::nullopt`. Required: `alias/host/username/database`
  for backends (port is optional — the connector falls back to the driver
  default; a non-empty port must be a decimal integer in 1..65535, checked with
  `otterstax::parse_port` from `utility/parse_port.hpp`, the same rule the
  connectors apply); `alias/access_key/secret_key` for s3. `parse_connections`
  turns a non-empty result into its `invalid_parameter`.

## Startup flow

```text
main.cpp
  → config::ConfigReader(resource).load(config_path)    — config.yaml → result_wrapper_t<ServiceConfig> (service.* + .connections)
        parse_connections validates every entry → the first incomplete one is an invalid_parameter;
        ConfigReader::load logs it and returns it → main logs it and returns 1 (server never starts:
        the reader runs on its own pmr resource before ComponentManager — engine, actors, pools — exists)
  → ComponentManager::register_connections(server_config.connections, server_config.connection_retry)
        (descriptors are already validated) — opens each backend:
        mysql/pg/ch: ConnectorManager::addConnection(conn::api_server::*Params)   — retried up to
                     connection_retry.max_attempts (delay_ms between tries)
        s3         : actor send &conn::s3::ConnectorManager::add_credentials      — stores the alias (no retry)
```

Two failure modes, deliberately different:

- **Invalid config** (a required field missing, a bad port, a file that is not
  YAML) → **fail-fast**: `load` returns the error, `main` returns 1, startup
  aborts. The operator must fix the file.
- **Backend unreachable** (valid config, DB still booting) → **best-effort**: the
  open is retried per `connection_retry`, and if it still fails the error is logged
  but startup continues (the other backends and the local engine stay usable).

## Where the file lives at runtime

The path comes from the `--config PATH` flag (default `config.yaml`), resolved
**relative to the server's working directory**. In containers the server runs
from `WORKDIR /app/build/Release` with no `--config`, so it reads
`/app/build/Release/config.yaml`, delivered by either:

- **Baking** — `Dockerfile.test` `COPY`s a stack `config.yaml` to that path (the
  integration-test backends). Good for CI / DinD where bind-mounts don't work.
- **Mounting** — compose files bind-mount a stack `config.yaml` onto that path
  (`examples/demo/compose.yml`, `compose.yml`, `benchmark/compose_benchmark.yml`).
  A mount overrides anything baked in.

A missing file → the server starts with default ports and no connections.

## config.yaml format

```yaml
service:
  flight_sql: { host: "0.0.0.0", port: 8815 }
  mysql:      { port: 8816 }        # wire-server port
  postgres:   { port: 8817 }        # wire-server port
  connection_retry: { max_attempts: 10, delay_ms: 2000 }

connections:
  mysql:
    - { alias: <a>, host: <h>, port: "<p>", username: <u>, password: <pw>, database: <db>, table: "" }
  postgresql:
    - { alias: <a>, host: <h>, port: "<p>", username: <u>, password: <pw>, database: <db>, schema: <s>, table: "" }
  clickhouse:
    - { alias: <a>, host: <h>, port: "<p>", username: <u>, password: <pw>, database: <db>, table: "" }
  s3:
    - { alias: <a>, access_key: <k>, secret_key: <s>, region: <r>, endpoint: <e> }
```

`port` is an **optional string** (empty → driver default; otherwise a decimal
integer in 1..65535, anything else aborts startup). `schema` defaults to
`public`. s3 `region`/`session_token`/`endpoint` are optional. `connection_retry`
is optional (defaults to `max_attempts: 1`, `delay_ms: 1000` = one-shot).
Malformed YAML → the server aborts startup (`conversion_failure` from `load`).

Wire-server settings live under the top-level `service:` node so they never
collide with the backends under `connections:` — `service.mysql`/`service.postgres`
are **wire-server ports**, `connections.mysql`/`connections.postgresql` are
**remote backends** (same names, different nesting).

## Instrumentation note

`ConfigReader::load` is a top-level TU entry point and carries
`OTX_ZONE_N("config::ConfigReader::load")` (see the top-level `CLAUDE.md` Tracy
rules).

## Adding a new backend type

1. Add a `*ConnectionDesc` struct + a `vector<...>` field on `ConnectionsConfig`
   (`connections/connection_config.hpp`).
2. Parse a new sub-section in `parse_connections` through an `entry_reader_t`
   (so a non-scalar field is reported, not thrown), and add its `validate_all`
   pass (`connections/connection_config_reader.cpp`).
3. Add a `validation_error(const <Desc>&)` overload for its required fields
   (`connections/connection_config_reader.{hpp,cpp}`).
4. Register it in `ComponentManager::register_connections` (descriptors are
   already validated at parse time — just convert the descriptor → the
   connector's param struct and call `addConnection` inside the retry helper).
5. Add a corresponding `*_connection_config.hpp` param struct under
   `connectors/api_connections/` if the manager needs one.
6. Extend the unit tests in `tests/unit/config/test_connection_config.cpp`.

## Tests

`tests/unit/config/` (`test_unit_config`) covers `parse_connections` (field
parsing, null/missing node, missing sections, pg `schema` default, optional s3
fields, order preservation, **`invalid_parameter` on an incomplete entry, a
malformed port or a non-scalar field**, empty `table`/omitted `port` allowed),
`validation_error` (required-field checks, optional port, port range/format, s3
keys), and `ConfigReader` (whole `config.yaml`: `service.*` ports +
`connection_retry` + embedded connections, defaults when file missing, retry
defaults, no-`connections:` section, **malformed YAML → `conversion_failure`, a
non-integer service port / an invalid connection or port →
`invalid_parameter`**). Nothing is asserted to throw.
