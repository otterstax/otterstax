# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`config/` is the configuration layer. There is **one config file** (`config.yaml`)
that carries both the wire-server settings **and**, under a `connections:` key,
every remote backend and s3 alias. It is parsed by `load_config` (built into the
`config` target, `config::config`), which depends only on `yaml-cpp` (plus the
engine's `core/result_wrapper.hpp`) — **no connector dependencies**.

The `connections:` section is the **single source of truth for connections**;
there is no runtime add/remove API. To change connections, edit `config.yaml` and
restart the server.

Actual registration of connections lives one layer up — `remote_backends_t::open`
(`backends/`, it owns the connector managers: the MySQL / PostgreSQL / ClickHouse
connections, opened and discovered) and `backends::register_s3_aliases` (the s3
aliases, `backends/backends.hpp`) — which keeps `config/` free of connector/actor dependencies.

## Structure

```
config/
├── config.hpp / config.cpp             — ServiceConfig + load_config (reads the whole config.yaml)
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
| a `service.*` value that cannot be read as its type (`port: abc`), or a service port outside 1..65535 (`port: 70000`) | `invalid_parameter` | the field (`service.mysql.port`) |
| a `service.runtime` pool size out of range, an empty directory, or one of those sections (or a pool) that is not a mapping | `invalid_parameter` | the field (`service.engine.data_dir`, `service.runtime.general.threads`) |
| a section that is present and not a mapping — the document itself (an empty file is the defaults), `service`, `service.flight_sql` / `mysql` / `postgres` / `spark_connect` / `connection_retry` (`service: {mysql: 8816}`) | `invalid_parameter` | the field (`service.mysql`, `(document)`) + "expected a mapping of keys" |
| a connection entry that is a scalar (`mysql: [name]`) | `invalid_parameter` | the section + entry position + "expected a mapping of keys" |
| a connection field that is not a scalar (`host: [a, b]`) | `invalid_parameter` | the section + entry position + field |
| an incomplete connection entry, or a malformed backend port | `invalid_parameter` | the section + `alias='…'` + the missing field / bad port |
| a connection whose alias is `federation` or `kafka` in any case, in any section | `invalid_parameter` | the section + `alias='…'` + "reserved" + what the name is |
| a backend alias (mysql / postgresql / clickhouse) equal to another one under the federation's case folding, or an s3 alias equal to another s3 alias | `invalid_parameter` | the section + `alias='…'` + "already taken by" the other entry |
| a `pool_size` below 1, or one that is not an integer | `invalid_parameter` | the section + `alias='…'` (or entry position) + `pool_size` |
| a `service.spark_connect.session_timeout_s` that is not an integer in [0, one year] | `invalid_parameter` | the field |

The yaml-cpp calls (`YAML::LoadFile`, `Node::as<T>()`) are the only throwing
sites; each is wrapped in a try/catch exactly where it is made and converted to
the value above (`yaml_scalar.hpp` for `as<T>()`, `load_config` for
`LoadFile`). A subscript (`node["key"]`) throws `BadSubscript` on a scalar, so
no key is read off a node before it is known to be a mapping: `require_map`
(`config.cpp`) at the top of every section reader (`read_port`,
`parse_flight_sql`, `parse_spark_connect`, `parse_connection_retry`,
`parse_engine`, `parse_runtime`, `pool_config`) and for `service` and the
document in `load_config`; `entry_reader_t` refuses a scalar connection entry
before its first key. Error messages live on the `std::pmr::memory_resource*` passed to
`load_config` / `parse_connections` — there is no default resource.

## Types

- `config::ServiceConfig` — `{ FlightSqlConfig flight_sql; MysqlConfig mysql;
  PostgresConfig postgres; SparkConnectConfig spark_connect; EngineConfig engine;
  RuntimeConfig runtime; ConnectionRetryConfig connection_retry;
  ConnectionsConfig connections; }`. The
  `flight_sql`/`mysql`/`postgres`/`spark_connect`/`engine`/`runtime`/`connection_retry`
  fields come from the top-level `service:` node; `connections` from the
  `connections:` node of the same file.
- `config::EngineConfig` (`service.engine`) — the embedded otterbrix engine:
  `data_dir` (default `DEFAULT_ENGINE_DATA_DIR` = `/tmp/test_collection_sql/base`,
  the directory `main.cpp` hard-coded; the engine's `.lock`, WAL and tables) and
  `log_dir` (empty → `data_dir`; `config::log_directory(engine)` answers it). `log_dir` is where
  **every** log file of the server goes: `main.cpp` reads the config first (a refused
  argument or file is written to stderr, there are no loggers yet), then
  `initialize_all_loggers(config::log_directory(...))`, and the engine's log is written
  through the `Main` logger's sinks (`runtime/CLAUDE.md`, "Engine log").
  `load_config` logs nothing. The engine's log level is `SPDLOG_ACTIVE_LEVEL`, the level
  of otterstax's own loggers (no config key); everything else the engine takes (the
  execution knobs) is otterbrix's own default. Plain data;
  `otterstax::runtime::make_engine_configuration` (`runtime/engine.hpp`) turns it
  into the engine's `configuration::config`, with `main_path`, the locked directory,
  set to `data_dir`. `main.cpp` opens the engine with it
  (`engine_t::open(runtime, server_config.engine)`, `runtime/CLAUDE.md`).
- `config::RuntimeConfig` (`service.runtime`) — **every thread pool of the
  server**, so `config.yaml` sizes all of them; nothing is sized from the machine
  in code except the documented auto value of a `threads: 0`. Ten `PoolConfig
  { threads; }`, `threads` in 0..4096. Five are actor-zeta pools: `general` / `exec` / `disk` (the engine's
  pools, default 3 threads, `threads >= 1`) and `sessions` / `backends` (default
  0 = auto). `otterstax::runtime::runtime_t` (`runtime/runtime.hpp`) builds its
  pools from it; `main.cpp` builds the process's `runtime_t` from it. Auto for
  `sessions` is `max(2, hardware threads)`. **`backends`**: a statement holds a
  backends thread for its whole remote call (the connection checkout and the wait
  for the backend's answer), so this pool is what isolates the remote connections
  from each other — with fewer threads than connections the surplus connections
  idle, and one slow alias can take every thread from the others. Auto (0) is
  therefore `max(2, hardware threads)` and at least the sum of `pool_size` over
  every mysql / postgresql / clickhouse entry
  (`otterstax::backends::backends_threads`, resolved by `main.cpp` before it builds
  the runtime); an explicit value is taken as is, and below that sum
  `backends_threads` logs a warning. The other five (default 0) are plain thread
  pools that are not `runtime_t`'s:
  `mysql_io` / `postgres_io` (the asio pool of each wire server; a connection
  holds one of its threads while its statement runs, so the pool bounds the
  statements in flight over that wire), `flight_sql_io` (the Flight SQL
  `GrpcContext` threads; its adapter blocks the calling thread for the length of a
  query), `spark_connect_io` (one `GrpcContext` per thread; the handlers are
  coroutines) — each 0 = `max(2, hardware threads)`, resolved by
  `otterstax::runtime::resolve_threads` where `main.cpp` builds the servers — and
  `connector_io`, the asio io pool each of the three remote connector managers
  owns. `main.cpp` turns it into per-manager sizes with
  `otterstax::backends::connector_io_threads(threads, connections)`
  (`backends/remote_backends.hpp`): `threads: N` gives each manager N; 0 (the
  default) gives each manager one thread per connection `connections:` opens to its
  backend kind (the sum of the entries' `pool_size`), at least one — a statement
  holds an io thread while it runs and a connection runs one statement at a time,
  so fewer would queue statements that already hold a connection and more could
  never all be busy (`connectors/CLAUDE.md`, "ConnectorManager Pattern").
- `config::SparkConnectConfig` — `{ std::string host = "0.0.0.0";
  uint16_t port = 15002; std::uint32_t session_timeout_s = 3600; }`: the listen
  address `main.cpp` hands to the Spark Connect gRPC server
  (`frontend::spark::SparkConnectServerConfig`), and how long a Spark session may
  go without an RPC before it is closed (Spark Connect's own
  `spark.connect.session.manager.defaultSessionTimeout`, 60m; 0 keeps sessions
  until ReleaseSession; `frontend/CLAUDE.md`, Spark Connect).
- `config::ConnectionRetryConfig` — `{ int max_attempts = 1; int delay_ms = 1000; }`
  (plain data, lives in `connections/connection_config.hpp`). Startup retry policy
  for opening backend connections. `max_attempts <= 1` = single attempt.
- `config::ConnectionsConfig` — `{ vector<MysqlConnectionDesc> mysql;
  vector<PgConnectionDesc> postgresql; vector<ChConnectionDesc> clickhouse;
  vector<S3CredentialDesc> s3; }`. The descriptor structs are plain data
  (`alias/host/port/username/password/database[/schema]/table` for backends,
  plus `int pool_size = 1` on each of them;
  `alias/access_key/secret_key/region/session_token/endpoint` for s3).
  `pool_size` (the number of connections kept open to the backend, validated
  `>= 1`) is copied by `remote_backends_t::open` into the
  `conn::api_server::*Params`; the manager opens that many connections for the
  alias (`connectors/CLAUDE.md`, "Connection pools").

## Parsing split

- `[[nodiscard]] load_config(resource, path)` →
  `core::result_wrapper_t<ServiceConfig>`. Reads the whole file: parses the
  `service:` node for the `flight_sql`/`mysql`/`postgres`/`spark_connect` ports
  (one reader, `read_port`: an integer in 1..65535 through `integer_in_range`),
  the `flight_sql` / `spark_connect` hosts, `connection_retry`, `engine` and
  `runtime` (each scalar through `yaml_scalar<T>`; integers range-checked,
  sections checked to be mappings before a key is read; a key the reader does
  not know is ignored), then delegates the `connections:` node to
  `parse_connections`. A missing file is the default `ServiceConfig` (not an
  error). Every failure is returned and nothing is logged: `main.cpp` reads the
  file before it creates the loggers (they go under `service.engine.log_dir`,
  which the file names), so a refused file reaches the operator as `main`'s own
  line on stderr (`main` logs the file it loaded once the loggers exist).
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
  connectors apply); `alias/access_key/secret_key` for s3. After the required
  fields (before the port) every section refuses the **reserved aliases**,
  compared case-insensitively (`reserved_connection_aliases` in
  `connection_config_reader.hpp`): `federation` (`reserved_connection_alias`,
  defined as the federation's `metadata_database` — a header-only constant), the
  engine database holding the remote-table metadata (`federation.remote_tables` /
  `remote_columns`, `otterbrix/federation/metadata.hpp`), and
  `kafka` (`reserved_kafka_alias`), the engine database of the Kafka objects
  (`otterstax::kafka::KAFKA_DATABASE_NAME`, which `config` cannot include — a
  `static_assert` in `backends/backends.cpp` pins the two spellings together). An
  alias is the first qualifier of a federated name, so a connection under either
  would shadow that database. `pool_size` is checked last (every backend section). `parse_connections` turns a non-empty
  result into its `invalid_parameter`.
- After every entry passed, `parse_connections` checks the **aliases**: the
  mysql / postgresql / clickhouse aliases share one namespace — an alias is the
  connection uid every federated name starts with and the federation resolves it
  case-insensitively (`federation::fold_identifier`, ASCII lower case; spelled
  again in the reader because `config` links nothing of the federation) — so two
  entries equal under that folding, in one section or two, are refused: one
  backend would be unreachable and its tables recorded under the other's uid.
  s3 aliases are another namespace (the `s3_alias` option of CREATE EXTERNAL
  TABLE / COPY ... TO, looked up exactly by `conn::s3::ConnectorManager`, where a
  second entry would silently replace the first): an s3 alias equal to a backend
  alias is fine, two equal s3 aliases are refused.

## Startup flow

```text
main.cpp
  → command line (--config)                              — a refused argument goes to stderr
  → config::load_config(resource, config_path)          — config.yaml → result_wrapper_t<ServiceConfig> (service.* + .connections)
        parse_connections validates every entry, then the aliases → the first problem is an invalid_parameter;
        main writes it to stderr and returns 1 (server never starts: the reader runs on its own pmr resource
        before any module — runtime, engine, actors, pools — exists, and before any logger)
  → initialize_all_loggers(config::log_directory(server_config.engine))   — every log file under log_dir
  → backends::backends_threads(runtime.backends.threads, connections)   — the backends pool against the
                                                  connections (a warning when an explicit size is below them)
  → runtime_t(server_config.runtime with that backends size)
  → remote_backends_t(resource, {.connector_threads = connector_io_threads(runtime.connector_io.threads, connections),
                                  .statements = runtime.backends_scheduler()}).open(connections, connection_retry)
        first the descriptors the connector can never accept (a malformed port, pool_size < 1) — a
        configuration error, checked before any connection opens, returned so main returns 1 — then each backend:
        mysql/pg/ch: ConnectorManager::addConnection(conn::api_server::*Params, pool_size included) + discover —
                     retried up to connection_retry.max_attempts (delay_ms between tries)
  → engine_t::open(runtime, server_config.engine, federation::make_primitives())
  → the s3 connector / file / S3 / Kafka actors spawned (main's locals)
  → backends::register_s3_aliases(resource, s3_connector, server_config.connections)
        s3         : actor send &conn::s3::ConnectorManager::add_credentials      — stores the alias (no retry)
  → backends::publish_federation_metadata(resource, *engine, *remote)
                                                — rows of every configured connection written, rows of
                                                  a connection no longer configured removed
```

Two failure modes, deliberately different:

- **Invalid config** (a required field missing, a bad port, a file that is not
  YAML) → **fail-fast**: `load` returns the error, `main` returns 1, startup
  aborts. The operator must fix the file.
- **Backend unreachable** (valid config, DB still booting) → **best-effort**: the
  open is retried per `connection_retry`, and if it still fails the error is logged
  but startup continues (the other backends and the local engine stay usable).
  A backend that answers something the federation metadata cannot record (a
  discovered column without a name, …) is left out at once, not retried. Which of
  the three a failure is, the step that failed says (`open_failure_kind_t`,
  `backends/connection_retry.hpp`) — never the error's code, which may be one the
  backend, the connector or the engine produced: an `invalid_parameter` from the
  metadata or the engine does not abort startup.

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
  spark_connect: { host: "0.0.0.0", port: 15002, session_timeout_s: 3600 }   # Spark Connect gRPC server
  connection_retry: { max_attempts: 10, delay_ms: 2000 }
  engine:                           # embedded otterbrix engine; every key optional
    data_dir: /tmp/test_collection_sql/base   # holds .lock, the WAL and the tables
    log_dir: <dir>                  # default data_dir; every log file of the server
  runtime:                          # thread pools; every key optional
    general:  { threads: 3 }        # engine pools: threads >= 1
    exec:     { threads: 3 }
    disk:     { threads: 3 }
    sessions: { threads: 0 }        # 0 = max(2, hardware threads)
    backends: { threads: 0 }        # 0 = max(2, hardware threads, sum of every connection's pool_size)
    connector_io: { threads: 0 }    # io threads of EACH connector manager; 0 = its connections' pool_size sum (>= 1)
    mysql_io:         { threads: 0 }   # the wire servers' asio pools; 0 = max(2, hardware threads)
    postgres_io:      { threads: 0 }
    flight_sql_io:    { threads: 0 }   # Flight SQL GrpcContext threads
    spark_connect_io: { threads: 0 }   # Spark Connect GrpcContexts, one per thread

connections:
  mysql:
    - { alias: <a>, host: <h>, port: "<p>", username: <u>, password: <pw>, database: <db>, table: "" }
  postgresql:
    - { alias: <a>, host: <h>, port: "<p>", username: <u>, password: <pw>, database: <db>, schema: <s>, table: "" }
  clickhouse:
    - { alias: <a>, host: <h>, port: "<p>", username: <u>, password: <pw>, database: <db>, table: "", pool_size: 1 }
  s3:
    - { alias: <a>, access_key: <k>, secret_key: <s>, region: <r>, endpoint: <e> }
```

`alias` must not be `federation` or `kafka` (any case) in any section: the names
are reserved for the engine databases of the remote-table metadata and of the
Kafka objects, and such an entry aborts
startup, and so does an alias another backend entry already takes (any case, any
backend section) or another s3 entry already takes. `port` is an **optional string** (empty → driver default; otherwise a decimal
integer in 1..65535, anything else aborts startup). mysql / postgresql entries take
an optional `pool_size` (integer `>= 1`, default 1: the connections the manager
keeps open to that backend), clickhouse entries too. `schema` defaults to
`public`. s3 `region`/`session_token`/`endpoint` are optional. `connection_retry`
is optional (defaults to `max_attempts: 1`, `delay_ms: 1000` = one-shot), and so
is `spark_connect` (defaults to `0.0.0.0:15002`, either key alone overriding its
own default).
Malformed YAML → the server aborts startup (`conversion_failure` from `load`).

Wire-server settings live under the top-level `service:` node so they never
collide with the backends under `connections:` — `service.mysql`/`service.postgres`
are **wire-server ports**, `connections.mysql`/`connections.postgresql` are
**remote backends** (same names, different nesting).

## Instrumentation note

`load_config` is a top-level TU entry point and carries
`OTX_ZONE_N("config::load_config")` (see the top-level `CLAUDE.md` Tracy
rules).

## Adding a new backend type

1. Add a `*ConnectionDesc` struct + a `vector<...>` field on `ConnectionsConfig`
   (`connections/connection_config.hpp`).
2. Parse a new sub-section in `parse_connections` through an `entry_reader_t`
   (so a non-scalar field is reported, not thrown), and add its `validate_all`
   pass (`connections/connection_config_reader.cpp`).
3. Add a `validation_error(const <Desc>&)` overload for its required fields
   (`connections/connection_config_reader.{hpp,cpp}`).
4. Open it in `remote_backends_t::open` (descriptors are already validated
   at parse time — just convert the descriptor → the connector's param struct
   and call `addConnection` + the discovery inside the retry helper).
5. Add a corresponding `*_connection_config.hpp` param struct under
   `connectors/api_connections/` if the manager needs one.
6. Extend the unit tests in `tests/unit/config/test_connection_config.cpp`.

## Tests

`tests/unit/config/` (`test_unit_config`) covers `parse_connections` (field
parsing, null/missing node, missing sections, pg `schema` default, optional s3
fields, order preservation, **`invalid_parameter` on an incomplete entry, a
malformed port, a non-scalar field, a bad `pool_size`, or the reserved alias
`federation` or `kafka` in any case and any section** (an alias merely containing
the word is accepted), **a backend alias taken twice across sections in any case, two equal
s3 aliases** (an s3 alias equal to a backend alias, or differing in case, is
accepted), empty `table`/omitted `port` allowed),
`validation_error` (required-field checks, optional port, port range/format, s3
keys, the reserved alias reported after the missing fields and before the port), and `load_config` (whole `config.yaml`: `service.*` ports +
`connection_retry` + embedded connections, a connection under the reserved alias
aborting the load, defaults when file missing, retry
defaults, the `spark_connect` host/port and their `0.0.0.0:15002` default when
the node is absent, `session_timeout_s` (default 3600, 0 accepted, range-checked),
a backend alias taken twice aborting the load, no-`connections:` section, **malformed YAML →
`conversion_failure`, a non-integer service port (`service.mysql.port`,
`service.spark_connect.port`), a service port outside 1..65535 / an invalid connection or port →
`invalid_parameter`**, every `service` section — `service` itself and the
document included — given as a scalar or a sequence → `invalid_parameter`
naming it, an empty file the defaults, and a scalar connection entry in every
section → `invalid_parameter` naming the entry). `test_engine_config.cpp` covers `service.engine` /
`service.runtime`: the defaults when the sections or the file are absent, every
key read, one key overriding only its own default, the keys an older file may
still carry (`wal_dir`, `disk_dir`, `log_level`, `executor_pool_size`,
`dml_flush_row_threshold`, `pump`, a pool's `max_throughput`) ignored like any
unknown key, and `invalid_parameter` naming the field for every refused value
(an empty or non-scalar directory, non-mapping sections, engine pools with 0 threads, out-of-range or
textual pool sizes, a `connector_io` that is not a mapping or whose `threads` is
negative, past 4096 or text; `connector_io.threads` read, 0 kept as auto; the
four frontend pools read, one alone overriding only its own default, and refused
the same way). How `connector_io_threads` sizes the managers, how
`backends_threads` sizes the backends pool against the connections are pinned in
`tests/system/test_process_assembly.cpp`. Nothing is asserted to throw.
