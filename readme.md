# OtterStax

[![Unit Tests](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml)

A SQL federation server: one endpoint over several databases, reached through the MySQL and
PostgreSQL wire protocols, Apache Arrow Flight SQL and Spark Connect. OtterStax is built on the
Otterbrix query engine, which plans and runs every statement; the tables of the remote databases
are tables of its catalog, read and written through their backends.

## Features

- Protocols: MySQL, PostgreSQL, Flight SQL, Spark Connect (PySpark 3.5–4.2)
- Federation: MariaDB / MySQL, PostgreSQL and ClickHouse backends in one statement; a query block
  over one backend is pushed down to it whole
- Transactions over local and remote tables (PostgreSQL / MySQL backends), SAVEPOINT included
- **File and S3 external tables**: `CREATE EXTERNAL TABLE` loads parquet, csv or ndjson from a local
  path or an `s3://…` URI; `COPY (<select>) TO '…'` exports a query result — see
  [S3 / file external tables](#s3--file-external-tables)
- Kafka sources and streams (`CREATE SOURCE` / `CREATE STREAM`)
- Docker ready: compose files bring up the backends and the server

## Protocols

| Protocol      | Default port |
|---------------|-------------:|
| Flight SQL    | 8815 |
| MySQL         | 8816 |
| PostgreSQL    | 8817 |
| Spark Connect | 15002 |

```bash
psql -h localhost -p 8817 -U demo local        # `local` is the engine database of your own tables
mysql -h 127.0.0.1 -P 8816 -u demo
```

A PostgreSQL client names its place: a connection alias, `alias.namespace`, `local` or
`local.namespace` (above, `local`). A name that is neither a connection nor `local` is refused
(`FATAL 3D000`); naming nothing leaves the session no place and short names refuse.

## Connection configuration

There is a **single config file, `config.yaml`**: the server settings under `service:` and, under
`connections:`, every remote backend and S3 alias. It is read **once at startup** — there is no
runtime add / remove API; edit the file and restart.

The path is `--config PATH` (default `config.yaml`, relative to the working directory). The Docker
images run the server from `/app/build/Release` with no `--config`, so the compose files
bind-mount a `config.yaml` onto `/app/build/Release/config.yaml` (`Dockerfile.test` bakes one in).
A missing file starts the server with default ports and no connections.

```yaml
service:
  flight_sql: { host: "0.0.0.0", port: 8815 }
  mysql:      { port: 8816 }           # wire-server port, not a backend
  postgres:   { port: 8817 }           # wire-server port, not a backend
  spark_connect: { host: "0.0.0.0", port: 15002 }
  connection_retry: { max_attempts: 10, delay_ms: 2000 }   # optional; default one attempt

connections:
  mysql:
    - alias: mysql                     # the first part of every name of this connection
      host: demo-mariadb
      port: "3306"                     # a string; optional (empty: the driver's default)
      username: demo
      password: demo
      database: bill
      table: ""                        # optional: a name limits the startup discovery to it
  postgresql:
    - alias: pg
      host: demo-postgres
      port: "5432"
      username: demo
      password: demo
      database: shop
      schema: shop                     # optional: empty = every user schema of the database
  clickhouse:
    - alias: ch
      host: demo-clickhouse
      port: "9000"
      username: demo
      password: demo
      database: ev
      pool_size: 2                     # optional: connections kept open (default 4)
  s3:
    - alias: demo_s3                   # referenced by s3_alias = '...'
      access_key: rustfsadmin
      secret_key: rustfsadmin
      region: us-east-1                # optional
      endpoint: demo-rustfs:9000       # optional; http + path-style (RustFS and other S3-compatible stores)
```

A backend alias is lower-case letters, digits and `_` (not starting with a digit); `federation`, `main`
and `local` are reserved. An invalid entry or malformed YAML **aborts startup**; a valid connection whose
backend is unreachable is retried per `service.connection_retry` and, if still down, logged — the
server starts and its statements fail until the backend is back.

### Names

A table is written in three parts, `connection.namespace.table` — the first part is a connection, or
`local` for your own tables. Through one connection every backend names two levels: a PostgreSQL
namespace is a schema of the connection's configured database, a MySQL / ClickHouse namespace one of
its databases.

```sql
SELECT o.order_id, c.name
FROM   mysql.bill.orders o
JOIN   pg.shop.customers c ON o.customer_id = c.customer_id;
```

Short names (`table`, `schema.table`) are completed by the session's place — only what you chose at
connect (the PostgreSQL `database`, the MySQL handshake database / `USE`, Spark's current database):
a connection (`uid`), one of its namespaces (`uid.ns`), or `local` / `local.ns` for your own tables.
Without a choice there is no place and short names refuse; four-part names are refused.

A table created on the backend after startup is found on its first use.

Ready-made `config.yaml` files ship with each stack: `examples/demo/`, `examples/quick_start/`,
`tests/scripts/`, `scripts/database/` and `benchmark/` (plus a template at the repo root).

## S3 / file external tables

```sql
CREATE SCHEMA eg;

-- Load (the format is taken from the extension when omitted)
CREATE EXTERNAL TABLE eg.t
    WITH (location = '/path/to/data.parquet', format = 'parquet');

CREATE EXTERNAL TABLE eg.r
    WITH (s3_alias = 'demo_s3', location = 's3://bucket/data.csv', format = 'csv');

-- Export
COPY (SELECT * FROM eg.t WHERE …) TO '/tmp/out.ndjson' WITH (format = 'ndjson');
COPY (SELECT * FROM eg.t) TO 's3://bucket/exported/out.csv' WITH (s3_alias = 'demo_s3', format = 'csv');
```

- Formats: `parquet` (snappy, brotli, zlib, lz4, zstd), `csv`, `ndjson`, read and write.
- The target is named as in `CREATE TABLE`; its schema must exist.
- A loaded table is an ordinary engine table: `SELECT`, `INSERT`, JOINs with local, external and
  remote tables, `COPY ... TO`. Keep JOIN-key widths equal on both sides (an int32 against an
  int64 key matches nothing; see [`CLAUDE.md`](CLAUDE.md#external-tables--file--s3)).
- S3 objects are streamed, never staged on local disk.

## Requirements

- CMake 3.15+, a C++20 compiler with coroutines, Conan 2.x
- Docker & Docker Compose, Python 3 (clients and tests)

## Building

otterbrix (`1.0.0b2-rc-4`) and actor-zeta come from the `otterbrix` Conan remote; `--build missing`
builds otterbrix from its recipe where the remote has no binary for your configuration (see
[`CLAUDE.md`](CLAUDE.md#critical-dependency-versions-conanfilepy)). Size the build jobs to your RAM
(about 1.5 GB per job).

```bash
pip install "conan>=2.0" && conan profile detect --force
conan remote add otterbrix https://conan.otterbrix.com
conan install conanfile.py --build missing -s build_type=Release -s compiler.cppstd=20
cmake -S . -B build/Release -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake --build build/Release --parallel <JOBS>
./build/Release/server --config config.yaml
```

```
--help, -h        Show help message
--config <path>   Path to configuration file (default: config.yaml)
```

## Running with Docker Compose

```bash
python3 fixtures/generate_data.py  # test data
docker compose up                  # databases + server; connections from scripts/database/config.yaml
```

`examples/quick_start/` and `examples/demo/` are self-contained stacks with their own READMEs.

## Testing

```bash
./docker-run-tests.sh [-j N] [--tracy | --tracy-sep]
```

Builds the test image (its `ctest` step runs the C++ suites), brings up MariaDB / PostgreSQL /
ClickHouse / RustFS / Kafka, runs the Python integration suite and the PySpark client matrix, then
tears everything down.

| Option | Description |
| ------ | ----------- |
| `-j N` | Parallel jobs for the image build (capped to Docker's RAM when omitted) |
| `--tracy` | One Tracy profile for the whole run → `tracy_profiles/<timestamp>/otterstax.tracy` |
| `--tracy-sep` | One Tracy profile per test (the server restarts between tests) |

### Benchmarks

```bash
./benchmark/scripts/run_benchmark.sh [OPTIONS]
```

Builds the images, starts the backends, loads data, runs latency tests over the wire frontends and
writes `benchmark_results/<timestamp>/`.

| Option | Description |
| ------ | ----------- |
| `--repetitions N` | Repetitions per sub-test (default: 10) |
| `--frontend F` | `mysql` \| `postgres` \| `arrow`; may be repeated; default: `mysql postgres` |
| `--bench T [T …]` | `simple_select` `complex_select` `join_same_instance` `join_cross_engine` `join_all` `external_load` `external_join` `external_dump` `external_join_cross` `external_join_all` (an `external_*` test starts RustFS and registers the `bench_rustfs` alias) |
| `--rebuild` / `--clear` | Rebuild the images / also drop the DB volumes |
| `-j N` | Parallel build jobs (auto-capped by RAM) |
| `--tracy` / `--tracy-sep` | Tracy capture, whole run / per test |
| `--perf` / `--perf-alloc` | `perf` CPU call graph (99 Hz, dwarf) / plus a `malloc` uprobe |

## Repository layout

```
otterstax/
├── frontend/      # Flight SQL, MySQL, PostgreSQL, Spark Connect servers
├── session/       # query_session_t: one per client connection
├── otterbrix/     # parser, SQL generator, translators, engine requests, federation (backend actors)
├── backends/      # remote connections: agents and backend actors, startup discovery
├── catalog/       # discovery, registration in the engine's catalog, owned names
├── connectors/    # MySQL / PostgreSQL / ClickHouse / S3 / file connectors
├── integration/   # S3Manager, Kafka runtime
├── runtime/       # thread pools and the embedded engine
├── config/        # config.yaml reader
├── tests/         # Catch2 suites, Python integration suite
├── benchmark/     # benchmark harness
├── examples/      # demo and quick start stacks
└── fixtures/      # test data generation
```

## Contributing

Contributions are welcome. Please follow the repository's style and add tests for new functionality.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
