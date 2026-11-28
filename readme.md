# OtterStax

[![Unit Tests](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml)

A SQL federation server: one endpoint over several databases, reached through the MySQL and
PostgreSQL wire protocols, Apache Arrow Flight SQL and Spark Connect. OtterStax is built on the
Otterbrix query engine, which plans and executes queries against local and remote tables. Remote
tables appear in its catalog and are read and written through their backends.

## Features

- Protocols: MySQL, PostgreSQL, Flight SQL, Spark Connect (PySpark 3.5–4.2)
- Federation: query MariaDB / MySQL, PostgreSQL and ClickHouse backends in one statement. A block
  over one backend can be pushed down whole when its dialect supports the plan. Reads can also
  combine backend scans with execution in Otterbrix.
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

The PostgreSQL wire (8817) is published by the quick start stack, `examples/quick_start/`; the
repository's own `compose.yml` publishes Flight SQL (8815), the MySQL wire (8816) and Tracy (8086).

Agents querying a running OtterStax server can use the [OtterStax SQL skill](.claude/skills/otterstax-sql/SKILL.md)
for connection settings, federated SQL examples and dialect constraints.

A PostgreSQL client names its initial place: a connection alias, `alias.namespace`, `local` or
`local.namespace` (above, `local`). A name that is neither a connection nor `local` is refused
(`FATAL 3D000`). Without a place, short names fail. Use `SET search_path = 'local.ns'`
with an existing namespace to change the place on the same connection.

## Connection configuration

There is a **single config file, `config.yaml`**: the server settings under `service:` and, under
`connections:`, every remote backend and S3 alias. It is read **once at startup** — there is no
runtime add / remove API; edit the file and restart.

The path is `--config PATH` (default `config.yaml`, relative to the working directory). The file
must exist and must set `service.engine.data_dir`, which holds the engine's tables, WAL and,
by default, logs. If either is absent, the server writes the reason to stderr and exits with code 1.
The public image reads `/otterstax/config.yaml` (see [Docker](#docker)).

```yaml
service:
  flight_sql: { host: "0.0.0.0", port: 8815 }
  mysql:      { port: 8816 }           # wire-server port, not a backend
  postgres:   { port: 8817 }           # wire-server port, not a backend
  spark_connect: { host: "0.0.0.0", port: 15002 }
  connection_retry: { max_attempts: 10, delay_ms: 2000 }   # optional; default one attempt
  engine: { data_dir: /tmp/test_collection_sql/base }     # required

connections:
  mysql:
    - alias: mysql                     # the first part of every name of this connection
      host: demo-mariadb
      port: "3306"                     # a string; optional (empty: the driver's default)
      username: demo
      password: demo
      database: bill
      table: ""                        # optional: scope discovery to this table
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

On the [quick start stack](examples/quick_start/README.md), this query returns 1000 matches:

```sql
SELECT count(*) AS matches
FROM sales.ops.orders o
JOIN pgshop.shop.customers c ON c.customer_id = o.customer_id;
```

For another stack, use its configured connection aliases and backend namespaces.

Short names (`table`, `schema.table`) use the session's current place: a connection (`uid`), one
of its namespaces (`uid.ns`), or `local` / `local.ns` for your own tables. The PostgreSQL `database`
sets the initial place. Change it with `SET search_path = 'local.ns'`, using an existing namespace.
MySQL uses its handshake database and `USE`. Spark uses its current database.
Without a place short names fail. Four-part table names are refused.

A table added after startup can be discovered on first use within the configured connection scope.
With `table:` set, other tables are excluded. The server retries the statement only if registration
succeeds, it can rebuild the statement and the session is outside a transaction block. A miss
inside a block can register the table while returning an error. Run `ROLLBACK` before retrying.

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
- A loaded table is an ordinary engine table. Use `SELECT`, `INSERT`, JOINs with local, external
  and remote tables, and `COPY ... TO`.
- S3 objects are streamed, never staged on local disk.

## Requirements

- CMake 3.24+, a C++20 compiler with coroutines, Conan 2.x, Bison and Flex
- Docker & Docker Compose, Python 3 (clients and tests)

## Building

otterbrix (`1.0.0b2-rc-7`) and actor-zeta come from the `otterbrix` Conan remote; `--build missing`
builds otterbrix from its recipe where the remote has no binary for your configuration (see
[`CLAUDE.md`](CLAUDE.md#critical-dependency-versions-conanfilepy)). Size the build jobs to your RAM
(about 1.5 GB per job).
On macOS with Homebrew Bison, pass `-DBISON_EXECUTABLE=/opt/homebrew/opt/bison/bin/bison`
to CMake. The system Bison is too old for the parser build.

```bash
pip install "conan>=2.0" && conan profile detect --force
conan remote add otterbrix https://conan.otterbrix.com
conan install conanfile.py --build missing -s build_type=Release -s compiler.cppstd=20
cmake -S . -B build/Release -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake --build build/Release --parallel <JOBS>
./build/Release/server --config config.yaml
```

```
--help, -h        Show help message
--config <path>   Path to configuration file (default: config.yaml)
```

## Docker

```bash
docker pull ghcr.io/otterstax/otterstax:0.2.0

docker run -d --name otterstax --stop-timeout 60 \
  -p 8815:8815 -p 8816:8816 -p 8817:8817 -p 15002:15002 \
  -v otterstax-data:/otterstax/data \
  ghcr.io/otterstax/otterstax:0.2.0
```

The image is published for Linux amd64, one tag per release and no `latest`: the newest version is
listed on the [package page](https://github.com/otterstax/otterstax/pkgs/container/otterstax) and
under [Releases](https://github.com/otterstax/otterstax/releases). Two paths matter:

- `/otterstax/config.yaml` is the config. The image ships one that sets only the data directory,
  so the command above starts with default ports and no connections. To supply
  connection settings, add `-v "$PWD/config.yaml:/otterstax/config.yaml:ro"` before the image
  name. Set the following value in that file to use the named volume in this example:

  ```yaml
  service:
    engine:
      data_dir: /otterstax/data
  ```

- `/otterstax/data` holds local tables, the WAL, local Kafka offsets and, by default,
  log files. The named volume in the command above keeps those files across container replacement.

The config is read once. To apply a change, edit the file and `docker restart otterstax`. On
`docker stop` the server drains the open sessions (up to 30 s) and writes a final checkpoint,
which is what `--stop-timeout 60` leaves room for. `docker logs otterstax` shows the startup and
any refusal of the config.

## Running with Docker Compose

```bash
python3 -m pip install Faker
python3 fixtures/generate_data.py  # test data
./scripts/deps-image.sh release    # once: the image with the prebuilt dependencies
docker compose up                  # databases + server; connections from scripts/database/config.yaml
```

This stack builds the server from the current checkout. It publishes Flight SQL (8815),
the MySQL wire (8816) and the Tracy port (8086, used when built with Tracy).
The root stack's engine data is in its container, at the path set in
`scripts/database/config.yaml`. The quick start and demo use the published Linux
amd64 runtime image. Both publish PostgreSQL on 8817, so run one at a time. See
`examples/quick_start/README.md` or `examples/demo/CLAUDE.md`.

To use a build from this checkout in either example stack, build the runtime
image once from the repository root:

```bash
./scripts/deps-image.sh release
docker build --build-arg BUILD_JOBS=0 -t otterstax:local .
```

Use `OTTERSTAX_IMAGE=otterstax:local docker compose up --wait` from the quick start
directory, or `OTTERSTAX_IMAGE=otterstax:local examples/demo/up.sh` from the
repository root. To persist the choice, or when Compose runs through sudo, set
`OTTERSTAX_IMAGE=otterstax:local` in the example directory's Compose environment
file (`.env`).

## Testing

```bash
./docker-run-tests.sh [-j N] [--tracy | --tracy-sep]
```

The runner builds the test image, brings up MariaDB / PostgreSQL / ClickHouse /
RustFS / Kafka, runs the Python integration suite and PySpark client matrix,
then tears down the `otterstax-tests` Compose project when it reaches Step 9.
When building the server image, the runner resolves the release dependency image
(or the ASAN image for `ENABLE_ASAN=ON`). The normal test image build runs CTest.
Both Tracy modes skip CTest, and `IMAGE_TAG` uses a prebuilt test server image.
To verify the C++ suites, run a normal build without Tracy or `IMAGE_TAG`, or run
`ctest --test-dir build/Release --output-on-failure` after
building locally with `BUILD_TESTS=ON`.

Before running, the script removes containers and volumes from previous
runs of that project. The manual stack and its generated host SQL files under
the root `init` directory are kept. If the manual stack is using port 8086, run
`docker compose stop` from the repository root to free the port while keeping its
containers and data. Use `docker compose start` after testing.

An early exit, such as a backend startup timeout, can leave the test stack
running. Inspect its state and logs from the repository root before retrying:

```bash
docker compose -p otterstax-tests -f compose.test.yml ps -a
docker compose -p otterstax-tests -f compose.test.yml logs
```

When you no longer need the test volumes for diagnosis, remove the test stack:

```bash
docker compose -p otterstax-tests -f compose.test.yml down --volumes --remove-orphans
```

| Option | Description |
| ------ | ----------- |
| `-j N` | Parallel jobs for the image build (capped to Docker's RAM when omitted) |
| `--tracy` | One Tracy profile for the whole run → `tracy_profiles/<timestamp>/otterstax.tracy` |
| `--tracy-sep` | One Tracy profile per test (the server restarts between tests) |

### Benchmarks

Run the automated benchmark on a disposable Docker environment, with no
manual benchmark stack and no manual project selected via `COMPOSE_PROJECT_NAME`.
Each run reloads the benchmark fixtures and removes its Compose containers and
volumes at exit.

```bash
./benchmark/scripts/run_benchmark.sh [OPTIONS]
```

Builds the images, starts the backends, loads data, runs latency tests over the wire frontends and
writes `benchmark_results/<timestamp>/`.

| Option | Description |
| ------ | ----------- |
| `--repetitions N` | Repetitions per sub-test (default: 10) |
| `--frontend F` | `mysql` \| `postgres` \| `arrow`; may be repeated; default: `mysql postgres arrow` |
| `--bench T [T …]` | `simple_select` `complex_select` `join_same_instance` `join_cross_engine` `join_all` `external_load` `external_join` `external_dump` `external_join_cross` `external_join_all` `kafka_ingest` `kafka_produce` `kafka_stream` (an `external_*` test starts RustFS and registers the `bench_rustfs` alias; a `kafka_*` test starts Redpanda; neither runs over `arrow`) |
| `--rebuild` / `--clear` | Force image builds / additionally remove images and backend volumes before the run |
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
