# OtterStax

[![Unit Tests](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml)

A high-performance SQL federation server that provides unified access to multiple
databases through MySQL, PostgreSQL, and Apache Arrow FlightSQL wire protocols.
OtterStax is built on top of the Otterbrix query engine and is docker-friendly
for easy integration testing and deployment.

## Features

- Multi-protocol support: MySQL, PostgreSQL and FlightSQL
- Database federation: query multiple MariaDB/MySQL, PostgreSQL, and ClickHouse
  backends through a single endpoint
- Otterbrix integration: powered by the Otterbrix query engine
- **File and S3 external tables**: `CREATE EXTERNAL TABLE` ingests parquet,
  csv, or ndjson from a local path or an `s3://…` URI into the engine; `COPY
  (<select>) TO '…'` exports any query result back to a local file or an S3
  object — see [S3 / file external tables](#s3--file-external-tables) below.
- Docker ready: compose files to bring up test databases and the server

## Architecture

OtterStax runs several protocol servers concurrently:

| Protocol   | Default Port | Description |
|------------|--------------:|-------------|
| FlightSQL  | 8815          | Apache Arrow FlightSQL protocol |
| MySQL      | 8816          | MySQL wire protocol |
| PostgreSQL | 8817          | PostgreSQL wire protocol |

Server settings and all remote backends / S3 aliases live in a **single
`config.yaml`** and are read once at server startup (see
[Connection configuration](#connection-configuration)).

## Connection configuration

There is a **single config file, `config.yaml`**. It holds the wire-server
settings and, under a `connections:` section, all remote backends (MariaDB/MySQL,
PostgreSQL, ClickHouse) and S3 aliases. It is read **once at server startup**; the
`connections:` section is the single source of truth for connections —
**there is no runtime add/remove/update API**. To change connections, edit
`config.yaml` and restart the server.

### Locating the file

The path is passed with `--config PATH` and defaults to `config.yaml`, resolved
**relative to the server's working directory**:

```bash
./build/server --config path/to/config.yaml
```

In the provided Docker images the server runs from `/app/build/Release` with no
`--config`, so it reads `/app/build/Release/config.yaml` — the compose files
bind-mount a stack-specific `config.yaml` onto that path (and `Dockerfile.test`
bakes one in for the integration tests). A missing file means the server starts
with default ports and no connections.

### File format

```yaml
service:
  flight_sql:
    host: "0.0.0.0"
    port: 8815
  mysql:
    port: 8816          # wire-server port (NOT a backend)
  postgres:
    port: 8817          # wire-server port (NOT a backend)
  connection_retry:     # optional; startup retry for opening backends
    max_attempts: 10    #   default 1 (one-shot)
    delay_ms: 2000      #   default 1000

connections:
  mysql:
    - alias: mysql      # outermost qualifier in federated SQL: mysql.bill.<table>
      host: demo-mariadb
      port: "3306"      # note: port is a string; optional (empty → driver default)
      username: demo
      password: demo
      database: bill
      table: ""

  postgresql:
    - alias: pg
      host: demo-postgres
      port: "5432"
      username: demo
      password: demo
      database: shop
      schema: shop      # optional, defaults to "public"
      table: ""

  clickhouse:
    - alias: ch
      host: demo-clickhouse
      port: "9000"
      username: demo
      password: demo
      database: ev
      table: ""

  s3:
    - alias: demo_s3    # referenced by CREATE EXTERNAL TABLE / COPY ... TO via s3_alias=
      access_key: minioadmin
      secret_key: minioadmin
      region: us-east-1         # optional
      endpoint: demo-minio:9000 # optional; switches to http + path-style (MinIO)
```

Wire-server settings live under the top-level `service:` node so
`service.mysql`/`service.postgres` (wire-server ports) never collide with the
backends under `connections:`. All `connections:` sections are optional; each
backend requires `alias`/`host`/`username`/`database` (`port`/`table` optional).
An entry missing a required field **aborts startup** (fail-fast — the config must
be fixed), as does malformed YAML. A missing file means the server starts with no
registered connections. A *valid* connection whose backend is unreachable is
retried per `service.connection_retry` and, if still down, logged without
aborting startup (the other backends and local engine stay usable).

### Using an alias

Once registered, an alias is the outermost database qualifier in federated SQL:

```sql
SELECT * FROM mysql.bill.orders
JOIN pg.shop.customers ON mysql.bill.orders.customer_id = pg.shop.customers.id;
```

and the `s3_alias` for external tables:

```sql
CREATE EXTERNAL TABLE otter.regions
    WITH (s3_alias = 'demo_s3', location = 's3://demo-bucket/regions.csv', format = 'csv');
```

Ready-made `config.yaml` files ship with each runnable stack: `examples/demo/`,
`examples/simple/example_connetion/`, `tests/scripts/`, `scripts/database/`, and
`benchmark/` (plus a template at the repo root).

## S3 / file external tables

OtterStax can pull data into the engine from a local file or an S3-compatible
object store (AWS S3, MinIO, …) and dump query results back out, all over the
standard wire protocols. The surface SQL is two grammar extensions:

```sql
-- Load (auto-detects format from the file extension when omitted)
CREATE EXTERNAL TABLE db.t
    WITH (location = '/path/to/data.parquet', format = 'parquet');

CREATE EXTERNAL TABLE db.t
    WITH (s3_alias = 'minio1',
          location = 's3://bucket/data.csv',
          format   = 'csv');

-- Export (inner SELECT is re-parsed and executed by the engine)
COPY (SELECT * FROM db.t WHERE …) TO '/tmp/out.ndjson'
    WITH (format = 'ndjson');

COPY (SELECT * FROM db.t) TO 's3://bucket/exported/out.csv'
    WITH (s3_alias = 'minio1', format = 'csv');
```

- **Supported formats**: `parquet`, `csv`, `ndjson` (read and write).
- **Parquet compression** read/write: snappy, brotli, zlib (gzip), lz4, zstd
  (uncompressed too).
- **Format auto-detection**: optional — inferred from the file extension when
  the `format` option is omitted.
- **After load**: the external table becomes a normal otterbrix-internal
  table; `SELECT`, `INSERT`, JOIN against other tables, `COPY ... TO` all
  work the same way. JOINs between any combination of loaded external
  sources execute in the engine in a single statement, and so do JOINs that
  mix a **registered remote backend** (MariaDB / MySQL / PostgreSQL /
  ClickHouse) with an otterbrix-internal table — the backend manager fetches
  its slice into the engine as `raw_data` and the JOIN runs in-process. (See
  the deep-dive in [`CLAUDE.md`](CLAUDE.md#working-join-shapes) for the
  width-of-the-JOIN-key footgun to keep in mind.)
- **S3 credentials**: declare a named alias in the `connections.s3:` section of
  `config.yaml`, then reference it through `s3_alias=...`:

  ```yaml
  s3:
    - alias: minio1
      access_key: "…"
      secret_key: "…"
      region: us-east-1
      endpoint: minio:9000
  ```

  The `endpoint` field switches the underlying Arrow S3 client to http +
  path-style addressing (needed for MinIO); omit it to target real AWS.

Implementation lives under `connectors/{s3,file}/` (raw I/O),
`integration/s3/` (the `S3Manager` orchestrator), and
`otterbrix/parser/grammar_extention/{s3,file}/` (the parser extensions). The
top-level [`CLAUDE.md`](CLAUDE.md#external-tables--file--s3) has the full
end-to-end actor flow.

## Requirements

### Build

- CMake 3.15+
- C++20-capable compiler with coroutine support
- Conan (2.x) package manager

### Runtime (for local testing)

- Docker & Docker Compose
- Python 3.x (used by clients and test helpers)

- Docker & Docker Compose
- Python 3.x (used by clients and test helpers)

These instructions assume you are in the repository root.

### Using Docker Compose (recommended for integration testing)

1. Generate test data:

```bash
python fixtures/generate_data.py
```

2. Start services (databases + server):

```bash
docker compose up
```

3. Configure database connections. The `docker compose up` stack mounts
   `scripts/database/config.yaml` into the server; edit its `connections:`
   section to point at your backends and s3 aliases, then restart the server —
   connections are read only at startup (there is no runtime registration step).

4. Run example queries using the Python client:

```bash
python examples/simple/flight_sql_example.py examples/simple/example_1.txt
```

3. (Optional) Add example database connections used by clients:

1. Install and configure Conan (example using a recommended version):

```bash
pip install "conan>=2.0"
conan profile detect --force
# (optional) add Otterbrix remote if required by your conanfile
# conan remote add otterbrix http://conan.otterbrix.com
```

2. Create build directory and install dependencies:

```bash
mkdir -p build && cd build
conan install ../conanfile.py --build missing -s build_type=Release
```

3. Configure and build with CMake:

```bash
cmake -S .. -B . -DCMAKE_BUILD_TYPE=Release
cmake --build . -- -j$(nproc)
```

4. Run the server (from the build directory or repo root if configured):

```bash
./build/server
```

## Command-line Options

```
--help, -h        Show help message
--config <path>   Path to configuration file (default: config.yaml)
```

## Testing

### Run integration tests (Docker-based)

```bash
./build/server
```

The script brings up MariaDB/PostgreSQL/ClickHouse containers, loads test data,
starts the OtterStax server, runs the full Python integration test suite, then
tears everything down.

| Option | Description |
| ------ | ----------- |
| `-h`, `--help` | Print usage and exit |
| `--tracy` | Collect a single Tracy profile for the entire run → `tracy_profiles/<timestamp>/otterstax.tracy` |
| `--tracy-sep` | Collect one Tracy profile **per test** → `tracy_profiles/<timestamp>/<test_name>.tracy`. Otterstax is restarted between tests so each file is cleanly finalised. |
| `-j N` | Parallel jobs for the Docker image build (default: all cores inside the container) |

### Benchmark suite

```bash
./benchmark/scripts/run_benchmark.sh [OPTIONS]
```

Runs the full benchmark harness: builds images, starts backend DBs (MariaDB, PostgreSQL,
ClickHouse), initialises data, runs latency tests across MySQL and PostgreSQL wire frontends,
and writes results to `benchmark_results/<timestamp>/`.

| Option | Description |
| ------ | ----------- |
| `--repetitions N` | Repetitions per sub-test (default: 10) |
| `--frontend F` | `mysql` \| `postgres` \| `arrow`; may be repeated; default: `mysql postgres` |
| `--bench T [T …]` | Tests to run: `simple_select` `complex_select` `join_same_instance` `join_cross_engine` `join_all` `external_load` `external_join` `external_dump` `external_join_cross` `external_join_all`. Selecting any `external_*` test auto-starts MinIO, generates s3/file fixtures, and registers the `bench_minio` s3 alias (mysql/postgres frontends only). |
| `--rebuild` | Force image rebuild even if images already exist |
| `--clear` | Remove images + DB volumes, rebuild from scratch |
| `-j N` | Parallel cmake build jobs (auto-capped by available RAM) |
| `--tracy` | Continuous Tracy capture → `<out-dir>/benchmark.tracy` |
| `--tracy-sep` | Per-test Tracy capture → `<out-dir>/<frontend>_<test>.tracy` |
| `--perf` | CPU call-graph sampling with `perf` (99 Hz, dwarf unwind). Outputs `benchmark.perf.data` + `benchmark.perf` (drag into [speedscope.app](https://www.speedscope.app)). |
| `--perf-alloc` | Like `--perf` but also attaches a `malloc` uprobe so allocation call-sites appear in the call graph alongside CPU samples. Implies `--perf`. |

```bash
# CPU flamegraph for the whole benchmark run
./benchmark/scripts/run_benchmark.sh --frontend postgres --bench simple_select complex_select --perf

# CPU + allocation hotspots
./benchmark/scripts/run_benchmark.sh --frontend postgres --bench simple_select complex_select --perf-alloc
```

#### Examples

```bash
# Plain run
./docker-run-tests.sh

# Fast build using 4 cores
./docker-run-tests.sh -j4

# Collect a single Tracy profile for the whole suite
./docker-run-tests.sh --tracy

# Collect a separate Tracy file for every individual test
./docker-run-tests.sh --tracy-sep

# Tracy-sep with a faster build
./docker-run-tests.sh --tracy-sep -j8
```

Tracy output is written to `tracy_profiles/` in the repository root and is
never deleted automatically; each run creates a new timestamped sub-directory.

### Unit tests (containerized)

## Testing

### Run integration tests (Docker-based)

```
otterstax/
├── frontend/           # Protocol servers (FlightSQL, MySQL, PostgreSQL)
├── catalog/            # Metadata catalog + connection type registry
├── connectors/         # MySQL / PG / ClickHouse / S3 / file connectors
├── component_manager/  # Component lifecycle management
├── config/             # Configuration loading and runtime settings
├── integration/        # Actor wrappers bridging Scheduler ↔ connectors (incl. S3Manager)
├── otterbrix/          # Otterbrix engine integration + s3/file grammar extensions
├── scheduler/          # Query routing actor
├── cmake/              # Build helpers (parser-extension macro)
├── tests/              # Catch2 C++ tests + python integration suite
├── examples/           # Demo stack (examples/demo/) + simple client examples (examples/simple/)
└── fixtures/           # Test data generation
```

## Contributing

Contributions are welcome. Please follow the repository's style and add tests
for new functionality where appropriate.

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE)
file for details.
