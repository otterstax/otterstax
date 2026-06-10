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
- Docker ready: compose files to bring up test databases and the server

## Architecture

OtterStax runs several protocol servers concurrently:

| Protocol   | Default Port | Description |
|------------|--------------:|-------------|
| FlightSQL  | 8815          | Apache Arrow FlightSQL protocol |
| MySQL      | 8816          | MySQL wire protocol |
| PostgreSQL | 8817          | PostgreSQL wire protocol |
| HTTP       | 8085          | Connection manager REST API |

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

3. (Optional) Add example database connections used by clients:

```bash
examples/simple/example_connetion/add_connections.sh
```

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
| `--bench T [T …]` | Tests to run: `simple_select` `complex_select` `join_same_instance` `join_cross_engine` `join_all` |
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
├── catalog/            # Metadata catalog
├── connectors/         # Database connectors and HTTP server
├── component_manager/  # Component lifecycle management
├── config/             # Configuration loading and runtime settings
├── db_integration/     # Database integration layer
├── otterbrix/          # Otterbrix query engine integration
├── routes/             # Query routing
├── scheduler/          # Query scheduling
├── tests/              # Tests (integration/unit)
├── examples/          # Demo stack (examples/demo/) + simple client examples (examples/simple/)
└── fixtures/           # Test data generation
```

## Contributing

Contributions are welcome. Please follow the repository's style and add tests
for new functionality where appropriate.

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE)
file for details.
