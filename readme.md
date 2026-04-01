# OtterStax

[![Unit Tests](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml)

A high-performance SQL federation server that provides unified access to multiple
databases through MySQL, PostgreSQL, and Apache Arrow FlightSQL wire protocols.
OtterStax is built on top of the Otterbrix query engine and is docker-friendly
for easy integration testing and deployment.

## Features

- Multi-protocol support: MySQL, PostgreSQL and FlightSQL
- Database federation: query multiple MariaDB/MySQL and PostgreSQL backends
  through a single endpoint
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

## Quick Start

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
cd client_example/example_connetion
./add_connections_maria_db.sh
```

4. Run example queries using the Python client:

```bash
python client_example/client.py example_1.txt
```

### Building from Source

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

Make the test runner executable and run it:

```bash
chmod +x ./docker-run-tests.sh
./docker-run-tests.sh
```

This script will bring up MariaDB/PostgreSQL containers, generate and load
test data, start the OtterStax server, run the Python integration tests,
and then clean up.

### Unit tests (containerized)

```bash
docker build -f Dockerfile.test -t otterstax-test .
```

## Project Structure

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
├── client_example/     # Python client and examples
└── fixtures/           # Test data generation
```

## Contributing

Contributions are welcome. Please follow the repository's style and add tests
for new functionality where appropriate.

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE)
file for details.
