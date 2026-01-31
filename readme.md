# OtterStax

[![Unit Tests](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/otterstax/otterstax/actions/workflows/integration-tests.yml)

A high-performance SQL federation server that provides unified access to multiple databases through MySQL, PostgreSQL, and Apache Arrow FlightSQL wire protocols.

## Features

- **Multi-Protocol Support**: Connect using MySQL, PostgreSQL, or FlightSQL clients
- **Database Federation**: Query multiple MariaDB/MySQL,PostgreSQL databases through a single endpoint
- **Otterbrix Integration**: Powered by the Otterbrix query engine
- **Docker Ready**: Easy deployment with Docker and Docker Compose

## Architecture

OtterStax runs multiple protocol servers simultaneously:

| Protocol   | Default Port | Description                          |
|------------|--------------|--------------------------------------|
| FlightSQL  | 8815         | Apache Arrow FlightSQL protocol      |
| MySQL      | 8816         | MySQL wire protocol                  |
| PostgreSQL | 8817         | PostgreSQL wire protocol             |
| HTTP       | 8085         | Connection management REST API       |

## Requirements

### For Building
- CMake 3.15+
- C++20 compatible compiler with coroutine support
- Conan 2.x package manager

### For Running with Docker
- Docker
- Docker Compose

## Quick Start

### Using Docker Compose

1. Generate test data:
   ```bash
   python fixtures/generate_data.py
   ```

2. Start all services:
   ```bash
   docker compose up
   ```

3. Add database connections:
   ```bash
   cd client_example
   ./example_connection/add_connections_maria_db.sh
   ```

4. Run queries using the Python client:
   ```bash
   python client.py example_1.txt
   ```

### Building from Source

1. Install Conan and configure the remote:
   ```bash
   pip install conan==2.15.0
   conan profile detect --force
   conan remote add otterbrix http://conan.otterbrix.com
   ```

2. Install dependencies:
   ```bash
   mkdir build && cd build
   conan install ../conanfile.py --build missing -s build_type=Release -s compiler.cppstd=gnu17
   ```

3. Build:
   ```bash
   cmake .. -DCMAKE_TOOLCHAIN_FILE=./build/Release/generators/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
   cmake --build . -- -j $(nproc)
   ```

4. Run the server:
   ```bash
   ./server --help
   ```

## Command Line Options

```
--host-flight     FlightSQL server host (default: 0.0.0.0)
--port-flight     FlightSQL server port (default: 8815)
--port-mysql      MySQL server port (default: 8816)
--port-postgres   PostgreSQL server port (default: 8817)
--port-http       Connection manager HTTP port (default: 8085)
```

## Testing

### Run All Tests with Docker

```bash
chmod +x ./docker-run-tests.sh
./docker-run-tests.sh
```

This script orchestrates a full integration test cycle:
1. Cleans up previous state
2. Creates test data
3. Starts MariaDB instances
4. Runs OtterStax server
5. Executes Python test suite
6. Cleans up containers

### Run Unit Tests Only

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
├── db_integration/     # Database integration layer
├── otterbrix/          # Otterbrix query engine integration
├── routes/             # Query routing
├── scheduler/          # Query scheduling
├── tests/              # Unit tests (Catch2)
├── client_example/     # Python client examples
└── fixtures/           # Test data generation
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
