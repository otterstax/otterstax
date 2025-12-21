# OtterStax

## How to Run

1. Run `fixtures/generate_data.py` to generate test data.
2. Run `docker compose up` to start all services.
3. Run `cd client_example`.
4. Run `example_connection/add_connections_maria_db.sh` to add connections to the `otterbrix_app`.
5. Run any client locally with `example_1.txt` or `example_2.txt`:
  ```sh
  python client.py example_1.txt > result_2.txt
  ```

## How to Test

### Run test script:
```sh
chmod +x ./docker-run-tests.sh
./docker-run-tests.sh
```

### Run integration tests with Docker Compose:
```sh
docker compose -f compose.test.yml up --build
```

## CI/CD

This project uses GitHub Actions for continuous integration:

| Workflow | Description | Trigger |
|----------|-------------|---------|
| `unit-tests.yml` | C++ unit and mock tests (Catch2) | Push/PR to main, develop |
| `integration.yml` | Integration tests with Docker Compose | Push/PR to main, develop |

### Unit & Mock Tests
- Builds project with Conan/CMake
- Runs `test_utils` (unit tests)
- Runs `test_mysql_front` (mock tests)

### Integration Tests
- Starts MariaDB instances
- Runs otterstax server
- Executes Python test client

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
