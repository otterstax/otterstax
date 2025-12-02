# Sqlflight Server with Otterbrix Integration

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

### Install dependencies: 
Install faker via pip 

```sh
    pip install faker
```

### Run test script:
  ```sh
  chmod +x ./docker-run-tests.sh~~~~
  ./docker-run-tests.sh
  ```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
