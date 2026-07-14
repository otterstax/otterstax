# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Test Suites

All C++ tests use Catch2 and are built with `-DBUILD_TESTS=ON`.

| Directory | Binary | What it tests |
|-----------|--------|---------------|
| `tests/system/` | `test_system` | Full Scheduler→Worker pool pipeline with real Otterbrix + mock connectors. Tests own an `actor_zeta::scheduler::sharing_scheduler` via `make_az_scheduler()` and await Scheduler/Worker futures through the asio bridge helper `await_session()` (see `tests/system/test_scheduler.cpp`, `test_scheduler_concurrent.cpp`, `test_file_ingestion.cpp`). In-image `FinalizeS3` listener cleans up Arrow's S3 client at exit (`main.cpp`). |
| `tests/unit/parser/` | `test_parser` | Subquery extractor, qualifier rewriter, SQL generator, and the s3/file grammar extensions (parse stage, registry routing, transform → `external_node_t`, trailing-`;` tolerance) |
| `tests/unit/schema/` | `test_unit_schema` | `schema_utils` namespace |
| `tests/unit/utility/` | `test_unit_utility` | `session` helpers (the old `cv_wrapper` is gone — frontends await futures via `frontend/common/asio_future_bridge.hpp` instead) |
| `tests/mysql-front/` | `test_mysql_front` | MySQL protocol reader/writer, result-set encoding |

Python integration tests in `tests/` (e.g. `test_flightsql_client_mysql_backend.py`) run against live Docker containers via `docker-run-tests.sh`.

## Mock Layer (`tests/mock/`)

System tests build a full actor graph using real Otterbrix but injected mock connectors:

| Mock file | Replaces |
|-----------|----------|
| `mock/parser.hpp` | `IParser` — `make_mock_parser` / `make_throwing_mock_parser` are passed as `parser_factory_fn` to the Scheduler so each `Worker` builds its own mock instance (codex rule 10: no shared parser between actors) |
| `mock/sql_db_connector.hpp` | `mysql::IConnector` — returns fixed result rows |
| `mock/pg_db_connector.hpp` | `pg::IConnector` |
| `mock/ch_db_connector.hpp` | `ch::IConnector` |
| `mock/otterbrix.hpp` | `IDataManager` (`SimpleMockOtterbrixManager`) |
| `mock/parser.hpp` | `IParser` — returns pre-built `ParsedQueryData` |
| `mock/mock_config.hpp` | Otterbrix config (`configuration::config::default_config()`) |

Connectors are injected via the `connector_factory` constructor parameter on `ConnectorManager`. Otterbrix is injected via `OtterbrixManager(resource, make_unique<SimpleMockOtterbrixManager>())`.

## Running a Single Test

```bash
# Run one Catch2 test by name
./build/test_system "[test name]"

# List all available test names
./build/test_system --list-tests

# Run with verbose output
./build/test_system -s
```

## Python Test Structure

Python tests share common helpers via `config.py` (connection params) and `query_data.py` (expected result fixtures). File names encode: `test_{protocol}_client_{backend}_backend[_mutable].py`. The `test_cross_backend_queries*.py` files exercise mixed-backend joins.

External-table tests (s3/file grammar extension) over the MySQL wire — driven via the shared `external_helpers.py` module and a small family of `test_mysql_*.py` files:

- `test_{schema_,}mysql_{file,s3}.py` — the originals; verify schema discovery, row counts, per-campaign aggregates, `COPY` round-trips, and JOINs across the two joinable fixtures `regions.parquet` (200 rows) and `web_events.csv` (5000 rows).
- `test_mysql_file_ndjson.py` — adds `campaigns.ndjson` (50 rows) to confirm ndjson load + COPY round-trip via the file source.
- `test_mysql_join_sql_s3_to_s3.py` — full federated → external round-trip: `CREATE EXTERNAL TABLE` from s3 parquet, stage backend rows into otterbrix-internal, JOIN, persist, `COPY (…) TO 's3://…'` as csv, read back via a fresh external table.
- `test_mysql_join_otb_local_s3.py` — `CREATE TABLE` + `INSERT VALUES` into otterbrix-internal then JOIN against an s3-loaded external; positive control for the "all sides resolved to engine-internal tables" path.
- `test_mysql_join_otb_local_backend.py` — backend on the LEFT, otterbrix-internal `CREATE TABLE` on the RIGHT, JOIN on a STRING key. Mirrors the shape `examples/demo/sql/step_4.sql` runs in the live demo: backend manager fetches the sql slice through the existing single-backend dispatch and inlines it as `raw_data`; the engine resolves the symbolic local side by its stamped `table_oid` and JOINs them in-process. The string JOIN key avoids the int width sensitivity called out in FIX_JOIN.md.
- `test_mysql_join_otb_local_s3_file.py` — three-origin JOIN: s3 parquet `regions` ⋈ local file csv `web_events` ⋈ otterbrix-internal `weights` (`CREATE TABLE`+`INSERT VALUES`), all on `campaign_id` (int64 on every side). Smaller-scale shadow of the `external_join_all` benchmark — catches regressions in the "everything resolved to engine-internal" multi-source JOIN before the benchmark suite runs.

Fixtures live in `tests/minio/fixtures/` (regenerable by `generate_external_fixtures.py`; needs `pyarrow` + `faker`). The python suite uses the single-bucket MinIO in `compose.test.yml` (mount `/fixtures` for local-file mode, `GET /s3/add_credentials` registers MinIO for s3 mode). The standalone two-MinIO debug stack under `tests/minio/` (`docker-compose.yml`, `manual_run.sh`, `s3cmd.sh`) is unrelated to the suite — see `tests/minio/CLAUDE.md`.

**JOIN-key width sensitivity** is the common gotcha in the JOIN tests
above. The engine silently returns zero rows when an equi-JOIN compares an
int32 column against an int64 column — no error. Two places to keep in
sync:

- Local `CREATE TABLE` columns joining against parquet-loaded data: declare
  the local side `bigint` to match the int64 the parquet loader emits.
- Backend `INT` columns (which `mysql_to_chunk` and friends pass through as
  int32) joining against a local `bigint`: either stage the backend slice
  into a local `bigint` table first (`test_mysql_join_sql_s3_to_s3.py`) or
  join on a string column instead (`test_mysql_join_otb_local_backend.py`,
  mirroring `examples/demo/sql/step_4.sql`). See `FIX_JOIN.md` at repo root.

**Cleanup discipline.** `ExternalTableTester.cleanup()` runs DROPs for every
`(db, table)` pair the test appended to `_created`. Most tests in this
family call it at the **end** of `run()` rather than from a `try/finally`,
so a failed `assert` skips the cleanup and leaves engine state behind. That
leak can cascade into the next test in the suite (we hit a 400-row
`regions` count in one run because the previous test failed before
cleaning up). Wrap with `try/finally` when you add a new test here.

Run a single Python test file:
```bash
python -m pytest tests/test_flightsql_client_mysql_backend.py -v
```
(Requires live Docker stack — run `docker compose up -d` first and register connections via `add_connections_maria_db.sh`.)

### Convention: one class, each case an explicit `test_*` method

**Do NOT cram every scenario into one giant `main()` with inline blocks — it is unreadable.** Structure an integration test file as a single class whose cases are named methods. Reference: `test_pg_client_pg_backend.py` and the `test_kafka_*.py` family.

- One `class <Name>Test` per file; connection params / topic names / fixtures in `__init__` (read `--local` host via `config.py`).
- `setup(self)` creates the shared objects (CREATE TABLE/SOURCE/STREAM, produce seed data); `cleanup(self)` drops them and closes the connection.
- **Each test case is its own `def test_<behaviour>(self)` with a one-line docstring** — one observable behaviour per method, named for what it asserts.
- Shared helpers (`create_topic`, `consume`, `assert_equal`, …) are methods on the class, not free functions duplicated inline.
- `run_all_tests(self)`: `try: setup(); test_a(); test_b(); finally: cleanup()`. **Let exceptions propagate** — do not `except: print("fail")` and swallow, or the green ALL-PASSED banner will lie (a real bug in some older tests).
- `main_test()` parses `--local`, runs `run_all_tests()`, prints the pass/fail banner, returns `0`/`1`; `sys.exit(main_test())` at the bottom.
