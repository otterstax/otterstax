# Microbenchmark Coverage Guide

Google Benchmark suite for the hot paths in OtterStax's translation, parsing, and query-generation layers.
Binary: `build/Release/benchmark/microbench/otterstax_bench`
Run script: `benchmark/microbench/run-bench.sh` (handles LD_LIBRARY_PATH and JSON repair).

---

## Source files and what they cover

### `bench_translators.cpp`

Data conversion paths between backend wire formats, `data_chunk_t`, and Arrow.

#### `ch_to_chunk` — ClickHouse Block → `data_chunk_t`

| Benchmark | Rows | Notes |
|-----------|------|-------|
| `BM_ch_to_chunk_100` | 100 | |
| `BM_ch_to_chunk_1k` | 1 000 | |
| `BM_ch_to_chunk_10k` | 10 000 | |
| `BM_ch_to_chunk_100k` | 100 000 | |
| `BM_ch_to_chunk_multiblock_10k` | 10 000 | Two blocks of 5 000 rows — the multi-block overload |
| `BM_ch_to_struct` | 0 | Schema extraction only (`tsl::ch_to_struct`), no row data |

Synthetic block: 3 columns — Int32 `id`, Float64 `score`, String `name`.

#### `pg_to_struct` — PostgreSQL schema extraction

| Benchmark | Notes |
|-----------|-------|
| `BM_pg_to_struct` | Empty `PGresult*` (0 cols, 0 rows) — measures dispatch overhead only |

**Why no row data:** the suite measures schema dispatch only. Row-level `pg_to_chunk` is unit-tested on manufactured results (`tests/unit/translators/pg_result_fixture.cpp`, via `PQsetResultAttrs`/`PQsetvalue`) and exercised end to end by the system tests (`tests/system/`).

#### `merge_schemas` — unify multiple column-schema vectors

| Benchmark | Input | Total columns |
|-----------|-------|--------------|
| `BM_merge_schemas_2x3col` | 2 schemas × 3 cols | 6 |
| `BM_merge_schemas_5x6col` | 5 schemas × 6 cols | 30 |

`tsl::merge_schemas` is called when result sets from paginated backend fetches need to be unified before constructing `data_chunk_t`.

#### `chunk_to_arrow` — schema conversion (`to_arrow_schema`)

Two overloads exist; both are benchmarked:

| Benchmark | Overload | Columns |
|-----------|----------|---------|
| `BM_chunk_to_arrow_schema_10col` | `to_arrow_schema(complex_logical_type&)` — struct overload | 10 |
| `BM_chunk_to_arrow_schema_50col` | same | 50 |
| `BM_chunk_to_arrow_schema_vec_10col` | `to_arrow_schema(std::pmr::vector<...>&)` — vector overload | 10 |
| `BM_chunk_to_arrow_schema_vec_50col` | same | 50 |

The vector overload is the one called by the MySQL and PostgreSQL wire-protocol frontend writers; the struct overload is used by the FlightSQL path.

#### Flight SQL data path — `data_chunk_t` → Arrow `RecordBatch` → IPC payload

| Benchmark | Rows |
|-----------|------|
| `BM_flight_wire_full_100` | 100 |
| `BM_flight_wire_full_1k` | 1 000 |
| `BM_flight_wire_full_10k` | 10 000 |

This is the hot path for every query result delivered over the Flight SQL frontend: `chunk_to_record_batch`, then `arrow::ipc::GetRecordBatchPayload` (what `rpc/flight_server.cpp` frames into a DoGet message). The schema is pre-built outside the loop; `ch_to_chunk` runs inside the loop because `data_chunk_t` is not copyable (move-only). **The reported time therefore includes `ch_to_chunk`, the Arrow builder pass and the IPC serialization.** Use `BM_ch_to_chunk_*` to isolate the first.

#### MySQL column-type mapping — no separate benchmark

The MySQL mapping has one table (`to_local_translator`, internal to
`mysql_to_chunk.cpp`): each arm names both the engine type a column is read as and
the reader that writes its values, so there is no standalone enum → type function
to measure. It is exercised once per column per query by `mysql_to_chunk` and by
`tsl::mysql_to_struct`, the schema discovery reads off the same table.

**Why no `mysql_to_chunk`:** a `boost::mysql::results` carrying ROWS must be
populated via the live wire protocol. Its column definitions alone can be
manufactured through `boost::mysql::detail::access`
(`tests/unit/translators/test_mysql_types.cpp`), which is what the type table is
pinned with; row-level MySQL translation is covered by system tests.

---

### `bench_file_translators.cpp`

File-format translators used by the file/S3 ingestion path (`connectors/file`, `integration/s3`).

Synthetic data: 2 columns — integer `id`, string `name` (`row_i`).

#### Input — `csv_to_chunk` / `ndjson_to_chunk` / `parquet_to_chunk`

| Benchmark | Rows | Notes |
|-----------|------|-------|
| `BM_csv_to_chunk/{1000,10000}` | 1 k / 10 k | In-memory buffer overload (no disk I/O) |
| `BM_ndjson_to_chunk/{1000,10000}` | 1 k / 10 k | NDJSON, buffer overload |
| `BM_parquet_to_chunk/{1000,10000}` | 1 k / 10 k | Parquet buffer built once outside the loop |

Input benchmarks use the **buffer** overloads so only the parse + `data_chunk_t` construction is measured.

#### Output — `chunk_to_csv` / `chunk_to_ndjson` / `chunk_to_parquet`

| Benchmark | Rows | Notes |
|-----------|------|-------|
| `BM_chunk_to_csv/{1000,10000}` | 1 k / 10 k | |
| `BM_chunk_to_ndjson/{1000,10000}` | 1 k / 10 k | |
| `BM_chunk_to_parquet/{1000,10000}` | 1 k / 10 k | |

`chunk_to_*` only expose a file-path API, so output timings **include the filesystem write** to `/tmp`. The source `data_chunk_t` is built once outside the loop (it is move-only, but `chunk_to_*` take it by const ref and so reuse it).

**Arrow runtime note:** the conan cache holds several `libarrow.so` builds; only the one CMake links here ships the CSV/JSON/Parquet readers. `otterstax_bench` therefore pins an absolute `DT_RPATH` to `${CMAKE_BINARY_DIR}/lib` with `--disable-new-dtags` (see `CMakeLists.txt`) so the correct libarrow wins over `run-bench.sh`'s `LD_LIBRARY_PATH` glob.

---

### `bench_parser.cpp`

SQL parsing.

#### `GreenplumParser::parse()` — full parse + logical-plan construction

Each benchmark reuses one `GreenplumParser` instance across iterations (the parser is stateless between calls).

| Benchmark | SQL shape | Key clauses |
|-----------|-----------|-------------|
| `BM_parse_simple_select` | Single table, no qualifier | `WHERE` |
| `BM_parse_join_3table` | 3-table JOIN, same backend | `INNER JOIN × 2` |
| `BM_parse_cross_backend` | 2-backend JOIN (MySQL × PG) | `INNER JOIN` |
| `BM_parse_subquery` | Subquery in FROM, single backend | `GROUP BY`, subquery |
| `BM_parse_complex_select` | Aggregation, single backend | `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT` |
| `BM_parse_three_backend_join` | 3-backend JOIN (MySQL × PG × CH) | `INNER JOIN × 2`, `WHERE`, `LIMIT` |

---

### `bench_query_gen.cpp`

SQL generation for the backends, exercised via `sql_gen::generate_query`.

#### `generate_query` — one query block per dialect

The query block (the aggregate that reads the remote table, with its clauses) is parsed once by
`GreenplumParser` before the timing loop — the shape the federation's pushdown rule hands the
generator (`otterbrix/federation/rules/pushdown.hpp`); only `generate_query` is timed.

| Benchmark | Backend | Block |
|-----------|---------|-------|
| `BM_generate_query_mysql_filter` | MySQL | projection, `WHERE`, `ORDER BY`, `LIMIT` |
| `BM_generate_query_pg_aggregate` | PostgreSQL | `count` / `sum`, `WHERE`, `GROUP BY`, `HAVING` |
| `BM_generate_query_ch_filter` | ClickHouse | projection, `WHERE` with `AND` |

---

### `bench_grammar_extension.cpp`

The `s3` / `file` SQL parser extensions (`otterbrix/parser/grammar_extention/{s3,file}`), which teach the
engine `CREATE EXTERNAL TABLE … WITH (…)` and `COPY (<select>) TO '<location>' WITH (…)`. Two layers are
measured. Each iteration creates a fresh `std::pmr::monotonic_buffer_resource` (mirrors `GreenplumParser`,
which arena-scopes every parse; also bounds memory since a monotonic resource only frees on destruction).

Links `otterbrix::s3_extension` + `otterbrix::file_extension` (they export their headers as PUBLIC includes).

#### Isolated extension parse stage — `s3_ext::parse` / `file_ext::parse`

| Benchmark | Statement | Notes |
|-----------|-----------|-------|
| `BM_s3_ext_parse_create` | `CREATE EXTERNAL TABLE … 's3://…'` | option-list + s3 claim check |
| `BM_s3_ext_parse_copy` | `COPY (SELECT …) TO 's3://…'` | also the C++ driver's balanced-paren scan that strips the inner SELECT |
| `BM_file_ext_parse_create` | `CREATE EXTERNAL TABLE … '/data/…'` | local-path claim |
| `BM_file_ext_parse_copy` | `COPY (SELECT …) TO '/data/…'` | |

#### Registry-routed full parse — `raw_parser(arena, sql, registry)`

The realistic cost paid when the statement is typed at the wire: the **core Greenplum parser runs first and
rejects**, then the extension claims. The registry (both extensions added) is built once outside the loop.

| Benchmark | SQL | Path |
|-----------|-----|------|
| `BM_registry_parse_s3_create` | s3 CREATE EXTERNAL TABLE | core reject → `s3` claim |
| `BM_registry_parse_file_create` | file CREATE EXTERNAL TABLE | core reject → `file` claim |
| `BM_registry_parse_core_select` | `SELECT id, name FROM orders WHERE …` | core claims; extensions never consulted (dispatch-overhead baseline) |

The two `*_create` registry benchmarks are dominated by the core-parser-rejection cost (the same for both);
`BM_registry_parse_core_select` is the contrast where the core parser accepts.

**`transform()` is not separately benchmarked:** it is a placeholder that emits a single-row data node (see
`grammar_extention/CLAUDE.md`); there is no meaningful work to isolate until the real lowering lands.

---

### `bench_kafka_translators.cpp`

The Kafka value-conversion hot paths (`integration/kafka/detail/kafka_reader.cpp`),
broker-free. These are the per-batch conversions the SOURCE poller and STREAM
worker run for every Kafka message; the end-to-end `kafka_*` benchmarks time them
behind a live broker, this isolates the pure CPU cost. Dataset mirrors
`kafka_ingest`: 5 primitive columns (id/campaign_id/ts BIGINT, event_type STRING,
amount DOUBLE).

| Benchmark | Rows | Path |
|-----------|------|------|
| `BM_json_to_chunk/{1000,10000,100000}` | 1 k / 10 k / 100 k | JSON payloads → `data_chunk_t` (ingest inner loop) |
| `BM_chunk_to_json/{1000,10000,100000}` | 1 k / 10 k / 100 k | `data_chunk_t` → JSON payloads (stream/produce inner loop) |

Both report `items_per_second` (rows/s). `data_chunk_t` is move-only, so
`BM_chunk_to_json` builds the chunk once outside the loop (`chunk_to_json` takes it
by const ref). Links `kafka_runtime` (a separate `target_link_libraries` call in
`CMakeLists.txt`, which also brings `kafka_grammar` for `kafka_column_t`).

## What is NOT benchmarkable at this level

| Function | Reason | Covered by |
|----------|--------|------------|
| `tsl::mysql_to_chunk` | `boost::mysql::results` requires live wire data | System tests |
| `tsl::pg_to_chunk` (with rows) | not benchmarked; rows are manufactured only in the unit tests (`pg_result_fixture.cpp`) | Unit + system tests |
| `chunk_to_record_batch` in isolation | `data_chunk_t` is non-copyable; `ch_to_chunk` is bundled in the loop | `BM_ch_to_chunk_*` for input, `BM_flight_wire_full_*` for combined |

---

## Adding a new benchmark

### Translator benchmark (`bench_translators.cpp`)

1. Use `make_ch_block(N)` + `tsl::ch_to_chunk(res, blocks, {})` (the block in a vector, no declared types) to get a synthetic `data_chunk_t`. It answers `core::result_wrapper_t<data_chunk_t>` (`conversion_failure` for a value with no reading as its column's type): check `has_error()` and end the run with `state.SkipWithError(result.error().what.c_str())` before taking `value()`, as the `BM_ch_to_chunk_*` benchmarks do. A declared type (by result position) applies only to a wire column that represents it (see `otterbrix/CLAUDE.md`).
2. `data_chunk_t` is **non-copyable**. If you need N iterations over the same data, either hold a `clickhouse::Block` outside the loop and call `ch_to_chunk` inside, or redesign to avoid copies.
3. If the benchmark needs `GreenplumParser` (e.g., testing a path that touches the parser), include `utility/logger.hpp` and add a `LoggerInit` guard as in `bench_parser.cpp` — the parser requires named spdlog loggers to be registered.

### Parser benchmark (`bench_parser.cpp`)

1. Add a new `const char* kMySQL = "..."` fixture in the anonymous namespace.
2. For `GreenplumParser::parse()` benchmarks: construct one `GreenplumParser` outside the loop (it is stateless and safe to reuse).
3. Use `std::pmr::unsynchronized_pool_resource pool` outside the loop for the parser's resource; allocations accumulate across iterations without being freed mid-loop (intentional — mirrors production behaviour).

### Query-generation benchmark (`bench_query_gen.cpp`)

1. Add the SQL as a `const char*` fixture: one query block over ONE remote table written with its
   alias (`alias.db.schema.table`), the shape the federation's pushdown rule hands the generator.
2. `parse_block(parser, sql, uid)` parses it once outside the timing loop and finds the aggregate
   that reads the uid's table (`block_of`); `bench_block` times `sql_gen::generate_query` over it
   with the block's own target (`statement_target_t{block.node->target()}`) and parameters.
3. A JOIN or a statement over two servers is not one query block: the pushdown rule never sends it
   whole, so it is not a generator benchmark.

---

## Known constraints

**Exit 139 (SIGSEGV at process exit):** The Greenplum SQL parser (`raw_parser()`) has global state that races with spdlog's file-sink destructor on process exit. All benchmarks complete before the crash fires. `run-bench.sh` tolerates exit 139 and repairs truncated JSON automatically. See `benchmark/CLAUDE.md` for details.

**`assert` is compiled away in Release (`-DNDEBUG`).** The `assert(!parsed.has_error())` / `assert(block.node)` guards in `bench_query_gen.cpp`'s `parse_block` do nothing in the production build. Check a new fixture in a Debug build (or a one-off Catch2 test) first: a statement that does not parse, or whose block names another uid, dereferences null in Release.

**`std::pmr::unsynchronized_pool_resource` accumulates allocations** across benchmark iterations (it only frees on destruction). This is intentional: it matches the production allocation pattern where the pool lives for the lifetime of one query. The pool object must be declared outside the state loop.
