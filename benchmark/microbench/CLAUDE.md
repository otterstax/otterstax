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

**Why no row data:** libpq has no public API to insert synthetic rows into a `PGresult` without a live wire-protocol connection. Row-level `pg_to_chunk` is exercised by the system tests (`tests/system/`).

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

#### `ChunkBatchReader::ReadNext` — full `data_chunk_t` → Arrow `RecordBatch`

| Benchmark | Rows |
|-----------|------|
| `BM_chunk_to_arrow_full_100` | 100 |
| `BM_chunk_to_arrow_full_1k` | 1 000 |
| `BM_chunk_to_arrow_full_10k` | 10 000 |

This is the hot path for every query result delivered over the FlightSQL frontend. The schema is pre-built outside the loop; `ch_to_chunk` runs inside the loop because `data_chunk_t` is not copyable (move-only). **The reported time therefore includes both `ch_to_chunk` and the Arrow builder pass.** Use `BM_ch_to_chunk_*` to isolate the former.

#### `mysql_to_complex` — MySQL column-type mapping

| Benchmark | Notes |
|-----------|-------|
| `BM_mysql_type_mapping_all` | All 13 type/signedness combinations in one iteration |

`tsl::mysql_to_complex` is called once per column per query when the MySQL connector builds its translator table. The batch exercises every branch of the switch.

**Why no `mysql_to_chunk`:** `boost::mysql::results` must be populated via the live wire protocol; no test-construction API exists. Row-level MySQL translation is covered by system tests.

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

SQL parsing and subquery extraction.

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

#### `otterstax::parser::prepare_sql()` — subquery extraction + qualifier promotion

`prepare_sql` only produces stubs for SQL that contains subqueries wrapped in parentheses. Flat JOINs with 4-part table names go through a different scheduler path and produce 0 stubs.

| Benchmark | SQL | Stubs produced |
|-----------|-----|----------------|
| `BM_prepare_sql_simple` | Simple SELECT, no 4-part names | 0 (no-op rewrite) |
| `BM_prepare_sql_cross_backend` | MySQL × PG flat JOIN | 0 (flat JOIN, no stubs) |
| `BM_prepare_sql_subquery` | Single-backend subquery in FROM | 1 |
| `BM_prepare_sql_complex_select` | Aggregation, single-backend | 0 |
| `BM_prepare_sql_three_backend` | MySQL × PG × CH flat JOIN | 0 (flat JOIN, no stubs) |

The `cross_backend` and `three_backend` prepare_sql benchmarks measure the parsing + promote pass on realistic multi-backend SQL even though those queries happen to produce 0 stubs (the stub path fires only when subqueries are present in the SQL).

---

### `bench_query_gen.cpp`

SQL qualifier rewriting and table reference formatting, exercised via `sql_gen::replace_qualifiers` and `sql_gen::table_reference`.

#### SQL fixture design — subquery requirement

**All SQL fixtures that feed `replace_qualifiers` must use the subquery form:**

```sql
-- correct: subquery in parentheses → produces a stub
SELECT * FROM (SELECT id FROM mysql.bill.schema.orders WHERE ...) o

-- wrong: flat table reference → produces 0 stubs
SELECT id FROM mysql.bill.schema.orders WHERE ...
```

`prepare_sql` produces `subquery_stub_t` objects only for SQL-level subqueries (`T_RangeSubselect` AST nodes). Flat JOINs with 4-part names never generate stubs and must not be used in `replace_qualifiers` benchmarks.

Stub ordering follows left-to-right position of the opening parenthesis in the SQL string.

#### `replace_qualifiers` — per-backend qualifier rewriting (isolated)

The stub is extracted once before the timing loop; only `replace_qualifiers` is timed.

| Benchmark | Backend | SQL source | Stub index |
|-----------|---------|------------|------------|
| `BM_replace_qualifiers_mysql` | MySQL | `kMysqlOnlySql` | stubs[0] |
| `BM_replace_qualifiers_pg` | PostgreSQL | `kCrossBackendSql` | stubs[1] (last) |
| `BM_replace_qualifiers_ch` | ClickHouse | `kChSql` | stubs[0] |
| `BM_replace_qualifiers_empty` | MySQL | local SQL (no qualifiers) | — |

`BM_replace_qualifiers_empty` is the no-op baseline: zero qualifier rewrites, just the string copy.

#### `pipeline` — `prepare_sql` + `replace_qualifiers` end-to-end

Mirrors the exact runtime sequence: one `prepare_sql` call followed by one `replace_qualifiers` per backend stub. Both steps are inside the timing loop.

| Benchmark | Backends | Stubs | SQL |
|-----------|----------|-------|-----|
| `BM_pipeline_mysql_single_source` | MySQL | 1 | `kMysqlOnlySql` |
| `BM_pipeline_cross_backend` | MySQL + PG | 2 | `kCrossBackendSql` |
| `BM_pipeline_three_backend` | MySQL + PG + CH | 3 | `kThreeBackendSql` |

#### `table_reference` — backend-specific table name formatting

Measures `sql_gen::table_reference(qualified_name_t, backend_type_t)` in isolation. Called once per table per query to produce `db.table` (MySQL) or `schema.table` (PG/CH) references.

| Benchmark | Backend |
|-----------|---------|
| `BM_table_reference_mysql` | MySQL |
| `BM_table_reference_pg` | PostgreSQL |
| `BM_table_reference_ch` | ClickHouse |

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

## What is NOT benchmarkable at this level

| Function | Reason | Covered by |
|----------|--------|------------|
| `tsl::mysql_to_chunk` | `boost::mysql::results` requires live wire data | System tests |
| `tsl::pg_to_chunk` (with rows) | libpq has no public row-insertion API for synthetic `PGresult` | System tests |
| `sql_gen::generate_query` | Requires Otterbrix logical-plan `node_ptr`, not exposed at unit level | System tests |
| `ChunkBatchReader::ReadNext` in isolation | `data_chunk_t` is non-copyable; `ch_to_chunk` is bundled in the loop | `BM_ch_to_chunk_*` for input, `BM_chunk_to_arrow_full_*` for combined |

---

## Adding a new benchmark

### Translator benchmark (`bench_translators.cpp`)

1. Use `make_ch_block(N)` + `tsl::ch_to_chunk(res, block)` to get a synthetic `data_chunk_t`.
2. `data_chunk_t` is **non-copyable**. If you need N iterations over the same data, either hold a `clickhouse::Block` outside the loop and call `ch_to_chunk` inside, or redesign to avoid copies.
3. If the benchmark needs `GreenplumParser` (e.g., testing a path that touches the parser), include `utility/logger.hpp` and add a `LoggerInit` guard as in `bench_parser.cpp` — the parser requires named spdlog loggers to be registered.

### Parser benchmark (`bench_parser.cpp`)

1. Add a new `const char* kMySQL = "..."` fixture in the anonymous namespace.
2. For `GreenplumParser::parse()` benchmarks: construct one `GreenplumParser` outside the loop (it is stateless and safe to reuse).
3. For `prepare_sql` benchmarks: use `std::pmr::unsynchronized_pool_resource pool` outside the loop; allocations accumulate across iterations without being freed mid-loop (intentional — mirrors production behaviour).
4. If the benchmark is about `prepare_sql` stub behaviour, verify the stub count first using the unit tests in `tests/unit/parser/test_subquery_extractor.cpp`.

### Query-generation benchmark (`bench_query_gen.cpp`)

1. SQL that feeds `replace_qualifiers` must contain **SQL-level subqueries** (i.e., `(SELECT ... FROM alias.db.schema.table ...)` wrapped in parentheses). Flat JOINs produce 0 stubs and must not be used.
2. Stub ordering is left-to-right by paren position — the first `(` in the SQL string produces `stubs[0]`.
3. To verify stub count and ordering for new SQL, run the matching unit test pattern in `tests/unit/parser/test_subquery_extractor.cpp`, or write a one-off Catch2 test.
4. Pre-extract stubs outside the timing loop for isolated `replace_qualifiers` benchmarks; include `prepare_sql` inside the loop only for pipeline benchmarks.

---

## Known constraints

**Exit 139 (SIGSEGV at process exit):** The Greenplum SQL parser (`raw_parser()`) has global state that races with spdlog's file-sink destructor on process exit. All benchmarks complete before the crash fires. `run-bench.sh` tolerates exit 139 and repairs truncated JSON automatically. See `benchmark/CLAUDE.md` for details.

**`assert` is compiled away in Release (`-DNDEBUG`).** The `assert(!r.stubs.empty())` guards in `bench_query_gen.cpp` do nothing in the production build. If you add a new benchmark that accesses `r.stubs[N]`, verify the stub count with the unit tests first — an out-of-bounds access in Release silently produces UB or a crash.

**`std::pmr::unsynchronized_pool_resource` accumulates allocations** across benchmark iterations (it only frees on destruction). This is intentional: it matches the production allocation pattern where the pool lives for the lifetime of one query. The pool object must be declared outside the state loop.
