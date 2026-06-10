// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Google Benchmark targets for the data-translation hot paths:
//   ch_to_chunk        — ClickHouse Block → data_chunk_t
//   pg_to_struct       — PGresult* schema → complex_logical_type (schema only)
//   merge_schemas      — merge multiple column-schema vectors into one
//   chunk_to_arrow     — data_chunk_t schema → arrow::Schema (both overloads)
//   ChunkBatchReader   — data_chunk_t → arrow::RecordBatch (full data pipeline)
//   mysql_to_complex   — single column-type mapping (called once per column per query)
//
// mysql_to_chunk and pg_to_chunk with real row data cannot be benchmarked at unit
// level: boost::mysql::results requires live wire-protocol data, and libpq has no
// public API to insert synthetic rows into a PGresult.  Those paths are exercised
// by the system tests.

#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "otterbrix/translators/input/mysql_to_complex.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "frontend/flight_sql_server/batch_reader.hpp"

#include <benchmark/benchmark.h>

#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

#include <libpq-fe.h>

#include <memory_resource>
#include <vector>

using namespace components::types;
using namespace components::vector;
using boost::mysql::column_type;

namespace {

// Build a synthetic ClickHouse block with 3 columns (Int32, Float64, String) and N rows.
clickhouse::Block make_ch_block(int rows) {
    auto col_id    = std::make_shared<clickhouse::ColumnInt32>();
    auto col_score = std::make_shared<clickhouse::ColumnFloat64>();
    auto col_name  = std::make_shared<clickhouse::ColumnString>();
    for (int i = 0; i < rows; ++i) {
        col_id->Append(i);
        col_score->Append(static_cast<double>(i) * 0.5);
        col_name->Append("row_" + std::to_string(i));
    }
    clickhouse::Block b;
    b.AppendColumn("id",    col_id);
    b.AppendColumn("score", col_score);
    b.AppendColumn("name",  col_name);
    return b;
}

// Build a flat STRUCT type with N integer columns for schema-conversion benchmarks.
complex_logical_type make_wide_struct(int ncols) {
    std::vector<complex_logical_type> fields;
    fields.reserve(ncols);
    for (int i = 0; i < ncols; ++i) {
        fields.emplace_back(logical_type::INTEGER);
        fields.back().set_alias("col_" + std::to_string(i));
    }
    return complex_logical_type::create_struct("", std::move(fields));
}

} // namespace

// ── ch_to_chunk ──────────────────────────────────────────────────────────────

static void BM_ch_to_chunk_100(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto block = make_ch_block(100);
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 100);
}
BENCHMARK(BM_ch_to_chunk_100);

static void BM_ch_to_chunk_1k(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto block = make_ch_block(1000);
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 1000);
}
BENCHMARK(BM_ch_to_chunk_1k);

static void BM_ch_to_chunk_10k(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto block = make_ch_block(10000);
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 10000);
}
BENCHMARK(BM_ch_to_chunk_10k);

static void BM_ch_to_chunk_100k(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto block = make_ch_block(100000);
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 100000);
}
BENCHMARK(BM_ch_to_chunk_100k);

// Multi-block: two blocks of 5 000 rows each = 10 000 total.
static void BM_ch_to_chunk_multiblock_10k(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    std::vector<clickhouse::Block> blocks{make_ch_block(5000), make_ch_block(5000)};
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, blocks);
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 10000);
}
BENCHMARK(BM_ch_to_chunk_multiblock_10k);

// Schema extraction only (no row data).
static void BM_ch_to_struct(benchmark::State& state) {
    auto block = make_ch_block(0); // zero rows, schema present
    for (auto _ : state) {
        auto s = tsl::ch_to_struct(block);
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(BM_ch_to_struct);

// ── chunk_to_arrow schema conversion ─────────────────────────────────────────

static void BM_chunk_to_arrow_schema_10col(benchmark::State& state) {
    auto struct_t = make_wide_struct(10);
    for (auto _ : state) {
        auto schema = to_arrow_schema(struct_t);
        benchmark::DoNotOptimize(schema);
    }
}
BENCHMARK(BM_chunk_to_arrow_schema_10col);

static void BM_chunk_to_arrow_schema_50col(benchmark::State& state) {
    auto struct_t = make_wide_struct(50);
    for (auto _ : state) {
        auto schema = to_arrow_schema(struct_t);
        benchmark::DoNotOptimize(schema);
    }
}
BENCHMARK(BM_chunk_to_arrow_schema_50col);

// ── mysql_to_complex (called once per column per query) ────────────────────

static void BM_mysql_type_mapping_all(benchmark::State& state) {
    static const std::vector<std::pair<column_type, bool>> cases = {
        {column_type::tinyint,  false}, {column_type::tinyint,   true},
        {column_type::smallint, false}, {column_type::smallint,  true},
        {column_type::int_,     false}, {column_type::int_,      true},
        {column_type::bigint,   false}, {column_type::bigint,    true},
        {column_type::float_,   false}, {column_type::double_,   false},
        {column_type::bit,      false},
        {column_type::varchar,  false}, {column_type::blob,      false},
    };
    for (auto _ : state) {
        for (auto& [ct, us] : cases) {
            auto t = tsl::mysql_to_complex(ct, us);
            benchmark::DoNotOptimize(t);
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(cases.size()));
}
BENCHMARK(BM_mysql_type_mapping_all);

// ── pg_to_struct (schema extraction only) ────────────────────────────────────
// Row-level pg_to_chunk is not benchmarkable at unit level: libpq provides no
// public API to insert synthetic rows into a PGresult (see tests/unit/translators/
// test_pg_to_chunk.cpp).  This measures the schema-dispatch overhead alone.

static void BM_pg_to_struct(benchmark::State& state) {
    PGresult* r = PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK);
    for (auto _ : state) {
        auto s = tsl::pg_to_struct(r);
        benchmark::DoNotOptimize(s);
    }
    PQclear(r);
}
BENCHMARK(BM_pg_to_struct);

// ── merge_schemas ─────────────────────────────────────────────────────────────
// Called when OtterStax needs to unify the schemas of multiple result sets from
// the same backend (e.g. paginated fetches) before constructing the data_chunk_t.

static void BM_merge_schemas_2x3col(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    std::pmr::vector<std::pmr::vector<complex_logical_type>> schemas(&pool);
    for (int s = 0; s < 2; ++s) {
        std::pmr::vector<complex_logical_type> schema(&pool);
        schema.emplace_back(logical_type::INTEGER);
        schema.back().set_alias("id_" + std::to_string(s));
        schema.emplace_back(logical_type::DOUBLE);
        schema.back().set_alias("score_" + std::to_string(s));
        schema.emplace_back(logical_type::STRING_LITERAL);
        schema.back().set_alias("name_" + std::to_string(s));
        schemas.push_back(std::move(schema));
    }
    for (auto _ : state) {
        auto merged = tsl::merge_schemas(schemas);
        benchmark::DoNotOptimize(merged);
    }
    state.SetItemsProcessed(state.iterations() * 6);
}
BENCHMARK(BM_merge_schemas_2x3col);

static void BM_merge_schemas_5x6col(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    std::pmr::vector<std::pmr::vector<complex_logical_type>> schemas(&pool);
    for (int s = 0; s < 5; ++s) {
        std::pmr::vector<complex_logical_type> schema(&pool);
        for (int c = 0; c < 6; ++c) {
            schema.emplace_back(logical_type::INTEGER);
            schema.back().set_alias("s" + std::to_string(s) + "_c" + std::to_string(c));
        }
        schemas.push_back(std::move(schema));
    }
    for (auto _ : state) {
        auto merged = tsl::merge_schemas(schemas);
        benchmark::DoNotOptimize(merged);
    }
    state.SetItemsProcessed(state.iterations() * 30);
}
BENCHMARK(BM_merge_schemas_5x6col);

// ── chunk_to_arrow schema: vector overload ────────────────────────────────────
// The existing BM_chunk_to_arrow_schema_* benchmarks test the struct overload.
// This covers to_arrow_schema(std::pmr::vector<complex_logical_type>&), which is
// the overload called from the MySQL and PostgreSQL frontend result-set writers.

static void BM_chunk_to_arrow_schema_vec_10col(benchmark::State& state) {
    std::pmr::vector<complex_logical_type> types;
    types.reserve(10);
    for (int i = 0; i < 10; ++i) {
        types.emplace_back(logical_type::INTEGER);
        types.back().set_alias("col_" + std::to_string(i));
    }
    for (auto _ : state) {
        auto schema = to_arrow_schema(types);
        benchmark::DoNotOptimize(schema);
    }
}
BENCHMARK(BM_chunk_to_arrow_schema_vec_10col);

static void BM_chunk_to_arrow_schema_vec_50col(benchmark::State& state) {
    std::pmr::vector<complex_logical_type> types;
    types.reserve(50);
    for (int i = 0; i < 50; ++i) {
        types.emplace_back(logical_type::INTEGER);
        types.back().set_alias("col_" + std::to_string(i));
    }
    for (auto _ : state) {
        auto schema = to_arrow_schema(types);
        benchmark::DoNotOptimize(schema);
    }
}
BENCHMARK(BM_chunk_to_arrow_schema_vec_50col);

// ── ChunkBatchReader::ReadNext (full data chunk → Arrow RecordBatch) ─────────
// This is the hot path for every result delivered via the FlightSQL frontend.
// data_chunk_t is not copyable, so the ClickHouse block is kept outside the loop
// and ch_to_chunk is re-run each iteration.  The reported time therefore includes
// both ch_to_chunk and the Arrow serialisation; use BM_ch_to_chunk_* to isolate
// the former.

static void BM_chunk_to_arrow_full_100(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto block = make_ch_block(100);
    auto schema = to_arrow_schema(tsl::ch_to_struct(block));
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        auto reader = ChunkBatchReader::Make(schema, std::move(chunk)).ValueOrDie();
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = reader->ReadNext(&batch);
        benchmark::DoNotOptimize(status);
        benchmark::DoNotOptimize(batch);
    }
    state.SetItemsProcessed(state.iterations() * 100);
}
BENCHMARK(BM_chunk_to_arrow_full_100);

static void BM_chunk_to_arrow_full_1k(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto block = make_ch_block(1000);
    auto schema = to_arrow_schema(tsl::ch_to_struct(block));
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        auto reader = ChunkBatchReader::Make(schema, std::move(chunk)).ValueOrDie();
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = reader->ReadNext(&batch);
        benchmark::DoNotOptimize(status);
        benchmark::DoNotOptimize(batch);
    }
    state.SetItemsProcessed(state.iterations() * 1000);
}
BENCHMARK(BM_chunk_to_arrow_full_1k);

static void BM_chunk_to_arrow_full_10k(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto block = make_ch_block(10000);
    auto schema = to_arrow_schema(tsl::ch_to_struct(block));
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        auto reader = ChunkBatchReader::Make(schema, std::move(chunk)).ValueOrDie();
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = reader->ReadNext(&batch);
        benchmark::DoNotOptimize(status);
        benchmark::DoNotOptimize(batch);
    }
    state.SetItemsProcessed(state.iterations() * 10000);
}
BENCHMARK(BM_chunk_to_arrow_full_10k);
