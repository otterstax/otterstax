// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Google Benchmark targets for the data-translation hot paths:
//   ch_to_chunk        — ClickHouse Block → data_chunk_t
//   pg_to_struct       — PGresult* schema → complex_logical_type (schema only)
//   merge_schemas      — merge multiple column-schema vectors into one
//   chunk_to_arrow     — data_chunk_t schema → arrow::Schema (both overloads)
//   chunks_to_ipc      — data_chunk_t → Flight SQL IPC batch (full data pipeline)
//
// mysql_to_chunk with real row data cannot be benchmarked at unit level:
// boost::mysql::results requires live wire-protocol data for its ROWS. pg_to_chunk
// rows can be manufactured through PQsetResultAttrs/PQsetvalue
// (tests/unit/translators/pg_result_fixture.cpp); this suite measures only its
// schema dispatch.

#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "frontend/flight_sql/chunk_to_ipc.hpp"
#include "frontend/flight_sql/ipc/ipc_writer.hpp"

#include <benchmark/benchmark.h>

#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

#include <libpq-fe.h>

#include <memory_resource>
#include <vector>

using namespace components::types;
using namespace components::vector;

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
    std::pmr::vector<complex_logical_type> fields(std::pmr::new_delete_resource());
    fields.reserve(ncols);
    for (int i = 0; i < ncols; ++i) {
        fields.emplace_back(logical_type::INTEGER);
        fields.back().set_alias("col_" + std::to_string(i));
    }
    return complex_logical_type::create_struct("", std::move(fields));
}

// A schema the translator cannot map to Arrow skips the benchmark (nullptr, run
// marked with the error) instead of measuring a stream that could never start.
std::shared_ptr<arrow::Schema> arrow_schema_or_skip(benchmark::State& state,
                                                    std::pmr::memory_resource* res,
                                                    const complex_logical_type& struct_t) {
    auto converted = to_arrow_schema(res, struct_t);
    if (converted.has_error()) {
        state.SkipWithError(converted.error().what.c_str());
        return nullptr;
    }
    return std::move(converted.value());
}

} // namespace

// ── ch_to_chunk ──────────────────────────────────────────────────────────────

static void BM_ch_to_chunk_100(benchmark::State& state) {
    auto* res = std::pmr::new_delete_resource();
    auto block = make_ch_block(100);
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        if (chunk.has_error()) {
            state.SkipWithError(chunk.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 100);
}
BENCHMARK(BM_ch_to_chunk_100);

static void BM_ch_to_chunk_1k(benchmark::State& state) {
    auto* res = std::pmr::new_delete_resource();
    auto block = make_ch_block(1000);
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        if (chunk.has_error()) {
            state.SkipWithError(chunk.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 1000);
}
BENCHMARK(BM_ch_to_chunk_1k);

static void BM_ch_to_chunk_10k(benchmark::State& state) {
    auto* res = std::pmr::new_delete_resource();
    auto block = make_ch_block(10000);
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        if (chunk.has_error()) {
            state.SkipWithError(chunk.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 10000);
}
BENCHMARK(BM_ch_to_chunk_10k);

static void BM_ch_to_chunk_100k(benchmark::State& state) {
    auto* res = std::pmr::new_delete_resource();
    auto block = make_ch_block(100000);
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, block);
        if (chunk.has_error()) {
            state.SkipWithError(chunk.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 100000);
}
BENCHMARK(BM_ch_to_chunk_100k);

// Multi-block: two blocks of 5 000 rows each = 10 000 total.
static void BM_ch_to_chunk_multiblock_10k(benchmark::State& state) {
    auto* res = std::pmr::new_delete_resource();
    std::vector<clickhouse::Block> blocks{make_ch_block(5000), make_ch_block(5000)};
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(res, blocks);
        if (chunk.has_error()) {
            state.SkipWithError(chunk.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * 10000);
}
BENCHMARK(BM_ch_to_chunk_multiblock_10k);

// Schema extraction only (no row data).
static void BM_ch_to_struct(benchmark::State& state) {
    auto* res  = std::pmr::new_delete_resource();
    auto block = make_ch_block(0); // zero rows, schema present
    for (auto _ : state) {
        auto s = tsl::ch_to_struct(res, block);
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(BM_ch_to_struct);

// ── chunk_to_arrow schema conversion ─────────────────────────────────────────

static void BM_chunk_to_arrow_schema_10col(benchmark::State& state) {
    auto struct_t = make_wide_struct(10);
    for (auto _ : state) {
        auto schema = to_arrow_schema(std::pmr::new_delete_resource(), struct_t);
        benchmark::DoNotOptimize(schema);
    }
}
BENCHMARK(BM_chunk_to_arrow_schema_10col);

static void BM_chunk_to_arrow_schema_50col(benchmark::State& state) {
    auto struct_t = make_wide_struct(50);
    for (auto _ : state) {
        auto schema = to_arrow_schema(std::pmr::new_delete_resource(), struct_t);
        benchmark::DoNotOptimize(schema);
    }
}
BENCHMARK(BM_chunk_to_arrow_schema_50col);

// ── pg_to_struct (schema extraction only) ────────────────────────────────────
// Measures the schema-dispatch overhead alone on an empty PGresult.

static void BM_pg_to_struct(benchmark::State& state) {
    auto*     res = std::pmr::new_delete_resource();
    PGresult* r   = PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK);
    for (auto _ : state) {
        auto s = tsl::pg_to_struct(res, r);
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
    std::pmr::vector<complex_logical_type> types(std::pmr::new_delete_resource());
    types.reserve(10);
    for (int i = 0; i < 10; ++i) {
        types.emplace_back(logical_type::INTEGER);
        types.back().set_alias("col_" + std::to_string(i));
    }
    for (auto _ : state) {
        auto schema = to_arrow_schema(std::pmr::new_delete_resource(), types);
        benchmark::DoNotOptimize(schema);
    }
}
BENCHMARK(BM_chunk_to_arrow_schema_vec_10col);

static void BM_chunk_to_arrow_schema_vec_50col(benchmark::State& state) {
    std::pmr::vector<complex_logical_type> types(std::pmr::new_delete_resource());
    types.reserve(50);
    for (int i = 0; i < 50; ++i) {
        types.emplace_back(logical_type::INTEGER);
        types.back().set_alias("col_" + std::to_string(i));
    }
    for (auto _ : state) {
        auto schema = to_arrow_schema(std::pmr::new_delete_resource(), types);
        benchmark::DoNotOptimize(schema);
    }
}
BENCHMARK(BM_chunk_to_arrow_schema_vec_50col);

// ── chunks_to_ipc (full data chunk → Flight SQL IPC batch) ───────────────────
// This is the hot path for every result delivered via the FlightSQL frontend:
// the chunk run converts to the IPC model and the batches serialize to the
// messages DoGet streams. data_chunk_t is not copyable, so the ClickHouse block
// is kept outside the loop and ch_to_chunk is re-run each iteration, its chunk
// handed to the converter as a one-element payload. The reported time therefore
// includes both ch_to_chunk and the IPC serialisation; use BM_ch_to_chunk_* to
// isolate the former.

flight::ipc::SchemaPtr ipc_schema_or_skip(benchmark::State& state,
                                          const complex_logical_type& struct_t) {
    try {
        return flight::conv::schema_to_ipc(struct_t);
    } catch (const std::exception& e) {
        state.SkipWithError(e.what());
        return nullptr;
    }
}

static void BM_chunk_to_ipc_full_100(benchmark::State& state) {
    auto* res = std::pmr::new_delete_resource();
    auto block = make_ch_block(100);
    auto struct_t = tsl::ch_to_struct(res, block);
    auto schema = ipc_schema_or_skip(state, struct_t);
    if (!schema) {
        return;
    }
    for (auto _ : state) {
        auto converted = tsl::ch_to_chunk(res, block);
        if (converted.has_error()) {
            state.SkipWithError(converted.error().what.c_str());
            break;
        }
        std::pmr::vector<data_chunk_t> chunks(res);
        chunks.push_back(std::move(converted.value()));
        session_payload payload{struct_t, std::move(chunks), 0, NodeTag::T_SelectStmt};
        auto batches = flight::conv::chunks_to_ipc(payload, schema);
        for (const auto& batch : batches) {
            auto message = flight::ipc::serialize_record_batch(batch);
            benchmark::DoNotOptimize(message.bare_message);
            benchmark::DoNotOptimize(message.body);
        }
    }
    state.SetItemsProcessed(state.iterations() * 100);
}
BENCHMARK(BM_chunk_to_ipc_full_100);

static void BM_chunk_to_ipc_full_1k(benchmark::State& state) {
    auto* res = std::pmr::new_delete_resource();
    auto block = make_ch_block(1000);
    auto struct_t = tsl::ch_to_struct(res, block);
    auto schema = ipc_schema_or_skip(state, struct_t);
    if (!schema) {
        return;
    }
    for (auto _ : state) {
        auto converted = tsl::ch_to_chunk(res, block);
        if (converted.has_error()) {
            state.SkipWithError(converted.error().what.c_str());
            break;
        }
        std::pmr::vector<data_chunk_t> chunks(res);
        chunks.push_back(std::move(converted.value()));
        session_payload payload{struct_t, std::move(chunks), 0, NodeTag::T_SelectStmt};
        auto batches = flight::conv::chunks_to_ipc(payload, schema);
        for (const auto& batch : batches) {
            auto message = flight::ipc::serialize_record_batch(batch);
            benchmark::DoNotOptimize(message.bare_message);
            benchmark::DoNotOptimize(message.body);
        }
    }
    state.SetItemsProcessed(state.iterations() * 1000);
}
BENCHMARK(BM_chunk_to_ipc_full_1k);

static void BM_chunk_to_ipc_full_10k(benchmark::State& state) {
    auto* res = std::pmr::new_delete_resource();
    auto block = make_ch_block(10000);
    auto struct_t = tsl::ch_to_struct(res, block);
    auto schema = ipc_schema_or_skip(state, struct_t);
    if (!schema) {
        return;
    }
    for (auto _ : state) {
        auto converted = tsl::ch_to_chunk(res, block);
        if (converted.has_error()) {
            state.SkipWithError(converted.error().what.c_str());
            break;
        }
        std::pmr::vector<data_chunk_t> chunks(res);
        chunks.push_back(std::move(converted.value()));
        session_payload payload{struct_t, std::move(chunks), 0, NodeTag::T_SelectStmt};
        auto batches = flight::conv::chunks_to_ipc(payload, schema);
        for (const auto& batch : batches) {
            auto message = flight::ipc::serialize_record_batch(batch);
            benchmark::DoNotOptimize(message.bare_message);
            benchmark::DoNotOptimize(message.body);
        }
    }
    state.SetItemsProcessed(state.iterations() * 10000);
}
BENCHMARK(BM_chunk_to_ipc_full_10k);
