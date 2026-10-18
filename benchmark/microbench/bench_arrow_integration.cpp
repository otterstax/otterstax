// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The otterbrix Arrow integration of the file / S3 path, each benchmark next to the per-cell code
// it replaced (*_percell), on the same data: id int64, score double, flag bool, name utf8 (every
// 10th NULL). Output benchmarks use engine-shaped 1024-row chunks filled through set_value.

#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "otterbrix/translators/input/arrow_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "otterbrix/translators/output/chunk_to_csv.hpp"
#include "otterbrix/translators/output/chunk_to_parquet.hpp"

// otterbrix's parser headers #define DAY / SECOND, which collide with Arrow's
// TimeUnit/DateUnit enum values.
#undef DAY
#undef SECOND

#include <arrow/api.h>
#include <arrow/io/memory.h>

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <vector>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;
using components::vector::data_chunk_t;

namespace {

    // The engine's resource kind: thread-safe, frees go back to its pools.
    std::pmr::memory_resource* engine_like_resource() {
        static std::pmr::synchronized_pool_resource resource{std::pmr::new_delete_resource()};
        return &resource;
    }

    std::shared_ptr<arrow::RecordBatch> make_batch(int64_t rows) {
        arrow::Int64Builder id;
        arrow::DoubleBuilder score;
        arrow::BooleanBuilder flag;
        arrow::StringBuilder name;
        for (int64_t i = 0; i < rows; ++i) {
            (void) id.Append(i);
            (void) score.Append(static_cast<double>(i) * 0.5);
            (void) flag.Append(i % 2 == 0);
            (void) (i % 10 == 0 ? name.AppendNull() : name.Append("name_" + std::to_string(i)));
        }
        std::shared_ptr<arrow::Array> id_arr, score_arr, flag_arr, name_arr;
        (void) id.Finish(&id_arr);
        (void) score.Finish(&score_arr);
        (void) flag.Finish(&flag_arr);
        (void) name.Finish(&name_arr);
        return arrow::RecordBatch::Make(arrow::schema({arrow::field("id", arrow::int64()),
                                                       arrow::field("score", arrow::float64()),
                                                       arrow::field("flag", arrow::boolean()),
                                                       arrow::field("name", arrow::utf8())}),
                                        rows,
                                        {id_arr, score_arr, flag_arr, name_arr});
    }

    std::pmr::vector<complex_logical_type> chunk_types(std::pmr::memory_resource* res) {
        std::pmr::vector<complex_logical_type> types{res};
        types.emplace_back(logical_type::BIGINT, "id");
        types.emplace_back(logical_type::DOUBLE, "score");
        types.emplace_back(logical_type::BOOLEAN, "flag");
        types.emplace_back(logical_type::STRING_LITERAL, "name");
        return types;
    }

    // An engine-shaped chunk: owned flat vectors filled through set_value.
    data_chunk_t make_chunk(std::pmr::memory_resource* res, int64_t first, uint64_t rows) {
        data_chunk_t chunk(res, chunk_types(res), rows);
        chunk.set_cardinality(rows);
        for (uint64_t r = 0; r < rows; ++r) {
            const auto i = first + static_cast<int64_t>(r);
            chunk.set_value(0, r, logical_value_t{res, i});
            chunk.set_value(1, r, logical_value_t{res, static_cast<double>(i) * 0.5});
            chunk.set_value(2, r, logical_value_t{res, i % 2 == 0});
            chunk.set_value(3,
                            r,
                            i % 10 == 0 ? logical_value_t{res, nullptr}
                                        : logical_value_t{res, std::string{"name_"} + std::to_string(i)});
        }
        return chunk;
    }

    std::pmr::vector<data_chunk_t> make_chunks(std::pmr::memory_resource* res, int chunks) {
        std::pmr::vector<data_chunk_t> run(res);
        for (int k = 0; k < chunks; ++k) {
            run.push_back(make_chunk(res, static_cast<int64_t>(k) * 1024, 1024));
        }
        return run;
    }

    // ── the per-cell baselines: what the shims did before the otterbrix integration ──────────

    // The old arrow_to_chunk: a logical_value_t per cell (for a string: GetString's copy, the
    // value's heap copy, then the vector's string heap).
    data_chunk_t arrow_to_chunk_percell(std::pmr::memory_resource* res, const arrow::RecordBatch& batch) {
        const auto rows = static_cast<uint64_t>(batch.num_rows());
        data_chunk_t chunk(res, chunk_types(res), rows);
        chunk.set_cardinality(rows);
        const auto& id = static_cast<const arrow::Int64Array&>(*batch.column(0));
        const auto& score = static_cast<const arrow::DoubleArray&>(*batch.column(1));
        const auto& flag = static_cast<const arrow::BooleanArray&>(*batch.column(2));
        const auto& name = static_cast<const arrow::StringArray&>(*batch.column(3));
        for (uint64_t r = 0; r < rows; ++r) {
            chunk.set_value(0, r, logical_value_t{res, id.Value(static_cast<int64_t>(r))});
        }
        for (uint64_t r = 0; r < rows; ++r) {
            chunk.set_value(1, r, logical_value_t{res, score.Value(static_cast<int64_t>(r))});
        }
        for (uint64_t r = 0; r < rows; ++r) {
            chunk.set_value(2, r, logical_value_t{res, static_cast<bool>(flag.Value(static_cast<int64_t>(r)))});
        }
        for (uint64_t r = 0; r < rows; ++r) {
            const auto i = static_cast<int64_t>(r);
            chunk.set_value(3,
                            r,
                            name.IsNull(i) ? logical_value_t{res, nullptr}
                                           : logical_value_t{res, std::string(name.GetString(i))});
        }
        return chunk;
    }

    // The old chunk_to_record_batch: chunk.value() per cell into an Arrow builder.
    std::shared_ptr<arrow::RecordBatch> chunk_to_batch_percell(const data_chunk_t& chunk) {
        const auto rows = static_cast<int64_t>(chunk.size());
        arrow::Int64Builder id;
        arrow::DoubleBuilder score;
        arrow::BooleanBuilder flag;
        arrow::StringBuilder name;
        for (int64_t r = 0; r < rows; ++r) {
            const auto u = static_cast<uint64_t>(r);
            auto v0 = chunk.value(0, u);
            (void) (v0.is_null() ? id.AppendNull() : id.Append(v0.value<int64_t>()));
            auto v1 = chunk.value(1, u);
            (void) (v1.is_null() ? score.AppendNull() : score.Append(v1.value<double>()));
            auto v2 = chunk.value(2, u);
            (void) (v2.is_null() ? flag.AppendNull() : flag.Append(v2.value<bool>()));
            auto v3 = chunk.value(3, u);
            (void) (v3.is_null() ? name.AppendNull() : name.Append(v3.value<const std::string&>()));
        }
        std::shared_ptr<arrow::Array> a0, a1, a2, a3;
        (void) id.Finish(&a0);
        (void) score.Finish(&a1);
        (void) flag.Finish(&a2);
        (void) name.Finish(&a3);
        return arrow::RecordBatch::Make(arrow::schema({arrow::field("id", arrow::int64()),
                                                       arrow::field("score", arrow::float64()),
                                                       arrow::field("flag", arrow::boolean()),
                                                       arrow::field("name", arrow::utf8())}),
                                        rows,
                                        {a0, a1, a2, a3});
    }

    void set_rows(benchmark::State& state, int64_t rows_per_iteration) {
        state.SetItemsProcessed(state.iterations() * rows_per_iteration);
    }

} // namespace

// ── import: Arrow → data_chunk_t ─────────────────────────────────────────────────────────────

static void BM_arrow_to_chunk_native(benchmark::State& state) {
    auto* res = engine_like_resource();
    const auto batch = make_batch(state.range(0));
    for (auto _ : state) {
        auto loaded = tsl::arrow_to_chunk(res, batch);
        if (loaded.has_error()) {
            state.SkipWithError(loaded.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(loaded.value().size());
    }
    set_rows(state, state.range(0));
}
BENCHMARK(BM_arrow_to_chunk_native)->Arg(1000)->Arg(10000)->Arg(100000);

static void BM_arrow_to_chunk_percell(benchmark::State& state) {
    auto* res = engine_like_resource();
    const auto batch = make_batch(state.range(0));
    for (auto _ : state) {
        auto loaded = arrow_to_chunk_percell(res, *batch);
        benchmark::DoNotOptimize(loaded.size());
    }
    set_rows(state, state.range(0));
}
BENCHMARK(BM_arrow_to_chunk_percell)->Arg(1000)->Arg(10000)->Arg(100000);

// ── export: data_chunk_t → Arrow, one engine chunk (1024 rows) ──────────────────────────────

static void BM_chunk_to_record_batch(benchmark::State& state) {
    auto* res = engine_like_resource();
    const auto chunk = make_chunk(res, 0, 1024);
    for (auto _ : state) {
        auto batch = chunk_to_record_batch(res, chunk);
        if (batch.has_error()) {
            state.SkipWithError(batch.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(batch.value()->num_rows());
    }
    set_rows(state, 1024);
}
BENCHMARK(BM_chunk_to_record_batch);

static void BM_chunk_to_file_batch(benchmark::State& state) {
    auto* res = engine_like_resource();
    const auto chunk = make_chunk(res, 0, 1024);
    for (auto _ : state) {
        auto batch = chunk_to_file_batch(res, chunk);
        if (batch.has_error()) {
            state.SkipWithError(batch.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(batch.value()->num_rows());
    }
    set_rows(state, 1024);
}
BENCHMARK(BM_chunk_to_file_batch);

static void BM_chunk_to_batch_percell(benchmark::State& state) {
    auto* res = engine_like_resource();
    const auto chunk = make_chunk(res, 0, 1024);
    for (auto _ : state) {
        auto batch = chunk_to_batch_percell(chunk);
        benchmark::DoNotOptimize(batch->num_rows());
    }
    set_rows(state, 1024);
}
BENCHMARK(BM_chunk_to_batch_percell);

// ── memory: Arrow's buffers on the caller's resource vs Arrow's own pool ─────────────────────

static void BM_arrow_pool_on_resource(benchmark::State& state) {
    arrow::MemoryPool* pool = tsl::arrow_pool(engine_like_resource());
    const auto size = state.range(0);
    for (auto _ : state) {
        uint8_t* data = nullptr;
        (void) pool->Allocate(size, &data);
        benchmark::DoNotOptimize(data);
        pool->Free(data, size);
    }
}
BENCHMARK(BM_arrow_pool_on_resource)->Arg(4 << 10)->Arg(1 << 20);

static void BM_arrow_default_pool(benchmark::State& state) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    const auto size = state.range(0);
    for (auto _ : state) {
        uint8_t* data = nullptr;
        (void) pool->Allocate(size, &data);
        benchmark::DoNotOptimize(data);
        pool->Free(data, size);
    }
}
BENCHMARK(BM_arrow_default_pool)->Arg(4 << 10)->Arg(1 << 20);

// ── writers into memory: a cursor's run of 1024-row chunks, no filesystem ───────────────────

static void BM_chunk_to_parquet_stream(benchmark::State& state) {
    auto* res = engine_like_resource();
    const auto chunks = make_chunks(res, static_cast<int>(state.range(0)));
    for (auto _ : state) {
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto written = tsl::chunk_to_parquet(res, chunks, sink);
        if (written.has_error()) {
            state.SkipWithError(written.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(sink->Tell().ValueOrDie());
    }
    set_rows(state, state.range(0) * 1024);
}
BENCHMARK(BM_chunk_to_parquet_stream)->Arg(1)->Arg(10);

static void BM_chunk_to_csv_stream(benchmark::State& state) {
    auto* res = engine_like_resource();
    const auto chunks = make_chunks(res, static_cast<int>(state.range(0)));
    for (auto _ : state) {
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto written = tsl::chunk_to_csv(res, chunks, sink);
        if (written.has_error()) {
            state.SkipWithError(written.error().what.c_str());
            break;
        }
        benchmark::DoNotOptimize(sink->Tell().ValueOrDie());
    }
    set_rows(state, state.range(0) * 1024);
}
BENCHMARK(BM_chunk_to_csv_stream)->Arg(1)->Arg(10);
