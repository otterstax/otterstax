// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Google Benchmark targets for the file-format translators used by the file/S3
// ingestion path (connectors/file, integration/s3):
//
//   input  : csv_to_chunk / ndjson_to_chunk / parquet_to_chunk
//   output : chunk_to_csv  / chunk_to_ndjson / chunk_to_parquet
//
// Input benchmarks use the in-memory buffer overloads so only the parse +
// data_chunk_t construction is measured (no disk I/O). Output benchmarks write to
// a temp file — chunk_to_* only expose a file-path API — so their timings include
// the filesystem write. The source data_chunk_t is built once outside the loop
// (data_chunk_t is move-only, but chunk_to_* take it by const ref so it is reused).

#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/ndjson_to_chunk.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_csv.hpp"
#include "otterbrix/translators/output/chunk_to_ndjson.hpp"
#include "otterbrix/translators/output/chunk_to_parquet.hpp"

// otterbrix's parser headers #define DAY / SECOND, which collide with Arrow's
// TimeUnit/DateUnit enum values used below.
#undef DAY
#undef SECOND

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/writer.h>

#include <benchmark/benchmark.h>

#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <string>

using components::vector::data_chunk_t;

namespace {

// "id,name\n0,row_0\n1,row_1\n..." with `n` data rows.
std::string make_csv(int n) {
    std::string s = "id,name\n";
    for (int i = 0; i < n; ++i)
        s += std::to_string(i) + ",row_" + std::to_string(i) + "\n";
    return s;
}

// NDJSON: one {"id":i,"name":"row_i"} object per line.
std::string make_ndjson(int n) {
    std::string s;
    for (int i = 0; i < n; ++i)
        s += "{\"id\":" + std::to_string(i) + ",\"name\":\"row_" + std::to_string(i) + "\"}\n";
    return s;
}

// An in-memory parquet buffer with `n` rows (int32 id, utf8 name).
std::shared_ptr<arrow::Buffer> make_parquet_buffer(int n) {
    arrow::Int32Builder  id_b;
    arrow::StringBuilder name_b;
    for (int i = 0; i < n; ++i) {
        id_b.Append(i).ok();
        name_b.Append("row_" + std::to_string(i)).ok();
    }
    std::shared_ptr<arrow::Array> id_arr, name_arr;
    id_b.Finish(&id_arr).ok();
    name_b.Finish(&name_arr).ok();

    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8())});
    auto table  = arrow::Table::Make(schema, {id_arr, name_arr});

    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 4096).ok();
    return sink->Finish().ValueOrDie();
}

data_chunk_t make_chunk(std::pmr::memory_resource* res, int n) {
    const std::string csv = make_csv(n);
    return tsl::csv_to_chunk(res, reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
}

} // namespace

// ── input: csv_to_chunk ───────────────────────────────────────────────────────

static void BM_csv_to_chunk(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    const std::string csv = make_csv(static_cast<int>(state.range(0)));
    const auto* data = reinterpret_cast<const uint8_t*>(csv.data());
    for (auto _ : state) {
        auto chunk = tsl::csv_to_chunk(res, data, csv.size());
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_csv_to_chunk)->Arg(1000)->Arg(10000);

// ── input: ndjson_to_chunk ────────────────────────────────────────────────────

static void BM_ndjson_to_chunk(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    const std::string json = make_ndjson(static_cast<int>(state.range(0)));
    const auto* data = reinterpret_cast<const uint8_t*>(json.data());
    for (auto _ : state) {
        auto chunk = tsl::ndjson_to_chunk(res, data, json.size());
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_ndjson_to_chunk)->Arg(1000)->Arg(10000);

// ── input: parquet_to_chunk ───────────────────────────────────────────────────

static void BM_parquet_to_chunk(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto buf = make_parquet_buffer(static_cast<int>(state.range(0)));
    for (auto _ : state) {
        auto chunk = tsl::parquet_to_chunk(res, buf->data(), static_cast<size_t>(buf->size()));
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_parquet_to_chunk)->Arg(1000)->Arg(10000);

// ── output: chunk_to_csv (includes file write) ────────────────────────────────

static void BM_chunk_to_csv(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto chunk = make_chunk(res, static_cast<int>(state.range(0)));
    const std::string path = "/tmp/otterstax_bench_chunk_to_csv.csv";
    for (auto _ : state) {
        tsl::chunk_to_csv(chunk, path);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
    std::filesystem::remove(path);
}
BENCHMARK(BM_chunk_to_csv)->Arg(1000)->Arg(10000);

// ── output: chunk_to_ndjson (includes file write) ─────────────────────────────

static void BM_chunk_to_ndjson(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto chunk = make_chunk(res, static_cast<int>(state.range(0)));
    const std::string path = "/tmp/otterstax_bench_chunk_to_ndjson.ndjson";
    for (auto _ : state) {
        tsl::chunk_to_ndjson(chunk, path);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
    std::filesystem::remove(path);
}
BENCHMARK(BM_chunk_to_ndjson)->Arg(1000)->Arg(10000);

// ── output: chunk_to_parquet (includes file write) ────────────────────────────

static void BM_chunk_to_parquet(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    auto chunk = make_chunk(res, static_cast<int>(state.range(0)));
    const std::string path = "/tmp/otterstax_bench_chunk_to_parquet.parquet";
    for (auto _ : state) {
        tsl::chunk_to_parquet(chunk, path);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
    std::filesystem::remove(path);
}
BENCHMARK(BM_chunk_to_parquet)->Arg(1000)->Arg(10000);
