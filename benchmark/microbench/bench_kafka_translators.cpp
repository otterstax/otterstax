// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Google Benchmark targets for the Kafka value-conversion hot paths (broker-free):
//   json_to_chunk  — Kafka JSON payloads -> data_chunk_t (the SOURCE-ingest inner loop)
//   chunk_to_json  — data_chunk_t -> JSON payloads (the STREAM / produce inner loop)
//
// The dataset mirrors the kafka_ingest e2e benchmark: five primitive columns
// (id/campaign_id/ts BIGINT, event_type STRING, amount DOUBLE) — exactly what
// json_to_chunk supports. These are the per-batch conversions the poller/stream
// threads run for every Kafka message; the end-to-end benchmarks time them behind
// a live broker, this isolates the pure CPU cost.

#include "integration/kafka/detail/kafka_reader.hpp"

#include <benchmark/benchmark.h>

#include <memory_resource>
#include <string>
#include <vector>

using otterstax::kafka::kafka_column_t;
using namespace components::types;

namespace {

    std::vector<kafka_column_t> make_columns() {
        std::vector<kafka_column_t> cols;
        cols.push_back({"id", complex_logical_type(logical_type::BIGINT)});
        cols.push_back({"campaign_id", complex_logical_type(logical_type::BIGINT)});
        cols.push_back({"event_type", complex_logical_type(logical_type::STRING_LITERAL)});
        cols.push_back({"amount", complex_logical_type(logical_type::DOUBLE)});
        cols.push_back({"ts", complex_logical_type(logical_type::BIGINT)});
        return cols;
    }

    std::vector<std::string> make_payloads(int rows) {
        static const char* const types[] = {"view", "click", "purchase"};
        std::vector<std::string> out;
        out.reserve(rows);
        for (int i = 0; i < rows; ++i) {
            out.push_back("{\"id\":" + std::to_string(i) + ",\"campaign_id\":" + std::to_string((i % 1000) + 1) +
                          ",\"event_type\":\"" + types[i % 3] + "\"" + ",\"amount\":" + std::to_string(i * 0.5) +
                          ",\"ts\":" + std::to_string(1700000000000LL + i) + "}");
        }
        return out;
    }

} // namespace

// ── json_to_chunk: parse a batch of JSON payloads into a data_chunk_t ──────────
static void BM_json_to_chunk(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    const auto columns = make_columns();
    const auto payloads = make_payloads(static_cast<int>(state.range(0)));
    for (auto _ : state) {
        auto chunk = otterstax::kafka::detail::json_to_chunk(res, columns, payloads);
        benchmark::DoNotOptimize(chunk);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_json_to_chunk)->Arg(1000)->Arg(10000)->Arg(100000);

static void BM_chunk_to_json(benchmark::State& state) {
    auto* res = std::pmr::get_default_resource();
    const auto columns = make_columns();
    const auto payloads = make_payloads(static_cast<int>(state.range(0)));

    // data_chunk_t is move-only; build it once outside the loop (chunk_to_json
    auto chunk = otterstax::kafka::detail::json_to_chunk(res, columns, payloads);
    for (auto _ : state) {
        auto out = otterstax::kafka::detail::chunk_to_json(chunk);
        benchmark::DoNotOptimize(out);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_chunk_to_json)->Arg(1000)->Arg(10000)->Arg(100000);
