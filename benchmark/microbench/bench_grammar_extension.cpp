// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Google Benchmark targets for the SQL parser grammar extensions
// (otterbrix/parser/grammar_extention/{s3,file}). Two layers are measured:
//   * the isolated extension parse stage  — s3_ext::parse / file_ext::parse
//   * the registry-routed raw_parser path — core parser rejects, then the
//     extension claims (the cost a CREATE EXTERNAL TABLE / COPY actually pays).
//
// A fresh std::pmr::monotonic_buffer_resource is created inside each iteration:
// this mirrors GreenplumParser, which arena-scopes every parse() call, and
// bounds memory because a monotonic resource only frees on destruction.

#include "s3_ast.hpp"
#include "s3_extension.hpp"
#include "file_ast.hpp"
#include "file_extension.hpp"

#include <components/sql/parser/extension.hpp>
#include <components/sql/parser/parser.h>

#include <benchmark/benchmark.h>

#include <memory_resource>

namespace {

// CREATE EXTERNAL TABLE — the WITH (...) clause is mandatory (the core grammar
// rejects it, which is why the statement reaches the extension at all).
const char* kS3Create =
    "CREATE EXTERNAL TABLE s3.trades WITH ("
    "  s3_alias = 'my_s3_alias',"
    "  location = 's3://bucket/data.parquet',"
    "  format   = 'parquet' )";

// COPY (...) TO — exercises the C++ driver's balanced-paren scan that strips the
// embedded SELECT before handing a flattened statement to the grammar.
const char* kS3Copy =
    "COPY (SELECT * FROM s3.trades WHERE status = 'active') "
    "TO 's3://bucket/trades_out.parquet' "
    "WITH ( s3_alias = 'my_s3_alias', format = 'parquet' )";

// Same statements with a local path: claimed by the `file` extension, not `s3`.
const char* kFileCreate =
    "CREATE EXTERNAL TABLE file.trades WITH ("
    "  location = '/data/trades.parquet',"
    "  format   = 'parquet' )";

const char* kFileCopy =
    "COPY (SELECT * FROM file.trades WHERE status = 'active') "
    "TO '/data/trades_out.parquet' "
    "WITH ( format = 'parquet' )";

// Plain core SQL: the core parser claims it, so no extension is consulted.
// Baseline for the registry-dispatch overhead on a normal query.
const char* kCoreSelect = "SELECT id, name FROM orders WHERE status = 'active'";

components::sql::parser::parser_extension_registry_t make_registry() {
    components::sql::parser::parser_extension_registry_t registry;
    [[maybe_unused]] auto s3   = registry.add(make_s3_extension());
    [[maybe_unused]] auto file = registry.add(make_file_extension());
    return registry;
}

} // namespace

// ── isolated extension parse stage ───────────────────────────────────────────

static void BM_s3_ext_parse_create(benchmark::State& state) {
    for (auto _ : state) {
        std::pmr::monotonic_buffer_resource arena;
        auto r = s3_ext::parse(&arena, kS3Create);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_s3_ext_parse_create);

static void BM_s3_ext_parse_copy(benchmark::State& state) {
    for (auto _ : state) {
        std::pmr::monotonic_buffer_resource arena;
        auto r = s3_ext::parse(&arena, kS3Copy);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_s3_ext_parse_copy);

static void BM_file_ext_parse_create(benchmark::State& state) {
    for (auto _ : state) {
        std::pmr::monotonic_buffer_resource arena;
        auto r = file_ext::parse(&arena, kFileCreate);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_file_ext_parse_create);

static void BM_file_ext_parse_copy(benchmark::State& state) {
    for (auto _ : state) {
        std::pmr::monotonic_buffer_resource arena;
        auto r = file_ext::parse(&arena, kFileCopy);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_file_ext_parse_copy);

// ── registry-routed raw_parser (core reject → extension claim) ────────────────
// The registry is built once outside the loop (it is reused per parser
// instance in production); only raw_parser is timed.

static void BM_registry_parse_s3_create(benchmark::State& state) {
    auto registry = make_registry();
    for (auto _ : state) {
        std::pmr::monotonic_buffer_resource arena;
        auto* tree = raw_parser(&arena, kS3Create, registry);
        benchmark::DoNotOptimize(tree);
    }
}
BENCHMARK(BM_registry_parse_s3_create);

static void BM_registry_parse_file_create(benchmark::State& state) {
    auto registry = make_registry();
    for (auto _ : state) {
        std::pmr::monotonic_buffer_resource arena;
        auto* tree = raw_parser(&arena, kFileCreate, registry);
        benchmark::DoNotOptimize(tree);
    }
}
BENCHMARK(BM_registry_parse_file_create);

// Baseline: core SQL is claimed by the core parser; the extensions are never
// consulted, so this isolates the registry-path overhead on a normal query.
static void BM_registry_parse_core_select(benchmark::State& state) {
    auto registry = make_registry();
    for (auto _ : state) {
        std::pmr::monotonic_buffer_resource arena;
        auto* tree = raw_parser(&arena, kCoreSelect, registry);
        benchmark::DoNotOptimize(tree);
    }
}
BENCHMARK(BM_registry_parse_core_select);
