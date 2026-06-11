// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Google Benchmark targets for SQL parsing:
//   GreenplumParser::parse()  — full parse + logical-plan construction
//   otterstax::parser::prepare_sql() — subquery extraction / qualifier promotion

#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"
#include "utility/logger.hpp"

#include <benchmark/benchmark.h>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <memory_resource>

// GreenplumParser requires named spdlog loggers to be registered before use.
// Silence them at OFF level so benchmark runs are quiet.
// spdlog::shutdown() is registered via atexit to avoid a race between the
// spdlog background-flush thread and static destructors at process exit.
namespace {
struct LoggerInit {
    LoggerInit() {
        initialize_all_loggers("/tmp/bench_logs");
        spdlog::set_level(spdlog::level::off);
        std::atexit([] { spdlog::shutdown(); });
    }
};
static const LoggerInit logger_init;
} // namespace

// GreenplumParser is stateless between parse() calls, so one instance is
// constructed once and reused across all iterations of a benchmark.

namespace {

const char* kSimpleSelect = "SELECT id, name, status FROM orders WHERE status = 'active'";

const char* kJoin3Table =
    "SELECT o.id, p.name, c.email "
    "FROM mysql.bill.schema.orders o "
    "INNER JOIN mysql.bill.schema.products p ON o.product_id = p.id "
    "INNER JOIN mysql.bill.schema.customers c ON o.customer_id = c.id";

const char* kCrossBackend =
    "SELECT o.id, p.name "
    "FROM mysql.bill.schema.orders o "
    "INNER JOIN pg.shop.shop.products p ON o.product_id = p.id";

const char* kSubquery =
    "SELECT category, total "
    "FROM (SELECT category, SUM(amount) AS total "
    "      FROM mysql.bill.schema.transactions "
    "      GROUP BY category) sub "
    "WHERE total > 1000";

// Mirrors the complex_select end-to-end benchmark: GROUP BY + HAVING + ORDER BY + LIMIT.
const char* kComplexSelect =
    "SELECT campaign_id, COUNT(*) AS impressions, SUM(cost) AS total_cost "
    "FROM mysql.bill.schema.impressions "
    "WHERE status = 'active' "
    "GROUP BY campaign_id "
    "HAVING COUNT(*) > 100 "
    "ORDER BY total_cost DESC "
    "LIMIT 1000";

// Three-backend cross-engine join: MySQL × PostgreSQL × ClickHouse.
// Produces three stubs in prepare_sql — the worst-case qualifier-rewriting path.
const char* kThreeBackendJoin =
    "SELECT o.id, p.name, e.session_id "
    "FROM mysql.bill.schema.orders o "
    "INNER JOIN pg.shop.shop.products p ON o.product_id = p.id "
    "INNER JOIN ch.ev.schema.events e ON o.campaign_id = e.campaign_id "
    "WHERE o.status = 'active' "
    "LIMIT 500";

} // namespace

// ── GreenplumParser::parse() ─────────────────────────────────────────────────

static void BM_parse_simple_select(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    GreenplumParser parser(&pool);
    for (auto _ : state) {
        auto r = parser.parse(kSimpleSelect);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_parse_simple_select);

static void BM_parse_join_3table(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    GreenplumParser parser(&pool);
    for (auto _ : state) {
        auto r = parser.parse(kJoin3Table);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_parse_join_3table);

static void BM_parse_cross_backend(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    GreenplumParser parser(&pool);
    for (auto _ : state) {
        auto r = parser.parse(kCrossBackend);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_parse_cross_backend);

static void BM_parse_subquery(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    GreenplumParser parser(&pool);
    for (auto _ : state) {
        auto r = parser.parse(kSubquery);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_parse_subquery);

static void BM_parse_complex_select(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    GreenplumParser parser(&pool);
    for (auto _ : state) {
        auto r = parser.parse(kComplexSelect);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_parse_complex_select);

static void BM_parse_three_backend_join(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    GreenplumParser parser(&pool);
    for (auto _ : state) {
        auto r = parser.parse(kThreeBackendJoin);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_parse_three_backend_join);

// ── otterstax::parser::prepare_sql() ─────────────────────────────────────────

static void BM_prepare_sql_simple(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    for (auto _ : state) {
        auto r = otterstax::parser::prepare_sql(kSimpleSelect, &pool);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_prepare_sql_simple);

// prepare_sql is most interesting when the SQL contains 4-part qualifiers that
// need to be extracted into stubs: that triggers the subquery-rewriting path.
static void BM_prepare_sql_cross_backend(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    for (auto _ : state) {
        auto r = otterstax::parser::prepare_sql(kCrossBackend, &pool);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_prepare_sql_cross_backend);

static void BM_prepare_sql_subquery(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    for (auto _ : state) {
        auto r = otterstax::parser::prepare_sql(kSubquery, &pool);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_prepare_sql_subquery);

static void BM_prepare_sql_complex_select(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    for (auto _ : state) {
        auto r = otterstax::parser::prepare_sql(kComplexSelect, &pool);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_prepare_sql_complex_select);

// Three-backend: produces three stubs, exercising the multi-backend split path.
static void BM_prepare_sql_three_backend(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    for (auto _ : state) {
        auto r = otterstax::parser::prepare_sql(kThreeBackendJoin, &pool);
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(BM_prepare_sql_three_backend);
