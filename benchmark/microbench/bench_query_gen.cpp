// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Google Benchmark targets for SQL query generation:
//   sql_gen::replace_qualifiers() — rewrites 4-part qualifiers in raw SQL stubs
//   sql_gen::table_reference()    — formats a db.collection / schema.collection string
//
// Note on sql_gen::generate_query():
//   generate_query() operates on logical-plan node_ptr objects produced by the
//   Otterbrix optimizer's aggregation/join rewriter.  In production those nodes
//   come from ParsedQueryData::otterbrix_params::external_nodes when the node
//   type is NOT schema_node_t (i.e. when has_raw_sql() is false).  Constructing
//   a synthetic node_ptr requires the Otterbrix logical-plan builder API, which
//   is not exposed at unit level.  The generate_query path is therefore exercised
//   by the system tests.  What IS measurable here is the prepare_sql + replace_qualifiers
//   pipeline, which is the hot path for all single-backend and most cross-backend
//   queries in production.

#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"

#include <benchmark/benchmark.h>

#include <cassert>
#include <memory_resource>
#include <string>

namespace {

otterstax::parser::extraction_result_t prep(const char* sql) {
    return otterstax::parser::prepare_sql(sql, std::pmr::get_default_resource());
}

// All SQL fixtures use the subquery form required by prepare_sql.  Stubs are
// only produced for SQL-level subqueries wrapped in parentheses; flat JOINs with
// 4-part table names produce 0 stubs and are handled differently by the scheduler.
// Ordering of subqueries in the SQL determines stub order (left-to-right by
// paren_open position).

// Single MySQL backend — produces 1 stub: stubs[0]=MySQL.
const char* kMysqlOnlySql =
    "SELECT * FROM ("
    "SELECT id, name, status FROM mysql.bill.schema.orders WHERE status = 'active'"
    ") o";

// MySQL × PostgreSQL cross-backend — produces 2 stubs:
//   stubs[0]=MySQL (first paren), stubs[1]=PostgreSQL (second paren).
const char* kCrossBackendSql =
    "SELECT o.product_id, p.category "
    "FROM (SELECT product_id FROM mysql.bill.schema.orders WHERE status = 'active') o "
    "INNER JOIN (SELECT id, category FROM pg.shop.shop.products) p ON p.id = o.product_id";

// Single ClickHouse backend — produces 1 stub: stubs[0]=ClickHouse.
const char* kChSql =
    "SELECT * FROM ("
    "SELECT session_id, ts FROM ch.ev.schema.sessions WHERE country = 'DE'"
    ") s";

// Three-backend: MySQL × PostgreSQL × ClickHouse — produces 3 stubs:
//   stubs[0]=MySQL, stubs[1]=PostgreSQL, stubs[2]=ClickHouse.
// replace_qualifiers must run once per backend — the worst-case
// qualifier-rewriting path for a single client query.
const char* kThreeBackendSql =
    "SELECT o.product_id, p.category, e.session_id "
    "FROM (SELECT product_id FROM mysql.bill.schema.orders WHERE status = 'active') o "
    "INNER JOIN (SELECT id, category FROM pg.shop.shop.products) p ON p.id = o.product_id "
    "INNER JOIN (SELECT session_id, campaign_id FROM ch.ev.schema.events WHERE country = 'DE') e "
    "    ON o.product_id = e.campaign_id";

} // namespace

// ── replace_qualifiers ────────────────────────────────────────────────────────

static void BM_replace_qualifiers_mysql(benchmark::State& state) {
    auto r = prep(kMysqlOnlySql);
    assert(!r.stubs.empty());
    const auto& stub = r.stubs[0];
    for (auto _ : state) {
        auto out = sql_gen::replace_qualifiers(stub.raw_sql, stub.qualifiers, backend_type_t::MySQL);
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_replace_qualifiers_mysql);

static void BM_replace_qualifiers_pg(benchmark::State& state) {
    auto r = prep(kCrossBackendSql);
    // The PG stub is the second stub in the cross-backend query.
    assert(r.stubs.size() >= 2);
    const auto& stub = r.stubs.back();
    for (auto _ : state) {
        auto out = sql_gen::replace_qualifiers(stub.raw_sql, stub.qualifiers, backend_type_t::PostgreSQL);
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_replace_qualifiers_pg);

static void BM_replace_qualifiers_ch(benchmark::State& state) {
    auto r = prep(kChSql);
    assert(!r.stubs.empty());
    const auto& stub = r.stubs[0];
    for (auto _ : state) {
        auto out = sql_gen::replace_qualifiers(stub.raw_sql, stub.qualifiers, backend_type_t::ClickHouse);
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_replace_qualifiers_ch);

// No-op: zero qualifier rewrites in the list.
static void BM_replace_qualifiers_empty(benchmark::State& state) {
    std::vector<otterstax::parser::qualifier_rewrite_t> no_quals;
    const std::string sql = "SELECT id, name FROM orders WHERE status = 'active'";
    for (auto _ : state) {
        auto out = sql_gen::replace_qualifiers(sql, no_quals, backend_type_t::MySQL);
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_replace_qualifiers_empty);

// ── Full parse + qualify pipeline ────────────────────────────────────────────
// Measures prepare_sql (stub extraction) + replace_qualifiers in one shot,
// mirroring the exact sequence executed for a single-source query at runtime.

static void BM_pipeline_mysql_single_source(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    for (auto _ : state) {
        auto r = otterstax::parser::prepare_sql(kMysqlOnlySql, &pool);
        assert(!r.stubs.empty());
        auto out = sql_gen::replace_qualifiers(r.stubs[0].raw_sql,
                                               r.stubs[0].qualifiers,
                                               backend_type_t::MySQL);
        benchmark::DoNotOptimize(out);
    }
}
BENCHMARK(BM_pipeline_mysql_single_source);

static void BM_pipeline_cross_backend(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    for (auto _ : state) {
        auto r = otterstax::parser::prepare_sql(kCrossBackendSql, &pool);
        assert(r.stubs.size() >= 2);
        auto out0 = sql_gen::replace_qualifiers(r.stubs[0].raw_sql,
                                                r.stubs[0].qualifiers,
                                                backend_type_t::MySQL);
        auto out1 = sql_gen::replace_qualifiers(r.stubs[1].raw_sql,
                                                r.stubs[1].qualifiers,
                                                backend_type_t::PostgreSQL);
        benchmark::DoNotOptimize(out0);
        benchmark::DoNotOptimize(out1);
    }
}
BENCHMARK(BM_pipeline_cross_backend);

// Three-backend: MySQL × PostgreSQL × ClickHouse — prepare_sql + 3 × replace_qualifiers.
static void BM_pipeline_three_backend(benchmark::State& state) {
    std::pmr::unsynchronized_pool_resource pool;
    for (auto _ : state) {
        auto r = otterstax::parser::prepare_sql(kThreeBackendSql, &pool);
        assert(r.stubs.size() >= 3);
        auto out0 = sql_gen::replace_qualifiers(r.stubs[0].raw_sql,
                                                r.stubs[0].qualifiers,
                                                backend_type_t::MySQL);
        auto out1 = sql_gen::replace_qualifiers(r.stubs[1].raw_sql,
                                                r.stubs[1].qualifiers,
                                                backend_type_t::PostgreSQL);
        auto out2 = sql_gen::replace_qualifiers(r.stubs[2].raw_sql,
                                                r.stubs[2].qualifiers,
                                                backend_type_t::ClickHouse);
        benchmark::DoNotOptimize(out0);
        benchmark::DoNotOptimize(out1);
        benchmark::DoNotOptimize(out2);
    }
}
BENCHMARK(BM_pipeline_three_backend);

// ── table_reference ───────────────────────────────────────────────────────────

static void BM_table_reference_mysql(benchmark::State& state) {
    collection_full_name_t name{"bill", "orders"};
    for (auto _ : state) {
        auto ref = sql_gen::table_reference(name, backend_type_t::MySQL);
        benchmark::DoNotOptimize(ref);
    }
}
BENCHMARK(BM_table_reference_mysql);

static void BM_table_reference_pg(benchmark::State& state) {
    collection_full_name_t name{"shop", "products"};
    for (auto _ : state) {
        auto ref = sql_gen::table_reference(name, backend_type_t::PostgreSQL);
        benchmark::DoNotOptimize(ref);
    }
}
BENCHMARK(BM_table_reference_pg);

static void BM_table_reference_ch(benchmark::State& state) {
    collection_full_name_t name{"events", "sessions"};
    for (auto _ : state) {
        auto ref = sql_gen::table_reference(name, backend_type_t::ClickHouse);
        benchmark::DoNotOptimize(ref);
    }
}
BENCHMARK(BM_table_reference_ch);
