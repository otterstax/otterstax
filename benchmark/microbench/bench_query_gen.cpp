// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Google Benchmark targets for SQL query generation:
//   sql_gen::generate_query() — the statement the federation's pushdown rule
//                               sends to a backend for one query block
//
// The query blocks come from GreenplumParser::parse(): the aggregate that reads
// the remote table (its name carries the uid slot) with its clauses — what the
// pushdown rule hands the generator. The rule generates over the engine's
// validated tree; the parse tree here has the same shape before validation,
// which is what the generator's own cost depends on. GreenplumParser needs the
// named loggers: bench_parser.cpp registers them before main in this binary.

#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"

#include <benchmark/benchmark.h>

#include <cassert>
#include <memory_resource>
#include <string>

namespace {

    // The first aggregate under (or at) `node` that reads a table of the connection `uid`.
    components::logical_plan::node_ptr block_of(const components::logical_plan::node_ptr& node, const std::string& uid) {
        if (!node) {
            return nullptr;
        }
        if (node->type() == components::logical_plan::node_type::aggregate_t &&
            node->target().unique_identifier.t == uid) {
            return node;
        }
        for (const auto& child : node->children()) {
            if (auto found = block_of(child, uid)) {
                return found;
            }
        }
        return nullptr;
    }

    struct block_t {
        ParsedQueryDataPtr parsed;
        components::logical_plan::node_ptr node;
    };

    block_t parse_block(GreenplumParser& parser, const char* sql, const std::string& uid) {
        auto parsed = parser.parse(sql);
        assert(!parsed.has_error());
        block_t block{std::move(parsed.value()), nullptr};
        block.node = block_of(block.parsed->otterbrix_params->node, uid);
        assert(block.node);
        return block;
    }

    void bench_block(benchmark::State& state, const char* sql, const std::string& uid, backend_type_t backend) {
        std::pmr::unsynchronized_pool_resource pool;
        GreenplumParser parser(&pool);
        auto block = parse_block(parser, sql, uid);
        const auto& params = block.parsed->otterbrix_params->params_node->parameters();
        const sql_gen::statement_target_t target{block.node->target()};
        for (auto _ : state) {
            auto out = sql_gen::generate_query(block.node, &params, backend, target, std::pmr::new_delete_resource());
            benchmark::DoNotOptimize(out);
        }
    }

    const char* kMysqlFilter = "SELECT id, name, status FROM mysql.bill.schema.orders WHERE status = 'active' "
                               "ORDER BY id LIMIT 100";
    const char* kPgAggregate = "SELECT category, count(*) AS n, sum(price) AS total FROM pg.shop.shop.products "
                               "WHERE price > 10 GROUP BY category HAVING count(*) > 1";
    const char* kChFilter = "SELECT session_id, ts FROM ch.ev.schema.sessions WHERE country = 'DE' AND ts > 100";

} // namespace

// ── generate_query ────────────────────────────────────────────────────────────

static void BM_generate_query_mysql_filter(benchmark::State& state) {
    bench_block(state, kMysqlFilter, "mysql", backend_type_t::MySQL);
}
BENCHMARK(BM_generate_query_mysql_filter);

static void BM_generate_query_pg_aggregate(benchmark::State& state) {
    bench_block(state, kPgAggregate, "pg", backend_type_t::PostgreSQL);
}
BENCHMARK(BM_generate_query_pg_aggregate);

static void BM_generate_query_ch_filter(benchmark::State& state) {
    bench_block(state, kChFilter, "ch", backend_type_t::ClickHouse);
}
BENCHMARK(BM_generate_query_ch_filter);
