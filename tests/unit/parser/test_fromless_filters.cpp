// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// give_fromless_filters_a_row (otterbrix/parser/parser.hpp): a query block without FROM that filters gets a one-row
// source, at every depth, and nothing else changes (otterbrix rc-7 aborts opening the source-less filter).

#include "otterbrix/parser/parser.hpp"
#include "types/otterbrix.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/node_data.hpp>

namespace {
    namespace lp = components::logical_plan;

    // The query blocks of the plan (every sub-query) that are source-less filters: none may be left.
    std::size_t sourceless_filters(const lp::node_ptr& node) {
        if (!node) {
            return 0;
        }
        std::size_t found = 0;
        for (const auto& child : node->children()) {
            found += sourceless_filters(child);
        }
        if (node->type() == lp::node_type::aggregate_t && node->target().empty()) {
            bool match = false;
            bool source = false;
            for (const auto& child : node->children()) {
                const auto t = child->type();
                match = match || t == lp::node_type::match_t;
                source = source || !(t == lp::node_type::match_t || t == lp::node_type::group_t ||
                                     t == lp::node_type::select_t || t == lp::node_type::sort_t ||
                                     t == lp::node_type::limit_t || t == lp::node_type::having_t);
            }
            found += match && !source ? 1 : 0;
        }
        return found;
    }

    std::size_t sourceless_filters(const lp::execution_plan_t& plan) {
        std::size_t found = 0;
        for (const auto& query : plan.sub_queries) {
            found += sourceless_filters(query);
        }
        return found;
    }
} // namespace

TEST_CASE("parser: a query block without FROM that filters reads one row") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    for (const char* sql : {"SELECT 1 WHERE true",
                            "SELECT count(*) WHERE 1 = 1",
                            "SELECT * FROM t WHERE EXISTS (SELECT 1 WHERE true)",
                            "SELECT (SELECT 1 WHERE true)",
                            "SELECT 1 UNION ALL SELECT 2 WHERE true",
                            "WITH a AS (SELECT 1 AS x WHERE true) SELECT * FROM a",
                            "SELECT * FROM (SELECT 1 AS x WHERE true) s",
                            "INSERT INTO t (id) SELECT 1 WHERE true"}) {
        INFO(sql);
        auto parsed = parser.parse(sql);
        REQUIRE_FALSE(parsed.has_error());
        CHECK(sourceless_filters(*parsed.value()->plan) == 0);
    }
    // The block's first child is the one-row source.
    auto parsed = parser.parse("SELECT 1 WHERE true");
    REQUIRE_FALSE(parsed.has_error());
    const auto& root = plan_root(*parsed.value()->plan);
    REQUIRE_FALSE(root->children().empty());
    REQUIRE(root->children().front()->type() == lp::node_type::data_t);
    CHECK(static_cast<const lp::node_data_t&>(*root->children().front()).size() == 1);
}

TEST_CASE("parser: a block with a source or without a filter is left as it is") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    for (const char* sql : {"SELECT 1", "SELECT 1 ORDER BY 1", "SELECT x FROM (VALUES (1)) v(x) WHERE x = 1",
                            "SELECT id FROM t WHERE id = 1"}) {
        INFO(sql);
        auto parsed = parser.parse(sql);
        REQUIRE_FALSE(parsed.has_error());
        const auto& root = plan_root(*parsed.value()->plan);
        for (const auto& child : root->children()) {
            if (child->type() == lp::node_type::data_t) {
                // VALUES' own data, never a second source
                CHECK(std::string{sql}.find("VALUES") != std::string::npos);
            }
        }
    }
}
