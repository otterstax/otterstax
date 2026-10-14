// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// An engine-only reproduction kept from the old mixed-plan path: a GROUP BY on a
// table-qualified STRING key over raw data chunks. The federated shapes it was
// found through (a cross-server JOIN with GROUP BY, a remote table JOINed with a
// local composite table — engine defect D1) are covered over the federation in
// tests/federation/test_federation_joins.cpp.

#include <catch2/catch_all.hpp>

#include "session_stack.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/vector/data_chunk.hpp>

#include <deque>
#include <iostream>
#include <memory_resource>

// Pure-engine reproduction: NO otterstax components involved. The plan is
// produced by the ENGINE's own raw_parser + transformer, table aggregates are
// swapped for node_data via public logical_plan API, and the plan is executed
// on the engine's dispatcher (test_engine_t::plan). A crash here is an
// otterbrix defect by construction.
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

TEST_CASE("pure engine: group by string key over node_data", "[engine-group-by-string]") {
    // Scoped arena, declared first so it outlives every object below
    // (engine included). Synchronized: the engine dispatcher may allocate
    // from it off the test thread.
    std::pmr::synchronized_pool_resource case_arena;
    auto* resource = &case_arena;
    // The engine is opened before the plan exists and so outlives it: running a
    // plan stamps engine-owned functions into its expressions, which a plan
    // destroyed after the engine would free into a resource already gone.
    otterstax::test::test_engine_t inst("/tmp/otterstax_pure_engine_probe");
    inst.remove_on_exit();

    const char* sql = R"(
        SELECT c.campaign_name, COUNT(p.product_id) as product_count, AVG(p.price) as avg_product_price
        FROM db1.campaigns c
        INNER JOIN pgdb.products p ON p.campaign_id = c.campaign_id
        GROUP BY c.campaign_name ORDER BY product_count DESC;)";

    // The raw AST lives in an arena, exactly like GreenplumParser does in
    // production — raw_parser allocations are never freed individually.
    std::pmr::monotonic_buffer_resource arena(resource);
    auto* raw = raw_parser(&arena, sql);
    REQUIRE(raw != nullptr);
    auto* res = reinterpret_cast<::Node*>(linitial(raw));
    components::sql::transform::transformer transformer(resource);
    auto binder = transformer.transform(components::sql::transform::pg_cell_to_node_cast(res));
    REQUIRE_FALSE(binder.has_error());
    auto finalized = binder.finalize();
    REQUIRE_FALSE(finalized.has_error());
    auto root = finalized.value().sub_queries.back();
    auto root_params = finalized.value().parameters;
    REQUIRE(root);

    auto make_chunk = [&](std::initializer_list<std::pair<const char*, components::types::logical_type>> names) {
        std::pmr::vector<components::types::complex_logical_type> cols(resource);
        for (auto& [n, t] : names) {
            cols.emplace_back(t);
            cols.back().set_alias(n);
        }
        components::vector::data_chunk_t chunk(resource, cols, 2);
        for (size_t c = 0; c < cols.size(); ++c) {
            for (size_t r = 0; r < 2; ++r) {
                switch (cols[c].type()) {
                    case components::types::logical_type::INTEGER:
                        chunk.set_value(c,
                                        r,
                                        components::types::logical_value_t(resource, static_cast<int32_t>(r + 1)));
                        break;
                    case components::types::logical_type::DOUBLE:
                        chunk.set_value(c, r, components::types::logical_value_t(resource, 100.5 * (r + 1)));
                        break;
                    default:
                        chunk.set_value(
                            c,
                            r,
                            components::types::logical_value_t(resource, std::string("n_") + std::to_string(r)));
                        break;
                }
            }
        }
        chunk.set_cardinality(2);
        return chunk;
    };
    using lt = components::types::logical_type;

    // Swap the two table aggregates (relname campaigns/products) for raw data.
    std::deque<components::logical_plan::node_ptr> walk{root};
    size_t swapped = 0;
    while (!walk.empty()) {
        auto n = walk.front();
        walk.pop_front();
        for (auto& child : n->children()) {
            if (child && child->type() == components::logical_plan::node_type::aggregate_t) {
                const auto& rel = static_cast<const components::logical_plan::node_aggregate_t&>(*child).target().collection.t;
                if (rel == "campaigns") {
                    child =
                        components::logical_plan::make_node_raw_data(resource,
                                                                     make_chunk({{"campaign_id", lt::INTEGER},
                                                                                 {"campaign_name", lt::STRING_LITERAL},
                                                                                 {"budget", lt::DOUBLE}}));
                    ++swapped;
                    continue;
                }
                if (rel == "products") {
                    child =
                        components::logical_plan::make_node_raw_data(resource,
                                                                     make_chunk({{"product_id", lt::INTEGER},
                                                                                 {"campaign_id", lt::INTEGER},
                                                                                 {"product_name", lt::STRING_LITERAL},
                                                                                 {"price", lt::DOUBLE}}));
                    ++swapped;
                    continue;
                }
            }
            if (child) {
                walk.push_back(child);
            }
        }
    }
    REQUIRE(swapped == 2);

    auto cursor = inst.plan(components::logical_plan::execution_plan_t{resource, root, root_params});
    std::cout << "pure engine execute: err=" << (cursor ? cursor->is_error() : true) << "\n";
    REQUIRE(cursor);
}
