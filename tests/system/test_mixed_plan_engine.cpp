// Reproduce the cross-backend GROUP BY segfault in-process.
#include <catch2/catch_all.hpp>

#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <iostream>
#include <memory_resource>

using namespace components;

TEST_CASE("cross-backend GROUP BY downstream calls") {
    // Scoped arena, declared first so it outlives every object below. No engine
    // runs in this case — only the parser, generator and schema helpers — so
    // everything allocates from the test thread.
    std::pmr::synchronized_pool_resource case_arena(std::pmr::new_delete_resource());
    auto* resource = &case_arena;
    GreenplumParser parser(resource);
    const char* sql = R"(
        SELECT c.campaign_name,
               COUNT(p.product_id) as product_count,
               AVG(p.price) as avg_product_price
        FROM campaigns.db1.schema.campaigns c
        INNER JOIN products.pgdb.public.products p ON p.campaign_id = c.campaign_id
        GROUP BY c.campaign_name
        ORDER BY product_count DESC;)";
    auto result = parser.parse(sql);
    REQUIRE_FALSE(result.has_error());
    auto data = std::move(result.value());
    auto& nodes = data->otterbrix_params->external_nodes;
    std::cout << "batches=" << nodes.size() << "\n";

    // Fake STRUCT schemas like the store would hold.
    std::pmr::vector<types::complex_logical_type> campaigns_cols(resource);
    for (const char* n : {"campaign_id", "campaign_name", "budget"}) {
        campaigns_cols.emplace_back(types::logical_type::INTEGER);
        campaigns_cols.back().set_alias(n);
    }
    std::pmr::vector<types::complex_logical_type> products_cols(resource);
    for (const char* n : {"product_id", "campaign_id", "product_name", "price"}) {
        products_cols.emplace_back(types::logical_type::INTEGER);
        products_cols.back().set_alias(n);
    }

    for (size_t b = 0; b < nodes.size(); ++b) {
        for (size_t i = 0; i < nodes[b].size(); ++i) {
            auto* node = nodes[b][i].node;
            auto& target = nodes[b][i].target;
            std::cout << "node type=" << static_cast<int>((*node)->type()) << " name=" << target.name.to_string()
                      << "\n";
            if ((*node)->type() != logical_plan::node_type::aggregate_t) {
                continue;
            }
            auto agg = static_cast<logical_plan::node_aggregate_t&>(**node);
            auto schema_types = target.name.collection == "campaigns" ? campaigns_cols : products_cols;
            std::cout << "  calling generate_query...\n";
            auto q = sql_gen::generate_query(*node,
                                             &data->otterbrix_params->params_node->parameters(),
                                             backend_type_t::MySQL,
                                             target,
                                             nodes[b],
                                             resource);
            REQUIRE_FALSE(q.has_error());
            std::cout << "  SQL: " << q.value() << "\n";
            std::cout << "  calling aggregate_filter_schema...\n";
            auto initial_schema =
                schema_utils::aggregate_filter_schema(agg, data->otterbrix_params->params_node.get(), schema_types);
            std::cout << "  aggregate_filter_schema OK, type=" << static_cast<int>(initial_schema.type()) << "\n";
            auto node_schema = schema_utils::make_node_schema(target.name,
                                                              std::move(initial_schema),
                                                              logical_plan::node_aggregate_t(agg));
            std::cout << "  make_node_schema OK\n";
        }
    }
    std::cout << "ALL DOWNSTREAM CALLS OK\n";
}

#include "otterbrix/config.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include <components/logical_plan/node_data.hpp>
#include <components/vector/data_chunk.hpp>
#include <otterbrix/otterbrix.hpp>

// Engine defect (otterbrix a13-rc-1): operator_group_t fallback aggregation
// dereferences null when the GROUP BY key is a table-qualified STRING column
// over raw node_data chunks (key extraction yields NA -> NA-typed result
TEST_CASE("mixed plan with node_data executes in engine", "[engine-group-by-string]") {
    // Scoped arena, declared first so it outlives every object below
    // (engine included). Synchronized: the engine dispatcher may allocate
    // from it off the test thread.
    std::pmr::synchronized_pool_resource case_arena;
    auto* resource = &case_arena;
    GreenplumParser parser(resource);
    const char* sql = R"(
        SELECT c.campaign_name,
               COUNT(p.product_id) as product_count,
               AVG(p.price) as avg_product_price
        FROM campaigns.db1.schema.campaigns c
        INNER JOIN products.pgdb.public.products p ON p.campaign_id = c.campaign_id
        GROUP BY c.campaign_name
        ORDER BY product_count DESC;)";
    auto result = parser.parse(sql);
    REQUIRE_FALSE(result.has_error());
    auto data = std::move(result.value());

    auto make_chunk = [&](std::initializer_list<std::pair<const char*, types::logical_type>> names) {
        std::pmr::vector<types::complex_logical_type> cols(resource);
        for (auto& [n, t] : names) {
            cols.emplace_back(t);
            cols.back().set_alias(n);
        }
        vector::data_chunk_t chunk(resource, cols, 2);
        for (size_t c = 0; c < cols.size(); ++c) {
            for (size_t r = 0; r < 2; ++r) {
                switch (cols[c].type()) {
                    case types::logical_type::INTEGER:
                        chunk.set_value(c, r, types::logical_value_t(resource, static_cast<int32_t>(r + 1)));
                        break;
                    case types::logical_type::DOUBLE:
                        chunk.set_value(c, r, types::logical_value_t(resource, 100.5 * (r + 1)));
                        break;
                    default:
                        chunk.set_value(c,
                                        r,
                                        types::logical_value_t(resource, std::string("name_") + std::to_string(r)));
                        break;
                }
            }
        }
        chunk.set_cardinality(2);
        return chunk;
    };

    auto& nodes = data->otterbrix_params->external_nodes;
    for (size_t b = 0; b < nodes.size(); ++b) {
        for (size_t i = 0; i < nodes[b].size(); ++i) {
            auto& target = nodes[b][i].target;
            auto chunk = target.name.collection == "campaigns"
                             ? make_chunk({{"campaign_id", types::logical_type::INTEGER},
                                           {"campaign_name", types::logical_type::STRING_LITERAL},
                                           {"budget", types::logical_type::DOUBLE}})
                             : make_chunk({{"product_id", types::logical_type::INTEGER},
                                           {"campaign_id", types::logical_type::INTEGER},
                                           {"product_name", types::logical_type::STRING_LITERAL},
                                           {"price", types::logical_type::DOUBLE}});
            *nodes[b][i].node = logical_plan::make_node_raw_data(resource, std::move(chunk));
        }
    }

    auto cfg = make_create_config("/tmp/otterstax_mixed_probe");
    auto inst = db::make_otterbrix_engine(cfg);
    auto manager = make_otterbrix_manager(inst);
    std::cout << "executing mixed plan in engine...\n";
    auto cursor = manager->execute_plan(data->otterbrix_params);
    std::cout << "execute_plan returned: err=" << (cursor ? cursor->is_error() : true);
    if (cursor && cursor->is_error())
        std::cout << " what=" << cursor->get_error().what.c_str();
    if (cursor && !cursor->is_error())
        std::cout << " rows=" << cursor->size();
    std::cout << "\n";
}

static void run_mixed_variant(const char* tag, const char* sql, bool string_names, bool double_price) {
    // Scoped arena, declared first so it outlives every object below
    // (engine included). Synchronized: the engine dispatcher may allocate
    // from it off the test thread.
    std::pmr::synchronized_pool_resource case_arena;
    auto* resource = &case_arena;
    GreenplumParser parser(resource);
    auto result = parser.parse(sql);
    REQUIRE_FALSE(result.has_error());
    auto data = std::move(result.value());
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
    auto& nodes = data->otterbrix_params->external_nodes;
    for (size_t b = 0; b < nodes.size(); ++b) {
        for (size_t i = 0; i < nodes[b].size(); ++i) {
            auto& target = nodes[b][i].target;
            auto chunk = target.name.collection == "campaigns"
                             ? make_chunk({{"campaign_id", lt::INTEGER},
                                           {"campaign_name", string_names ? lt::STRING_LITERAL : lt::INTEGER},
                                           {"budget", lt::DOUBLE}})
                             : make_chunk({{"product_id", lt::INTEGER},
                                           {"campaign_id", lt::INTEGER},
                                           {"product_name", string_names ? lt::STRING_LITERAL : lt::INTEGER},
                                           {"price", double_price ? lt::DOUBLE : lt::INTEGER}});
            *nodes[b][i].node = components::logical_plan::make_node_raw_data(resource, std::move(chunk));
        }
    }
    auto cfg = make_create_config(std::string("/tmp/otterstax_mp_") + tag);
    auto inst = db::make_otterbrix_engine(cfg);
    auto manager = make_otterbrix_manager(inst);
    std::cout << "[" << tag << "] executing...\n";
    auto cursor = manager->execute_plan(data->otterbrix_params);
    std::cout << "[" << tag << "] done err=" << (cursor ? cursor->is_error() : true) << "\n";
}

static const char* k_groupby_sql = R"(
    SELECT c.campaign_name, COUNT(p.product_id) as product_count, AVG(p.price) as avg_product_price
    FROM campaigns.db1.schema.campaigns c
    INNER JOIN products.pgdb.public.products p ON p.campaign_id = c.campaign_id
    GROUP BY c.campaign_name ORDER BY product_count DESC;)";

TEST_CASE("mixed plan group by integer key with double avg") { run_mixed_variant("A", k_groupby_sql, false, true); }
TEST_CASE("mixed plan group by string key", "[engine-group-by-string]") {
    run_mixed_variant("B", k_groupby_sql, true, false);
}

// Pure-engine reproduction: NO otterstax components involved. The plan is
// produced by the ENGINE's own raw_parser + transformer, table aggregates are
// swapped for node_data via public logical_plan API, and the plan is executed
// through wrapper_dispatcher. A crash here is an otterbrix defect by
// construction.
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

TEST_CASE("pure engine: group by string key over node_data", "[engine-group-by-string]") {
    // Scoped arena, declared first so it outlives every object below
    // (engine included). Synchronized: the engine dispatcher may allocate
    // from it off the test thread.
    std::pmr::synchronized_pool_resource case_arena;
    auto* resource = &case_arena;

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
    auto root = binder.node_ptr();
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
                const auto& rel = static_cast<const components::logical_plan::node_aggregate_t&>(*child).relname().t;
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

    auto cfg = make_create_config("/tmp/otterstax_pure_engine_probe");
    auto inst = db::make_otterbrix_engine(cfg);
    auto cursor = inst->dispatcher()->execute_plan(
        otterbrix::session_id_t(),
        components::logical_plan::execution_plan_t{resource, root, binder.params_ptr()});
    std::cout << "pure engine execute: err=" << (cursor ? cursor->is_error() : true) << "\n";
    REQUIRE(cursor);
}

// Engine defect D1: a backend slice (node_raw_data, exactly what a
// ConnectorManager substitutes) JOINed against a LOCAL table on a struct-field
// key, with a WHERE predicate on the local side that leaves no rows, hands
// operator_join_t::build_layout_ zero build chunks and it dereferences null.
// Without the predicate an empty table scans as one empty chunk; with a plain
// column as the key the same predicate is survived. Pure engine — no live
// backend. The demo's step_4 is this shape.
//
// Four separate TEST_CASEs, not SECTIONs: a SIGSEGV aborts the process, and
// each variant must be attributable on its own.
#include <filesystem>

namespace {

    // The customers slice a PostgreSQL backend returns: Ann in Berlin, Bob in Tel Aviv.
    components::vector::data_chunk_t make_customers_slice(std::pmr::memory_resource* resource) {
        std::pmr::vector<components::types::complex_logical_type> cols(resource);
        for (const char* n : {"name", "addr_city"}) {
            cols.emplace_back(components::types::logical_type::STRING_LITERAL);
            cols.back().set_alias(n);
        }
        components::vector::data_chunk_t chunk(resource, cols, 2);
        chunk.set_value(0, 0, components::types::logical_value_t(resource, std::string("Ann")));
        chunk.set_value(1, 0, components::types::logical_value_t(resource, std::string("Berlin")));
        chunk.set_value(0, 1, components::types::logical_value_t(resource, std::string("Bob")));
        chunk.set_value(1, 1, components::types::logical_value_t(resource, std::string("Tel Aviv")));
        chunk.set_cardinality(2);
        return chunk;
    }

    // Local otter.warehouses in the demo's step_3a shape (ENUM + two composites +
    // bigints); `seed_rows` adds the single warehouse BER-1 located in Berlin.
    // The external slot is replaced by the customers slice and the plan runs in
    // the engine; a correct engine returns `expected_rows` without an error.
    void run_backend_join_over_local_composite(const char* tag, const char* sql, bool seed_rows, size_t expected_rows) {
        // Scoped arena, declared first so it outlives every object below
        // (engine included). Synchronized: the engine dispatcher may allocate
        // from it off the test thread.
        std::pmr::synchronized_pool_resource case_arena(std::pmr::new_delete_resource());
        auto* resource = &case_arena;

        // Engine state persists across runs: start from an empty data_dir.
        const std::string data_dir = std::string("/tmp/otterstax_join_d1_") + tag;
        std::filesystem::remove_all(data_dir);
        auto cfg = make_create_config(data_dir);
        auto inst = db::make_otterbrix_engine(cfg);
        auto manager = make_otterbrix_manager(inst);

        for (const char* ddl : {"CREATE DATABASE otter;",
                                "CREATE TYPE tier_t AS ENUM('bronze','silver','gold');",
                                "CREATE TYPE address_t AS (city STRING, country STRING, zip STRING);",
                                "CREATE TYPE spec_t AS (weight_1 INT, weight_2 INT, weight_3 INT, weight_4 INT, "
                                "weight_5 INT, weight_6 INT, primary_barcode STRING, secondary_barcode STRING, "
                                "photo STRING);",
                                "CREATE TABLE otter.warehouses (warehouse_id STRING, code STRING, tier tier_t, "
                                "location address_t, spec spec_t, priority_high BIGINT, priority_med BIGINT, "
                                "priority_low BIGINT);"}) {
            auto c = manager->execute_sql(ddl);
            REQUIRE(c);
            INFO("ddl: " << ddl << " err: " << (c->is_error() ? c->get_error().what.c_str() : ""));
            REQUIRE_FALSE(c->is_error());
        }
        if (seed_rows) {
            auto c = manager->execute_sql(
                "INSERT INTO otter.warehouses (warehouse_id, code, tier, location, priority_high) VALUES "
                "('w-1', 'BER-1', 'silver', ROW('Berlin','DE','10115'), 1);");
            REQUIRE(c);
            INFO("seed err: " << (c->is_error() ? c->get_error().what.c_str() : ""));
            REQUIRE_FALSE(c->is_error());
        }

        GreenplumParser parser(resource);
        auto parsed = parser.parse(sql);
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());

        auto& nodes = data->otterbrix_params->external_nodes;
        size_t substituted = 0;
        for (auto& batch : nodes) {
            for (auto& entry : batch) {
                *entry.node = components::logical_plan::make_node_raw_data(resource, make_customers_slice(resource));
                ++substituted;
            }
        }
        REQUIRE(substituted == 1);

        auto cursor = manager->execute_plan(data->otterbrix_params);
        REQUIRE(cursor);
        INFO("join err: " << (cursor->is_error() ? cursor->get_error().what.c_str() : ""));
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == expected_rows);
    }

    // Struct field access on the JOIN key and the demo's local-side predicate (step_4).
    constexpr const char* k_d1_join_struct_key = "SELECT c.name, w.code FROM pg.shop.public.customers c "
                                                 "INNER JOIN otter.warehouses w ON c.addr_city = (w.location).city "
                                                 "WHERE (w.location).country IN ('DE','IL','US');";
    // Plain column on the JOIN key; the same build side and predicate, no struct access in the key.
    constexpr const char* k_d1_join_plain_key = "SELECT c.name, w.code FROM pg.shop.public.customers c "
                                                "INNER JOIN otter.warehouses w ON c.addr_city = w.code "
                                                "WHERE (w.location).country IN ('DE','IL','US');";

} // namespace

// No warehouse: the predicate leaves the build side with zero chunks.
TEST_CASE("D1: backend slice JOIN local composite table on struct key over empty local table",
          "[engine-defect-d1]") {
    run_backend_join_over_local_composite("struct_empty", k_d1_join_struct_key, false, 0);
}

// BER-1 is located in Berlin, DE: Ann matches, Bob does not.
TEST_CASE("D1: backend slice JOIN local composite table on struct key over seeded local table",
          "[engine-defect-d1]") {
    run_backend_join_over_local_composite("struct_seeded", k_d1_join_struct_key, true, 1);
}

TEST_CASE("D1: backend slice JOIN local composite table on plain key over empty local table",
          "[engine-defect-d1]") {
    run_backend_join_over_local_composite("plain_empty", k_d1_join_plain_key, false, 0);
}

// The code 'BER-1' is no customer's city: the JOIN runs to completion with no match.
TEST_CASE("D1: backend slice JOIN local composite table on plain key over seeded local table",
          "[engine-defect-d1]") {
    run_backend_join_over_local_composite("plain_seeded", k_d1_join_plain_key, true, 0);
}
