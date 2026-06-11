// Reproduce the cross-backend GROUP BY segfault in-process.
#include <catch2/catch.hpp>

#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "scheduler/schema_utils.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <iostream>

using namespace components;

TEST_CASE("cross-backend GROUP BY downstream calls") {
    auto* resource = std::pmr::get_default_resource();
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
                                             nodes[b]);
            std::cout << "  SQL: " << q << "\n";
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
    auto* resource = std::pmr::get_default_resource();
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
    auto inst = otterbrix::make_otterbrix(cfg);
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
    auto* resource = std::pmr::get_default_resource();
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
    auto inst = otterbrix::make_otterbrix(cfg);
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
    auto* resource = std::pmr::get_default_resource();

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
    auto inst = otterbrix::make_otterbrix(cfg);
    auto cursor = inst->dispatcher()->execute_plan(
        otterbrix::session_id_t(),
        components::logical_plan::execution_plan_t{resource, root, binder.params_ptr()});
    std::cout << "pure engine execute: err=" << (cursor ? cursor->is_error() : true) << "\n";
    REQUIRE(cursor);
}
