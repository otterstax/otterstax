// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/parser/name_resolution.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"
#include "scheduler/schema_utils.hpp"

#include <catch2/catch.hpp>
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node.hpp>
#include <core/result_wrapper.hpp>

#include <algorithm>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

namespace {
    ParsedQueryDataPtr parse_or_die(GreenplumParser& parser, const std::string& sql) {
        auto result = parser.parse(sql);
        REQUIRE_FALSE(result.has_error());
        return std::move(result.value());
    }

    // One external slot: the node together with its parser-resolved target
    // (external_targets is filled 1:1 with external_nodes).
    struct flat_external_t {
        components::logical_plan::node_ptr node;
        otterstax::names::resolved_target_t target;
    };

    std::vector<flat_external_t> external_nodes_flat(const ParsedQueryDataPtr& parsed) {
        std::vector<flat_external_t> out;
        const auto& nodes = parsed->otterbrix_params->external_nodes;
        const auto& targets = parsed->otterbrix_params->external_targets;
        REQUIRE(targets.size() == nodes.size());
        for (size_t batch = 0; batch < nodes.size(); ++batch) {
            REQUIRE(targets[batch].size() == nodes[batch].size());
            for (size_t i = 0; i < nodes[batch].size(); ++i) {
                out.push_back(flat_external_t{*nodes[batch][i], targets[batch][i]});
            }
        }
        return out;
    }

    flat_external_t find_by_uid(const std::vector<flat_external_t>& nodes, const std::string& uid) {
        auto it = std::find_if(nodes.begin(), nodes.end(), [&uid](const auto& n) {
            return n.target.name.unique_identifier == uid;
        });
        return it != nodes.end() ? *it : flat_external_t{};
    }

    bool is_schema_node_with_raw_sql(const components::logical_plan::node_ptr& n) {
        if (!n || n->type() != components::logical_plan::node_type::unused) {
            return false;
        }
        const auto& sn = static_cast<const schema_utils::schema_node_t&>(*n);
        return sn.has_raw_sql();
    }
} // namespace

TEST_CASE("integration: 4-part qualifier in cross-source JOIN") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "SELECT * FROM mysql.bill.schema.orders o INNER JOIN pg.shop.shop.products p ON o.product_id = p.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 2);

    auto mysql_node = find_by_uid(nodes, "mysql");
    auto pg_node = find_by_uid(nodes, "pg");
    REQUIRE(mysql_node.node);
    REQUIRE(pg_node.node);

    REQUIRE_FALSE(is_schema_node_with_raw_sql(mysql_node.node));
    REQUIRE_FALSE(is_schema_node_with_raw_sql(pg_node.node));

    REQUIRE(mysql_node.target.name.database == "bill");
    REQUIRE(mysql_node.target.name.collection == "orders");
    REQUIRE(pg_node.target.name.schema == "shop");
    REQUIRE(pg_node.target.name.collection == "products");
}

TEST_CASE("integration: 4-part qualifier untouched") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed =
        parse_or_die(parser,
                     "SELECT * FROM uid1.db1.sch1.test1 INNER JOIN uid2.db2.sch2.test2 ON test1.id = test2.id;");
    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 2);

    auto n1 = find_by_uid(nodes, "uid1");
    auto n2 = find_by_uid(nodes, "uid2");
    REQUIRE(n1.node);
    REQUIRE(n2.node);
    REQUIRE(n1.target.name.database == "db1");
    REQUIRE(n1.target.name.schema == "sch1");
    REQUIRE(n1.target.name.collection == "test1");
    REQUIRE(n2.target.name.collection == "test2");
}

TEST_CASE("integration: no external_node") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT id FROM demo_warehouses;");
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 0);
}

TEST_CASE("integration: derived table into schema_node") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "SELECT p.category, COUNT(*) FROM ("
                               "SELECT product_id FROM mysql.bill.schema.orders WHERE ts >= '2026-04-18'"
                               ") o INNER JOIN pg.shop.shop.products p ON p.id = o.product_id "
                               "GROUP BY p.category;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 2);

    auto mysql_stub = find_by_uid(nodes, "mysql");
    auto pg_real = find_by_uid(nodes, "pg");
    REQUIRE(mysql_stub.node);
    REQUIRE(pg_real.node);

    REQUIRE(is_schema_node_with_raw_sql(mysql_stub.node));
    const auto& raw = static_cast<const schema_utils::schema_node_t&>(*mysql_stub.node).raw_sql();
    REQUIRE(raw.find("FROM mysql.bill.schema.orders") != std::string::npos);
    REQUIRE(raw.find("ts >= '2026-04-18'") != std::string::npos);
    REQUIRE_FALSE(static_cast<const schema_utils::schema_node_t&>(*mysql_stub.node).qualifiers().empty());

    // The schema node itself carries the resolved stub name as well.
    REQUIRE(static_cast<const schema_utils::schema_node_t&>(*mysql_stub.node).name() == mysql_stub.target.name);

    REQUIRE_FALSE(is_schema_node_with_raw_sql(pg_real.node));
    REQUIRE(pg_real.target.name.schema == "shop");
}

TEST_CASE("integration: qualified + local untouched") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "SELECT c.name FROM pg.shop.shop.customers c "
                               "INNER JOIN demo_warehouses w ON c.id = w.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 1);
    REQUIRE(nodes[0].target.name.unique_identifier == "pg");

    // It's a real aggregate, NOT a wrapped raw_sql — proves wrap was skipped.
    REQUIRE_FALSE(is_schema_node_with_raw_sql(nodes[0].node));
    REQUIRE(nodes[0].target.name.schema == "shop");
    REQUIRE(nodes[0].target.name.collection == "customers");
}

TEST_CASE("integration: cross-source subqueries") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed =
        parse_or_die(parser,
                     "SELECT c.name FROM pg.shop.shop.customers c "
                     "INNER JOIN (SELECT user_id FROM ch.ev.schema.sessions WHERE ts >= '2026-04-12') s "
                     "  ON s.user_id = c.id "
                     "INNER JOIN (SELECT customer_id FROM mysql.bill.schema.orders WHERE ts >= '2026-04-12') o "
                     "  ON o.customer_id = c.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 3);

    auto pg_node = find_by_uid(nodes, "pg");
    auto ch_stub = find_by_uid(nodes, "ch");
    auto mysql_stub = find_by_uid(nodes, "mysql");
    REQUIRE(pg_node.node);
    REQUIRE(ch_stub.node);
    REQUIRE(mysql_stub.node);

    REQUIRE_FALSE(is_schema_node_with_raw_sql(pg_node.node));

    REQUIRE(is_schema_node_with_raw_sql(ch_stub.node));
    REQUIRE(is_schema_node_with_raw_sql(mysql_stub.node));

    const auto& ch_raw = static_cast<const schema_utils::schema_node_t&>(*ch_stub.node).raw_sql();
    const auto& mysql_raw = static_cast<const schema_utils::schema_node_t&>(*mysql_stub.node).raw_sql();
    REQUIRE(ch_raw.find("FROM ch.ev.schema.sessions") != std::string::npos);
    REQUIRE(mysql_raw.find("FROM mysql.bill.schema.orders") != std::string::npos);
}
