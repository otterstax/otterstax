// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

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

    std::vector<components::logical_plan::node_ptr> external_nodes_flat(const ParsedQueryDataPtr& parsed) {
        std::vector<components::logical_plan::node_ptr> out;
        for (const auto& batch : parsed->otterbrix_params->external_nodes) {
            for (auto* slot : batch) {
                out.push_back(*slot);
            }
        }
        return out;
    }

    components::logical_plan::node_ptr find_by_uid(const std::vector<components::logical_plan::node_ptr>& nodes,
                                                   const std::string& uid) {
        auto it = std::find_if(nodes.begin(), nodes.end(), [&uid](const auto& n) {
            return n->collection_full_name().unique_identifier == uid;
        });
        return it != nodes.end() ? *it : components::logical_plan::node_ptr{};
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
    REQUIRE(mysql_node);
    REQUIRE(pg_node);

    REQUIRE_FALSE(is_schema_node_with_raw_sql(mysql_node));
    REQUIRE_FALSE(is_schema_node_with_raw_sql(pg_node));

    REQUIRE(mysql_node->collection_full_name().database == "bill");
    REQUIRE(mysql_node->collection_full_name().collection == "orders");
    REQUIRE(pg_node->collection_full_name().schema == "shop");
    REQUIRE(pg_node->collection_full_name().collection == "products");
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
    REQUIRE(n1);
    REQUIRE(n2);
    REQUIRE(n1->collection_full_name().database == "db1");
    REQUIRE(n1->collection_full_name().schema == "sch1");
    REQUIRE(n1->collection_full_name().collection == "test1");
    REQUIRE(n2->collection_full_name().collection == "test2");
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
    REQUIRE(mysql_stub);
    REQUIRE(pg_real);

    REQUIRE(is_schema_node_with_raw_sql(mysql_stub));
    const auto& raw = static_cast<const schema_utils::schema_node_t&>(*mysql_stub).raw_sql();
    REQUIRE(raw.find("FROM mysql.bill.schema.orders") != std::string::npos);
    REQUIRE(raw.find("ts >= '2026-04-18'") != std::string::npos);
    REQUIRE_FALSE(static_cast<const schema_utils::schema_node_t&>(*mysql_stub).qualifiers().empty());

    REQUIRE_FALSE(is_schema_node_with_raw_sql(pg_real));
    REQUIRE(pg_real->collection_full_name().schema == "shop");
}

TEST_CASE("integration: qualified + local untouched") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "SELECT c.name FROM pg.shop.shop.customers c "
                               "INNER JOIN demo_warehouses w ON c.id = w.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 1);
    REQUIRE(nodes[0]->collection_full_name().unique_identifier == "pg");

    // It's a real aggregate, NOT a wrapped raw_sql — proves wrap was skipped.
    REQUIRE_FALSE(is_schema_node_with_raw_sql(nodes[0]));
    REQUIRE(nodes[0]->collection_full_name().schema == "shop");
    REQUIRE(nodes[0]->collection_full_name().collection == "customers");
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
    REQUIRE(pg_node);
    REQUIRE(ch_stub);
    REQUIRE(mysql_stub);

    REQUIRE_FALSE(is_schema_node_with_raw_sql(pg_node));

    REQUIRE(is_schema_node_with_raw_sql(ch_stub));
    REQUIRE(is_schema_node_with_raw_sql(mysql_stub));

    const auto& ch_raw = static_cast<const schema_utils::schema_node_t&>(*ch_stub).raw_sql();
    const auto& mysql_raw = static_cast<const schema_utils::schema_node_t&>(*mysql_stub).raw_sql();
    REQUIRE(ch_raw.find("FROM ch.ev.schema.sessions") != std::string::npos);
    REQUIRE(mysql_raw.find("FROM mysql.bill.schema.orders") != std::string::npos);
}
