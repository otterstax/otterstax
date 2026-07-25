// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/parser/name_resolution.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"

#include <catch2/catch_all.hpp>

#include <components/logical_plan/node.hpp>
#include <memory_resource>
#include <string>
#include <vector>

// ── Helpers ────────────────────────────────────────────────────────────────────

namespace {

ParsedQueryDataPtr parse_or_die(GreenplumParser& p, const std::string& sql) {
    auto r = p.parse(sql);
    REQUIRE_FALSE(r.has_error());
    return std::move(r.value());
}

// One external slot: node + its parser-resolved target + owning batch index.
struct flat_external_t {
    components::logical_plan::node_ptr node;
    otterstax::names::resolved_target_t target;
    size_t batch{0};
};

// Collect all external nodes from all batches into a flat list.
std::vector<flat_external_t> flat_externals(const ParsedQueryDataPtr& parsed) {
    std::vector<flat_external_t> out;
    const auto& nodes = parsed->otterbrix_params->external_nodes;
    for (size_t batch = 0; batch < nodes.size(); ++batch) {
        for (size_t i = 0; i < nodes[batch].size(); ++i) {
            // Every external slot must carry a plan-node reference.
            REQUIRE(nodes[batch][i].node != nullptr);
            out.push_back(flat_external_t{*nodes[batch][i].node, nodes[batch][i].target, batch});
        }
    }
    return out;
}

// Find the first node whose unique_identifier matches uid.
flat_external_t find_by_uid(
    const std::vector<flat_external_t>& nodes,
    const std::string& uid) {
    for (const auto& n : nodes)
        if (n.node && n.target.name.unique_identifier == uid)
            return n;
    return {};
}

// The external entries of the batch a flat slot came from — generate_query
// uses their targets to resolve the inner SELECT table of INSERT ... SELECT.
const std::pmr::vector<external_entry_t>&
batch_targets_of(const ParsedQueryDataPtr& parsed, const flat_external_t& slot) {
    return parsed->otterbrix_params->external_nodes[slot.batch];
}

// Returns true when the node is a schema_node_t that carries raw SQL (stub path).
bool is_raw_sql_stub(const components::logical_plan::node_ptr& n) {
    if (!n || n->type() != components::logical_plan::node_type::unused)
        return false;
    return static_cast<const schema_utils::schema_node_t&>(*n).has_raw_sql();
}

} // namespace

// ── sql_gen::table_reference ──────────────────────────────────────────────────

TEST_CASE("table_reference: MySQL uses database.collection") {
    qualified_name_t name{"bill", "", "orders"};
    auto ref = sql_gen::table_reference(name, backend_type_t::MySQL);
    REQUIRE(ref == "`bill`.`orders`");
}

TEST_CASE("table_reference: PostgreSQL uses schema.collection") {
    qualified_name_t name{"", "public", "products"};
    auto ref = sql_gen::table_reference(name, backend_type_t::PostgreSQL);
    REQUIRE(ref == "\"public\".\"products\"");
}

TEST_CASE("table_reference: ClickHouse uses database.collection (same as MySQL)") {
    // ClickHouse has no schema level: table_reference emits database.collection.
    qualified_name_t name{"events", "", "sessions"};
    auto ref = sql_gen::table_reference(name, backend_type_t::ClickHouse);
    REQUIRE(ref == "`events`.`sessions`");
}

TEST_CASE("table_reference: 2-arg constructor, MySQL") {
    // 2-arg ctor sets database=bill, collection=orders, schema=""
    qualified_name_t name{"bill", "orders"};
    auto ref = sql_gen::table_reference(name, backend_type_t::MySQL);
    REQUIRE(ref == "`bill`.`orders`");
}

// ── sql_gen::generate_query ───────────────────────────────────────────────────
// generate_query() is called on logical-plan nodes that are NOT raw-SQL stubs.
// These arise from 4-part qualifiers in a direct JOIN (not a derived-table subquery).

TEST_CASE("generate_query: MySQL node produces db.collection reference") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "SELECT o.id, p.name "
        "FROM mysql.bill.schema.orders o "
        "INNER JOIN pg.shop.shop.products p ON o.product_id = p.id;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);
    REQUIRE_FALSE(is_raw_sql_stub(mysql_node.node));

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto sql = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node));

    // MySQL table reference must be quoted db.collection (no schema segment)
    REQUIRE_FALSE(sql.empty());
    REQUIRE(sql.find("`bill`.`orders`") != std::string::npos);
    // 4-part qualifier must not leak into the generated SQL
    REQUIRE(sql.find("mysql.bill.schema.orders") == std::string::npos);
}

TEST_CASE("generate_query: LIMIT is pushed down to the remote backend SQL") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders LIMIT 5;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);
    REQUIRE_FALSE(is_raw_sql_stub(mysql_node.node));

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto sql = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node));

    // Regression: generate_select ignored the node_limit_t child, so remote
    // backends fetched every row. The LIMIT must reach the pushed-down SQL.
    REQUIRE_FALSE(sql.empty());
    REQUIRE(sql.find("LIMIT 5") != std::string::npos);
}

TEST_CASE("generate_query: LIMIT with OFFSET is pushed down") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders LIMIT 5 OFFSET 2;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto sql = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node));

    REQUIRE(sql.find("LIMIT 5") != std::string::npos);
    REQUIRE(sql.find("OFFSET 2") != std::string::npos);
}

TEST_CASE("generate_query: no LIMIT clause when the query has none") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders WHERE id > 0;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto sql = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node));

    REQUIRE(sql.find("LIMIT") == std::string::npos);
}

// b2-rc-2's grammar accepts DML LIMIT (DELETE/UPDATE ... LIMIT n) and attaches
// a node_limit_t child. Regression: the generator used to ignore it, silently
// deleting/updating every matching remote row.
TEST_CASE("generate_query: DELETE ... LIMIT reaches MySQL SQL, throws for PostgreSQL") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "DELETE FROM mysql.bill.schema.orders WHERE id > 0 LIMIT 5;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto sql = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node));
    REQUIRE(sql.find("LIMIT 5") != std::string::npos);

    REQUIRE_THROWS(sql_gen::generate_query(mysql_node.node,
                                           &params,
                                           backend_type_t::PostgreSQL,
                                           mysql_node.target,
                                           batch_targets_of(parsed, mysql_node)));
}

TEST_CASE("generate_query: UPDATE ... LIMIT reaches MySQL SQL, throws for PostgreSQL") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "UPDATE mysql.bill.schema.orders SET name = 'x' WHERE id > 0 LIMIT 3;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto sql = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node));
    REQUIRE(sql.find("LIMIT 3") != std::string::npos);

    REQUIRE_THROWS(sql_gen::generate_query(mysql_node.node,
                                           &params,
                                           backend_type_t::PostgreSQL,
                                           mysql_node.target,
                                           batch_targets_of(parsed, mysql_node)));
}

TEST_CASE("generate_query: DELETE without LIMIT emits no LIMIT clause") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "DELETE FROM mysql.bill.schema.orders WHERE id > 0;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto sql = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node));
    REQUIRE(sql.find("LIMIT") == std::string::npos);
}

TEST_CASE("generate_query: PostgreSQL node produces schema.collection reference") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "SELECT o.id, p.name "
        "FROM mysql.bill.schema.orders o "
        "INNER JOIN pg.shop.shop.products p ON o.product_id = p.id;");

    auto nodes = flat_externals(parsed);
    auto pg_node = find_by_uid(nodes, "pg");
    REQUIRE(pg_node.node);
    REQUIRE_FALSE(is_raw_sql_stub(pg_node.node));

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto sql = sql_gen::generate_query(pg_node.node,
                                       &params,
                                       backend_type_t::PostgreSQL,
                                       pg_node.target,
                                       batch_targets_of(parsed, pg_node));

    // PostgreSQL table reference must be quoted schema.collection
    REQUIRE_FALSE(sql.empty());
    REQUIRE(sql.find("\"shop\".\"products\"") != std::string::npos);
    REQUIRE(sql.find("pg.shop.shop.products") == std::string::npos);
}

TEST_CASE("generate_query: same node, different backends produce different references") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "SELECT a.id FROM uid1.db1.sch1.test1 a INNER JOIN uid2.db2.sch2.test2 b ON a.id = b.id;");

    auto nodes = flat_externals(parsed);
    auto n1 = find_by_uid(nodes, "uid1");
    REQUIRE(n1.node);
    const auto& params = parsed->otterbrix_params->params_node->parameters();

    auto mysql_sql = sql_gen::generate_query(n1.node,
                                             &params,
                                             backend_type_t::MySQL,
                                             n1.target,
                                             batch_targets_of(parsed, n1));
    auto pg_sql    = sql_gen::generate_query(n1.node,
                                             &params,
                                             backend_type_t::PostgreSQL,
                                             n1.target,
                                             batch_targets_of(parsed, n1));

    // MySQL: `db1`.`test1`   PG: "sch1"."test1"
    REQUIRE(mysql_sql.find("`db1`.`test1`")        != std::string::npos);
    REQUIRE(pg_sql.find("\"sch1\".\"test1\"")      != std::string::npos);
}

TEST_CASE("generate_query: stringstream overload produces the same output") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "SELECT o.id FROM mysql.bill.schema.orders o "
        "INNER JOIN pg.shop.shop.products p ON o.id = p.id;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();

    // String overload
    auto sql_str = sql_gen::generate_query(mysql_node.node,
                                           &params,
                                           backend_type_t::MySQL,
                                           mysql_node.target,
                                           batch_targets_of(parsed, mysql_node));

    // Stream overload
    std::stringstream ss;
    sql_gen::generate_query(ss,
                            mysql_node.node,
                            &params,
                            backend_type_t::MySQL,
                            mysql_node.target,
                            batch_targets_of(parsed, mysql_node));

    // The string overload appends the statement terminator; the stream
    // overload emits the bare statement so callers can keep composing.
    REQUIRE(sql_str == ss.str() + ";");
}

// ── replace_qualifiers edge cases not covered in test_replace_qualifiers.cpp ──

TEST_CASE("replace_qualifiers: 3-part qualifier treated as db.schema.collection (uid promoted)") {
    // prepare_sql promotes the first segment to uid when only 3 parts are present.
    // Scoped arena: libotterbrix_sql's parse tree allocates through this resource
    // and is never explicitly freed — get_default_resource would leak it (LSAN).
    std::pmr::monotonic_buffer_resource arena;
    auto r = otterstax::parser::prepare_sql(
        "SELECT id FROM (SELECT id FROM mysql.bill.orders WHERE status = 'paid') o;",
        &arena);

    REQUIRE(r.stubs.size() == 1);
    auto out = sql_gen::replace_qualifiers(r.stubs[0].raw_sql, r.stubs[0].qualifiers, backend_type_t::MySQL);
    // After rewrite the 3-part name becomes quoted db.collection for MySQL
    REQUIRE(out.find("FROM `bill`.`orders`") != std::string::npos);
    REQUIRE(out.find("mysql.bill.orders") == std::string::npos);
}

TEST_CASE("replace_qualifiers: multiple qualifiers in one stub") {
    std::pmr::monotonic_buffer_resource arena;
    auto r = otterstax::parser::prepare_sql(
        "SELECT * FROM ("
        "SELECT id FROM mysql.db.sc.t1 UNION ALL SELECT id FROM mysql.db.sc.t2"
        ") u;",
        &arena);

    REQUIRE(r.stubs.size() == 1);
    auto out = sql_gen::replace_qualifiers(r.stubs[0].raw_sql, r.stubs[0].qualifiers, backend_type_t::MySQL);
    // Both 4-part names rewritten
    REQUIRE(out.find("`db`.`t1`") != std::string::npos);
    REQUIRE(out.find("`db`.`t2`") != std::string::npos);
    REQUIRE(out.find("mysql.db.sc.t1") == std::string::npos);
    REQUIRE(out.find("mysql.db.sc.t2") == std::string::npos);
}
