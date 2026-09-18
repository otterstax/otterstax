// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/parser/name_resolution.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"

#include <catch2/catch_all.hpp>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/logical_value.hpp>

#include <cstdint>
#include <memory_resource>
#include <ostream>
#include <string>
#include <vector>

using namespace components::expressions;
using namespace components::logical_plan;
using namespace components::types;

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

// generate_query for the external slot of `parsed` registered under `uid`.
core::result_wrapper_t<std::string> generate_for(const ParsedQueryDataPtr& parsed,
                                                 const std::string& uid,
                                                 backend_type_t backend,
                                                 std::pmr::memory_resource* resource) {
    auto nodes = flat_externals(parsed);
    auto slot = find_by_uid(nodes, uid);
    REQUIRE(slot.node);
    const auto& params = parsed->otterbrix_params->params_node->parameters();
    return sql_gen::generate_query(slot.node, &params, backend, slot.target, batch_targets_of(parsed, slot), resource);
}

// Parse + generate for the `mysql` slot; the generated statement must succeed.
std::string sql_for(GreenplumParser& parser,
                    const std::string& sql,
                    backend_type_t backend,
                    std::pmr::memory_resource* resource) {
    auto parsed = parse_or_die(parser, sql);
    auto gen = generate_for(parsed, "mysql", backend, resource);
    INFO("error: " << std::string_view{gen.error().what});
    REQUIRE_FALSE(gen.has_error());
    return gen.value();
}

// Parse + generate for the `mysql` slot; the generator must refuse.
core::error_code_t error_for(GreenplumParser& parser,
                             const std::string& sql,
                             backend_type_t backend,
                             std::pmr::memory_resource* resource) {
    auto parsed = parse_or_die(parser, sql);
    auto gen = generate_for(parsed, "mysql", backend, resource);
    REQUIRE(gen.has_error());
    return gen.error().type;
}

// Hand-built plans: the parser is not the only producer of plan nodes, and a
// hand-built node pins one exact shape without depending on transformer details.
const qualified_name_t orders_name{"mysql", "bill", "schema", "orders"};

otterstax::names::resolved_target_t orders_target() {
    return otterstax::names::resolved_target_t{components::catalog::INVALID_OID, orders_name, {}};
}

node_match_ptr orders_match(std::pmr::memory_resource* resource, const expression_ptr& predicate) {
    return make_node_match(resource, core::dbname_t{"bill"}, core::relname_t{"orders"}, predicate);
}

node_limit_ptr orders_limit(std::pmr::memory_resource* resource, const limit_t& limit) {
    return make_node_limit(resource, core::dbname_t{"bill"}, core::relname_t{"orders"}, limit);
}

node_aggregate_ptr orders_select(std::pmr::memory_resource* resource) {
    return make_node_aggregate(resource, core::dbname_t{"bill"}, core::relname_t{"orders"});
}

core::result_wrapper_t<std::string>
generate_manual(const node_ptr& node,
                const parameter_node_t& params,
                backend_type_t backend,
                std::pmr::memory_resource* resource,
                const otterstax::names::resolved_target_t& target = orders_target()) {
    std::pmr::vector<external_entry_t> empty_batch{resource};
    return sql_gen::generate_query(node, &params.parameters(), backend, target, empty_batch, resource);
}

// The single literal of `VALUES (<literal>)`.
std::string single_value_literal(const std::string& values_sql) {
    const auto open = values_sql.find('(');
    const auto close = values_sql.rfind(')');
    REQUIRE(open != std::string::npos);
    REQUIRE(close != std::string::npos);
    return values_sql.substr(open + 1, close - open - 1);
}

} // namespace

// ── sql_gen::table_reference ──────────────────────────────────────────────────

TEST_CASE("table_reference: MySQL uses database.collection") {
    qualified_name_t name{"bill", "", "orders"};
    auto ref = sql_gen::table_reference(name, backend_type_t::MySQL, std::pmr::new_delete_resource());
    REQUIRE_FALSE(ref.has_error());
    REQUIRE(ref.value() == "`bill`.`orders`");
}

TEST_CASE("table_reference: PostgreSQL uses schema.collection") {
    qualified_name_t name{"", "public", "products"};
    auto ref = sql_gen::table_reference(name, backend_type_t::PostgreSQL, std::pmr::new_delete_resource());
    REQUIRE_FALSE(ref.has_error());
    REQUIRE(ref.value() == "\"public\".\"products\"");
}

TEST_CASE("table_reference: ClickHouse uses database.collection (same as MySQL)") {
    // ClickHouse has no schema level: table_reference emits database.collection.
    qualified_name_t name{"events", "", "sessions"};
    auto ref = sql_gen::table_reference(name, backend_type_t::ClickHouse, std::pmr::new_delete_resource());
    REQUIRE_FALSE(ref.has_error());
    REQUIRE(ref.value() == "`events`.`sessions`");
}

TEST_CASE("table_reference: 2-arg constructor, MySQL") {
    // 2-arg ctor sets database=bill, collection=orders, schema=""
    qualified_name_t name{"bill", "orders"};
    auto ref = sql_gen::table_reference(name, backend_type_t::MySQL, std::pmr::new_delete_resource());
    REQUIRE_FALSE(ref.has_error());
    REQUIRE(ref.value() == "`bill`.`orders`");
}

// Unknown and Mixed are routing verdicts and Otterbrix is the local engine; a
// reference in any of them would be a guessed spelling sent to a backend.
TEST_CASE("table_reference: a backend without a SQL dialect is invalid_parameter") {
    qualified_name_t name{"bill", "", "orders"};
    for (auto backend : {backend_type_t::Unknown, backend_type_t::Mixed, backend_type_t::Otterbrix}) {
        INFO("backend = " << static_cast<int>(backend));
        auto ref = sql_gen::table_reference(name, backend, std::pmr::new_delete_resource());
        REQUIRE(ref.has_error());
        REQUIRE(ref.error().type == core::error_code_t::invalid_parameter);
    }
}

TEST_CASE("table_reference: an empty name is invalid_parameter in every dialect") {
    for (auto backend : {backend_type_t::MySQL, backend_type_t::PostgreSQL, backend_type_t::ClickHouse}) {
        INFO("backend = " << static_cast<int>(backend));
        auto ref = sql_gen::table_reference(qualified_name_t{}, backend, std::pmr::new_delete_resource());
        REQUIRE(ref.has_error());
        REQUIRE(ref.error().type == core::error_code_t::invalid_parameter);
    }
}

// ── sql_gen::generate_query ───────────────────────────────────────────────────
// generate_query() is called on logical-plan nodes that are NOT raw-SQL stubs.
// These arise from 4-part qualifiers in a direct JOIN (not a derived-table subquery).

TEST_CASE("generate_query: MySQL node produces db.collection reference") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
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
    auto gen = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    // MySQL table reference must be quoted db.collection (no schema segment)
    REQUIRE_FALSE(sql.empty());
    REQUIRE(sql.find("`bill`.`orders`") != std::string::npos);
    // 4-part qualifier must not leak into the generated SQL
    REQUIRE(sql.find("mysql.bill.schema.orders") == std::string::npos);
}

TEST_CASE("generate_query: LIMIT is pushed down to the remote backend SQL") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders LIMIT 5;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);
    REQUIRE_FALSE(is_raw_sql_stub(mysql_node.node));

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    // Regression: generate_select ignored the node_limit_t child, so remote
    // backends fetched every row. The LIMIT must reach the pushed-down SQL.
    REQUIRE_FALSE(sql.empty());
    REQUIRE(sql.find("LIMIT 5") != std::string::npos);
}

TEST_CASE("generate_query: LIMIT with OFFSET is pushed down") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders LIMIT 5 OFFSET 2;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    REQUIRE(sql.find("LIMIT 5") != std::string::npos);
    REQUIRE(sql.find("OFFSET 2") != std::string::npos);
}

TEST_CASE("generate_query: no LIMIT clause when the query has none") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders WHERE id > 0;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    REQUIRE(sql.find("LIMIT") == std::string::npos);
}

// ── OFFSET without LIMIT ──────────────────────────────────────────────────────
// limit_t stores limit_ (-1 = unlimit) and offset_ (0 = none) INDEPENDENTLY, and
// the transformer builds a node_limit_t when EITHER is present. Gating the whole
// clause on limit() >= 0 therefore dropped a bare OFFSET: the backend returned
// the window starting at row 0, and because the manager replaces this node's slot
// with the fetched rows, nothing downstream re-applies the skip.

TEST_CASE("generate_query: bare OFFSET is pushed down (PostgreSQL)") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders OFFSET 10;");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);
    REQUIRE_FALSE(is_raw_sql_stub(node.node));

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::PostgreSQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    // PostgreSQL accepts a bare OFFSET, so no row-count sentinel is needed.
    REQUIRE(sql.find("OFFSET 10") != std::string::npos);
    REQUIRE(sql.find("LIMIT") == std::string::npos);
}

TEST_CASE("generate_query: bare OFFSET is pushed down (MySQL)") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders OFFSET 10;");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    // MySQL's grammar requires a row_count before OFFSET; the manual documents
    // 2^64-1 for "everything from here on". The offset itself must reach the wire
    // and the unlimit sentinel must never leak through as a negative literal.
    // A bare `OFFSET 10` is a MySQL syntax error, so the whole clause is pinned.
    REQUIRE(sql.find("OFFSET 10") != std::string::npos);
    REQUIRE(sql.find("LIMIT -1") == std::string::npos);
    REQUIRE(sql.find(" LIMIT 18446744073709551615 OFFSET 10;") != std::string::npos);
}

TEST_CASE("generate_query: bare OFFSET is pushed down (ClickHouse)") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // ClickHouse adds limit+offset without saturating: the MySQL sentinel would
    // overflow to zero rows, so it must get the bare OFFSET.
    auto sql =
        sql_for(parser, "SELECT * FROM mysql.bill.schema.orders OFFSET 10;", backend_type_t::ClickHouse, resource);
    REQUIRE(sql.find(" OFFSET 10;") != std::string::npos);
    REQUIRE(sql.find("LIMIT") == std::string::npos);
}

TEST_CASE("generate_query: LIMIT ALL OFFSET n keeps the OFFSET") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // LIMIT ALL parses to A_Const/T_Null, which leaves limit_ at the unlimit
    // sentinel — the same node shape as a bare OFFSET.
    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders LIMIT ALL OFFSET 7;");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::PostgreSQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    REQUIRE(sql.find("OFFSET 7") != std::string::npos);
}

TEST_CASE("generate_query: LIMIT ALL OFFSET n for ClickHouse and MySQL") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const std::string query = "SELECT * FROM mysql.bill.schema.orders LIMIT ALL OFFSET 7;";

    auto ch = sql_for(parser, query, backend_type_t::ClickHouse, resource);
    REQUIRE(ch.find(" OFFSET 7;") != std::string::npos);
    REQUIRE(ch.find("LIMIT") == std::string::npos);

    auto mysql = sql_for(parser, query, backend_type_t::MySQL, resource);
    REQUIRE(mysql.find(" LIMIT 18446744073709551615 OFFSET 7;") != std::string::npos);
}

TEST_CASE("generate_query: OFFSET 0 emits no limit clause at all") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // Degenerate (-1, 0) node: nothing to push down on any backend.
    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders OFFSET 0;");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    REQUIRE(sql.find("LIMIT") == std::string::npos);
    REQUIRE(sql.find("OFFSET") == std::string::npos);
}

TEST_CASE("generate_query: LIMIT 0 emits LIMIT 0") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // Pins the >= 0 boundary the bug class lives on: LIMIT 0 is a real window.
    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders LIMIT 0;");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    REQUIRE(sql.find("LIMIT 0") != std::string::npos);
}

TEST_CASE("generate_query: negative LIMIT is invalid_parameter, not silently dropped") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    // -1 is the unlimit sentinel; anything below it is a corrupt window.
    auto node = orders_select(resource);
    node->append_child(orders_limit(resource, limit_t(-5)));

    auto gen = generate_manual(node, params, backend_type_t::MySQL, resource);
    REQUIRE(gen.has_error());
    REQUIRE(gen.error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("generate_query: negative OFFSET is invalid_parameter, not silently dropped") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    auto node = orders_select(resource);
    node->append_child(orders_limit(resource, limit_t(-1, -3)));

    for (auto backend : {backend_type_t::MySQL, backend_type_t::PostgreSQL, backend_type_t::ClickHouse}) {
        auto gen = generate_manual(node, params, backend, resource);
        INFO("backend = " << static_cast<int>(backend));
        REQUIRE(gen.has_error());
        REQUIRE(gen.error().type == core::error_code_t::invalid_parameter);
    }
}

// ── HAVING ────────────────────────────────────────────────────────────────────
// HAVING is a first-class node_having_t child of the aggregate, and
// otterbrix/parser/parser.cpp treats having_t as an outer-aggregate marker. The
// node is replaced by the fetched rows, so the backend has to apply the
// predicate itself — wrong ROWS otherwise, not just a wrong window.

TEST_CASE("generate_query: HAVING is pushed down to the remote backend SQL") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "SELECT category, SUM(price) AS total FROM mysql.bill.schema.orders "
                               "GROUP BY category HAVING SUM(price) > 100;");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);
    REQUIRE_FALSE(is_raw_sql_stub(node.node));

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    REQUIRE(sql.find("GROUP BY") != std::string::npos);
    REQUIRE(sql.find("HAVING") != std::string::npos);
    // SQL clause order: HAVING must follow GROUP BY.
    REQUIRE(sql.find("GROUP BY") < sql.find("HAVING"));
}

TEST_CASE("generate_query: HAVING renders between GROUP BY and ORDER BY / LIMIT") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "SELECT category, COUNT(*) AS cnt FROM mysql.bill.schema.orders "
                               "GROUP BY category HAVING COUNT(*) > 2 ORDER BY category ASC LIMIT 5;");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::PostgreSQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    const auto group = sql.find("GROUP BY");
    const auto having = sql.find("HAVING");
    const auto order = sql.find("ORDER BY");
    const auto limit = sql.find("LIMIT");
    REQUIRE(having != std::string::npos);
    REQUIRE(group < having);
    REQUIRE(having < order);
    REQUIRE(order < limit);
}

TEST_CASE("generate_query: no HAVING clause when the query has none") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // Negative control: a plain GROUP BY must not grow a bare HAVING.
    auto parsed = parse_or_die(parser,
                               "SELECT category, SUM(price) AS total FROM mysql.bill.schema.orders "
                               "GROUP BY category;");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    REQUIRE(sql.find("HAVING") == std::string::npos);
}

// The transformer rewrites a HAVING aggregate into a reference to the group
// aggregate's output alias. PostgreSQL does not resolve SELECT aliases inside
// HAVING, and a HAVING-only aggregate gets a hidden __having_<fn>_<n> alias that
// exists nowhere on the backend: the generated HAVING must spell the aggregate
// call itself, and the hidden alias must never appear in the statement.

TEST_CASE("generate_query: HAVING spells the aggregate call — full statement per backend") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const std::string query = "SELECT category, SUM(price) AS total FROM mysql.bill.schema.orders "
                              "GROUP BY category HAVING SUM(price) > 100;";

    REQUIRE(sql_for(parser, query, backend_type_t::PostgreSQL, resource) ==
            "SELECT \"category\", SUM(\"price\") AS \"total\" FROM \"schema\".\"orders\" "
            "GROUP BY \"category\" HAVING SUM(\"price\") > 100;");
    REQUIRE(sql_for(parser, query, backend_type_t::MySQL, resource) ==
            "SELECT `category`, SUM(`price`) AS `total` FROM `bill`.`orders` "
            "GROUP BY `category` HAVING SUM(`price`) > 100;");
    REQUIRE(sql_for(parser, query, backend_type_t::ClickHouse, resource) ==
            "SELECT `category`, SUM(`price`) AS `total` FROM `bill`.`orders` "
            "GROUP BY `category` HAVING SUM(`price`) > 100;");
}

TEST_CASE("generate_query: aggregate only in HAVING stays out of the SELECT list") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql = sql_for(parser,
                       "SELECT category FROM mysql.bill.schema.orders GROUP BY category HAVING SUM(price) > 100;",
                       backend_type_t::PostgreSQL,
                       resource);
    REQUIRE(sql == "SELECT \"category\" FROM \"schema\".\"orders\" GROUP BY \"category\" HAVING SUM(\"price\") > 100;");
    REQUIRE(sql.find("__having_") == std::string::npos);
}

TEST_CASE("generate_query: arithmetic over an aggregate in HAVING is rendered, not dropped") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql = sql_for(parser,
                       "SELECT category, SUM(price) AS total FROM mysql.bill.schema.orders "
                       "GROUP BY category HAVING SUM(price) * 2 > 100;",
                       backend_type_t::PostgreSQL,
                       resource);
    REQUIRE(sql.find("HAVING (SUM(\"price\") * 2) > 100;") != std::string::npos);
    REQUIRE(sql.find("HAVING  >") == std::string::npos);
}

// Two aggregates of one function differing only in their argument: the HAVING
// operand must bind to the SUM whose argument list matches, not to the first
// SUM of the SELECT list — resolving it by function name alone writes SUM(a).
TEST_CASE("generate_query: two aggregates of one function with different arguments in HAVING",
          "[engine-defect-having]") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql = sql_for(parser,
                       "SELECT category, SUM(a) AS sa, SUM(b) AS sb FROM mysql.bill.schema.orders "
                       "GROUP BY category HAVING SUM(b) > 1;",
                       backend_type_t::PostgreSQL,
                       resource);
    REQUIRE(sql.find("HAVING SUM(\"b\") > 1;") != std::string::npos);
}

// ── literal escaping ──────────────────────────────────────────────────────────
// Every parameter site renders through one backend-aware writer. An apostrophe
// in a parameter closes the literal early unless doubled — a syntax error at
// best, an injection into the generated remote statement for a chosen value.

TEST_CASE("generate_query: string parameter is escaped for PostgreSQL") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // SQL-level '' is one apostrophe: the parameter value is O'Brien.
    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders WHERE note = 'O''Brien';");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::PostgreSQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    REQUIRE(sql.find("'O''Brien'") != std::string::npos);
    REQUIRE(sql.find("'O'Brien'") == std::string::npos);
}

TEST_CASE("generate_query: string parameter is escaped for ClickHouse") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders WHERE note = 'O''Brien';");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::ClickHouse,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    REQUIRE(sql.find("'O''Brien'") != std::string::npos);
}

TEST_CASE("generate_query: string parameter is escaped for MySQL too") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT * FROM mysql.bill.schema.orders WHERE note = 'O''Brien';");

    auto nodes = flat_externals(parsed);
    auto node = find_by_uid(nodes, "mysql");
    REQUIRE(node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       node.target,
                                       batch_targets_of(parsed, node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    // '' is standard SQL and MySQL honours it in every sql_mode, including
    // NO_BACKSLASH_ESCAPES where a backslash escape would not work.
    REQUIRE(sql.find("'O''Brien'") != std::string::npos);
    REQUIRE(sql.find("'O'Brien'") == std::string::npos);
}

TEST_CASE("generate_values: apostrophes are escaped on every backend") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(components::types::logical_type::STRING_LITERAL, "note");

    components::vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, components::types::logical_value_t{resource, "O'Brien"});
    chunk.set_cardinality(1);

    for (auto backend : {backend_type_t::MySQL, backend_type_t::PostgreSQL, backend_type_t::ClickHouse}) {
        std::stringstream ss;
        auto err = sql_gen::generate_values(ss, chunk, backend, resource);
        INFO("backend = " << static_cast<int>(backend));
        REQUIRE_FALSE(err.contains_error());
        REQUIRE(ss.str().find("'O''Brien'") != std::string::npos);
        REQUIRE(ss.str().find("'O'Brien'") == std::string::npos);
    }
}

// MySQL (default sql_mode) and ClickHouse read a backslash as an escape: an
// unescaped trailing backslash swallows the closing quote, and `\'` opens the
// literal to injection. PostgreSQL (standard_conforming_strings) reads it
// literally, where doubling it would change the value.
TEST_CASE("generate_values: backslash is escaped per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(components::types::logical_type::STRING_LITERAL, "note");

    components::vector::data_chunk_t chunk(resource, fields);
    chunk.set_cardinality(1);

    auto render = [&](const char* value, backend_type_t backend) {
        chunk.set_value(0, 0, components::types::logical_value_t{resource, value});
        std::stringstream ss;
        auto err = sql_gen::generate_values(ss, chunk, backend, resource);
        REQUIRE_FALSE(err.contains_error());
        return single_value_literal(ss.str());
    };

    REQUIRE(render("a\\", backend_type_t::MySQL) == "'a\\\\'");
    REQUIRE(render("a\\", backend_type_t::ClickHouse) == "'a\\\\'");
    REQUIRE(render("a\\", backend_type_t::PostgreSQL) == "'a\\'");

    REQUIRE(render("\\' OR 1=1 -- ", backend_type_t::MySQL) == "'\\\\'' OR 1=1 -- '");
    REQUIRE(render("\\' OR 1=1 -- ", backend_type_t::ClickHouse) == "'\\\\'' OR 1=1 -- '");
    REQUIRE(render("\\' OR 1=1 -- ", backend_type_t::PostgreSQL) == "'\\'' OR 1=1 -- '");
}

TEST_CASE("generate_values: TINYINT and UTINYINT render as numbers, not characters") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(components::types::logical_type::TINYINT, "t");
    fields.emplace_back(components::types::logical_type::UTINYINT, "u");

    components::vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, components::types::logical_value_t{resource, int8_t{65}});
    chunk.set_value(1, 0, components::types::logical_value_t{resource, uint8_t{200}});
    chunk.set_cardinality(1);

    std::stringstream ss;
    auto err = sql_gen::generate_values(ss, chunk, backend_type_t::MySQL, resource);
    REQUIRE_FALSE(err.contains_error());
    REQUIRE(ss.str() == "VALUES (65, 200)");

    // 39 is the apostrophe: streamed as a char it would open a literal.
    chunk.set_value(0, 0, components::types::logical_value_t{resource, int8_t{39}});
    std::stringstream quote;
    err = sql_gen::generate_values(quote, chunk, backend_type_t::MySQL, resource);
    REQUIRE_FALSE(err.contains_error());
    REQUIRE(quote.str() == "VALUES (39, 200)");
}

TEST_CASE("generate_values: FLOAT and DOUBLE literals round-trip exactly") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(components::types::logical_type::DOUBLE, "d");
    components::vector::data_chunk_t doubles(resource, fields);
    doubles.set_cardinality(1);

    for (double value : {0.1, 1234567.125, 1e-7, 3.141592653589793, -2.5e300}) {
        doubles.set_value(0, 0, components::types::logical_value_t{resource, value});
        std::stringstream ss;
        auto err = sql_gen::generate_values(ss, doubles, backend_type_t::PostgreSQL, resource);
        REQUIRE_FALSE(err.contains_error());
        INFO("literal: " << ss.str());
        REQUIRE(std::stod(single_value_literal(ss.str())) == value);
    }

    std::pmr::vector<components::types::complex_logical_type> float_fields(resource);
    float_fields.emplace_back(components::types::logical_type::FLOAT, "f");
    components::vector::data_chunk_t floats(resource, float_fields);
    floats.set_cardinality(1);

    for (float value : {0.1f, 1234.5678f, 1e-7f, 3.1415927f}) {
        floats.set_value(0, 0, components::types::logical_value_t{resource, value});
        std::stringstream ss;
        auto err = sql_gen::generate_values(ss, floats, backend_type_t::MySQL, resource);
        REQUIRE_FALSE(err.contains_error());
        INFO("literal: " << ss.str());
        REQUIRE(std::stof(single_value_literal(ss.str())) == value);
    }
}

TEST_CASE("generate_values: an unsupported value type is unimplemented_yet, not broken SQL") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(components::types::logical_type::DATE, "d");
    components::vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, components::types::logical_value_t{resource, core::date::date_t{core::date::days{1}}});
    chunk.set_cardinality(1);

    std::stringstream ss;
    auto err = sql_gen::generate_values(ss, chunk, backend_type_t::MySQL, resource);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_values: a backend without a dialect is invalid_parameter") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(components::types::logical_type::INTEGER, "i");
    components::vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, components::types::logical_value_t{resource, 1});
    chunk.set_cardinality(1);

    std::stringstream ss;
    auto err = sql_gen::generate_values(ss, chunk, backend_type_t::Unknown, resource);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::invalid_parameter);
}

TEST_CASE("generate_query: string parameter is escaped in UPDATE SET and in a SELECT constant") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto set_sql = sql_for(parser,
                           "UPDATE mysql.bill.schema.orders SET name = 'O''Brien' WHERE id > 0;",
                           backend_type_t::PostgreSQL,
                           resource);
    REQUIRE(set_sql.find("SET \"name\" = 'O''Brien'") != std::string::npos);
    REQUIRE(set_sql.find("'O'Brien'") == std::string::npos);

    auto select_sql =
        sql_for(parser, "SELECT 'O''Brien' AS n FROM mysql.bill.schema.orders;", backend_type_t::PostgreSQL, resource);
    REQUIRE(select_sql.find("SELECT 'O''Brien' AS \"n\" FROM") != std::string::npos);
}

// ── unsupported values inside a statement ─────────────────────────────────────
// A value the writer cannot spell must fail the whole statement; the earlier
// void writers left the error unread and the broken SQL went to the backend.

TEST_CASE("generate_query: unsupported parameter type in WHERE fails the statement") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    auto id = params.add_parameter(logical_value_t{resource, core::date::date_t{core::date::days{1}}});
    auto node = orders_select(resource);
    node->append_child(
        orders_match(resource, make_compare_expression(resource, compare_type::eq, components::expressions::key_t{resource, "d"}, id)));

    auto gen = generate_manual(node, params, backend_type_t::MySQL, resource);
    REQUIRE(gen.has_error());
    REQUIRE(gen.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: unsupported parameter type in UPDATE SET fails the statement") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    auto id = params.add_parameter(logical_value_t{resource, core::date::date_t{core::date::days{1}}});
    // An assignment is the value expression itself, named by the column it
    // assigns: here the value is the bound parameter.
    std::pmr::vector<expression_ptr> updates{resource};
    auto assigned =
        make_scalar_expression(resource, scalar_type::constant, components::expressions::key_t{resource, "d"});
    assigned->append_param(id);
    updates.emplace_back(assigned);
    auto node = make_node_update(resource,
                                 orders_match(resource, make_compare_expression(resource, compare_type::all_true)),
                                 orders_limit(resource, limit_t::unlimit()),
                                 updates);

    auto gen = generate_manual(node, params, backend_type_t::MySQL, resource);
    REQUIRE(gen.has_error());
    REQUIRE(gen.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: unbound parameter is invalid_parameter, never NULL") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    auto node = orders_select(resource);
    node->append_child(orders_match(
        resource,
        make_compare_expression(resource, compare_type::eq, components::expressions::key_t{resource, "id"}, core::parameter_id_t{99})));

    auto gen = generate_manual(node, params, backend_type_t::PostgreSQL, resource);
    REQUIRE(gen.has_error());
    REQUIRE(gen.error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("generate_query: a backend without a dialect is invalid_parameter") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    auto node = orders_select(resource);
    for (auto backend : {backend_type_t::Unknown, backend_type_t::Mixed, backend_type_t::Otterbrix}) {
        auto gen = generate_manual(node, params, backend, resource);
        INFO("backend = " << static_cast<int>(backend));
        REQUIRE(gen.has_error());
        REQUIRE(gen.error().type == core::error_code_t::invalid_parameter);
    }
}

// A statement whose target has no name has no table to name on the backend;
// a placeholder identifier would reach the backend as an unknown table.
TEST_CASE("generate_query: an empty target name is invalid_parameter and never a placeholder table") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);
    const otterstax::names::resolved_target_t nameless{components::catalog::INVALID_OID, qualified_name_t{}, {}};

    node_ptr drop = make_node_drop(resource, drop_target_kind::collection);
    node_ptr select = orders_select(resource);
    for (auto backend : {backend_type_t::MySQL, backend_type_t::PostgreSQL, backend_type_t::ClickHouse}) {
        INFO("backend = " << static_cast<int>(backend));
        auto dropped = generate_manual(drop, params, backend, resource, nameless);
        REQUIRE(dropped.has_error());
        REQUIRE(dropped.error().type == core::error_code_t::invalid_parameter);
        auto selected = generate_manual(select, params, backend, resource, nameless);
        REQUIRE(selected.has_error());
        REQUIRE(selected.error().type == core::error_code_t::invalid_parameter);
    }
}

// ── expression operands ───────────────────────────────────────────────────────

TEST_CASE("generate_query: arithmetic in the SELECT list is rendered with its alias") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql = sql_for(parser, "SELECT a + 1 AS x FROM mysql.bill.schema.orders;", backend_type_t::MySQL, resource);
    REQUIRE(sql == "SELECT (`a` + 1) AS `x` FROM `bill`.`orders`;");

    auto nested =
        sql_for(parser, "SELECT (a + 1) * 2 AS x FROM mysql.bill.schema.orders;", backend_type_t::PostgreSQL, resource);
    REQUIRE(nested == "SELECT ((\"a\" + 1) * 2) AS \"x\" FROM \"schema\".\"orders\";");
}

TEST_CASE("generate_query: arithmetic operand in WHERE is rendered, not dropped") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql =
        sql_for(parser, "SELECT * FROM mysql.bill.schema.orders WHERE a + 1 = 2;", backend_type_t::MySQL, resource);
    REQUIRE(sql == "SELECT * FROM `bill`.`orders` WHERE (`a` + 1) = 2;");
}

TEST_CASE("generate_query: aggregate over an expression keeps its argument") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql = sql_for(parser,
                       "SELECT SUM(price * qty) AS s FROM mysql.bill.schema.orders;",
                       backend_type_t::MySQL,
                       resource);
    REQUIRE(sql.find("SUM((`price` * `qty`)) AS `s`") != std::string::npos);
    REQUIRE(sql.find("SUM()") == std::string::npos);
}

TEST_CASE("generate_query: COUNT(DISTINCT col) keeps DISTINCT") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql = sql_for(parser,
                       "SELECT COUNT(DISTINCT category) AS c FROM mysql.bill.schema.orders;",
                       backend_type_t::PostgreSQL,
                       resource);
    REQUIRE(sql.find("COUNT(DISTINCT \"category\") AS \"c\"") != std::string::npos);
}

TEST_CASE("generate_query: SELECT DISTINCT is pushed down") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql =
        sql_for(parser, "SELECT DISTINCT category FROM mysql.bill.schema.orders;", backend_type_t::MySQL, resource);
    REQUIRE(sql == "SELECT DISTINCT `category` FROM `bill`.`orders`;");
}

TEST_CASE("generate_query: DISTINCT ON is PostgreSQL-only") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const std::string query =
        "SELECT DISTINCT ON (category) category, price FROM mysql.bill.schema.orders ORDER BY category;";

    auto pg = sql_for(parser, query, backend_type_t::PostgreSQL, resource);
    REQUIRE(pg.find("SELECT DISTINCT ON (\"category\") \"category\", \"price\" FROM") != std::string::npos);

    REQUIRE(error_for(parser, query, backend_type_t::MySQL, resource) == core::error_code_t::unimplemented_yet);
    REQUIRE(error_for(parser, query, backend_type_t::ClickHouse, resource) == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: GROUP BY over an expression is rendered") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    auto node = orders_select(resource);
    auto group = make_node_group(resource, core::dbname_t{"bill"}, core::relname_t{"orders"});
    auto bucket = make_scalar_expression(resource, scalar_type::add);
    bucket->append_param(components::expressions::key_t{resource, "a"});
    bucket->append_param(params.add_parameter(logical_value_t{resource, 1}));
    auto key = make_scalar_expression(resource, scalar_type::group_field, components::expressions::key_t{resource, "bucket"});
    key->append_param(param_storage{expression_ptr{bucket}});
    group->append_expression(key);
    group->append_expression(make_aggregate_expression(resource, "count", components::expressions::key_t{resource, "count"}));
    node->append_child(group);

    auto gen = generate_manual(node, params, backend_type_t::MySQL, resource);
    REQUIRE_FALSE(gen.has_error());
    REQUIRE(gen.value() == "SELECT COUNT(*) AS `count` FROM `bill`.`orders` GROUP BY (`a` + 1);");
}

// ── predicates ────────────────────────────────────────────────────────────────

TEST_CASE("generate_query: IS NULL / IS NOT NULL are rendered") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto is_null = sql_for(parser,
                           "SELECT * FROM mysql.bill.schema.orders WHERE note IS NULL;",
                           backend_type_t::PostgreSQL,
                           resource);
    REQUIRE(is_null =="SELECT * FROM \"schema\".\"orders\" WHERE \"note\" IS NULL;");

    auto is_not_null = sql_for(parser,
                               "SELECT * FROM mysql.bill.schema.orders WHERE note IS NOT NULL;",
                               backend_type_t::MySQL,
                               resource);
    REQUIRE(is_not_null == "SELECT * FROM `bill`.`orders` WHERE `note` IS NOT NULL;");
}

TEST_CASE("generate_query: NOT renders as NOT (...), never the PostgreSQL-only !( form") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    for (auto backend : {backend_type_t::MySQL, backend_type_t::PostgreSQL, backend_type_t::ClickHouse}) {
        auto sql = sql_for(parser, "SELECT * FROM mysql.bill.schema.orders WHERE NOT (id > 0);", backend, resource);
        INFO("backend = " << static_cast<int>(backend) << " sql = " << sql);
        REQUIRE(sql.find("WHERE NOT (") != std::string::npos);
        REQUIRE(sql.find("!(") == std::string::npos);
    }
}

TEST_CASE("generate_query: LIKE is rendered as LIKE and never as a regular expression") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // The engine lowers LIKE to regexp_like(subject, pattern, flags) whose
    // parameter carries the LIKE pattern itself, not a regular expression: a
    // regex spelling (`~`, REGEXP, match()) would read `%` and `_` literally and
    // match nothing.
    const std::string query = "SELECT * FROM mysql.bill.schema.orders WHERE name LIKE 'a%';";
    REQUIRE(sql_for(parser, query, backend_type_t::PostgreSQL, resource) ==
            "SELECT * FROM \"schema\".\"orders\" WHERE \"name\" LIKE 'a%';");
    REQUIRE(sql_for(parser, query, backend_type_t::MySQL, resource) ==
            "SELECT * FROM `bill`.`orders` WHERE `name` LIKE 'a%';");
    REQUIRE(sql_for(parser, query, backend_type_t::ClickHouse, resource) ==
            "SELECT * FROM `bill`.`orders` WHERE `name` LIKE 'a%';");

    // Negation is the `n` flag, not a wrapping NOT, so it stays one predicate.
    const std::string negated = "SELECT * FROM mysql.bill.schema.orders WHERE name NOT LIKE 'a%';";
    REQUIRE(sql_for(parser, negated, backend_type_t::PostgreSQL, resource) ==
            "SELECT * FROM \"schema\".\"orders\" WHERE \"name\" NOT LIKE 'a%';");
    REQUIRE(sql_for(parser, negated, backend_type_t::MySQL, resource) ==
            "SELECT * FROM `bill`.`orders` WHERE `name` NOT LIKE 'a%';");

    // MySQL has no ILIKE: LOWER() on both sides is case-insensitive whatever the
    // column collation.
    const std::string icase = "SELECT * FROM mysql.bill.schema.orders WHERE name ILIKE 'a%';";
    REQUIRE(sql_for(parser, icase, backend_type_t::PostgreSQL, resource) ==
            "SELECT * FROM \"schema\".\"orders\" WHERE \"name\" ILIKE 'a%';");
    REQUIRE(sql_for(parser, icase, backend_type_t::ClickHouse, resource) ==
            "SELECT * FROM `bill`.`orders` WHERE `name` ILIKE 'a%';");
    REQUIRE(sql_for(parser, icase, backend_type_t::MySQL, resource) ==
            "SELECT * FROM `bill`.`orders` WHERE LOWER(`name`) LIKE LOWER('a%');");
}

TEST_CASE("generate_query: ANY / ALL predicates are unimplemented_yet, not silently emptied") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    auto id = params.add_parameter(logical_value_t{resource, logical_type::NA});
    for (auto type : {compare_type::any, compare_type::all}) {
        auto node = orders_select(resource);
        node->append_child(
            orders_match(resource, make_compare_expression(resource, type, components::expressions::key_t{resource, "id"}, id)));
        auto gen = generate_manual(node, params, backend_type_t::PostgreSQL, resource);
        REQUIRE(gen.has_error());
        REQUIRE(gen.error().type == core::error_code_t::unimplemented_yet);
    }
}

TEST_CASE("generate_query: ARRAY literal per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    std::vector<logical_value_t> elements{logical_value_t{resource, 1}, logical_value_t{resource, 2}};
    auto id = params.add_parameter(
        logical_value_t::create_array(resource, complex_logical_type{logical_type::INTEGER}, elements));
    auto node = orders_select(resource);
    node->append_child(
        orders_match(resource, make_compare_expression(resource, compare_type::eq, components::expressions::key_t{resource, "tags"}, id)));

    auto pg = generate_manual(node, params, backend_type_t::PostgreSQL, resource);
    REQUIRE_FALSE(pg.has_error());
    REQUIRE(pg.value() == "SELECT * FROM \"schema\".\"orders\" WHERE \"tags\" = ARRAY[1, 2];");

    auto ch = generate_manual(node, params, backend_type_t::ClickHouse, resource);
    REQUIRE_FALSE(ch.has_error());
    REQUIRE(ch.value() == "SELECT * FROM `bill`.`orders` WHERE `tags` = [1, 2];");

    auto mysql = generate_manual(node, params, backend_type_t::MySQL, resource);
    REQUIRE(mysql.has_error());
    REQUIRE(mysql.error().type == core::error_code_t::unimplemented_yet);
}

// ── ORDER BY ──────────────────────────────────────────────────────────────────

TEST_CASE("generate_query: NULLS FIRST / LAST reach the backend") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const std::string query = "SELECT * FROM mysql.bill.schema.orders ORDER BY category ASC NULLS FIRST;";

    auto pg = sql_for(parser, query, backend_type_t::PostgreSQL, resource);
    REQUIRE(pg.find("ORDER BY \"category\" ASC NULLS FIRST;") != std::string::npos);

    auto ch = sql_for(parser, query, backend_type_t::ClickHouse, resource);
    REQUIRE(ch.find("ORDER BY `category` ASC NULLS FIRST;") != std::string::npos);

    // MySQL has no NULLS FIRST/LAST: an IS NULL sort key ahead of the column
    // pins the NULL rows to the requested end.
    auto mysql = sql_for(parser, query, backend_type_t::MySQL, resource);
    REQUIRE(mysql.find("ORDER BY `category` IS NULL DESC, `category` ASC;") != std::string::npos);

    auto last = sql_for(parser,
                        "SELECT * FROM mysql.bill.schema.orders ORDER BY category DESC NULLS LAST;",
                        backend_type_t::MySQL,
                        resource);
    REQUIRE(last.find("ORDER BY `category` IS NULL ASC, `category` DESC;") != std::string::npos);
}

// ── DML ───────────────────────────────────────────────────────────────────────

// The grammar accepts DML LIMIT (DELETE/UPDATE ... LIMIT n) and attaches
// a node_limit_t child. Regression: the generator used to ignore it, silently
// deleting/updating every matching remote row.
TEST_CASE("generate_query: DELETE ... LIMIT reaches MySQL SQL, returns unimplemented_yet for PostgreSQL") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "DELETE FROM mysql.bill.schema.orders WHERE id > 0 LIMIT 5;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();
    REQUIRE(sql.find("LIMIT 5") != std::string::npos);

    auto rejected = sql_gen::generate_query(mysql_node.node,
                                           &params,
                                           backend_type_t::PostgreSQL,
                                           mysql_node.target,
                                           batch_targets_of(parsed, mysql_node), resource);
    REQUIRE(rejected.has_error());
    REQUIRE(rejected.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: UPDATE ... LIMIT reaches MySQL SQL, returns unimplemented_yet for PostgreSQL") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "UPDATE mysql.bill.schema.orders SET name = 'x' WHERE id > 0 LIMIT 3;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();
    REQUIRE(sql.find("LIMIT 3") != std::string::npos);

    auto rejected = sql_gen::generate_query(mysql_node.node,
                                           &params,
                                           backend_type_t::PostgreSQL,
                                           mysql_node.target,
                                           batch_targets_of(parsed, mysql_node), resource);
    REQUIRE(rejected.has_error());
    REQUIRE(rejected.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: DELETE / UPDATE ... LIMIT return unimplemented_yet for ClickHouse") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    REQUIRE(error_for(parser,
                      "DELETE FROM mysql.bill.schema.orders WHERE id > 0 LIMIT 5;",
                      backend_type_t::ClickHouse,
                      resource) == core::error_code_t::unimplemented_yet);
    REQUIRE(error_for(parser,
                      "UPDATE mysql.bill.schema.orders SET name = 'x' WHERE id > 0 LIMIT 3;",
                      backend_type_t::ClickHouse,
                      resource) == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: DELETE without LIMIT emits no LIMIT clause") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "DELETE FROM mysql.bill.schema.orders WHERE id > 0;");

    auto nodes = flat_externals(parsed);
    auto mysql_node = find_by_uid(nodes, "mysql");
    REQUIRE(mysql_node.node);

    const auto& params = parsed->otterbrix_params->params_node->parameters();
    auto gen = sql_gen::generate_query(mysql_node.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       mysql_node.target,
                                       batch_targets_of(parsed, mysql_node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();
    REQUIRE(sql.find("LIMIT") == std::string::npos);
}

TEST_CASE("generate_query: DELETE without WHERE emits no WHERE clause") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // The transformer attaches a match-everything predicate; on the backend
    // that is the absence of WHERE, not a dangling one.
    auto sql = sql_for(parser, "DELETE FROM mysql.bill.schema.orders;", backend_type_t::MySQL, resource);
    REQUIRE(sql == "DELETE FROM `bill`.`orders`;");
}

TEST_CASE("generate_query: UPDATE emits SET once with comma-separated assignments") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql = sql_for(parser,
                       "UPDATE mysql.bill.schema.orders SET a = 1, b = 2 WHERE id > 0;",
                       backend_type_t::MySQL,
                       resource);
    REQUIRE(sql == "UPDATE `bill`.`orders` SET `a` = 1, `b` = 2 WHERE `id` > 0;");
}

// ClickHouse 23.8 has no UPDATE statement: an in-place update is the
// ALTER TABLE ... UPDATE mutation, which takes the same assignments and a
// mandatory WHERE.
TEST_CASE("generate_query: UPDATE for ClickHouse is the ALTER TABLE ... UPDATE mutation") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto sql = sql_for(parser,
                       "UPDATE mysql.bill.schema.orders SET a = 1, b = 2 WHERE id > 0;",
                       backend_type_t::ClickHouse,
                       resource);
    REQUIRE(sql == "ALTER TABLE `bill`.`orders` UPDATE `a` = 1, `b` = 2 WHERE `id` > 0;");
}

// Both ClickHouse forms — lightweight DELETE and ALTER ... UPDATE — are a
// syntax error without WHERE; a statement with no predicate touches every
// row, which a constant-true filter spells. Other dialects keep emitting no
// clause (see "DELETE without WHERE emits no WHERE clause").
TEST_CASE("generate_query: DELETE / UPDATE without WHERE spell the mandatory ClickHouse filter as WHERE 1") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    REQUIRE(sql_for(parser, "DELETE FROM mysql.bill.schema.orders;", backend_type_t::ClickHouse, resource) ==
            "DELETE FROM `bill`.`orders` WHERE 1;");
    REQUIRE(sql_for(parser, "UPDATE mysql.bill.schema.orders SET a = 1;", backend_type_t::ClickHouse, resource) ==
            "ALTER TABLE `bill`.`orders` UPDATE `a` = 1 WHERE 1;");
    REQUIRE(sql_for(parser, "UPDATE mysql.bill.schema.orders SET a = 1;", backend_type_t::MySQL, resource) ==
            "UPDATE `bill`.`orders` SET `a` = 1;");
    REQUIRE(sql_for(parser, "UPDATE mysql.bill.schema.orders SET a = 1;", backend_type_t::PostgreSQL, resource) ==
            "UPDATE \"schema\".\"orders\" SET \"a\" = 1;");
}

TEST_CASE("generate_query: UPDATE SET operators are spelled per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // `^` is power in PostgreSQL but XOR in MySQL; ClickHouse has neither
    // operator, only functions.
    const std::string power = "UPDATE mysql.bill.schema.orders SET a = a ^ 2 WHERE id > 0;";
    REQUIRE(sql_for(parser, power, backend_type_t::PostgreSQL, resource).find("SET \"a\" = (\"a\" ^ 2)") !=
            std::string::npos);
    REQUIRE(sql_for(parser, power, backend_type_t::MySQL, resource).find("SET `a` = POWER(`a`, 2)") !=
            std::string::npos);
    // ClickHouse spells the statement as ALTER TABLE ... UPDATE, so the
    // assignment follows UPDATE rather than SET.
    REQUIRE(sql_for(parser, power, backend_type_t::ClickHouse, resource).find("UPDATE `a` = pow(`a`, 2)") !=
            std::string::npos);

    const std::string xor_op = "UPDATE mysql.bill.schema.orders SET a = a # 3 WHERE id > 0;";
    REQUIRE(sql_for(parser, xor_op, backend_type_t::PostgreSQL, resource).find("(\"a\" # 3)") != std::string::npos);
    REQUIRE(sql_for(parser, xor_op, backend_type_t::MySQL, resource).find("(`a` ^ 3)") != std::string::npos);
    REQUIRE(sql_for(parser, xor_op, backend_type_t::ClickHouse, resource).find("bitXor(`a`, 3)") !=
            std::string::npos);

    const std::string shift = "UPDATE mysql.bill.schema.orders SET a = a << 1 WHERE id > 0;";
    REQUIRE(sql_for(parser, shift, backend_type_t::MySQL, resource).find("(`a` << 1)") != std::string::npos);
    REQUIRE(sql_for(parser, shift, backend_type_t::ClickHouse, resource).find("bitShiftLeft(`a`, 1)") !=
            std::string::npos);

    const std::string sqrt = "UPDATE mysql.bill.schema.orders SET a = |/ a WHERE id > 0;";
    REQUIRE(sql_for(parser, sqrt, backend_type_t::PostgreSQL, resource).find("SQRT(\"a\")") != std::string::npos);
    REQUIRE(sql_for(parser, sqrt, backend_type_t::MySQL, resource).find("SQRT(`a`)") != std::string::npos);
    REQUIRE(sql_for(parser, sqrt, backend_type_t::ClickHouse, resource).find("sqrt(`a`)") != std::string::npos);
}

TEST_CASE("generate_query: DELETE ... USING with a resolved second table per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // The plan keeps the USING source as a child of the delete node, and the
    // parser resolves that child into the slot's from_name — which is what
    // makes the statement one two-table DELETE on the backend.
    auto parsed =
        parse_or_die(parser, "DELETE FROM mysql.bill.schema.orders USING mysql.bill.schema.other WHERE id > 0;");
    auto nodes = flat_externals(parsed);
    auto slot = find_by_uid(nodes, "mysql");
    REQUIRE(slot.node);
    REQUIRE(slot.node->type() == node_type::delete_t);
    const auto& target = slot.target;
    REQUIRE(target.from_name == qualified_name_t{"mysql", "bill", "schema", "other"});
    const auto& params = parsed->otterbrix_params->params_node->parameters();
    const auto& batch = batch_targets_of(parsed, slot);

    auto pg = sql_gen::generate_query(slot.node, &params, backend_type_t::PostgreSQL, target, batch, resource);
    REQUIRE_FALSE(pg.has_error());
    REQUIRE(pg.value() == "DELETE FROM \"schema\".\"orders\" USING \"schema\".\"other\" WHERE \"id\" > 0;");

    // MySQL's multi-table form names the table rows are deleted from before FROM
    // and every table it reads after it.
    auto mysql = sql_gen::generate_query(slot.node, &params, backend_type_t::MySQL, target, batch, resource);
    REQUIRE_FALSE(mysql.has_error());
    REQUIRE(mysql.value() == "DELETE `bill`.`orders` FROM `bill`.`orders`, `bill`.`other` WHERE `id` > 0;");

    auto ch = sql_gen::generate_query(slot.node, &params, backend_type_t::ClickHouse, target, batch, resource);
    REQUIRE(ch.has_error());
    REQUIRE(ch.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: DELETE ... USING together with LIMIT is unimplemented_yet") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "DELETE FROM mysql.bill.schema.orders USING mysql.bill.schema.other WHERE id > 0 LIMIT 5;");
    auto nodes = flat_externals(parsed);
    auto slot = find_by_uid(nodes, "mysql");
    REQUIRE(slot.node);
    auto target = slot.target;
    target.from_name = qualified_name_t{"mysql", "bill", "schema", "other"};
    const auto& params = parsed->otterbrix_params->params_node->parameters();

    // MySQL's multi-table DELETE takes no LIMIT.
    auto gen = sql_gen::generate_query(slot.node,
                                       &params,
                                       backend_type_t::MySQL,
                                       target,
                                       batch_targets_of(parsed, slot),
                                       resource);
    REQUIRE(gen.has_error());
    REQUIRE(gen.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: UPDATE ... FROM with a resolved second table per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // The plan keeps the FROM source as a child of the update node, and the
    // parser resolves that child into the slot's from_name — which is what
    // makes the statement one two-table UPDATE on the backend.
    auto parsed = parse_or_die(
        parser,
        "UPDATE mysql.bill.schema.orders SET name = 'x' FROM mysql.bill.schema.other WHERE id > 0;");
    auto nodes = flat_externals(parsed);
    auto slot = find_by_uid(nodes, "mysql");
    REQUIRE(slot.node);
    REQUIRE(slot.node->type() == node_type::update_t);
    const auto& target = slot.target;
    REQUIRE(target.from_name == qualified_name_t{"mysql", "bill", "schema", "other"});
    const auto& params = parsed->otterbrix_params->params_node->parameters();
    const auto& batch = batch_targets_of(parsed, slot);

    auto pg = sql_gen::generate_query(slot.node, &params, backend_type_t::PostgreSQL, target, batch, resource);
    REQUIRE_FALSE(pg.has_error());
    REQUIRE(pg.value() ==
            "UPDATE \"schema\".\"orders\" SET \"name\" = 'x' FROM \"schema\".\"other\" WHERE \"id\" > 0;");

    // MySQL's multi-table form names every table before SET, and the assigned
    // column carries the target's qualifier: a name the target does not have
    // would otherwise resolve against the source and be written there.
    auto mysql = sql_gen::generate_query(slot.node, &params, backend_type_t::MySQL, target, batch, resource);
    REQUIRE_FALSE(mysql.has_error());
    REQUIRE(mysql.value() ==
            "UPDATE `bill`.`orders`, `bill`.`other` SET `bill`.`orders`.`name` = 'x' WHERE `id` > 0;");

    auto ch = sql_gen::generate_query(slot.node, &params, backend_type_t::ClickHouse, target, batch, resource);
    REQUIRE(ch.has_error());
    REQUIRE(ch.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: DELETE ... USING with an unresolved source sub-plan is refused") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // The engine keeps the USING source as a child sub-plan of the delete node.
    // The parser resolves a second name only for a PLAIN table source; a join
    // tree, a derived table and a subquery stub leave from_name empty, which is
    // the shape pinned here. Without a resolved second table the statement
    // cannot be expressed on the backend; emitting a single-table DELETE would
    // delete the wrong rows.
    auto parsed = parse_or_die(parser,
                               "DELETE FROM mysql.bill.schema.orders USING mysql.bill.schema.other "
                               "WHERE orders.id = other.id;");
    auto nodes = flat_externals(parsed);
    flat_external_t slot;
    for (const auto& n : nodes) {
        if (n.node && n.node->type() == node_type::delete_t) {
            slot = n;
        }
    }
    REQUIRE(slot.node);
    auto target = slot.target;
    target.from_name = qualified_name_t{};
    const auto& params = parsed->otterbrix_params->params_node->parameters();

    auto gen = sql_gen::generate_query(slot.node,
                                       &params,
                                       backend_type_t::PostgreSQL,
                                       target,
                                       batch_targets_of(parsed, slot),
                                       resource);
    REQUIRE(gen.has_error());
    REQUIRE(gen.error().type == core::error_code_t::unimplemented_yet);
}

// ── DDL ───────────────────────────────────────────────────────────────────────

TEST_CASE("generate_query: CREATE TABLE is qualified and typed per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const std::string query = "CREATE TABLE mysql.bill.schema.t (id BIGINT, ok BOOLEAN);";
    REQUIRE(sql_for(parser, query, backend_type_t::MySQL, resource) ==
            "CREATE TABLE `bill`.`t` (`id` bigint, `ok` boolean);");
    REQUIRE(sql_for(parser, query, backend_type_t::PostgreSQL, resource) ==
            "CREATE TABLE \"schema\".\"t\" (\"id\" int8, \"ok\" boolean);");
    REQUIRE(sql_for(parser, query, backend_type_t::ClickHouse, resource) ==
            "CREATE TABLE `bill`.`t` (`id` Int64, `ok` Bool);");
}

TEST_CASE("generate_query: CREATE TABLE column without a name is invalid_parameter") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    std::vector<components::table::column_definition_t> columns;
    columns.emplace_back(std::string{}, complex_logical_type{logical_type::BIGINT});
    node_ptr node = make_node_create_collection(resource,
                                                core::relname_t{"t"},
                                                std::move(columns),
                                                std::vector<components::table::table_constraint_t>{});

    auto gen = generate_manual(node, params, backend_type_t::MySQL, resource);
    REQUIRE(gen.has_error());
    REQUIRE(gen.error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("generate_query: DROP TABLE is qualified per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const std::string query = "DROP TABLE mysql.bill.schema.t;";
    REQUIRE(sql_for(parser, query, backend_type_t::MySQL, resource) == "DROP TABLE `bill`.`t`;");
    REQUIRE(sql_for(parser, query, backend_type_t::PostgreSQL, resource) == "DROP TABLE \"schema\".\"t\";");
    REQUIRE(sql_for(parser, query, backend_type_t::ClickHouse, resource) == "DROP TABLE `bill`.`t`;");
}

TEST_CASE("generate_query: DROP INDEX per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    node_ptr node = make_node_drop(resource, drop_target_kind::index);
    auto target = orders_target();
    target.from_name = qualified_name_t{"mysql", "bill", "schema", "idx_orders_id"};

    auto mysql = generate_manual(node, params, backend_type_t::MySQL, resource, target);
    REQUIRE_FALSE(mysql.has_error());
    REQUIRE(mysql.value() == "DROP INDEX `idx_orders_id` ON `bill`.`orders`;");

    auto pg = generate_manual(node, params, backend_type_t::PostgreSQL, resource, target);
    REQUIRE_FALSE(pg.has_error());
    REQUIRE(pg.value() == "DROP INDEX \"schema\".\"idx_orders_id\";");

    auto ch = generate_manual(node, params, backend_type_t::ClickHouse, resource, target);
    REQUIRE_FALSE(ch.has_error());
    REQUIRE(ch.value() == "ALTER TABLE `bill`.`orders` DROP INDEX `idx_orders_id`;");

    auto nameless = generate_manual(node, params, backend_type_t::MySQL, resource);
    REQUIRE(nameless.has_error());
    REQUIRE(nameless.error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("generate_query: CREATE INDEX is qualified; ClickHouse is unimplemented_yet") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    auto index = make_node_create_index(resource, core::indexname_t{std::string{"idx_orders_id"}});
    index->keys().emplace_back(resource, "id");
    node_ptr node = index;

    auto mysql = generate_manual(node, params, backend_type_t::MySQL, resource);
    REQUIRE_FALSE(mysql.has_error());
    REQUIRE(mysql.value() == "CREATE INDEX `idx_orders_id` ON `bill`.`orders` (`id`);");

    auto pg = generate_manual(node, params, backend_type_t::PostgreSQL, resource);
    REQUIRE_FALSE(pg.has_error());
    REQUIRE(pg.value() == "CREATE INDEX \"idx_orders_id\" ON \"schema\".\"orders\" (\"id\");");

    auto ch = generate_manual(node, params, backend_type_t::ClickHouse, resource);
    REQUIRE(ch.has_error());
    REQUIRE(ch.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: unsupported node type and drop kind are unimplemented_yet") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);

    node_ptr match = orders_match(resource, make_compare_expression(resource, compare_type::all_true));
    auto node_gen = generate_manual(match, params, backend_type_t::MySQL, resource);
    REQUIRE(node_gen.has_error());
    REQUIRE(node_gen.error().type == core::error_code_t::unimplemented_yet);

    node_ptr view = make_node_drop(resource, drop_target_kind::view);
    auto drop_gen = generate_manual(view, params, backend_type_t::MySQL, resource);
    REQUIRE(drop_gen.has_error());
    REQUIRE(drop_gen.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("generate_query: PostgreSQL node produces schema.collection reference") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
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
    auto gen = sql_gen::generate_query(pg_node.node,
                                       &params,
                                       backend_type_t::PostgreSQL,
                                       pg_node.target,
                                       batch_targets_of(parsed, pg_node), resource);
    REQUIRE_FALSE(gen.has_error());
    const auto& sql = gen.value();

    // PostgreSQL table reference must be quoted schema.collection
    REQUIRE_FALSE(sql.empty());
    REQUIRE(sql.find("\"shop\".\"products\"") != std::string::npos);
    REQUIRE(sql.find("pg.shop.shop.products") == std::string::npos);
}

TEST_CASE("generate_query: same node, different backends produce different references") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
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
                                             batch_targets_of(parsed, n1), resource);
    auto pg_sql    = sql_gen::generate_query(n1.node,
                                             &params,
                                             backend_type_t::PostgreSQL,
                                             n1.target,
                                             batch_targets_of(parsed, n1), resource);

    REQUIRE_FALSE(mysql_sql.has_error());
    REQUIRE_FALSE(pg_sql.has_error());
    // MySQL: `db1`.`test1`   PG: "sch1"."test1"
    REQUIRE(mysql_sql.value().find("`db1`.`test1`")   != std::string::npos);
    REQUIRE(pg_sql.value().find("\"sch1\".\"test1\"") != std::string::npos);
}

TEST_CASE("generate_query: stringstream overload produces the same output") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
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
                                           batch_targets_of(parsed, mysql_node), resource);

    // Stream overload
    std::stringstream ss;
    auto stream_err = sql_gen::generate_query(ss,
                                              mysql_node.node,
                                              &params,
                                              backend_type_t::MySQL,
                                              mysql_node.target,
                                              batch_targets_of(parsed, mysql_node), resource);
    REQUIRE_FALSE(stream_err.contains_error());

    // The string overload appends the statement terminator; the stream
    // overload emits the bare statement so callers can keep composing.
    REQUIRE_FALSE(sql_str.has_error());
    REQUIRE(sql_str.value() == ss.str() + ";");
}

// ── replace_qualifiers edge cases not covered in test_replace_qualifiers.cpp ──

TEST_CASE("replace_qualifiers: 3-part qualifier treated as db.schema.collection (uid promoted)") {
    // prepare_sql promotes the first segment to uid when only 3 parts are present.
    // Scoped arena: libotterbrix_sql's parse tree allocates through this resource
    // and is never explicitly freed — get_default_resource would leak it (LSAN).
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto r = otterstax::parser::prepare_sql(
        "SELECT id FROM (SELECT id FROM mysql.bill.orders WHERE status = 'paid') o;",
        &arena,
        &arena);

    REQUIRE(r.stubs.size() == 1);
    auto rendered =
        sql_gen::replace_qualifiers(r.stubs[0].raw_sql, r.stubs[0].qualifiers, backend_type_t::MySQL, &arena);
    REQUIRE_FALSE(rendered.has_error());
    const std::string& out = rendered.value();
    // After rewrite the 3-part name becomes quoted db.collection for MySQL
    REQUIRE(out.find("FROM `bill`.`orders`") != std::string::npos);
    REQUIRE(out.find("mysql.bill.orders") == std::string::npos);
}

TEST_CASE("replace_qualifiers: multiple qualifiers in one stub") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto r = otterstax::parser::prepare_sql(
        "SELECT * FROM ("
        "SELECT id FROM mysql.db.sc.t1 UNION ALL SELECT id FROM mysql.db.sc.t2"
        ") u;",
        &arena,
        &arena);

    REQUIRE(r.stubs.size() == 1);
    auto rendered =
        sql_gen::replace_qualifiers(r.stubs[0].raw_sql, r.stubs[0].qualifiers, backend_type_t::MySQL, &arena);
    REQUIRE_FALSE(rendered.has_error());
    const std::string& out = rendered.value();
    // Both 4-part names rewritten
    REQUIRE(out.find("`db`.`t1`") != std::string::npos);
    REQUIRE(out.find("`db`.`t2`") != std::string::npos);
    REQUIRE(out.find("mysql.db.sc.t1") == std::string::npos);
    REQUIRE(out.find("mysql.db.sc.t2") == std::string::npos);
}

// ── Characterization: whole outcomes pinned byte for byte ─────────────────────
// Each case records the complete observable outcome of one statement shape on
// every dialect: the whole generated statement, or the whole refusal — the
// stage that refused, its code and its message. Only what would reach a
// backend is compared, never the plan the parser built, so a change in how a
// statement is lowered shows up here as a changed string. Where the recorded
// outcome is not what the statement asks for, the case says so; the string is
// still the current output.

namespace {

enum class stage_t
{
    generated,
    parse_refused,
    generate_refused
};

struct outcome_t {
    stage_t stage;
    core::error_code_t code;
    std::string text;

    bool operator==(const outcome_t& other) const {
        return stage == other.stage && code == other.code && text == other.text;
    }
};

std::ostream& operator<<(std::ostream& os, const outcome_t& outcome) {
    const char* stage = "generated";
    if (outcome.stage == stage_t::parse_refused) {
        stage = "parse_refused";
    } else if (outcome.stage == stage_t::generate_refused) {
        stage = "generate_refused";
    }
    return os << "{" << stage << ", code " << static_cast<int>(outcome.code) << ", \"" << outcome.text << "\"}";
}

outcome_t sql_is(std::string sql) { return outcome_t{stage_t::generated, core::error_code_t::none, std::move(sql)}; }

outcome_t generate_refuses(core::error_code_t code, std::string what) {
    return outcome_t{stage_t::generate_refused, code, std::move(what)};
}

outcome_t parse_refuses(core::error_code_t code, std::string what) {
    return outcome_t{stage_t::parse_refused, code, std::move(what)};
}

outcome_t outcome_of_generation(const core::result_wrapper_t<std::string>& gen) {
    if (gen.has_error()) {
        return generate_refuses(gen.error().type, std::string{gen.error().what.c_str()});
    }
    return sql_is(gen.value());
}

// generate_query for the `mysql` slot of an already parsed statement.
outcome_t outcome_of_parsed(const ParsedQueryDataPtr& parsed,
                            backend_type_t backend,
                            std::pmr::memory_resource* resource) {
    auto nodes = flat_externals(parsed);
    auto slot = find_by_uid(nodes, "mysql");
    REQUIRE(slot.node);
    const auto& params = parsed->otterbrix_params->params_node->parameters();
    return outcome_of_generation(
        sql_gen::generate_query(slot.node, &params, backend, slot.target, batch_targets_of(parsed, slot), resource));
}

outcome_t outcome_of(GreenplumParser& parser,
                     const std::string& sql,
                     backend_type_t backend,
                     std::pmr::memory_resource* resource) {
    auto parsed = parser.parse(sql);
    if (parsed.has_error()) {
        return parse_refuses(parsed.error().type, std::string{parsed.error().what.c_str()});
    }
    return outcome_of_parsed(parsed.value(), backend, resource);
}

// The `mysql` slot of an already parsed statement, generated with the FROM /
// USING source OVERRIDDEN to `source`. The parser resolves a plain-table source
// itself; this replaces its answer so one parsed statement can be generated
// against sources it never names — another backend, a local table, a table the
// plan does not read — without a statement per case.
outcome_t outcome_with_source(const ParsedQueryDataPtr& parsed,
                              const qualified_name_t& source,
                              backend_type_t backend,
                              std::pmr::memory_resource* resource) {
    auto nodes = flat_externals(parsed);
    auto slot = find_by_uid(nodes, "mysql");
    REQUIRE(slot.node);
    auto target = slot.target;
    target.from_name = source;
    const auto& params = parsed->otterbrix_params->params_node->parameters();
    return outcome_of_generation(
        sql_gen::generate_query(slot.node, &params, backend, target, batch_targets_of(parsed, slot), resource));
}

struct dialect_outcomes_t {
    outcome_t mysql;
    outcome_t pg;
    outcome_t ch;
};

void check_dialects(GreenplumParser& parser,
                    const std::string& sql,
                    const dialect_outcomes_t& expected,
                    std::pmr::memory_resource* resource) {
    CAPTURE(sql);
    CHECK(outcome_of(parser, sql, backend_type_t::MySQL, resource) == expected.mysql);
    CHECK(outcome_of(parser, sql, backend_type_t::PostgreSQL, resource) == expected.pg);
    CHECK(outcome_of(parser, sql, backend_type_t::ClickHouse, resource) == expected.ch);
}

// `UPDATE ... SET a = <value> WHERE id = 1;` for each value of the table.
struct set_value_case_t {
    const char* value;
    dialect_outcomes_t expected;
};

void check_set_values(GreenplumParser& parser,
                      const std::vector<set_value_case_t>& cases,
                      std::pmr::memory_resource* resource) {
    for (const auto& c : cases) {
        check_dialects(parser,
                       std::string{"UPDATE mysql.bill.schema.orders SET a = "} + c.value + " WHERE id = 1;",
                       c.expected,
                       resource);
    }
}

// The value expressions an UPDATE assignment can carry. An assignment IS the
// value, named by the column it assigns, so these build only the value and
// orders_update_of_a names it.
expression_ptr read_key(std::pmr::memory_resource* resource, const char* name) {
    auto value = make_scalar_expression(resource, scalar_type::get_field);
    value->append_param(components::expressions::key_t{resource, name});
    return value;
}

expression_ptr read_parameter(std::pmr::memory_resource* resource, core::parameter_id_t id) {
    auto value = make_scalar_expression(resource, scalar_type::constant);
    value->append_param(id);
    return value;
}

// A scalar operator over one or two operands; a null operand is left out, which
// is how the malformed "binary operator with one operand" shape is built.
expression_ptr calculate(std::pmr::memory_resource* resource,
                         scalar_type type,
                         const expression_ptr& left,
                         const expression_ptr& right) {
    auto op = make_scalar_expression(resource, type);
    if (left) {
        op->append_param(param_storage{left});
    }
    if (right) {
        op->append_param(param_storage{right});
    }
    return op;
}

// The function call the transformer builds for the PostgreSQL operator
// spellings: `^` (pow), `|/` (sqrt), `||/` (cbrt), `!` / `!!` (factorial) and
// `@` (abs).
expression_ptr call_of(std::pmr::memory_resource* resource,
                       std::string name,
                       const expression_ptr& first,
                       const expression_ptr& second) {
    std::pmr::vector<param_storage> args{resource};
    if (first) {
        args.emplace_back(param_storage{first});
    }
    if (second) {
        args.emplace_back(param_storage{second});
    }
    return make_function_expression(resource, std::move(name), std::move(args));
}

// UPDATE bill.orders SET <updates> with no predicate and no LIMIT.
node_ptr orders_update(std::pmr::memory_resource* resource, const std::pmr::vector<expression_ptr>& updates) {
    return make_node_update(resource,
                            orders_match(resource, make_compare_expression(resource, compare_type::all_true)),
                            orders_limit(resource, limit_t::unlimit()),
                            updates);
}

// UPDATE bill.orders SET a = <value>.
node_ptr orders_update_of_a(std::pmr::memory_resource* resource, const expression_ptr& value) {
    std::pmr::vector<expression_ptr> updates{resource};
    value->key() = components::expressions::key_t{resource, "a"};
    updates.emplace_back(value);
    return orders_update(resource, updates);
}

void check_manual_dialects(const node_ptr& node,
                           const parameter_node_t& params,
                           const dialect_outcomes_t& expected,
                           std::pmr::memory_resource* resource) {
    CHECK(outcome_of_generation(generate_manual(node, params, backend_type_t::MySQL, resource)) == expected.mysql);
    CHECK(outcome_of_generation(generate_manual(node, params, backend_type_t::PostgreSQL, resource)) == expected.pg);
    CHECK(outcome_of_generation(generate_manual(node, params, backend_type_t::ClickHouse, resource)) == expected.ch);
}

// `UPDATE ... SET a = <value> WHERE id = 1;` on the three dialects, the value
// as each dialect spells it.
dialect_outcomes_t set_a_is(const std::string& mysql, const std::string& pg, const std::string& ch) {
    return dialect_outcomes_t{sql_is("UPDATE `bill`.`orders` SET `a` = " + mysql + " WHERE `id` = 1;"),
                              sql_is("UPDATE \"schema\".\"orders\" SET \"a\" = " + pg + " WHERE \"id\" = 1;"),
                              sql_is("ALTER TABLE `bill`.`orders` UPDATE `a` = " + ch + " WHERE `id` = 1;")};
}

// The hand-built `UPDATE bill.orders SET a = <value>` without a predicate on
// the three dialects; ClickHouse spells the missing predicate as WHERE 1.
dialect_outcomes_t manual_set_a_is(const std::string& mysql, const std::string& pg, const std::string& ch) {
    return dialect_outcomes_t{sql_is("UPDATE `bill`.`orders` SET `a` = " + mysql + ";"),
                              sql_is("UPDATE \"schema\".\"orders\" SET \"a\" = " + pg + ";"),
                              sql_is("ALTER TABLE `bill`.`orders` UPDATE `a` = " + ch + " WHERE 1;")};
}

dialect_outcomes_t parse_refuses_everywhere(core::error_code_t code, const std::string& what) {
    return dialect_outcomes_t{parse_refuses(code, what), parse_refuses(code, what), parse_refuses(code, what)};
}

dialect_outcomes_t generate_refuses_everywhere(core::error_code_t code, const std::string& what) {
    return dialect_outcomes_t{generate_refuses(code, what), generate_refuses(code, what), generate_refuses(code, what)};
}

} // namespace

TEST_CASE("characterization: UPDATE SET constants and NULL per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    check_set_values(parser,
                     {
                         {"1", set_a_is("1", "1", "1")},
                         {"-1", set_a_is("-1", "-1", "-1")},
                         // A literal wider than 32 bits reaches the statement whole.
                         {"5000000000", set_a_is("5000000000", "5000000000", "5000000000")},
                         {"2.5", set_a_is("2.5", "2.5", "2.5")},
                         {"'x'", set_a_is("'x'", "'x'", "'x'")},
                         {"''", set_a_is("''", "''", "''")},
                         {"true", set_a_is("TRUE", "TRUE", "TRUE")},
                         {"false", set_a_is("FALSE", "FALSE", "FALSE")},
                         {"NULL", set_a_is("NULL", "NULL", "NULL")},
                     },
                     resource);

    check_dialects(
        parser,
        "UPDATE mysql.bill.schema.orders SET i = 1, s = 'x', n = NULL, d = 2.5 WHERE id = 7;",
        {sql_is("UPDATE `bill`.`orders` SET `i` = 1, `s` = 'x', `n` = NULL, `d` = 2.5 WHERE `id` = 7;"),
         sql_is("UPDATE \"schema\".\"orders\" SET \"i\" = 1, \"s\" = 'x', \"n\" = NULL, \"d\" = 2.5 WHERE \"id\" = 7;"),
         sql_is("ALTER TABLE `bill`.`orders` UPDATE `i` = 1, `s` = 'x', `n` = NULL, `d` = 2.5 WHERE `id` = 7;")},
        resource);
}

TEST_CASE("characterization: UPDATE SET bound parameters per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const std::string sql = "UPDATE mysql.bill.schema.orders SET note = $1, qty = $2 WHERE id = $3;";
    auto bound = [&](backend_type_t backend) {
        auto parsed = parse_or_die(parser, sql);
        auto& binder = parsed->binder();
        binder.bind(1, logical_value_t{resource, std::string{"it's a \\ path"}});
        binder.bind(2, logical_value_t{resource, std::int64_t{5}});
        binder.bind(3, logical_value_t{resource, std::int64_t{7}});
        auto finalized = binder.finalize();
        INFO("finalize error: " << finalized.error().what.c_str());
        REQUIRE_FALSE(finalized.has_error());
        return outcome_of_parsed(parsed, backend, resource);
    };
    CHECK(bound(backend_type_t::MySQL) ==
          sql_is("UPDATE `bill`.`orders` SET `note` = 'it''s a \\\\ path', `qty` = 5 WHERE `id` = 7;"));
    CHECK(bound(backend_type_t::PostgreSQL) ==
          sql_is("UPDATE \"schema\".\"orders\" SET \"note\" = 'it''s a \\ path', \"qty\" = 5 WHERE \"id\" = 7;"));
    CHECK(bound(backend_type_t::ClickHouse) ==
          sql_is("ALTER TABLE `bill`.`orders` UPDATE `note` = 'it''s a \\\\ path', `qty` = 5 WHERE `id` = 7;"));

    auto null_bound = [&](backend_type_t backend) {
        auto parsed = parse_or_die(parser, "UPDATE mysql.bill.schema.orders SET note = $1 WHERE id = $2;");
        auto& binder = parsed->binder();
        binder.bind(1, logical_value_t{resource, logical_type::NA});
        binder.bind(2, logical_value_t{resource, std::int64_t{7}});
        auto finalized = binder.finalize();
        INFO("finalize error: " << finalized.error().what.c_str());
        REQUIRE_FALSE(finalized.has_error());
        return outcome_of_parsed(parsed, backend, resource);
    };
    CHECK(null_bound(backend_type_t::MySQL) == sql_is("UPDATE `bill`.`orders` SET `note` = NULL WHERE `id` = 7;"));
    CHECK(null_bound(backend_type_t::PostgreSQL) ==
          sql_is("UPDATE \"schema\".\"orders\" SET \"note\" = NULL WHERE \"id\" = 7;"));
    CHECK(null_bound(backend_type_t::ClickHouse) ==
          sql_is("ALTER TABLE `bill`.`orders` UPDATE `note` = NULL WHERE `id` = 7;"));

    // Generated before any Bind.
    check_dialects(parser,
                   sql,
                   generate_refuses_everywhere(core::error_code_t::invalid_parameter,
                                               "plan references an unbound parameter #0"),
                   resource);
}

TEST_CASE("characterization: UPDATE SET arithmetic with parentheses per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    check_set_values(parser,
                     {
                         {"(a + 1) * 2", set_a_is("((`a` + 1) * 2)", "((\"a\" + 1) * 2)", "((`a` + 1) * 2)")},
                         {"a - (b / 2)", set_a_is("(`a` - (`b` / 2))", "(\"a\" - (\"b\" / 2))", "(`a` - (`b` / 2))")},
                         {"(a % 3) + (b - 1)",
                          set_a_is("((`a` % 3) + (`b` - 1))",
                                   "((\"a\" % 3) + (\"b\" - 1))",
                                   "((`a` % 3) + (`b` - 1))")},
                         {"a * (b + (c - 1))",
                          set_a_is("(`a` * (`b` + (`c` - 1)))",
                                   "(\"a\" * (\"b\" + (\"c\" - 1)))",
                                   "(`a` * (`b` + (`c` - 1)))")},
                         {"((a))", set_a_is("`a`", "\"a\"", "`a`")},
                         {"a + b * c", set_a_is("(`a` + (`b` * `c`))", "(\"a\" + (\"b\" * \"c\"))", "(`a` + (`b` * `c`))")},
                     },
                     resource);
}

TEST_CASE("characterization: UPDATE SET operators and functions written as SQL per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const auto parse_error = core::error_code_t::sql_parse_error;
    const auto unsupported = core::error_code_t::unimplemented_yet;
    // rc-3 lowers the operator spellings and the named calls to the same
    // function, so each pair below has one outcome.
    const dialect_outcomes_t cbrt_outcome{
        generate_refuses(unsupported, "cube root is not supported by this backend"),
        sql_is("UPDATE \"schema\".\"orders\" SET \"a\" = cbrt(\"a\") WHERE \"id\" = 1;"),
        sql_is("ALTER TABLE `bill`.`orders` UPDATE `a` = cbrt(`a`) WHERE `id` = 1;")};
    const dialect_outcomes_t factorial_outcome{
        generate_refuses(unsupported, "factorial is not supported by this backend"),
        sql_is("UPDATE \"schema\".\"orders\" SET \"a\" = factorial(\"a\") WHERE \"id\" = 1;"),
        generate_refuses(unsupported, "factorial is not supported by this backend")};
    check_set_values(
        parser,
        {
            // A unary sign goes through the common expression path: the minus is
            // scalar_type::unary_minus, the plus is the identity.
            {"-a", set_a_is("(-`a`)", "(-\"a\")", "(-`a`)")},
            {"+a", set_a_is("`a`", "\"a\"", "`a`")},
            {"@ a", set_a_is("ABS(`a`)", "ABS(\"a\")", "abs(`a`)")},
            {"abs(a)", set_a_is("ABS(`a`)", "ABS(\"a\")", "abs(`a`)")},
            {"|/ a", set_a_is("SQRT(`a`)", "SQRT(\"a\")", "sqrt(`a`)")},
            {"sqrt(a)", set_a_is("SQRT(`a`)", "SQRT(\"a\")", "sqrt(`a`)")},
            {"||/ a", cbrt_outcome},
            {"cbrt(a)", cbrt_outcome},
            {"!! a", factorial_outcome},
            {"factorial(a)", factorial_outcome},
            {"~ a", set_a_is("(~`a`)", "(~\"a\")", "bitNot(`a`)")},
            {"a & 6", set_a_is("(`a` & 6)", "(\"a\" & 6)", "bitAnd(`a`, 6)")},
            {"a | 6", set_a_is("(`a` | 6)", "(\"a\" | 6)", "bitOr(`a`, 6)")},
            {"a # 6", set_a_is("(`a` ^ 6)", "(\"a\" # 6)", "bitXor(`a`, 6)")},
            {"a ^ 2", set_a_is("POWER(`a`, 2)", "(\"a\" ^ 2)", "pow(`a`, 2)")},
            {"a << 1", set_a_is("(`a` << 1)", "(\"a\" << 1)", "bitShiftLeft(`a`, 1)")},
            {"a >> 1", set_a_is("(`a` >> 1)", "(\"a\" >> 1)", "bitShiftRight(`a`, 1)")},
            // A function is written in the spelling its dialect has: ClickHouse
            // matches its own function names case-sensitively, while it registers
            // the SQL-standard aggregates case-insensitively.
            {"lower(s)", set_a_is("LOWER(`s`)", "LOWER(\"s\")", "lower(`s`)")},
            {"coalesce(a, 0)", set_a_is("COALESCE(`a`, 0)", "COALESCE(\"a\", 0)", "coalesce(`a`, 0)")},
            // Still refused, by the transformer rather than the generator.
            {"s || 'x'", parse_refuses_everywhere(parse_error, "invalid compare operand")},
            // The cast keeps its column operand. Each dialect spells the target
            // type its own way: MySQL CAST takes SIGNED / UNSIGNED / CHAR rather
            // than a column type name.
            {"CAST(b AS bigint)", set_a_is("CAST(`b` AS SIGNED)", "CAST(\"b\" AS int8)", "CAST(`b` AS Int64)")},
        },
        resource);
}

TEST_CASE("characterization: length is pushed down as each dialect's byte-counting spelling") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // The engine's own length counts bytes, so each dialect is given the spelling
    // that counts bytes there rather than the one that shares the name: PostgreSQL
    // length() counts characters, and MariaDB in Oracle mode redefines LENGTH as
    // CHAR_LENGTH. ClickHouse keeps `length` — always bytes, and unlike its
    // OCTET_LENGTH alias it needs no version floor.
    check_set_values(parser,
                     {
                         {"length(s)", set_a_is("OCTET_LENGTH(`s`)", "OCTET_LENGTH(\"s\")", "length(`s`)")},
                     },
                     resource);

    check_dialects(parser,
                   "SELECT length(name) AS n FROM mysql.bill.schema.orders;",
                   {sql_is("SELECT OCTET_LENGTH(`name`) AS `n` FROM `bill`.`orders`;"),
                    sql_is("SELECT OCTET_LENGTH(\"name\") AS \"n\" FROM \"schema\".\"orders\";"),
                    sql_is("SELECT length(`name`) AS `n` FROM `bill`.`orders`;")},
                   resource);
}

TEST_CASE("characterization: every UPDATE SET expression kind per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    parameter_node_t params(resource);
    const auto two = params.add_parameter(logical_value_t{resource, 2});

    struct kind_case_t {
        const char* name;
        expression_ptr value;
        dialect_outcomes_t expected;
    };
    const auto binary = [&](scalar_type type) {
        return calculate(resource, type, read_key(resource, "a"), read_parameter(resource, two));
    };
    const auto unary = [&](scalar_type type) { return calculate(resource, type, read_key(resource, "a"), nullptr); };
    const auto math = [&](const char* name) { return call_of(resource, name, read_key(resource, "a"), nullptr); };
    const auto math_of_two = [&](const char* name) {
        return call_of(resource, name, read_key(resource, "a"), read_parameter(resource, two));
    };

    const auto unsupported = core::error_code_t::unimplemented_yet;
    std::vector<kind_case_t> cases;
    cases.push_back({"a column read", read_key(resource, "b"), manual_set_a_is("`b`", "\"b\"", "`b`")});
    cases.push_back({"a bound parameter", read_parameter(resource, two), manual_set_a_is("2", "2", "2")});
    cases.push_back({"add", binary(scalar_type::add), manual_set_a_is("(`a` + 2)", "(\"a\" + 2)", "(`a` + 2)")});
    cases.push_back(
        {"subtract", binary(scalar_type::subtract), manual_set_a_is("(`a` - 2)", "(\"a\" - 2)", "(`a` - 2)")});
    cases.push_back(
        {"multiply", binary(scalar_type::multiply), manual_set_a_is("(`a` * 2)", "(\"a\" * 2)", "(`a` * 2)")});
    cases.push_back({"divide", binary(scalar_type::divide), manual_set_a_is("(`a` / 2)", "(\"a\" / 2)", "(`a` / 2)")});
    cases.push_back({"mod", binary(scalar_type::mod), manual_set_a_is("(`a` % 2)", "(\"a\" % 2)", "(`a` % 2)")});
    cases.push_back(
        {"unary minus", unary(scalar_type::unary_minus), manual_set_a_is("(-`a`)", "(-\"a\")", "(-`a`)")});
    cases.push_back({"pow", math_of_two("pow"), manual_set_a_is("POWER(`a`, 2)", "(\"a\" ^ 2)", "pow(`a`, 2)")});
    cases.push_back({"sqrt", math("sqrt"), manual_set_a_is("SQRT(`a`)", "SQRT(\"a\")", "sqrt(`a`)")});
    cases.push_back({"cbrt",
                     math("cbrt"),
                     {generate_refuses(unsupported, "cube root is not supported by this backend"),
                      sql_is("UPDATE \"schema\".\"orders\" SET \"a\" = cbrt(\"a\");"),
                      sql_is("ALTER TABLE `bill`.`orders` UPDATE `a` = cbrt(`a`) WHERE 1;")}});
    cases.push_back({"factorial",
                     math("factorial"),
                     {generate_refuses(unsupported, "factorial is not supported by this backend"),
                      sql_is("UPDATE \"schema\".\"orders\" SET \"a\" = factorial(\"a\");"),
                      generate_refuses(unsupported, "factorial is not supported by this backend")}});
    cases.push_back({"abs", math("abs"), manual_set_a_is("ABS(`a`)", "ABS(\"a\")", "abs(`a`)")});
    cases.push_back(
        {"bit_and", binary(scalar_type::bit_and), manual_set_a_is("(`a` & 2)", "(\"a\" & 2)", "bitAnd(`a`, 2)")});
    cases.push_back(
        {"bit_or", binary(scalar_type::bit_or), manual_set_a_is("(`a` | 2)", "(\"a\" | 2)", "bitOr(`a`, 2)")});
    cases.push_back(
        {"bit_xor", binary(scalar_type::bit_xor), manual_set_a_is("(`a` ^ 2)", "(\"a\" # 2)", "bitXor(`a`, 2)")});
    cases.push_back({"bit_not", unary(scalar_type::bit_not), manual_set_a_is("(~`a`)", "(~\"a\")", "bitNot(`a`)")});
    cases.push_back({"shift_left",
                     binary(scalar_type::shift_left),
                     manual_set_a_is("(`a` << 2)", "(\"a\" << 2)", "bitShiftLeft(`a`, 2)")});
    cases.push_back({"shift_right",
                     binary(scalar_type::shift_right),
                     manual_set_a_is("(`a` >> 2)", "(\"a\" >> 2)", "bitShiftRight(`a`, 2)")});
    cases.push_back({"a cast of a column",
                     make_cast_expression(resource,
                                          param_storage{components::expressions::key_t{resource, "b"}},
                                          complex_logical_type{logical_type::BIGINT},
                                          components::casts::cast_t{},
                                          components::casts::cast_kind::cast),
                     manual_set_a_is("CAST(`b` AS SIGNED)", "CAST(\"b\" AS int8)", "CAST(`b` AS Int64)")});
    cases.push_back({"a binary kind without its right operand",
                     unary(scalar_type::add),
                     generate_refuses_everywhere(core::error_code_t::invalid_parameter,
                                                 "UPDATE SET expression is missing an operand")});
    cases.push_back({"a kind value outside the enumeration",
                     calculate(resource,
                               static_cast<scalar_type>(200),
                               read_key(resource, "a"),
                               read_parameter(resource, two)),
                     generate_refuses_everywhere(unsupported, "unsupported UPDATE SET expression")});
    // A name no dialect is known to spell the same way is not pushed down:
    // emitting it would call whatever the backend has under that name.
    cases.push_back({"a function the whitelist does not name",
                     math("date_trunc"),
                     generate_refuses_everywhere(unsupported,
                                                 "function is not pushed down to the backend: date_trunc")});

    SECTION("each kind as the value of SET a") {
        for (const auto& c : cases) {
            CAPTURE(c.name);
            check_manual_dialects(orders_update_of_a(resource, c.value), params, c.expected, resource);
        }
    }
    SECTION("an assignment that is not a SET expression") {
        // A value expression that names no column assigns nothing.
        std::pmr::vector<expression_ptr> updates{resource};
        updates.push_back(read_key(resource, "a"));
        check_manual_dialects(orders_update(resource, updates),
                              params,
                              generate_refuses_everywhere(core::error_code_t::invalid_parameter,
                                                          "UPDATE assignment is not a SET expression"),
                              resource);
    }
    SECTION("an UPDATE without assignments") {
        std::pmr::vector<expression_ptr> updates{resource};
        check_manual_dialects(
            orders_update(resource, updates),
            params,
            generate_refuses_everywhere(core::error_code_t::invalid_parameter, "UPDATE without SET assignments"),
            resource);
    }
}

TEST_CASE("characterization: UPDATE ... FROM through the parser per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // Records the outcome of the whole chain. The parser resolves the FROM
    // source — a plain table — into the slot's from_name, so a source on the
    // target's own backend is pushed down as ONE two-table statement; only
    // ClickHouse, whose mutation names a single table, refuses it.
    check_dialects(parser,
                   "UPDATE mysql.bill.schema.orders SET name = 'x' FROM mysql.bill.schema.other "
                   "WHERE orders.id = other.id;",
                   {sql_is("UPDATE `bill`.`orders`, `bill`.`other` SET `bill`.`orders`.`name` = 'x' "
                           "WHERE `bill`.`orders`.`id` = `bill`.`other`.`id`;"),
                    sql_is("UPDATE \"schema\".\"orders\" SET \"name\" = 'x' FROM \"schema\".\"other\" "
                           "WHERE \"schema\".\"orders\".\"id\" = \"schema\".\"other\".\"id\";"),
                    generate_refuses(core::error_code_t::unimplemented_yet,
                                     "a second table is not supported for ClickHouse on UPDATE")},
                   resource);
    // An unqualified source is a LOCAL table: it resolves to a uid-less name,
    // which the backend does not have, so the engine has to join it. ClickHouse
    // refuses on the second table before the backend of the source is looked at.
    check_dialects(parser,
                   "UPDATE mysql.bill.schema.orders SET name = 'x' FROM other WHERE orders.id = other.id;",
                   {generate_refuses(core::error_code_t::unimplemented_yet,
                                     "the FROM / USING source is not on the same backend as the target of UPDATE"),
                    generate_refuses(core::error_code_t::unimplemented_yet,
                                     "the FROM / USING source is not on the same backend as the target of UPDATE"),
                    generate_refuses(core::error_code_t::unimplemented_yet,
                                     "a second table is not supported for ClickHouse on UPDATE")},
                   resource);
}

TEST_CASE("characterization: DELETE ... USING through the parser per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // Records the outcome of the whole chain. The parser resolves the USING
    // source — a plain table — into the slot's from_name, so a source on the
    // target's own backend is pushed down as ONE two-table statement; only
    // ClickHouse, whose mutation names a single table, refuses it.
    check_dialects(parser,
                   "DELETE FROM mysql.bill.schema.orders USING mysql.bill.schema.other WHERE orders.id = other.id;",
                   {sql_is("DELETE `bill`.`orders` FROM `bill`.`orders`, `bill`.`other` "
                           "WHERE `bill`.`orders`.`id` = `bill`.`other`.`id`;"),
                    sql_is("DELETE FROM \"schema\".\"orders\" USING \"schema\".\"other\" "
                           "WHERE \"schema\".\"orders\".\"id\" = \"schema\".\"other\".\"id\";"),
                    generate_refuses(core::error_code_t::unimplemented_yet,
                                     "a second table is not supported for ClickHouse on DELETE")},
                   resource);
    // An unqualified source is a LOCAL table: it resolves to a uid-less name,
    // which the backend does not have, so the engine has to join it. ClickHouse
    // refuses on the second table before the backend of the source is looked at.
    check_dialects(parser,
                   "DELETE FROM mysql.bill.schema.orders USING other WHERE orders.id = other.id;",
                   {generate_refuses(core::error_code_t::unimplemented_yet,
                                     "the FROM / USING source is not on the same backend as the target of DELETE"),
                    generate_refuses(core::error_code_t::unimplemented_yet,
                                     "the FROM / USING source is not on the same backend as the target of DELETE"),
                    generate_refuses(core::error_code_t::unimplemented_yet,
                                     "a second table is not supported for ClickHouse on DELETE")},
                   resource);
}

TEST_CASE("characterization: UPDATE ... FROM pushed down with the source resolved") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "UPDATE mysql.bill.schema.orders SET name = other.name "
                               "FROM mysql.bill.schema.other WHERE orders.id = other.id;");
    const qualified_name_t same_backend{"mysql", "bill", "schema", "other"};

    // PostgreSQL names the source in FROM, MySQL in the multi-table form; both
    // take the join condition from WHERE, exactly as the plan does (the engine
    // cross-joins target and source and filters the result). A column is written
    // qualified by the table its side names, and the assigned column by the
    // target: on MySQL an unqualified name the target does not have would resolve
    // against the source table and the statement would write there.
    CHECK(outcome_with_source(parsed, same_backend, backend_type_t::PostgreSQL, resource) ==
          sql_is("UPDATE \"schema\".\"orders\" SET \"name\" = \"schema\".\"other\".\"name\" "
                 "FROM \"schema\".\"other\" WHERE \"schema\".\"orders\".\"id\" = \"schema\".\"other\".\"id\";"));
    CHECK(outcome_with_source(parsed, same_backend, backend_type_t::MySQL, resource) ==
          sql_is("UPDATE `bill`.`orders`, `bill`.`other` SET `bill`.`orders`.`name` = `bill`.`other`.`name` "
                 "WHERE `bill`.`orders`.`id` = `bill`.`other`.`id`;"));
    // A ClickHouse mutation names one table and takes no join.
    CHECK(outcome_with_source(parsed, same_backend, backend_type_t::ClickHouse, resource) ==
          generate_refuses(core::error_code_t::unimplemented_yet,
                           "a second table is not supported for ClickHouse on UPDATE"));

    // A source on another connection — or a local table, whose uid is empty — has
    // to be joined by the engine: naming it remotely would read a table the
    // backend does not have.
    const auto cross_backend =
        generate_refuses(core::error_code_t::unimplemented_yet,
                         "the FROM / USING source is not on the same backend as the target of UPDATE");
    CHECK(outcome_with_source(parsed,
                              qualified_name_t{"pg", "shop", "shop", "other"},
                              backend_type_t::MySQL,
                              resource) == cross_backend);
    CHECK(outcome_with_source(parsed, qualified_name_t{"", "bill", "", "other"}, backend_type_t::MySQL, resource) ==
          cross_backend);
    // The resolved name must be the table the plan actually reads.
    CHECK(outcome_with_source(parsed,
                              qualified_name_t{"mysql", "bill", "schema", "elsewhere"},
                              backend_type_t::MySQL,
                              resource) ==
          generate_refuses(core::error_code_t::invalid_parameter,
                           "the resolved FROM / USING table is not the table the plan reads on UPDATE"));
    // Target and source being one table needs aliases, which are not generated.
    CHECK(outcome_with_source(parsed,
                              qualified_name_t{"mysql", "bill", "schema", "orders"},
                              backend_type_t::MySQL,
                              resource) ==
          generate_refuses(core::error_code_t::unimplemented_yet,
                           "a FROM / USING source that is the target table itself needs table aliases on UPDATE"));
}

TEST_CASE("characterization: DELETE ... USING pushed down with the source resolved") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "DELETE FROM mysql.bill.schema.orders USING mysql.bill.schema.other "
                               "WHERE orders.id = other.id;");
    const qualified_name_t same_backend{"mysql", "bill", "schema", "other"};

    // PostgreSQL's USING, and MySQL's multi-table DELETE, which names the table
    // rows are deleted from before FROM and every table it reads after it; both
    // take the join condition from WHERE.
    CHECK(outcome_with_source(parsed, same_backend, backend_type_t::PostgreSQL, resource) ==
          sql_is("DELETE FROM \"schema\".\"orders\" USING \"schema\".\"other\" "
                 "WHERE \"schema\".\"orders\".\"id\" = \"schema\".\"other\".\"id\";"));
    CHECK(outcome_with_source(parsed, same_backend, backend_type_t::MySQL, resource) ==
          sql_is("DELETE `bill`.`orders` FROM `bill`.`orders`, `bill`.`other` "
                 "WHERE `bill`.`orders`.`id` = `bill`.`other`.`id`;"));
    CHECK(outcome_with_source(parsed, same_backend, backend_type_t::ClickHouse, resource) ==
          generate_refuses(core::error_code_t::unimplemented_yet,
                           "a second table is not supported for ClickHouse on DELETE"));
    CHECK(outcome_with_source(parsed,
                              qualified_name_t{"pg", "shop", "shop", "other"},
                              backend_type_t::PostgreSQL,
                              resource) ==
          generate_refuses(core::error_code_t::unimplemented_yet,
                           "the FROM / USING source is not on the same backend as the target of DELETE"));
}

TEST_CASE("characterization: DROP INDEX and CREATE INDEX through the parser per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const auto ch_refusal =
        generate_refuses(core::error_code_t::unimplemented_yet, "CREATE INDEX is not supported for ClickHouse");
    check_dialects(parser,
                   "DROP INDEX mysql.bill.schema.orders.idx_orders_id;",
                   {sql_is("DROP INDEX `idx_orders_id` ON `bill`.`orders`;"),
                    sql_is("DROP INDEX \"schema\".\"idx_orders_id\";"),
                    sql_is("ALTER TABLE `bill`.`orders` DROP INDEX `idx_orders_id`;")},
                   resource);
    check_dialects(parser,
                   "CREATE INDEX idx_orders_id ON mysql.bill.schema.orders (id);",
                   {sql_is("CREATE INDEX `idx_orders_id` ON `bill`.`orders` (`id`);"),
                    sql_is("CREATE INDEX \"idx_orders_id\" ON \"schema\".\"orders\" (\"id\");"),
                    ch_refusal},
                   resource);
    check_dialects(parser,
                   "CREATE INDEX idx_orders_id_name ON mysql.bill.schema.orders (id, name);",
                   {sql_is("CREATE INDEX `idx_orders_id_name` ON `bill`.`orders` (`id`, `name`);"),
                    sql_is("CREATE INDEX \"idx_orders_id_name\" ON \"schema\".\"orders\" (\"id\", \"name\");"),
                    ch_refusal},
                   resource);
}

TEST_CASE("characterization: constant WHERE predicates per dialect") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    check_dialects(parser,
                   "SELECT * FROM mysql.bill.schema.orders WHERE true;",
                   {sql_is("SELECT * FROM `bill`.`orders`;"),
                    sql_is("SELECT * FROM \"schema\".\"orders\";"),
                    sql_is("SELECT * FROM `bill`.`orders`;")},
                   resource);
    check_dialects(parser,
                   "SELECT * FROM mysql.bill.schema.orders WHERE false;",
                   {sql_is("SELECT * FROM `bill`.`orders` WHERE FALSE;"),
                    sql_is("SELECT * FROM \"schema\".\"orders\" WHERE FALSE;"),
                    sql_is("SELECT * FROM `bill`.`orders` WHERE FALSE;")},
                   resource);
    check_dialects(parser,
                   "SELECT * FROM mysql.bill.schema.orders WHERE 1 = 0;",
                   {sql_is("SELECT * FROM `bill`.`orders` WHERE 1 = 0;"),
                    sql_is("SELECT * FROM \"schema\".\"orders\" WHERE 1 = 0;"),
                    sql_is("SELECT * FROM `bill`.`orders` WHERE 1 = 0;")},
                   resource);
    check_dialects(parser,
                   "DELETE FROM mysql.bill.schema.orders WHERE true;",
                   {sql_is("DELETE FROM `bill`.`orders`;"),
                    sql_is("DELETE FROM \"schema\".\"orders\";"),
                    sql_is("DELETE FROM `bill`.`orders` WHERE 1;")},
                   resource);
    check_dialects(parser,
                   "DELETE FROM mysql.bill.schema.orders WHERE false;",
                   {sql_is("DELETE FROM `bill`.`orders` WHERE FALSE;"),
                    sql_is("DELETE FROM \"schema\".\"orders\" WHERE FALSE;"),
                    sql_is("DELETE FROM `bill`.`orders` WHERE FALSE;")},
                   resource);
    check_dialects(parser,
                   "UPDATE mysql.bill.schema.orders SET a = 1 WHERE false;",
                   {sql_is("UPDATE `bill`.`orders` SET `a` = 1 WHERE FALSE;"),
                    sql_is("UPDATE \"schema\".\"orders\" SET \"a\" = 1 WHERE FALSE;"),
                    sql_is("ALTER TABLE `bill`.`orders` UPDATE `a` = 1 WHERE FALSE;")},
                   resource);
}

TEST_CASE("characterization: CREATE TABLE and CREATE INDEX on an alias route to one external slot") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const std::string sql = GENERATE(Catch::Generators::as<std::string>{},
                                     "CREATE TABLE mysql.bill.schema.t (id BIGINT, qty BIGINT);",
                                     "CREATE INDEX idx_orders_id ON mysql.bill.schema.orders (id);");
    CAPTURE(sql);
    auto parsed = parse_or_die(parser, sql);
    auto nodes = flat_externals(parsed);
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 1);
    REQUIRE(nodes.size() == 1);
    REQUIRE(nodes[0].target.name.unique_identifier == "mysql");
    REQUIRE(nodes[0].target.name.database == "bill");
    REQUIRE(nodes[0].target.name.schema == "schema");
}
