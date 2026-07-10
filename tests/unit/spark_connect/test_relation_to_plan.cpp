// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/spark_connect_server/plan_translator/relation_to_plan.hpp"

#include <spark/connect/expressions.pb.h>
#include <spark/connect/relations.pb.h>

#include <components/logical_plan/forward.hpp>  // node_type
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/types/logical_value.hpp>

#include "otterbrix/parser/parser.hpp"  // ParsedQueryData
#include "types/otterbrix.hpp"          // OtterbrixStatement / external_entry_t

#include <catch2/catch.hpp>

#include <memory_resource>
#include <string>

namespace {

namespace sc = ::spark::connect;
namespace cl = components::logical_plan;
namespace ce = components::expressions;

// Builds a Read.NamedTable plan for `identifier`.
sc::Plan make_read_plan(const std::string& identifier) {
    sc::Plan plan;
    auto* read = plan.mutable_root()->mutable_read();
    read->mutable_named_table()->set_unparsed_identifier(identifier);
    return plan;
}

// Builds a Read.NamedTable relation for `identifier` (for use as an input).
sc::Relation make_read_relation(const std::string& identifier) {
    sc::Relation rel;
    auto* read = rel.mutable_read();
    read->mutable_named_table()->set_unparsed_identifier(identifier);
    return rel;
}

// Returns an UnresolvedAttribute expression referencing `name`.
sc::Expression make_attribute(const std::string& name) {
    sc::Expression expr;
    expr.mutable_unresolved_attribute()->set_unparsed_identifier(name);
    return expr;
}

} // namespace

// ── Read.NamedTable ───────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: Read.NamedTable federated alias.db.schema.table") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    auto plan = make_read_plan("alias1.db1.sch1.table1");

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());

    auto& parsed = result.value().parsed_data;
    REQUIRE(parsed != nullptr);
    REQUIRE(parsed->otterbrix_params->node != nullptr);
    CHECK(parsed->otterbrix_params->node->type() == cl::node_type::aggregate_t);

    REQUIRE(parsed->otterbrix_params->external_nodes_count == 1);
    REQUIRE(parsed->otterbrix_params->external_nodes.size() == 1);
    REQUIRE(parsed->otterbrix_params->external_nodes[0].size() == 1);

    const auto& target = parsed->otterbrix_params->external_nodes[0][0].target;
    CHECK(target.name.unique_identifier == "alias1");
    CHECK(target.name.collection == "table1");
}

TEST_CASE("relation_to_plan: Read.NamedTable without uid has no external nodes") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    auto plan = make_read_plan("db1.table1");

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());

    auto& parsed = result.value().parsed_data;
    REQUIRE(parsed != nullptr);
    CHECK(parsed->otterbrix_params->node->type() == cl::node_type::aggregate_t);
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 0);
    CHECK(parsed->otterbrix_params->external_nodes.empty());
}

// ── Filter ────────────────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: Filter with condition") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Plan plan;
    auto* filter = plan.mutable_root()->mutable_filter();
    *filter->mutable_input() = make_read_relation("db1.t1");
    // condition: id = 1
    auto* fn = filter->mutable_condition()->mutable_unresolved_function();
    fn->set_function_name("=");
    *fn->add_arguments() = make_attribute("id");
    fn->add_arguments()->mutable_literal()->set_integer(1);

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());

    auto& parsed = result.value().parsed_data;
    REQUIRE(parsed != nullptr);
    REQUIRE(parsed->otterbrix_params->node != nullptr);

    const auto& children = parsed->otterbrix_params->node->children();
    REQUIRE(children.size() == 1);
    CHECK(children[0]->type() == cl::node_type::match_t);
    CHECK(children[0]->expressions().size() == 1);
}

TEST_CASE("relation_to_plan: Filter with ExpressionString predicate (raw SQL)") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Plan plan;
    auto* filter = plan.mutable_root()->mutable_filter();
    *filter->mutable_input() = make_read_relation("db1.t1");
    // condition arrives as a raw SQL string, as PySpark's .filter("budget > 0")
    // sends it (Expression.ExpressionString), not a structured expression tree.
    filter->mutable_condition()->mutable_expression_string()->set_expression("budget > 0");

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());

    auto& parsed = result.value().parsed_data;
    REQUIRE(parsed != nullptr);
    REQUIRE(parsed->otterbrix_params->node != nullptr);

    const auto& children = parsed->otterbrix_params->node->children();
    REQUIRE(children.size() == 1);
    REQUIRE(children[0]->type() == cl::node_type::match_t);
    REQUIRE(children[0]->expressions().size() == 1);

    // The predicate must be in the executable transformer form: a plain key on
    // the field side and a bound parameter (neither a raw key nor a nested
    // expression) on the value side — NOT the non-executable Spark shape
    // compare(get_field_scalar, constant_scalar).
    const auto& pred = children[0]->expressions()[0];
    const auto* cmp = static_cast<const ce::compare_expression_t*>(pred.get());
    CHECK(cmp->type() == ce::compare_type::gt);
    CHECK(ce::is_key(cmp->left()));
    CHECK_FALSE(ce::is_key(cmp->right()));
    CHECK_FALSE(ce::is_expr(cmp->right()));

    // The constant 0 was materialised into the SHARED parameter node (single
    // id-space), so remote SQL generation / local execution can resolve it.
    const auto& pmap = parsed->otterbrix_params->params_node->parameters().parameters;
    REQUIRE(pmap.size() == 1);
    CHECK(pmap.begin()->second.value<int64_t>() == 0);
}

TEST_CASE("relation_to_plan: SQL relation binds its constants into the shared params") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Plan plan;
    // PySpark 4.0 sends spark.sql("...") as a Relation{kSql} root (Path B). Its
    // constants must be materialised into the plan's shared parameter node, or
    // execution fails with "value getter: parameter not bound".
    plan.mutable_root()->mutable_sql()->set_query("SELECT * FROM t WHERE x > 100");

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());

    auto& parsed = result.value().parsed_data;
    REQUIRE(parsed != nullptr);
    REQUIRE(parsed->otterbrix_params->params_node != nullptr);

    const auto& pmap = parsed->otterbrix_params->params_node->parameters().parameters;
    REQUIRE(pmap.size() == 1);
    CHECK(pmap.begin()->second.value<int64_t>() == 100);
}

// ── Join ────────────────────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: Join using_columns builds an equi-compare in key form") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Plan plan;
    auto* join = plan.mutable_root()->mutable_join();
    *join->mutable_left() = make_read_relation("db1.a");
    *join->mutable_right() = make_read_relation("db1.b");
    join->set_join_type(sc::Join::JOIN_TYPE_INNER);
    join->add_using_columns("campaign_id");

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());

    auto& parsed = result.value().parsed_data;
    REQUIRE(parsed != nullptr);
    const auto& children = parsed->otterbrix_params->node->children();
    REQUIRE(children.size() == 1);
    REQUIRE(children[0]->type() == cl::node_type::join_t);
    REQUIRE(children[0]->expressions().size() == 1);

    const auto& cond = children[0]->expressions()[0];
    const auto* cmp = static_cast<const ce::compare_expression_t*>(cond.get());
    CHECK(cmp->type() == ce::compare_type::eq);
    // Executable transformer form: both operands are plain keys, not get_field
    // scalar expressions (which neither the remote generator nor the local
    // value-getter can evaluate).
    REQUIRE(ce::is_key(cmp->left()));
    REQUIRE(ce::is_key(cmp->right()));
    // The keys must carry opposite sides — the join value-getter selects the
    // input chunk by key.side(), and an undefined-side bare key that occurs in
    // both inputs is rejected as ambiguous by the validator.
    CHECK(ce::as_key(cmp->left()).side() == ce::side_t::left);
    CHECK(ce::as_key(cmp->right()).side() == ce::side_t::right);
}

// ── Range ─────────────────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: Range materialises a single column named id") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Plan plan;
    auto* range = plan.mutable_root()->mutable_range();
    range->set_start(0);
    range->set_end(10);
    range->set_step(1);

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());

    auto& parsed = result.value().parsed_data;
    REQUIRE(parsed != nullptr);
    const auto& children = parsed->otterbrix_params->node->children();
    REQUIRE(children.size() == 1);
    REQUIRE(children[0]->type() == cl::node_type::data_t);

    // spark.range emits exactly one LongType column named "id". The alias is
    // load-bearing: an anonymous column (extension_ == nullptr) crashes the engine's
    // plan validator, which reads col.type.alias() without a has_alias() guard.
    const auto* data_node = static_cast<const cl::node_data_t*>(children[0].get());
    const auto col_types = data_node->data_chunk().types();
    REQUIRE(col_types.size() == 1);
    CHECK(col_types[0].has_alias());
    CHECK(std::string(col_types[0].alias()) == "id");
}

// ── Project ───────────────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: Project with expressions") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Plan plan;
    auto* project = plan.mutable_root()->mutable_project();
    *project->mutable_input() = make_read_relation("db1.t1");
    *project->add_expressions() = make_attribute("a");
    *project->add_expressions() = make_attribute("b");

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());

    auto& parsed = result.value().parsed_data;
    REQUIRE(parsed != nullptr);
    REQUIRE(parsed->otterbrix_params->node != nullptr);

    const auto& children = parsed->otterbrix_params->node->children();
    REQUIRE(children.size() == 1);
    CHECK(children[0]->type() == cl::node_type::select_t);
    REQUIRE(children[0]->expressions().size() == 2);
}

// ── Limit ─────────────────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: Limit") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Plan plan;
    auto* limit = plan.mutable_root()->mutable_limit();
    *limit->mutable_input() = make_read_relation("db1.t1");
    limit->set_limit(5);

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());

    auto& parsed = result.value().parsed_data;
    REQUIRE(parsed != nullptr);
    REQUIRE(parsed->otterbrix_params->node != nullptr);

    const auto& children = parsed->otterbrix_params->node->children();
    REQUIRE(children.size() == 1);
    REQUIRE(children[0]->type() == cl::node_type::limit_t);
    const auto* limit_node = static_cast<const cl::node_limit_t*>(children[0].get());
    CHECK(limit_node->limit().limit() == 5);
}

// ── contains_window ───────────────────────────────────────────────────────────

TEST_CASE("contains_window: true when Project carries a Window expression") {
    sc::Relation rel;
    rel.mutable_project()->add_expressions()->mutable_window();

    REQUIRE(frontend::spark::contains_window(rel));
}

TEST_CASE("contains_window: false for a plain Project") {
    sc::Relation rel;
    rel.mutable_project()->add_expressions()->mutable_literal()->set_integer(1);

    REQUIRE_FALSE(frontend::spark::contains_window(rel));
}
