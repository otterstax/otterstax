// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/spark_connect_server/plan_translator/relation_to_plan.hpp"

#include <spark/connect/expressions.pb.h>
#include <spark/connect/relations.pb.h>

#include <components/logical_plan/forward.hpp>  // node_type
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node.hpp>

#include "otterbrix/parser/parser.hpp"  // ParsedQueryData
#include "types/otterbrix.hpp"          // OtterbrixStatement / external_entry_t

#include <catch2/catch.hpp>

#include <memory_resource>
#include <string>

namespace {

namespace sc = ::spark::connect;
namespace cl = components::logical_plan;

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
