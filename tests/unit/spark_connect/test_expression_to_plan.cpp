// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/spark_connect_server/plan_translator/expression_to_plan.hpp"

#include <spark/connect/expressions.pb.h>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <catch2/catch.hpp>

#include <memory_resource>
#include <string>
#include <variant>

namespace {

namespace sc = ::spark::connect;
namespace ce = components::expressions;
namespace cl = components::logical_plan;
namespace ct = components::types;

// Builds an UnresolvedAttribute expression proto referencing `name`.
sc::Expression make_attribute(const std::string& name) {
    sc::Expression expr;
    expr.mutable_unresolved_attribute()->set_unparsed_identifier(name);
    return expr;
}

} // namespace

// ── Literal ───────────────────────────────────────────────────────────────────

TEST_CASE("expression_to_plan: literal boolean") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    expr.mutable_literal()->set_boolean(true);

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::scalar);
    auto* scalar = static_cast<ce::scalar_expression_t*>(out.get());
    REQUIRE(scalar->type() == ce::scalar_type::constant);
    REQUIRE(scalar->params().size() == 1);

    const auto id = std::get<core::parameter_id_t>(scalar->params().front());
    REQUIRE(params->parameter(id).value<bool>() == true);
}

TEST_CASE("expression_to_plan: literal integer") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    expr.mutable_literal()->set_integer(42);

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::scalar);
    auto* scalar = static_cast<ce::scalar_expression_t*>(out.get());
    REQUIRE(scalar->type() == ce::scalar_type::constant);
    REQUIRE(scalar->params().size() == 1);

    const auto id = std::get<core::parameter_id_t>(scalar->params().front());
    REQUIRE(params->parameter(id).value<int64_t>() == 42);
}

TEST_CASE("expression_to_plan: literal string") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    expr.mutable_literal()->set_string("hello");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::scalar);
    auto* scalar = static_cast<ce::scalar_expression_t*>(out.get());
    REQUIRE(scalar->type() == ce::scalar_type::constant);
    REQUIRE(scalar->params().size() == 1);

    const auto id = std::get<core::parameter_id_t>(scalar->params().front());
    REQUIRE(params->parameter(id) == ct::logical_value_t{resource, std::string{"hello"}});
}

// ── UnresolvedAttribute ───────────────────────────────────────────────────────

TEST_CASE("expression_to_plan: unresolved attribute simple name") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr = make_attribute("col");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::scalar);
    auto* scalar = static_cast<ce::scalar_expression_t*>(out.get());
    REQUIRE(scalar->type() == ce::scalar_type::get_field);
    REQUIRE(scalar->key().storage().size() == 1);
    REQUIRE(scalar->key().storage()[0] == "col");
}

TEST_CASE("expression_to_plan: unresolved attribute qualified name a.b.c") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr = make_attribute("a.b.c");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::scalar);
    auto* scalar = static_cast<ce::scalar_expression_t*>(out.get());
    REQUIRE(scalar->type() == ce::scalar_type::get_field);
    REQUIRE(scalar->key().storage().size() == 3);
    REQUIRE(scalar->key().storage()[0] == "a");
    REQUIRE(scalar->key().storage()[1] == "b");
    REQUIRE(scalar->key().storage()[2] == "c");
}

// ── UnresolvedFunction ────────────────────────────────────────────────────────

TEST_CASE("expression_to_plan: unresolved function count (aggregate)") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("count");
    *fn->add_arguments() = make_attribute("id");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::aggregate);
    auto* agg = static_cast<ce::aggregate_expression_t*>(out.get());
    CHECK(agg->function_name() == "count");
    CHECK_FALSE(agg->is_distinct());
    REQUIRE(agg->params().size() == 1);
}

TEST_CASE("expression_to_plan: unresolved function sum (aggregate)") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("sum");
    *fn->add_arguments() = make_attribute("amount");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::aggregate);
    auto* agg = static_cast<ce::aggregate_expression_t*>(out.get());
    CHECK(agg->function_name() == "sum");
    REQUIRE(agg->params().size() == 1);
}

TEST_CASE("expression_to_plan: unresolved function + (arithmetic operator)") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("+");
    *fn->add_arguments() = make_attribute("a");
    *fn->add_arguments() = make_attribute("b");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::scalar);
    auto* scalar = static_cast<ce::scalar_expression_t*>(out.get());
    CHECK(scalar->type() == ce::scalar_type::add);
    REQUIRE(scalar->params().size() == 2);
}

TEST_CASE("expression_to_plan: unresolved function = (comparison operator)") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("=");
    *fn->add_arguments() = make_attribute("id");
    sc::Expression rhs;
    rhs.mutable_literal()->set_integer(1);
    *fn->add_arguments() = rhs;

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::compare);
    auto* cmp = static_cast<ce::compare_expression_t*>(out.get());
    CHECK(cmp->type() == ce::compare_type::eq);
    CHECK(std::holds_alternative<ce::expression_ptr>(cmp->left()));
    CHECK(std::holds_alternative<ce::expression_ptr>(cmp->right()));
}

TEST_CASE("expression_to_plan: unresolved function and (logical union)") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("and");
    *fn->add_arguments() = make_attribute("flag_a");
    *fn->add_arguments() = make_attribute("flag_b");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::compare);
    auto* cmp = static_cast<ce::compare_expression_t*>(out.get());
    CHECK(cmp->type() == ce::compare_type::union_and);
    REQUIRE(cmp->children().size() == 2);
}

// ── Alias ─────────────────────────────────────────────────────────────────────

TEST_CASE("expression_to_plan: alias wraps child expression") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* al = expr.mutable_alias();
    al->add_name("total");
    *al->mutable_expr() = make_attribute("amount");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    CHECK(out->result_alias() == "total");
    REQUIRE(out->group() == ce::expression_group::scalar);
    auto* scalar = static_cast<ce::scalar_expression_t*>(out.get());
    CHECK(scalar->type() == ce::scalar_type::get_field);
    REQUIRE(scalar->key().storage().size() == 1);
    CHECK(scalar->key().storage()[0] == "amount");
}

// ── SortOrder ─────────────────────────────────────────────────────────────────

TEST_CASE("expression_to_plan: sort order ascending") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* so = expr.mutable_sort_order();
    so->set_direction(sc::Expression::SortOrder::SORT_DIRECTION_ASCENDING);
    *so->mutable_child() = make_attribute("ts");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::sort);
    auto* sort = static_cast<ce::sort_expression_t*>(out.get());
    CHECK(sort->order() == ce::sort_order::asc);
    CHECK(sort->key().storage()[0] == "ts");
}

TEST_CASE("expression_to_plan: sort order descending") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* so = expr.mutable_sort_order();
    so->set_direction(sc::Expression::SortOrder::SORT_DIRECTION_DESCENDING);
    *so->mutable_child() = make_attribute("ts");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::sort);
    auto* sort = static_cast<ce::sort_expression_t*>(out.get());
    CHECK(sort->order() == ce::sort_order::desc);
    CHECK(sort->key().storage()[0] == "ts");
}
