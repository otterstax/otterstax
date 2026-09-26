// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/spark_connect_server/plan_translator/expression_to_plan.hpp"

#include <spark/connect/expressions.pb.h>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <string>
#include <string_view>
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

TEST_CASE("expression_to_plan: avg averages its argument cast to DOUBLE") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("avg");
    *fn->add_arguments() = make_attribute("amount");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::aggregate);
    auto* agg = static_cast<ce::aggregate_expression_t*>(out.get());
    CHECK(agg->function_name() == "avg");
    REQUIRE(agg->params().size() == 1);
    // Spark's Average casts its input to DOUBLE; the engine's avg would keep a BIGINT
    // column's type and drop the fraction. The column is the cast's key, as in the SQL
    // transformer's avg(CAST(amount AS double)).
    REQUIRE(ce::is_expr(agg->params().front()));
    const auto& operand = ce::as_expr(agg->params().front());
    REQUIRE(operand->group() == ce::expression_group::cast);
    const auto* cast = static_cast<const ce::cast_expression_t*>(operand.get());
    CHECK(cast->result_type().type() == ct::logical_type::DOUBLE);
    CHECK(cast->kind() == components::casts::cast_kind::cast);
    REQUIRE(ce::is_key(cast->child()));
    CHECK(ce::as_key(cast->child()).as_string() == "amount");
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
    // The transformer's form compare(key_t, parameter_id_t): the column as its
    // key, the literal as the id it was bound to.
    REQUIRE(ce::is_key(cmp->left()));
    CHECK(ce::as_key(cmp->left()).as_string() == "id");
    REQUIRE(ce::is_parameter(cmp->right()));
    CHECK(params->parameter(ce::as_parameter(cmp->right())).value<int64_t>() == 1);
}

TEST_CASE("expression_to_plan: == compares a column key with a bound parameter") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // PySpark's Column.__eq__ sends the operator as "==".
    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("==");
    *fn->add_arguments() = make_attribute("region");
    fn->add_arguments()->mutable_literal()->set_string("north");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::compare);
    auto* cmp = static_cast<ce::compare_expression_t*>(out.get());
    CHECK(cmp->type() == ce::compare_type::eq);
    REQUIRE(ce::is_key(cmp->left()));
    CHECK(ce::as_key(cmp->left()).as_string() == "region");
    REQUIRE(ce::is_parameter(cmp->right()));
    CHECK(params->parameter(ce::as_parameter(cmp->right())).value<std::string_view>() == "north");
}

TEST_CASE("expression_to_plan: isNull compares a column key with a NULL placeholder") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("isnull");
    *fn->add_arguments() = make_attribute("amount");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::compare);
    auto* cmp = static_cast<ce::compare_expression_t*>(out.get());
    CHECK(cmp->type() == ce::compare_type::is_null);
    REQUIRE(ce::is_key(cmp->left()));
    CHECK(ce::as_key(cmp->left()).as_string() == "amount");
    REQUIRE(ce::is_parameter(cmp->right()));
    CHECK(params->parameter(ce::as_parameter(cmp->right())).is_null());
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
    REQUIRE(out->group() == ce::expression_group::scalar);
    auto* scalar = static_cast<ce::scalar_expression_t*>(out.get());
    CHECK(scalar->type() == ce::scalar_type::get_field);
    // The alias names the output (the key the engine and the SQL generator
    // read); the column it reads, amount, is the only parameter.
    REQUIRE(scalar->key().storage().size() == 1);
    CHECK(scalar->key().storage()[0] == "total");
    REQUIRE(scalar->params().size() == 1);
    REQUIRE(ce::is_key(scalar->params().front()));
    REQUIRE(ce::as_key(scalar->params().front()).storage().size() == 1);
    CHECK(ce::as_key(scalar->params().front()).storage()[0] == "amount");
}

TEST_CASE("expression_to_plan: alias names an aggregate by its key") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* al = expr.mutable_alias();
    al->add_name("total");
    auto* fn = al->mutable_expr()->mutable_unresolved_function();
    fn->set_function_name("sum");
    *fn->add_arguments() = make_attribute("amount");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::aggregate);
    auto* agg = static_cast<ce::aggregate_expression_t*>(out.get());
    CHECK(agg->function_name() == "sum");
    CHECK(agg->key().as_string() == "total");
}

TEST_CASE("expression_to_plan: count of star is an aggregate without arguments") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // F.count("*"): the parameterless count, named by its function.
    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("count");
    fn->add_arguments()->mutable_unresolved_star();

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::aggregate);
    auto* agg = static_cast<ce::aggregate_expression_t*>(out.get());
    CHECK(agg->function_name() == "count");
    CHECK(agg->params().empty());
    CHECK(agg->key().as_string() == "count");
}

TEST_CASE("expression_to_plan: count of a literal counts every row like count of star") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // df.count() and groupBy().count() send count(lit(1)). The engine evaluates
    // an argument that reads no column once per chunk, so the literal is not
    // kept: the count takes no argument, as count(*) does.
    sc::Expression expr;
    auto* fn = expr.mutable_unresolved_function();
    fn->set_function_name("count");
    fn->add_arguments()->mutable_literal()->set_integer(1);

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::aggregate);
    auto* agg = static_cast<ce::aggregate_expression_t*>(out.get());
    CHECK(agg->function_name() == "count");
    CHECK(agg->params().empty());
    CHECK(agg->key().as_string() == "count");
}

TEST_CASE("expression_to_plan: an aggregate over a constant argument is refused") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    struct constant_case {
        const char* function;
        bool distinct;
        bool null_literal;
        const char* message;
    };
    // Folded once per chunk rather than once per row, each would answer a
    // wrong result: count(NULL), countDistinct(lit(1)), sum(lit(2)).
    const constant_case cases[] = {
        {"count", false, true, "Spark count() over a constant argument is not supported"},
        {"count", true, false, "Spark count() over a constant argument is not supported"},
        {"sum", false, false, "Spark sum() over a constant argument is not supported"},
    };
    for (const auto& constant : cases) {
        INFO(constant.message);
        sc::Expression expr;
        auto* fn = expr.mutable_unresolved_function();
        fn->set_function_name(constant.function);
        fn->set_is_distinct(constant.distinct);
        auto* literal = fn->add_arguments()->mutable_literal();
        if (constant.null_literal) {
            literal->mutable_null();
        } else {
            literal->set_integer(2);
        }

        auto params = cl::make_parameter_node(resource);
        auto result = frontend::spark::expression_to_plan(expr, params, resource);
        REQUIRE(result.has_error());
        CHECK(result.error().type == core::error_code_t::unimplemented_yet);
        CHECK(std::string_view{result.error().what} == constant.message);
    }
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
    // rc-3 carries the sort key as the operand, the expression's own key stays empty.
    REQUIRE(ce::is_key(sort->operand()));
    CHECK(ce::as_key(sort->operand()).storage()[0] == "ts");
    // Spark's default for ascending: NULLs first.
    CHECK(sort->null_order() == ce::sort_null_order::nulls_first);
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
    REQUIRE(ce::is_key(sort->operand()));
    CHECK(ce::as_key(sort->operand()).storage()[0] == "ts");
    // Spark's default for descending: NULLs last.
    CHECK(sort->null_order() == ce::sort_null_order::nulls_last);
}

TEST_CASE("expression_to_plan: sort order keeps an explicit null placement") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Expression expr;
    auto* so = expr.mutable_sort_order();
    so->set_direction(sc::Expression::SortOrder::SORT_DIRECTION_ASCENDING);
    so->set_null_ordering(sc::Expression::SortOrder::SORT_NULLS_LAST);
    *so->mutable_child() = make_attribute("ts");

    auto params = cl::make_parameter_node(resource);
    auto result = frontend::spark::expression_to_plan(expr, params, resource);
    REQUIRE_FALSE(result.has_error());

    auto out = result.value();
    REQUIRE(out->group() == ce::expression_group::sort);
    auto* sort = static_cast<ce::sort_expression_t*>(out.get());
    CHECK(sort->order() == ce::sort_order::asc);
    CHECK(sort->null_order() == ce::sort_null_order::nulls_last);
}
