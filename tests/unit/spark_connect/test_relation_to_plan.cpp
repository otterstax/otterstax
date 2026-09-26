// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/spark_connect_server/plan_translator/relation_to_plan.hpp"

#include <spark/connect/expressions.pb.h>
#include <spark/connect/relations.pb.h>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/forward.hpp> // node_type
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/types/logical_value.hpp>
#include <core/result_wrapper.hpp>

#include "otterbrix/parser/parser.hpp" // ParsedQueryData
#include "types/otterbrix.hpp"         // OtterbrixStatement / external_entry_t

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <initializer_list>
#include <memory_resource>
#include <string>
#include <string_view>

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

    // Returns a LongType literal.
    sc::Expression make_long(int64_t value) {
        sc::Expression expr;
        expr.mutable_literal()->set_long_(value);
        return expr;
    }

    // Returns a StringType literal.
    sc::Expression make_string(const std::string& value) {
        sc::Expression expr;
        expr.mutable_literal()->set_string(value);
        return expr;
    }

    // Returns an UnresolvedFunction `name(args...)`, as PySpark's Column operators send it.
    sc::Expression make_call(const std::string& name, std::initializer_list<sc::Expression> args) {
        sc::Expression expr;
        auto* fn = expr.mutable_unresolved_function();
        fn->set_function_name(name);
        for (const auto& arg : args) {
            *fn->add_arguments() = arg;
        }
        return expr;
    }

    // Returns `child` renamed to `name`.
    sc::Expression make_alias(const sc::Expression& child, const std::string& name) {
        sc::Expression expr;
        auto* al = expr.mutable_alias();
        *al->mutable_expr() = child;
        al->add_name(name);
        return expr;
    }

    // Returns a bare `*`.
    sc::Expression make_star() {
        sc::Expression expr;
        expr.mutable_unresolved_star();
        return expr;
    }

    // Returns Filter(input, condition).
    sc::Relation make_filter(const sc::Relation& input, const sc::Expression& condition) {
        sc::Relation rel;
        *rel.mutable_filter()->mutable_input() = input;
        *rel.mutable_filter()->mutable_condition() = condition;
        return rel;
    }

    // Returns Limit(input, n).
    sc::Relation make_limit(const sc::Relation& input, int32_t n) {
        sc::Relation rel;
        *rel.mutable_limit()->mutable_input() = input;
        rel.mutable_limit()->set_limit(n);
        return rel;
    }

    // Returns Project(input, expressions...).
    sc::Relation make_project(const sc::Relation& input, std::initializer_list<sc::Expression> expressions) {
        sc::Relation rel;
        *rel.mutable_project()->mutable_input() = input;
        for (const auto& expr : expressions) {
            *rel.mutable_project()->add_expressions() = expr;
        }
        return rel;
    }

    // Returns Sort(input, one ascending key on `column`).
    sc::Relation make_sort(const sc::Relation& input, const std::string& column) {
        sc::Relation rel;
        *rel.mutable_sort()->mutable_input() = input;
        auto* order = rel.mutable_sort()->add_order();
        order->set_direction(sc::Expression::SortOrder::SORT_DIRECTION_ASCENDING);
        *order->mutable_child() = make_attribute(column);
        return rel;
    }

    // Returns Offset(input, n).
    sc::Relation make_offset(const sc::Relation& input, int32_t n) {
        sc::Relation rel;
        *rel.mutable_offset()->mutable_input() = input;
        rel.mutable_offset()->set_offset(n);
        return rel;
    }

    // Returns df.count() over `input`: an Aggregate of count(lit(1)), as PySpark sends it.
    sc::Relation make_count(const sc::Relation& input) {
        sc::Relation rel;
        auto* agg = rel.mutable_aggregate();
        *agg->mutable_input() = input;
        agg->set_group_type(sc::Aggregate::GROUP_TYPE_GROUPBY);
        *agg->add_aggregate_expressions() = make_call("count", {make_long(1)});
        return rel;
    }

    // Returns distinct() over `input`.
    sc::Relation make_distinct(const sc::Relation& input) {
        sc::Relation rel;
        *rel.mutable_deduplicate()->mutable_input() = input;
        rel.mutable_deduplicate()->set_all_columns_as_keys(true);
        return rel;
    }

    // Returns groupBy(key).agg(count(*)) over `input`.
    sc::Relation make_group_count(const sc::Relation& input, const std::string& key) {
        sc::Relation rel;
        auto* agg = rel.mutable_aggregate();
        *agg->mutable_input() = input;
        agg->set_group_type(sc::Aggregate::GROUP_TYPE_GROUPBY);
        *agg->add_grouping_expressions() = make_attribute(key);
        *agg->add_aggregate_expressions() = make_call("count", {make_star()});
        return rel;
    }

    // Wraps `root` into a Plan.
    sc::Plan make_plan(const sc::Relation& root) {
        sc::Plan plan;
        *plan.mutable_root() = root;
        return plan;
    }

    // The message of the unimplemented_yet refusal relation_to_plan answers for
    // `root`, copied off the resource it was built on.
    std::string refusal_of(const sc::Relation& root) {
        std::pmr::synchronized_pool_resource pool;
        auto result = frontend::spark::relation_to_plan(make_plan(root), &pool);
        REQUIRE(result.has_error());
        CHECK(result.error().type == core::error_code_t::unimplemented_yet);
        return std::string(result.error().what.begin(), result.error().what.end());
    }

    // The kinds of `node`'s children, in order, e.g. "aggregate,match".
    std::string child_kinds(const cl::node_ptr& node) {
        std::string kinds;
        for (const auto& child : node->children()) {
            if (!kinds.empty()) {
                kinds += ',';
            }
            switch (child->type()) {
                case cl::node_type::aggregate_t:
                    kinds += "aggregate";
                    break;
                case cl::node_type::match_t:
                    kinds += "match";
                    break;
                case cl::node_type::group_t:
                    kinds += "group";
                    break;
                case cl::node_type::select_t:
                    kinds += "select";
                    break;
                case cl::node_type::sort_t:
                    kinds += "sort";
                    break;
                case cl::node_type::limit_t:
                    kinds += "limit";
                    break;
                default:
                    kinds += "other";
                    break;
            }
        }
        return kinds;
    }

    // The aggregate a derived table reads: the first child of a fresh aggregate
    // that names no table, as the transformer shapes FROM (SELECT ...).
    const cl::node_ptr& derived_inner(const cl::node_ptr& root) {
        REQUIRE(root->type() == cl::node_type::aggregate_t);
        const auto& outer = static_cast<const cl::node_aggregate_t&>(*root);
        CHECK(outer.dbname().t.empty());
        CHECK(outer.relname().t.empty());
        REQUIRE_FALSE(root->children().empty());
        REQUIRE(root->children().front()->type() == cl::node_type::aggregate_t);
        return root->children().front();
    }

    // The single predicate of the root's match child.
    const ce::compare_expression_t* match_predicate(const cl::node_ptr& root) {
        const auto& children = root->children();
        REQUIRE(children.size() == 1);
        REQUIRE(children[0]->type() == cl::node_type::match_t);
        REQUIRE(children[0]->expressions().size() == 1);
        const auto& pred = children[0]->expressions()[0];
        REQUIRE(pred->group() == ce::expression_group::compare);
        return static_cast<const ce::compare_expression_t*>(pred.get());
    }

    // True when `cmp` compares the column `column` (left) with a parameter holding `value` (right).
    bool compares_column_with(const ce::compare_expression_t& cmp,
                              std::string_view column,
                              const cl::parameter_node_ptr& params,
                              int64_t value) {
        return ce::is_key(cmp.left()) && ce::as_key(cmp.left()).storage().size() == 1 &&
               std::string_view{ce::as_key(cmp.left()).storage()[0]} == column && ce::is_parameter(cmp.right()) &&
               params->parameter(ce::as_parameter(cmp.right())).value<int64_t>() == value;
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
    // A PostgreSQL table is mirrored under its schema: the external name keeps
    // the one the identifier named.
    CHECK(target.name.schema == "sch1");
}

TEST_CASE("relation_to_plan: Read.NamedTable reads one to four name parts as the SQL path does") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    struct expected_name {
        const char* identifier;
        const char* uid;
        const char* database;
        const char* schema;
        const char* table;
    };
    // One part is a table and two a local db.table; three are alias.db.table,
    // the database standing in for the schema as promote_three_part_qualifiers
    // has it (the catalog blanks it for MySQL and ClickHouse); four are
    // alias.db.schema.table.
    const expected_name cases[] = {
        {"t1", "", "", "", "t1"},
        {"db1.t1", "", "db1", "", "t1"},
        {"alias1.db1.t1", "alias1", "db1", "db1", "t1"},
        {"alias1.db1.sch1.t1", "alias1", "db1", "sch1", "t1"},
    };
    for (const auto& expected : cases) {
        INFO("identifier " << expected.identifier);
        auto result = frontend::spark::relation_to_plan(make_read_plan(expected.identifier), resource);
        REQUIRE_FALSE(result.has_error());
        const auto& statement = *result.value().parsed_data->otterbrix_params;
        REQUIRE(statement.node->type() == cl::node_type::aggregate_t);
        const auto& agg = static_cast<const cl::node_aggregate_t&>(*statement.node);
        CHECK(agg.uid().t == expected.uid);
        CHECK(agg.dbname().t == expected.database);
        CHECK(agg.relname().t == expected.table);
        if (std::string_view{expected.uid}.empty()) {
            // A local table: the engine resolves it, nothing is external.
            CHECK(statement.external_nodes_count == 0);
            continue;
        }
        REQUIRE(statement.external_nodes_count == 1);
        REQUIRE(statement.external_nodes.size() == 1);
        REQUIRE(statement.external_nodes[0].size() == 1);
        const auto& name = statement.external_nodes[0][0].target.name;
        CHECK(name.unique_identifier == expected.uid);
        CHECK(name.database == expected.database);
        CHECK(name.schema == expected.schema);
        CHECK(name.collection == expected.table);
    }
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

TEST_CASE("relation_to_plan: SQL relation keeps the schema of an external table it names") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // spark.sql("SELECT * FROM a.pgdb.public.t").filter("price > 100").select("name", "price")
    sc::Relation leaf;
    leaf.mutable_sql()->set_query("SELECT * FROM a.pgdb.public.t");
    sc::Relation filtered = make_filter(leaf, sc::Expression{});
    filtered.mutable_filter()->mutable_condition()->mutable_expression_string()->set_expression("price > 100");
    auto plan = make_plan(make_project(filtered, {make_attribute("name"), make_attribute("price")}));

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());
    const auto& statement = *result.value().parsed_data->otterbrix_params;
    REQUIRE(statement.external_nodes_count == 1);
    REQUIRE(statement.external_nodes.size() == 1);
    REQUIRE(statement.external_nodes[0].size() == 1);

    // The leaf's aggregate names (a, pgdb, t) only; a PostgreSQL table is
    // mirrored under its schema, which comes back from the leaf's parse tree.
    const auto& name = statement.external_nodes[0][0].target.name;
    CHECK(name.unique_identifier == "a");
    CHECK(name.database == "pgdb");
    CHECK(name.schema == "public");
    CHECK(name.collection == "t");
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

TEST_CASE("relation_to_plan: crossJoin carries a match-everything condition") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // crossJoin, and a join() without `on`: the planner reads the join's first
    // expression unconditionally, so neither may reach it without one.
    for (auto type : {sc::Join::JOIN_TYPE_CROSS, sc::Join::JOIN_TYPE_INNER}) {
        sc::Plan plan;
        auto* join = plan.mutable_root()->mutable_join();
        *join->mutable_left() = make_read_relation("db1.a");
        *join->mutable_right() = make_read_relation("db1.b");
        join->set_join_type(type);

        auto result = frontend::spark::relation_to_plan(plan, resource);
        REQUIRE_FALSE(result.has_error());
        const auto& children = result.value().parsed_data->otterbrix_params->node->children();
        REQUIRE(children.size() == 1);
        REQUIRE(children[0]->type() == cl::node_type::join_t);
        REQUIRE(children[0]->expressions().size() == 1);
        const auto& cond = children[0]->expressions()[0];
        REQUIRE(cond->group() == ce::expression_group::compare);
        CHECK(static_cast<const ce::compare_expression_t*>(cond.get())->type() == ce::compare_type::all_true);
    }
}

// ── Sort ──────────────────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: Sort maps Spark null placement explicitly") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Plan plan;
    auto* sort = plan.mutable_root()->mutable_sort();
    *sort->mutable_input() = make_read_relation("db1.t1");
    struct key_order {
        const char* column;
        sc::Expression::SortOrder::SortDirection direction;
        sc::Expression::SortOrder::NullOrdering nulls;
    };
    const key_order keys[] = {
        {"a", sc::Expression::SortOrder::SORT_DIRECTION_ASCENDING, sc::Expression::SortOrder::SORT_NULLS_UNSPECIFIED},
        {"b", sc::Expression::SortOrder::SORT_DIRECTION_DESCENDING, sc::Expression::SortOrder::SORT_NULLS_UNSPECIFIED},
        {"c", sc::Expression::SortOrder::SORT_DIRECTION_ASCENDING, sc::Expression::SortOrder::SORT_NULLS_LAST},
        {"d", sc::Expression::SortOrder::SORT_DIRECTION_DESCENDING, sc::Expression::SortOrder::SORT_NULLS_FIRST},
    };
    for (const auto& key : keys) {
        auto* order = sort->add_order();
        *order->mutable_child() = make_attribute(key.column);
        order->set_direction(key.direction);
        order->set_null_ordering(key.nulls);
    }

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());
    const auto& children = result.value().parsed_data->otterbrix_params->node->children();
    REQUIRE(children.size() == 1);
    REQUIRE(children[0]->type() == cl::node_type::sort_t);
    const auto& exprs = children[0]->expressions();
    REQUIRE(exprs.size() == 4);

    // Spark's default is NULLS FIRST ascending and NULLS LAST descending — the
    // opposite of the SQL rule nulls_default stands for — so it is spelled out.
    const ce::sort_null_order expected[] = {ce::sort_null_order::nulls_first,
                                            ce::sort_null_order::nulls_last,
                                            ce::sort_null_order::nulls_last,
                                            ce::sort_null_order::nulls_first};
    for (size_t i = 0; i < 4; ++i) {
        REQUIRE(exprs[i]->group() == ce::expression_group::sort);
        const auto* key = static_cast<const ce::sort_expression_t*>(exprs[i].get());
        REQUIRE(ce::is_key(key->operand()));
        CHECK(ce::as_key(key->operand()).as_string() == keys[i].column);
        CHECK(key->null_order() == expected[i]);
    }
    CHECK(static_cast<const ce::sort_expression_t*>(exprs[1].get())->order() == ce::sort_order::desc);
}

// ── SQL fragments ─────────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: SQL relation lowering a sub-query is refused") {
    // The sub-query's plan would be dropped with the fragment's execution plan.
    sc::Relation in_subquery;
    in_subquery.mutable_sql()->set_query("SELECT * FROM t WHERE x IN (SELECT y FROM u)");
    CHECK(refusal_of(in_subquery).find("sub-queries") != std::string::npos);

    sc::Relation filter_subquery = make_filter(make_read_relation("db1.t1"), sc::Expression{});
    filter_subquery.mutable_filter()->mutable_condition()->mutable_expression_string()->set_expression(
        "x IN (SELECT y FROM u)");
    CHECK(refusal_of(filter_subquery).find("sub-queries") != std::string::npos);
}

TEST_CASE("relation_to_plan: SQL relation that is not a single SELECT is refused") {
    sc::Relation dml;
    dml.mutable_sql()->set_query("DELETE FROM t WHERE x = 1");
    CHECK(refusal_of(dml).find("SELECT") != std::string::npos);
}

// ── Deduplicate ───────────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: dropDuplicates over a subset of columns is refused") {
    // A DISTINCT over every column would keep a row per distinct row, not one
    // per subset key.
    sc::Relation rel;
    *rel.mutable_deduplicate()->mutable_input() = make_read_relation("db1.t1");
    rel.mutable_deduplicate()->add_column_names("a");
    CHECK(refusal_of(rel).find("dropDuplicates") != std::string::npos);
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

    // The transformer's shape: the output list on a group, beside an empty
    // select (the validator dissolves a group without keys or aggregates).
    const auto& children = parsed->otterbrix_params->node->children();
    REQUIRE(children.size() == 2);
    CHECK(children[0]->type() == cl::node_type::group_t);
    REQUIRE(children[0]->expressions().size() == 2);
    CHECK(children[1]->type() == cl::node_type::select_t);
    CHECK(children[1]->expressions().empty());
}

TEST_CASE("relation_to_plan: Project names an aliased column by its key") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    auto plan = make_plan(
        make_project(make_read_relation("db1.t1"), {make_alias(make_attribute("a"), "x"), make_attribute("b")}));

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());
    const auto& children = result.value().parsed_data->otterbrix_params->node->children();
    REQUIRE(children.size() == 2);
    REQUIRE(children[0]->type() == cl::node_type::group_t);
    const auto& outputs = children[0]->expressions();
    REQUIRE(outputs.size() == 2);

    // `a AS x`: the output name is the key, the column read its only parameter.
    REQUIRE(outputs[0]->group() == ce::expression_group::scalar);
    const auto* renamed = static_cast<const ce::scalar_expression_t*>(outputs[0].get());
    CHECK(renamed->type() == ce::scalar_type::get_field);
    CHECK(renamed->key().as_string() == "x");
    REQUIRE(renamed->params().size() == 1);
    REQUIRE(ce::is_key(renamed->params().front()));
    CHECK(ce::as_key(renamed->params().front()).as_string() == "a");

    const auto* plain = static_cast<const ce::scalar_expression_t*>(outputs[1].get());
    CHECK(plain->type() == ce::scalar_type::get_field);
    CHECK(plain->key().as_string() == "b");
    CHECK(plain->params().empty());
}

TEST_CASE("relation_to_plan: Project of a lone star adds no clause") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    auto plan = make_plan(make_project(make_read_relation("db1.t1"), {make_star()}));

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());
    CHECK(result.value().parsed_data->otterbrix_params->node->children().empty());
}

// ── Aggregate ─────────────────────────────────────────────────────────────────

TEST_CASE("relation_to_plan: groupBy agg builds the group shape with named outputs") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    sc::Relation rel;
    auto* agg = rel.mutable_aggregate();
    *agg->mutable_input() = make_read_relation("db1.sales");
    agg->set_group_type(sc::Aggregate::GROUP_TYPE_GROUPBY);
    *agg->add_grouping_expressions() = make_attribute("region");
    *agg->add_aggregate_expressions() = make_alias(make_call("sum", {make_attribute("amount")}), "total");
    *agg->add_aggregate_expressions() = make_call("count", {make_star()});

    auto result = frontend::spark::relation_to_plan(make_plan(rel), resource);
    REQUIRE_FALSE(result.has_error());

    // rc-3 keeps the whole SELECT list in the group; the select beside it is empty.
    const auto& children = result.value().parsed_data->otterbrix_params->node->children();
    REQUIRE(children.size() == 2);
    REQUIRE(children[0]->type() == cl::node_type::group_t);
    CHECK(children[1]->type() == cl::node_type::select_t);
    CHECK(children[1]->expressions().empty());

    const auto& exprs = children[0]->expressions();
    REQUIRE(exprs.size() == 4);

    // The grouping key as a group_field marker...
    REQUIRE(exprs[0]->group() == ce::expression_group::scalar);
    const auto* marker = static_cast<const ce::scalar_expression_t*>(exprs[0].get());
    CHECK(marker->type() == ce::scalar_type::group_field);
    CHECK(marker->key().as_string() == "region");
    CHECK(marker->params().empty());

    // ...then the outputs in Spark's order: the grouping column first.
    REQUIRE(exprs[1]->group() == ce::expression_group::scalar);
    const auto* key_column = static_cast<const ce::scalar_expression_t*>(exprs[1].get());
    CHECK(key_column->type() == ce::scalar_type::get_field);
    CHECK(key_column->key().as_string() == "region");

    // An aliased aggregate is named by its key.
    REQUIRE(exprs[2]->group() == ce::expression_group::aggregate);
    const auto* sum = static_cast<const ce::aggregate_expression_t*>(exprs[2].get());
    CHECK(sum->function_name() == "sum");
    CHECK(sum->key().as_string() == "total");
    CHECK(sum->params().size() == 1);

    // count(*) takes no argument (the star kernel) and is named by its function.
    REQUIRE(exprs[3]->group() == ce::expression_group::aggregate);
    const auto* count = static_cast<const ce::aggregate_expression_t*>(exprs[3].get());
    CHECK(count->function_name() == "count");
    CHECK(count->key().as_string() == "count");
    CHECK(count->params().empty());
}

TEST_CASE("relation_to_plan: groupBy on a computed key marks it the way the transformer does") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // df.groupBy((df.amount % 2).alias("parity")).count()
    auto rel = make_group_count(make_read_relation("db1.sales"), "unused");
    *rel.mutable_aggregate()->mutable_grouping_expressions(0) =
        make_alias(make_call("%", {make_attribute("amount"), make_long(2)}), "parity");

    auto result = frontend::spark::relation_to_plan(make_plan(rel), resource);
    REQUIRE_FALSE(result.has_error());
    const auto& children = result.value().parsed_data->otterbrix_params->node->children();
    REQUIRE(children.size() == 2);
    REQUIRE(children[0]->type() == cl::node_type::group_t);
    const auto& exprs = children[0]->expressions();
    REQUIRE(exprs.size() == 3);

    // The marker is named __group_key_0 and carries the expression it groups by...
    REQUIRE(exprs[0]->group() == ce::expression_group::scalar);
    const auto* marker = static_cast<const ce::scalar_expression_t*>(exprs[0].get());
    CHECK(marker->type() == ce::scalar_type::group_field);
    CHECK(marker->key().as_string() == "__group_key_0");
    REQUIRE(marker->params().size() == 1);
    REQUIRE(ce::is_expr(marker->params().front()));
    CHECK(ce::as_expr(marker->params().front())->group() == ce::expression_group::scalar);

    // ...and the output is a second copy of that expression, named by the alias.
    REQUIRE(exprs[1]->group() == ce::expression_group::scalar);
    const auto* output = static_cast<const ce::scalar_expression_t*>(exprs[1].get());
    CHECK(output->type() == ce::scalar_type::mod);
    CHECK(output->key().as_string() == "parity");
    CHECK(exprs[1].get() != ce::as_expr(marker->params().front()).get());
}

// ── Operation chains (clause stacking) ────────────────────────────────────────

TEST_CASE("relation_to_plan: consecutive filters are ANDed into one match") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    auto inner = make_filter(make_read_relation("db1.t1"), make_call(">", {make_attribute("a"), make_long(1)}));
    auto plan = make_plan(make_filter(inner, make_call("<", {make_attribute("b"), make_long(2)})));

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());
    const auto& statement = *result.value().parsed_data->otterbrix_params;

    // One match — the engine keeps only the last one — holding both predicates.
    const auto* both = match_predicate(statement.node);
    CHECK(both->type() == ce::compare_type::union_and);
    REQUIRE(both->children().size() == 2);
    const auto* first = static_cast<const ce::compare_expression_t*>(both->children()[0].get());
    const auto* second = static_cast<const ce::compare_expression_t*>(both->children()[1].get());
    CHECK(first->type() == ce::compare_type::gt);
    CHECK(compares_column_with(*first, "a", statement.params_node, 1));
    CHECK(second->type() == ce::compare_type::lt);
    CHECK(compares_column_with(*second, "b", statement.params_node, 2));
}

TEST_CASE("relation_to_plan: an operation the clause order cannot stack reads a derived table") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    const auto read = make_read_relation("db1.t1");
    const auto gt_one = make_call(">", {make_attribute("a"), make_long(1)});
    struct derived_case {
        const char* chain;
        sc::Relation relation;
        const char* outer; // the fresh aggregate's children
        const char* inner; // the children of the aggregate it reads
    };
    // The clauses the engine keeps only once per aggregate, or runs in an order
    // that would change the rows: the operation reads a derived table instead,
    // shaped as the transformer shapes FROM (SELECT ...).
    const derived_case cases[] = {
        {"limit().filter()", make_filter(make_limit(read, 5), gt_one), "aggregate,match", "limit"},
        {"limit().orderBy()", make_sort(make_limit(read, 5), "a"), "aggregate,sort", "limit"},
        {"limit().groupBy()", make_group_count(make_limit(read, 5), "a"), "aggregate,group,select", "limit"},
        {"limit().select(sum)",
         make_project(make_limit(read, 5), {make_call("sum", {make_attribute("a")})}),
         "aggregate,group,select",
         "limit"},
        {"orderBy().orderBy()", make_sort(make_sort(read, "a"), "b"), "aggregate,sort", "sort"},
        {"orderBy().groupBy()", make_group_count(make_sort(read, "a"), "a"), "aggregate,group,select", "sort"},
        {"groupBy().filter()", make_filter(make_group_count(read, "a"), gt_one), "aggregate,match", "group,select"},
        {"groupBy().select()",
         make_project(make_group_count(read, "a"), {make_attribute("a")}),
         "aggregate,group,select",
         "group,select"},
        {"select().groupBy()",
         make_group_count(make_project(read, {make_attribute("a")}), "a"),
         "aggregate,group,select",
         "group,select"},
        {"select().select()",
         make_project(make_project(read, {make_attribute("a"), make_attribute("b")}), {make_attribute("a")}),
         "aggregate,group,select",
         "group,select"},
        {"distinct().filter()", make_filter(make_distinct(read), gt_one), "aggregate,match", ""},
        {"distinct().orderBy()", make_sort(make_distinct(read), "a"), "aggregate,sort", ""},
        {"distinct().select()", make_project(make_distinct(read), {make_attribute("a")}), "aggregate,group,select", ""},
        {"distinct().count()", make_count(make_distinct(read)), "aggregate,group,select", ""},
    };
    for (const auto& derived : cases) {
        INFO("chain " << derived.chain);
        auto result = frontend::spark::relation_to_plan(make_plan(derived.relation), resource);
        REQUIRE_FALSE(result.has_error());
        const auto& root = result.value().parsed_data->otterbrix_params->node;
        CHECK(child_kinds(root) == derived.outer);
        CHECK(child_kinds(derived_inner(root)) == derived.inner);
    }

    // distinct() after limit(): the distinct rows of the window.
    auto distinct = frontend::spark::relation_to_plan(make_plan(make_distinct(make_limit(read, 5))), resource);
    REQUIRE_FALSE(distinct.has_error());
    const auto& root = distinct.value().parsed_data->otterbrix_params->node;
    CHECK(child_kinds(root) == "aggregate");
    CHECK(static_cast<const cl::node_aggregate_t&>(*root).is_distinct());
    const auto& inner = derived_inner(root);
    CHECK(child_kinds(inner) == "limit");
    CHECK_FALSE(static_cast<const cl::node_aggregate_t&>(*inner).is_distinct());
}

TEST_CASE("relation_to_plan: a limit or an offset on a limit merges into one window") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    const auto read = make_read_relation("db1.t1");
    struct window_case {
        const char* chain;
        sc::Relation relation;
        int64_t limit;
        int64_t offset;
    };
    // Two windows in a row are one: rows [o1 + o2, o1 + min(l1 - o2, l2)).
    const window_case cases[] = {
        {"limit(5).limit(1)", make_limit(make_limit(read, 5), 1), 1, 0},
        {"limit(2).limit(5)", make_limit(make_limit(read, 2), 5), 2, 0},
        {"limit(10).offset(3)", make_offset(make_limit(read, 10), 3), 7, 3},
        {"limit(2).offset(5)", make_offset(make_limit(read, 2), 5), 0, 5},
        {"offset(3).limit(5)", make_limit(make_offset(read, 3), 5), 5, 3},
        {"offset(2).offset(3)", make_offset(make_offset(read, 2), 3), -1, 5},
    };
    for (const auto& window : cases) {
        INFO("chain " << window.chain);
        auto result = frontend::spark::relation_to_plan(make_plan(window.relation), resource);
        REQUIRE_FALSE(result.has_error());
        const auto& root = result.value().parsed_data->otterbrix_params->node;
        REQUIRE(child_kinds(root) == "limit");
        const auto& merged = static_cast<const cl::node_limit_t&>(*root->children().front()).limit();
        CHECK(merged.limit() == window.limit);
        CHECK(merged.offset() == window.offset);
    }

    // A spark.sql() leaf's own LIMIT merges the same way.
    sc::Relation sql;
    sql.mutable_sql()->set_query("SELECT * FROM t LIMIT 5");
    auto leaf = frontend::spark::relation_to_plan(make_plan(make_limit(sql, 3)), resource);
    REQUIRE_FALSE(leaf.has_error());
    const auto& root = leaf.value().parsed_data->otterbrix_params->node;
    REQUIRE(child_kinds(root) == "limit");
    CHECK(static_cast<const cl::node_limit_t&>(*root->children().front()).limit().limit() == 3);
}

TEST_CASE("relation_to_plan: reading a derived table through a renamed column is unimplemented_yet") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // The engine names a renamed column after its source in a select's output
    // schema, so a derived table cannot be read through the new name.
    const auto renamed = make_project(make_read_relation("db1.t1"), {make_alias(make_attribute("a"), "x")});
    const auto x_gt_one = make_call(">", {make_attribute("x"), make_long(1)});
    const std::string reads = "() reads the result of a select() that renames a column";
    CHECK(refusal_of(make_filter(renamed, x_gt_one)).find("filter" + reads) != std::string::npos);
    CHECK(refusal_of(make_sort(renamed, "x")).find("orderBy" + reads) != std::string::npos);
    CHECK(refusal_of(make_project(renamed, {make_attribute("x")})).find("select" + reads) != std::string::npos);
    CHECK(refusal_of(make_group_count(renamed, "x")).find("groupBy/agg" + reads) != std::string::npos);

    // An operation that reads no column by name still works: count() and
    // distinct() read a derived table of it, limit() stacks on it.
    for (const auto& reading_nothing : {make_count(renamed), make_distinct(renamed), make_limit(renamed, 3)}) {
        CHECK_FALSE(frontend::spark::relation_to_plan(make_plan(reading_nothing), resource).has_error());
    }
}

TEST_CASE("relation_to_plan: a remote read under a derived table stays the external slot") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // spark.table("a.db1.sch1.t1").limit(5).filter(...): the window is pushed to
    // the backend with the read, the filter runs in the engine over its rows.
    const auto gt_one = make_call(">", {make_attribute("a"), make_long(1)});
    auto result = frontend::spark::relation_to_plan(
        make_plan(make_filter(make_limit(make_read_relation("a.db1.sch1.t1"), 5), gt_one)),
        resource);
    REQUIRE_FALSE(result.has_error());
    const auto& statement = *result.value().parsed_data->otterbrix_params;
    CHECK(child_kinds(statement.node) == "aggregate,match");
    CHECK(static_cast<const cl::node_aggregate_t&>(*statement.node).uid().t.empty());
    const auto& inner = derived_inner(statement.node);
    CHECK(child_kinds(inner) == "limit");

    REQUIRE(statement.external_nodes_count == 1);
    REQUIRE(statement.external_nodes.size() == 1);
    REQUIRE(statement.external_nodes[0].size() == 1);
    const auto& slot = statement.external_nodes[0][0];
    CHECK(slot.node->get() == inner.get());
    CHECK(slot.target.name.unique_identifier == "a");
    CHECK(slot.target.name.schema == "sch1");
}

TEST_CASE("relation_to_plan: unionByName is refused") {
    // Columns paired by name need the inputs' schemas, which a translated plan
    // does not have; a positional union would stack mismatched columns.
    for (const bool allow_missing : {false, true}) {
        sc::Relation rel;
        auto* set_op = rel.mutable_set_op();
        *set_op->mutable_left_input() = make_read_relation("db1.a");
        *set_op->mutable_right_input() = make_read_relation("db1.b");
        set_op->set_set_op_type(sc::SetOperation::SET_OP_TYPE_UNION);
        set_op->set_is_all(true);
        set_op->set_by_name(true);
        set_op->set_allow_missing_columns(allow_missing);
        CHECK(refusal_of(rel) == "Spark unionByName() is not supported; use union() with the same column order");
    }
}

TEST_CASE("relation_to_plan: filter and orderBy may follow a select of plain columns") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    const auto projected = make_project(make_read_relation("db1.t1"), {make_attribute("a"), make_attribute("b")});
    const auto a_gt_one = make_call(">", {make_attribute("a"), make_long(1)});
    auto filtered = frontend::spark::relation_to_plan(make_plan(make_filter(projected, a_gt_one)), resource);
    REQUIRE_FALSE(filtered.has_error());
    const auto& children = filtered.value().parsed_data->otterbrix_params->node->children();
    REQUIRE(children.size() == 3);
    CHECK(children[0]->type() == cl::node_type::group_t);
    CHECK(children[1]->type() == cl::node_type::select_t);
    CHECK(children[2]->type() == cl::node_type::match_t);

    auto sorted = frontend::spark::relation_to_plan(make_plan(make_sort(projected, "a")), resource);
    CHECK_FALSE(sorted.has_error());

    // Below a computed or renamed column the filter would test another value:
    // over a computed column it reads a derived table, over a renamed one it is
    // refused (the engine resolves a renamed column by its source's name there).
    const auto x_gt_one = make_call(">", {make_attribute("x"), make_long(1)});
    const auto computed = make_project(make_read_relation("db1.t1"),
                                       {make_alias(make_call("+", {make_attribute("a"), make_long(1)}), "x")});
    auto derived = frontend::spark::relation_to_plan(make_plan(make_filter(computed, x_gt_one)), resource);
    REQUIRE_FALSE(derived.has_error());
    CHECK(child_kinds(derived.value().parsed_data->otterbrix_params->node) == "aggregate,match");
    const auto renamed = make_project(make_read_relation("db1.t1"), {make_alias(make_attribute("a"), "x")});
    CHECK(refusal_of(make_filter(renamed, x_gt_one))
              .find("filter() reads the result of a select() that renames a column") != std::string::npos);
}

TEST_CASE("relation_to_plan: filter after orderBy runs below the sort") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // df.orderBy("a").filter(df.a > 1): a filter keeps the order it finds, so
    // it commutes with the sort the engine runs after it.
    const auto a_gt_one = make_call(">", {make_attribute("a"), make_long(1)});
    auto result = frontend::spark::relation_to_plan(
        make_plan(make_filter(make_sort(make_read_relation("db1.t1"), "a"), a_gt_one)),
        resource);
    REQUIRE_FALSE(result.has_error());
    const auto& children = result.value().parsed_data->otterbrix_params->node->children();
    REQUIRE(children.size() == 2);
    CHECK(children[0]->type() == cl::node_type::sort_t);
    CHECK(children[1]->type() == cl::node_type::match_t);
}

TEST_CASE("relation_to_plan: a row-wise select after orderBy and limit keeps the top-N shape") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // df.orderBy("a").limit(3).select("a", F.col("b").alias("y")): the select
    // keeps every row and its order, so it commutes with the limit the engine
    // runs after it.
    const auto top = make_limit(make_sort(make_read_relation("db1.t1"), "a"), 3);
    auto result = frontend::spark::relation_to_plan(
        make_plan(make_project(top, {make_attribute("a"), make_alias(make_attribute("b"), "y")})),
        resource);
    REQUIRE_FALSE(result.has_error());
    const auto& children = result.value().parsed_data->otterbrix_params->node->children();
    REQUIRE(children.size() == 4);
    CHECK(children[0]->type() == cl::node_type::sort_t);
    CHECK(children[1]->type() == cl::node_type::limit_t);
    REQUIRE(children[2]->type() == cl::node_type::group_t);
    CHECK(children[2]->expressions().size() == 2);
    CHECK(children[3]->type() == cl::node_type::select_t);
}

// ── Structured column filters ─────────────────────────────────────────────────

TEST_CASE("relation_to_plan: structured filter compares a column key with a bound parameter") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    auto plan =
        make_plan(make_filter(make_read_relation("db1.t1"), make_call(">", {make_attribute("id"), make_long(5)})));

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());
    const auto& statement = *result.value().parsed_data->otterbrix_params;

    // The transformer's form compare(key_t, parameter_id_t), with the literal
    // bound in the plan's one parameter node.
    const auto* cmp = match_predicate(statement.node);
    CHECK(cmp->type() == ce::compare_type::gt);
    CHECK(compares_column_with(*cmp, "id", statement.params_node, 5));

    // Every literal is bound: no `$n` is left for the describe path to refuse.
    CHECK(statement.parameters_count == 0);
}

TEST_CASE("relation_to_plan: filter with the literal first is mirrored to lead with the column") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // lit(5) < id  is  id > 5
    auto lt = frontend::spark::relation_to_plan(
        make_plan(make_filter(make_read_relation("db1.t1"), make_call("<", {make_long(5), make_attribute("id")}))),
        resource);
    REQUIRE_FALSE(lt.has_error());
    const auto& lt_statement = *lt.value().parsed_data->otterbrix_params;
    const auto* gt = match_predicate(lt_statement.node);
    CHECK(gt->type() == ce::compare_type::gt);
    CHECK(compares_column_with(*gt, "id", lt_statement.params_node, 5));

    // lit(5) >= id  is  id <= 5; equality keeps its operator
    auto gte = frontend::spark::relation_to_plan(
        make_plan(make_filter(make_read_relation("db1.t1"), make_call(">=", {make_long(5), make_attribute("id")}))),
        resource);
    REQUIRE_FALSE(gte.has_error());
    const auto& gte_statement = *gte.value().parsed_data->otterbrix_params;
    const auto* lte = match_predicate(gte_statement.node);
    CHECK(lte->type() == ce::compare_type::lte);
    CHECK(compares_column_with(*lte, "id", gte_statement.params_node, 5));

    auto eq = frontend::spark::relation_to_plan(
        make_plan(make_filter(make_read_relation("db1.t1"), make_call("==", {make_long(7), make_attribute("id")}))),
        resource);
    REQUIRE_FALSE(eq.has_error());
    const auto& eq_statement = *eq.value().parsed_data->otterbrix_params;
    const auto* same = match_predicate(eq_statement.node);
    CHECK(same->type() == ce::compare_type::eq);
    CHECK(compares_column_with(*same, "id", eq_statement.params_node, 7));
}

TEST_CASE("relation_to_plan: isin filter is an OR of equalities on the column") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    const auto isin = make_call("in", {make_attribute("region"), make_string("north"), make_string("east")});
    auto plan = make_plan(make_filter(make_read_relation("db1.t1"), isin));

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());
    const auto& statement = *result.value().parsed_data->otterbrix_params;
    const auto* any = match_predicate(statement.node);
    CHECK(any->type() == ce::compare_type::union_or);
    REQUIRE(any->children().size() == 2);
    const char* expected[] = {"north", "east"};
    for (size_t i = 0; i < 2; ++i) {
        const auto* eq = static_cast<const ce::compare_expression_t*>(any->children()[i].get());
        CHECK(eq->type() == ce::compare_type::eq);
        REQUIRE(ce::is_key(eq->left()));
        CHECK(ce::as_key(eq->left()).as_string() == "region");
        REQUIRE(ce::is_parameter(eq->right()));
        CHECK(statement.params_node->parameter(ce::as_parameter(eq->right())).value<std::string_view>() == expected[i]);
    }
}

TEST_CASE("relation_to_plan: between filter is an AND of the two bounds") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    auto plan = make_plan(make_filter(make_read_relation("db1.t1"),
                                      make_call("between", {make_attribute("amount"), make_long(10), make_long(20)})));

    auto result = frontend::spark::relation_to_plan(plan, resource);
    REQUIRE_FALSE(result.has_error());
    const auto& statement = *result.value().parsed_data->otterbrix_params;
    const auto* all = match_predicate(statement.node);
    CHECK(all->type() == ce::compare_type::union_and);
    REQUIRE(all->children().size() == 2);
    const auto* lower = static_cast<const ce::compare_expression_t*>(all->children()[0].get());
    const auto* upper = static_cast<const ce::compare_expression_t*>(all->children()[1].get());
    CHECK(lower->type() == ce::compare_type::gte);
    CHECK(compares_column_with(*lower, "amount", statement.params_node, 10));
    CHECK(upper->type() == ce::compare_type::lte);
    CHECK(compares_column_with(*upper, "amount", statement.params_node, 20));
}

TEST_CASE("relation_to_plan: filter condition that is not a predicate is refused") {
    const auto read = make_read_relation("db1.t1");
    // A string function and a bare column: neither has a predicate form.
    CHECK_FALSE(refusal_of(make_filter(read, make_call("contains", {make_attribute("s"), make_string("x")}))).empty());
    CHECK_FALSE(refusal_of(make_filter(read, make_attribute("flag"))).empty());
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
