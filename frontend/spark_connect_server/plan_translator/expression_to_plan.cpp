// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "expression_to_plan.hpp"

#include <spark/connect/expressions.pb.h>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <google/protobuf/repeated_field.h>

#include <charconv>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace frontend::spark {

namespace {

namespace ce = components::expressions;
namespace cl = components::logical_plan;
namespace ct = components::types;
namespace sc = ::spark::connect;

using expr_result = core::result_wrapper_t<cl::expression_ptr>;

// --- error helpers (never throw) -----------------------------------------

expr_result make_error(core::error_code_t code, std::string_view what, std::pmr::memory_resource* resource) {
    return expr_result{core::error_t{code, std::pmr::string{what.data(), what.size(), resource}}};
}

expr_result unsupported(std::string_view what, std::pmr::memory_resource* resource) {
    return make_error(core::error_code_t::unimplemented_yet, what, resource);
}

expr_result bad_expr(std::string_view what, std::pmr::memory_resource* resource) {
    return make_error(core::error_code_t::sql_parse_error, what, resource);
}

// --- text helpers --------------------------------------------------------

bool ieq(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        char ca = a[i];
        char cb = b[i];
        if (ca >= 'A' && ca <= 'Z') { ca = static_cast<char>(ca - 'A' + 'a'); }
        if (cb >= 'A' && cb <= 'Z') { cb = static_cast<char>(cb - 'A' + 'a'); }
        if (ca != cb) { return false; }
    }
    return true;
}

std::string ascii_lower_std(std::string_view in) {
    std::string out;
    out.reserve(in.size());
    for (char c : in) {
        out.push_back(c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c);
    }
    return out;
}

// Split an unparsed_identifier on '.', honouring backtick-quoted segments
// (e.g. "a.`b.c`.d" -> ["a", "b.c", "d"]). Backticks are stripped.
std::pmr::vector<std::pmr::string> split_identifier(std::string_view ident, std::pmr::memory_resource* resource) {
    std::pmr::vector<std::pmr::string> parts{resource};
    std::pmr::string current{resource};
    bool in_backtick = false;
    for (char c : ident) {
        if (c == '`') {
            in_backtick = !in_backtick;
        } else if (c == '.' && !in_backtick) {
            parts.push_back(std::move(current));
            current = std::pmr::string{resource};
        } else {
            current.push_back(c);
        }
    }
    parts.push_back(std::move(current));
    return parts;
}

ce::key_t key_from_identifier(std::string_view ident, std::pmr::memory_resource* resource) {
    return ce::key_t{split_identifier(ident, resource)};
}

// --- operator/function name classification (function-scope lookup) ------
//
// Maps the Spark whitelist of operator names
//   + - * / %  = <> < > <= >=  and or not  isnull isnotnull  negative
// (plus the common Catalyst spellings) onto the Otterbrix compare/scalar
// enum values. Everything else resolves to `generic` -> function_expression.

enum class func_kind_t {
    aggregate,
    arithmetic,
    comparison,
    logical_union,
    null_check,
    unary_neg,
    generic,
};

struct func_class_t {
    func_kind_t kind{func_kind_t::generic};
    ce::scalar_type stype{ce::scalar_type::invalid};
    ce::compare_type ctype{ce::compare_type::invalid};
};

func_class_t classify_function(std::string_view name) {
    // Aggregates (count/sum/avg/min/max) -> make_aggregate_expression.
    if (ieq(name, "count") || ieq(name, "sum") ||
        ieq(name, "avg") || ieq(name, "min") || ieq(name, "max")) {
        return {func_kind_t::aggregate, ce::scalar_type::invalid, ce::compare_type::invalid};
    }
    // Arithmetic operators (+,-,*,/,%) -> scalar_expression.
    if (name == "+" || ieq(name, "add")) {
        return {func_kind_t::arithmetic, ce::scalar_type::add, ce::compare_type::invalid};
    }
    if (name == "-" || ieq(name, "subtract")) {
        return {func_kind_t::arithmetic, ce::scalar_type::subtract, ce::compare_type::invalid};
    }
    if (name == "*" || ieq(name, "multiply")) {
        return {func_kind_t::arithmetic, ce::scalar_type::multiply, ce::compare_type::invalid};
    }
    if (name == "/" || ieq(name, "divide")) {
        return {func_kind_t::arithmetic, ce::scalar_type::divide, ce::compare_type::invalid};
    }
    if (name == "%" || ieq(name, "mod") || ieq(name, "remainder")) {
        return {func_kind_t::arithmetic, ce::scalar_type::mod, ce::compare_type::invalid};
    }
    // Comparison operators (=,<>,<,>,<=,>=) -> compare_expression.
    if (name == "=" || ieq(name, "equal_to") || ieq(name, "equals")) {
        return {func_kind_t::comparison, ce::scalar_type::invalid, ce::compare_type::eq};
    }
    if (name == "<>" || name == "!=" || ieq(name, "not_equal_to")) {
        return {func_kind_t::comparison, ce::scalar_type::invalid, ce::compare_type::ne};
    }
    if (name == "<" || ieq(name, "less_than")) {
        return {func_kind_t::comparison, ce::scalar_type::invalid, ce::compare_type::lt};
    }
    if (name == ">" || ieq(name, "greater_than")) {
        return {func_kind_t::comparison, ce::scalar_type::invalid, ce::compare_type::gt};
    }
    if (name == "<=" || ieq(name, "less_than_or_equal")) {
        return {func_kind_t::comparison, ce::scalar_type::invalid, ce::compare_type::lte};
    }
    if (name == ">=" || ieq(name, "greater_than_or_equal")) {
        return {func_kind_t::comparison, ce::scalar_type::invalid, ce::compare_type::gte};
    }
    // Logical unions (and, or, not) -> compare_union_expression.
    if (ieq(name, "and")) {
        return {func_kind_t::logical_union, ce::scalar_type::invalid, ce::compare_type::union_and};
    }
    if (ieq(name, "or")) {
        return {func_kind_t::logical_union, ce::scalar_type::invalid, ce::compare_type::union_or};
    }
    if (ieq(name, "not")) {
        return {func_kind_t::logical_union, ce::scalar_type::invalid, ce::compare_type::union_not};
    }
    // Null checks (isnull, isnotnull) -> compare_expression{is_null,is_not_null}.
    if (ieq(name, "isnull")) {
        return {func_kind_t::null_check, ce::scalar_type::invalid, ce::compare_type::is_null};
    }
    if (ieq(name, "isnotnull")) {
        return {func_kind_t::null_check, ce::scalar_type::invalid, ce::compare_type::is_not_null};
    }
    // Unary negation (negative) -> scalar_expression{unary_minus}.
    if (ieq(name, "negative") || ieq(name, "unary_minus") || ieq(name, "uminus")) {
        return {func_kind_t::unary_neg, ce::scalar_type::unary_minus, ce::compare_type::invalid};
    }
    return {func_kind_t::generic, ce::scalar_type::invalid, ce::compare_type::invalid};
}

// Maps a small set of Spark type spellings (from Cast.type_str) to an
// Otterbrix logical_type. Anything more complex (struct/array/decimal with
// precision) is rejected as unsupported.
ct::logical_type simple_cast_type(std::string_view s) {
    if (ieq(s, "int") || ieq(s, "integer") || ieq(s, "int4") || ieq(s, "int32")) {
        return ct::logical_type::INTEGER;
    }
    if (ieq(s, "bigint") || ieq(s, "long") || ieq(s, "int8") || ieq(s, "int64")) {
        return ct::logical_type::BIGINT;
    }
    if (ieq(s, "smallint") || ieq(s, "short") || ieq(s, "int2")) {
        return ct::logical_type::SMALLINT;
    }
    if (ieq(s, "tinyint") || ieq(s, "byte")) {
        return ct::logical_type::TINYINT;
    }
    if (ieq(s, "double")) {
        return ct::logical_type::DOUBLE;
    }
    if (ieq(s, "float") || ieq(s, "real")) {
        return ct::logical_type::FLOAT;
    }
    if (ieq(s, "string") || ieq(s, "varchar") || ieq(s, "char") || ieq(s, "text")) {
        return ct::logical_type::STRING_LITERAL;
    }
    if (ieq(s, "boolean") || ieq(s, "bool")) {
        return ct::logical_type::BOOLEAN;
    }
    if (ieq(s, "date")) {
        return ct::logical_type::DATE;
    }
    if (ieq(s, "timestamp") || ieq(s, "datetime")) {
        return ct::logical_type::TIMESTAMP;
    }
    return ct::logical_type::INVALID;
}

// Returns a NULL logical value bound to `resource`.
ct::logical_value_t null_value(std::pmr::memory_resource* resource) {
    return ct::logical_value_t{resource, ct::complex_logical_type{ct::logical_type::NA}};
}

// --- expression variant handlers ----------------------------------------

expr_result handle_literal(const sc::Expression::Literal& lit,
                           cl::parameter_node_ptr params,
                           std::pmr::memory_resource* resource) {
    ct::logical_value_t value = null_value(resource);
    switch (lit.literal_type_case()) {
        case sc::Expression::Literal::kNull:
            break;
        case sc::Expression::Literal::kBoolean:
            value = ct::logical_value_t{resource, lit.boolean()};
            break;
        case sc::Expression::Literal::kByte:
            value = ct::logical_value_t{resource, static_cast<int64_t>(lit.byte())};
            break;
        case sc::Expression::Literal::kShort:
            value = ct::logical_value_t{resource, static_cast<int64_t>(lit.short_())};
            break;
        case sc::Expression::Literal::kInteger:
            value = ct::logical_value_t{resource, static_cast<int64_t>(lit.integer())};
            break;
        case sc::Expression::Literal::kLong:
            value = ct::logical_value_t{resource, static_cast<int64_t>(lit.long_())};
            break;
        case sc::Expression::Literal::kFloat:
            value = ct::logical_value_t{resource, static_cast<double>(lit.float_())};
            break;
        case sc::Expression::Literal::kDouble:
            value = ct::logical_value_t{resource, lit.double_()};
            break;
        case sc::Expression::Literal::kDecimal: {
            const std::string& dval = lit.decimal().value();
            double parsed = 0.0;
            const auto res = std::from_chars(dval.data(), dval.data() + dval.size(), parsed);
            if (res.ec != std::errc{}) {
                return bad_expr("Spark decimal literal is not a valid number", resource);
            }
            value = ct::logical_value_t{resource, parsed};
            break;
        }
        case sc::Expression::Literal::kString:
            value = ct::logical_value_t{resource, std::string{lit.string()}};
            break;
        case sc::Expression::Literal::kBinary:
            value = ct::logical_value_t{resource, std::string{lit.binary()}};
            break;
        case sc::Expression::Literal::kDate:
            value = ct::logical_value_t{resource, static_cast<int64_t>(lit.date())};
            break;
        case sc::Expression::Literal::kTimestamp:
            value = ct::logical_value_t{resource, static_cast<int64_t>(lit.timestamp())};
            break;
        case sc::Expression::Literal::kTimestampNtz:
            value = ct::logical_value_t{resource, static_cast<int64_t>(lit.timestamp_ntz())};
            break;
        default:
            return unsupported("unsupported Spark literal type", resource);
    }

    // Materialise the value into the parameter store and reference it from a
    // constant scalar expression — mirrors the SQL transformer's T_A_Const path.
    const auto id = params->add_parameter(std::move(value));
    auto expr = ce::make_scalar_expression(resource, ce::scalar_type::constant, ce::key_t{resource});
    expr->append_param(id);
    return expr;
}

expr_result handle_function(std::string_view name,
                            bool is_distinct,
                            bool is_udf,
                            const google::protobuf::RepeatedPtrField<sc::Expression>& args,
                            cl::parameter_node_ptr params,
                            std::pmr::memory_resource* resource) {
    if (is_udf) {
        return unsupported("user-defined functions are not supported in Spark expressions", resource);
    }

    const auto cls = classify_function(name);

    switch (cls.kind) {
        case func_kind_t::aggregate: {
            auto agg = ce::make_aggregate_expression(resource, ascii_lower_std(name));
            agg->set_distinct(is_distinct);
            for (const auto& arg : args) {
                auto child = expression_to_plan(arg, params, resource);
                if (child.has_error()) {
                    return child;
                }
                agg->append_param(child.value());
            }
            return agg;
        }
        case func_kind_t::arithmetic: {
            if (args.size() != 2) {
                return bad_expr("Spark arithmetic operator requires exactly 2 arguments", resource);
            }
            auto left = expression_to_plan(args.Get(0), params, resource);
            if (left.has_error()) {
                return left;
            }
            auto right = expression_to_plan(args.Get(1), params, resource);
            if (right.has_error()) {
                return right;
            }
            auto expr = ce::make_scalar_expression(resource, cls.stype);
            expr->append_param(left.value());
            expr->append_param(right.value());
            return expr;
        }
        case func_kind_t::comparison: {
            if (args.size() != 2) {
                return bad_expr("Spark comparison operator requires exactly 2 arguments", resource);
            }
            auto left = expression_to_plan(args.Get(0), params, resource);
            if (left.has_error()) {
                return left;
            }
            auto right = expression_to_plan(args.Get(1), params, resource);
            if (right.has_error()) {
                return right;
            }
            return ce::make_compare_expression(resource, cls.ctype, left.value(), right.value());
        }
        case func_kind_t::logical_union: {
            if (cls.ctype == ce::compare_type::union_not) {
                if (args.size() != 1) {
                    return bad_expr("Spark NOT requires exactly 1 argument", resource);
                }
                auto child = expression_to_plan(args.Get(0), params, resource);
                if (child.has_error()) {
                    return child;
                }
                auto expr = ce::make_compare_union_expression(resource, cls.ctype);
                expr->append_child(child.value());
                return expr;
            }
            if (args.size() < 2) {
                return bad_expr("Spark AND/OR require at least 2 arguments", resource);
            }
            auto expr = ce::make_compare_union_expression(resource, cls.ctype);
            for (const auto& arg : args) {
                auto child = expression_to_plan(arg, params, resource);
                if (child.has_error()) {
                    return child;
                }
                expr->append_child(child.value());
            }
            return expr;
        }
        case func_kind_t::null_check: {
            if (args.size() != 1) {
                return bad_expr("Spark isnull/isnotnull require exactly 1 argument", resource);
            }
            auto child = expression_to_plan(args.Get(0), params, resource);
            if (child.has_error()) {
                return child;
            }
            // is_null/is_not_null carry no real RHS; use a NULL placeholder param,
            // matching the SQL transformer's NullTest lowering.
            const auto dummy = params->add_parameter(null_value(resource));
            return ce::make_compare_expression(resource, cls.ctype, child.value(), dummy);
        }
        case func_kind_t::unary_neg: {
            if (args.size() != 1) {
                return bad_expr("Spark 'negative' requires exactly 1 argument", resource);
            }
            auto child = expression_to_plan(args.Get(0), params, resource);
            if (child.has_error()) {
                return child;
            }
            auto expr = ce::make_scalar_expression(resource, ce::scalar_type::unary_minus);
            expr->append_param(child.value());
            return expr;
        }
        case func_kind_t::generic: {
            std::pmr::vector<ce::param_storage> fargs{resource};
            fargs.reserve(args.size());
            for (const auto& arg : args) {
                auto child = expression_to_plan(arg, params, resource);
                if (child.has_error()) {
                    return child;
                }
                fargs.push_back(child.value());
            }
            return ce::make_function_expression(resource, ascii_lower_std(name), std::move(fargs));
        }
    }
    return unsupported("unsupported Spark function kind", resource);
}

}  // namespace

core::result_wrapper_t<components::logical_plan::expression_ptr>
expression_to_plan(const sc::Expression& expr,
                   components::logical_plan::parameter_node_ptr params,
                   std::pmr::memory_resource* resource) {
    using Expr = sc::Expression;

    switch (expr.expr_type_case()) {
        case Expr::kLiteral:
            return handle_literal(expr.literal(), params, resource);

        case Expr::kUnresolvedAttribute: {
            const auto& attr = expr.unresolved_attribute();
            auto key = key_from_identifier(attr.unparsed_identifier(), resource);
            return ce::make_scalar_expression(resource, ce::scalar_type::get_field, key);
        }

        case Expr::kUnresolvedFunction: {
            const auto& fn = expr.unresolved_function();
            return handle_function(fn.function_name(),
                                   fn.is_distinct(),
                                   fn.is_user_defined_function(),
                                   fn.arguments(),
                                   params,
                                   resource);
        }

        case Expr::kCallFunction: {
            const auto& fn = expr.call_function();
            return handle_function(fn.function_name(),
                                   /*is_distinct=*/false,
                                   /*is_udf=*/false,
                                   fn.arguments(),
                                   params,
                                   resource);
        }

        case Expr::kAlias: {
            const auto& al = expr.alias();
            auto child = expression_to_plan(al.expr(), params, resource);
            if (child.has_error()) {
                return child;
            }
            auto result = child.value();
            const auto& parts = al.name();
            if (!parts.empty()) {
                std::string alias_name;
                for (int i = 0; i < parts.size(); ++i) {
                    if (i > 0) {
                        alias_name += '.';
                    }
                    alias_name += parts.Get(i);
                }
                result->set_result_alias(alias_name);
            }
            return result;
        }

        case Expr::kCast: {
            const auto& c = expr.cast();
            ct::logical_type lt = ct::logical_type::INVALID;
            if (c.has_type_str()) {
                lt = simple_cast_type(c.type_str());
            } else if (c.has_type()) {
                return unsupported("structured DataType cast is not supported", resource);
            } else {
                return bad_expr("Spark cast without a target type", resource);
            }
            if (lt == ct::logical_type::INVALID) {
                return unsupported("unsupported Spark cast target type", resource);
            }
            auto child = expression_to_plan(c.expr(), params, resource);
            if (child.has_error()) {
                return child;
            }
            auto result = child.value();
            // Cast is only meaningful on a column reference (get_field): stamp
            // the target type onto its key. Complex casts are rejected above.
            if (result->group() == ce::expression_group::scalar) {
                auto* scalar = static_cast<ce::scalar_expression_t*>(result.get());
                if (scalar->type() == ce::scalar_type::get_field) {
                    scalar->key().set_cast_type(ct::complex_logical_type{lt});
                    return result;
                }
            }
            return unsupported("Spark cast is only supported on column references", resource);
        }

        case Expr::kSortOrder: {
            const auto& so = expr.sort_order();
            auto child = expression_to_plan(so.child(), params, resource);
            if (child.has_error()) {
                return child;
            }
            auto result = child.value();
            ce::sort_order order = ce::sort_order::asc;
            if (so.direction() == Expr::SortOrder::SORT_DIRECTION_DESCENDING) {
                order = ce::sort_order::desc;
            }
            // sort_expression carries a single key, so only column-reference
            // sort keys are representable (matches the SQL transformer).
            if (result->group() == ce::expression_group::scalar) {
                auto* scalar = static_cast<ce::scalar_expression_t*>(result.get());
                if (scalar->type() == ce::scalar_type::get_field) {
                    return ce::make_sort_expression(scalar->key(), order);
                }
            }
            return bad_expr("Spark sort key must be a column reference", resource);
        }

        case Expr::kUnresolvedStar:
            return ce::make_scalar_expression(resource, ce::scalar_type::star_expand, ce::key_t{resource});

        case Expr::kWindow:
            return unsupported("window expressions are not supported", resource);

        case Expr::kCommonInlineUserDefinedFunction:
            return unsupported("inline user-defined functions are not supported", resource);

        case Expr::kExpressionString:
            return unsupported("raw SQL expression strings are not supported", resource);

        case Expr::kUnresolvedRegex:
            return unsupported("regex column expansion is not supported", resource);

        case Expr::kUnresolvedExtractValue:
            return unsupported("value extraction expressions are not supported", resource);

        case Expr::kUpdateFields:
            return unsupported("struct field updates are not supported", resource);

        case Expr::kLambdaFunction:
            return unsupported("lambda functions are not supported", resource);

        case Expr::kUnresolvedNamedLambdaVariable:
            return unsupported("lambda variables are not supported", resource);

        case Expr::kNamedArgumentExpression:
            return unsupported("named arguments are not supported", resource);

        case Expr::kMergeAction:
            return unsupported("merge actions are not supported", resource);

        case Expr::kTypedAggregateExpression:
            return unsupported("typed aggregate expressions are not supported", resource);

        case Expr::kSubqueryExpression:
            return unsupported("subquery expressions are not supported", resource);

        default:
            return unsupported("unsupported or unset Spark Expression type", resource);
    }
}

}  // namespace frontend::spark
