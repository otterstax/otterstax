// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "expression_to_plan.hpp"
#include "utility/tracy_profiler.hpp"

#include <spark/connect/expressions.pb.h>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
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

#include <algorithm>
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

        // The recursive translation behind expression_to_plan, which only adds the
        // profiler zone of the whole tree.
        expr_result translate_expression(const sc::Expression& expr,
                                         cl::parameter_node_ptr params,
                                         std::pmr::memory_resource* resource);

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
                if (ca >= 'A' && ca <= 'Z') {
                    ca = static_cast<char>(ca - 'A' + 'a');
                }
                if (cb >= 'A' && cb <= 'Z') {
                    cb = static_cast<char>(cb - 'A' + 'a');
                }
                if (ca != cb) {
                    return false;
                }
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
        std::pmr::vector<std::pmr::string> split_identifier(std::string_view ident,
                                                            std::pmr::memory_resource* resource) {
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
        //   + - * / %  = == <> < > <= >=  and or not  isnull isnotnull  in between
        //   negative
        // (plus the common Catalyst spellings) onto the Otterbrix compare/scalar
        // enum values. Everything else resolves to `generic` -> function_expression.

        enum class func_kind_t
        {
            aggregate,
            arithmetic,
            comparison,
            logical_union,
            null_check,
            in_list,
            between,
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
            if (ieq(name, "count") || ieq(name, "sum") || ieq(name, "avg") || ieq(name, "min") || ieq(name, "max")) {
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
            // Comparison operators (=,<>,<,>,<=,>=) -> compare_expression. PySpark's
            // Column.__eq__ sends "==".
            if (name == "=" || name == "==" || ieq(name, "equal_to") || ieq(name, "equals")) {
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
            // Column.isin(...) arrives as in(column, value, ...).
            if (ieq(name, "in")) {
                return {func_kind_t::in_list, ce::scalar_type::invalid, ce::compare_type::union_or};
            }
            if (ieq(name, "between")) {
                return {func_kind_t::between, ce::scalar_type::invalid, ce::compare_type::union_and};
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

        // --- comparisons in the transformer's form --------------------------------
        //
        // The SQL transformer lowers `col > 5` to compare(key_t, parameter_id_t)
        // (transform_a_expr), the form the local executor and the remote SQL
        // generator evaluate. A structured Spark comparison is built the same way.

        // A comparison operand: a bare column reference is its key, a literal the
        // parameter id it was bound to in the shared parameter node, anything else
        // the expression itself.
        ce::param_storage as_operand(const cl::expression_ptr& expr, std::pmr::memory_resource* resource) {
            if (expr->group() == ce::expression_group::scalar) {
                const auto* scalar = static_cast<const ce::scalar_expression_t*>(expr.get());
                if (scalar->type() == ce::scalar_type::get_field && scalar->params().empty()) {
                    return ce::key_t{scalar->key(), resource};
                }
                if (scalar->type() == ce::scalar_type::constant && scalar->params().size() == 1 &&
                    ce::is_parameter(scalar->params().front())) {
                    return scalar->params().front();
                }
            }
            return expr;
        }

        // The operator that keeps a comparison's meaning when its operands swap.
        ce::compare_type mirrored(ce::compare_type type) {
            switch (type) {
                case ce::compare_type::gt:
                    return ce::compare_type::lt;
                case ce::compare_type::lt:
                    return ce::compare_type::gt;
                case ce::compare_type::gte:
                    return ce::compare_type::lte;
                case ce::compare_type::lte:
                    return ce::compare_type::gte;
                default:
                    return type;
            }
        }

        // compare(left, right) in the transformer's form. A literal written first
        // (`lit(5) < col`) moves to the right and the operator is mirrored, so the
        // column leads as it does in `col > 5`.
        ce::compare_expression_ptr make_comparison(ce::compare_type type,
                                                   const cl::expression_ptr& left,
                                                   const cl::expression_ptr& right,
                                                   std::pmr::memory_resource* resource) {
            auto lhs = as_operand(left, resource);
            auto rhs = as_operand(right, resource);
            if (ce::is_parameter(lhs) && !ce::is_parameter(rhs)) {
                std::swap(lhs, rhs);
                type = mirrored(type);
            }
            return ce::make_compare_expression(resource, type, lhs, rhs);
        }

        // Spark's NULL placement: NULLS FIRST / LAST as written, otherwise Spark's
        // default — first when ascending, last when descending. The engine's
        // nulls_default is the SQL rule, the opposite, so it is never left to it.
        ce::sort_null_order null_order_of(const sc::Expression::SortOrder& so, ce::sort_order order) {
            switch (so.null_ordering()) {
                case sc::Expression::SortOrder::SORT_NULLS_FIRST:
                    return ce::sort_null_order::nulls_first;
                case sc::Expression::SortOrder::SORT_NULLS_LAST:
                    return ce::sort_null_order::nulls_last;
                default:
                    return order == ce::sort_order::asc ? ce::sort_null_order::nulls_first
                                                        : ce::sort_null_order::nulls_last;
            }
        }

        bool is_bare_star(const sc::Expression& expr) {
            return expr.expr_type_case() == sc::Expression::kUnresolvedStar &&
                   !expr.unresolved_star().has_unparsed_target();
        }

        bool is_non_null_literal(const sc::Expression& expr) {
            return expr.expr_type_case() == sc::Expression::kLiteral &&
                   expr.literal().literal_type_case() != sc::Expression::Literal::kNull &&
                   expr.literal().literal_type_case() != sc::Expression::Literal::LITERAL_TYPE_NOT_SET;
        }

        // An expression built from literals alone, which reads no column.
        bool is_constant(const sc::Expression& expr) {
            switch (expr.expr_type_case()) {
                case sc::Expression::kLiteral:
                    return true;
                case sc::Expression::kUnresolvedFunction: {
                    const auto& args = expr.unresolved_function().arguments();
                    return std::all_of(args.begin(), args.end(), is_constant);
                }
                case sc::Expression::kCallFunction: {
                    const auto& args = expr.call_function().arguments();
                    return std::all_of(args.begin(), args.end(), is_constant);
                }
                case sc::Expression::kAlias:
                    return is_constant(expr.alias().expr());
                case sc::Expression::kCast:
                    return is_constant(expr.cast().expr());
                default:
                    return false;
            }
        }

        // CAST(<operand> AS DOUBLE) in the shape the SQL transformer gives an aggregate's
        // argument: a column is the cast's key, anything else its nested expression.
        cl::expression_ptr as_double(const cl::expression_ptr& operand, std::pmr::memory_resource* resource) {
            const ct::complex_logical_type target{ct::logical_type::DOUBLE};
            if (operand->group() == ce::expression_group::scalar) {
                const auto* scalar = static_cast<const ce::scalar_expression_t*>(operand.get());
                if (scalar->type() == ce::scalar_type::get_field && scalar->params().empty()) {
                    return ce::make_cast_expression(resource,
                                                    ce::key_t{scalar->key(), resource},
                                                    target,
                                                    components::casts::cast_t{},
                                                    components::casts::cast_kind::cast);
                }
            }
            return ce::make_cast_expression(resource,
                                            operand,
                                            target,
                                            components::casts::cast_t{},
                                            components::casts::cast_kind::cast);
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
                    // Named by its function until an Alias renames it: an aggregate's
                    // output column takes its name from key(), and without one it would
                    // take the name of its argument's column.
                    const auto function = ascii_lower_std(name);
                    auto agg = ce::make_aggregate_expression(resource, function, ce::key_t{resource, function});
                    agg->set_distinct(is_distinct);
                    // count(*), and count of a literal that is not NULL (PySpark's
                    // df.count() and groupBy().count() send count(lit(1))), count every
                    // row: the parameterless count, the engine's star kernel and COUNT(*)
                    // on a backend.
                    if (function == "count" && !is_distinct && args.size() == 1 &&
                        (is_bare_star(args.Get(0)) || is_non_null_literal(args.Get(0)))) {
                        return agg;
                    }
                    // The engine evaluates an argument that reads no column once per
                    // chunk, not once per row (execution_dag_t::run), so the aggregate
                    // would fold a single row of each chunk.
                    if (!args.empty() && std::all_of(args.begin(), args.end(), is_constant)) {
                        const std::string what = "Spark " + function + "() over a constant argument is not supported";
                        return unsupported(what, resource);
                    }
                    for (const auto& arg : args) {
                        auto child = translate_expression(arg, params, resource);
                        if (child.has_error()) {
                            return child;
                        }
                        // Spark's Average casts its input to DOUBLE (DECIMAL aside) and answers
                        // DOUBLE. The engine's avg answers its input's type and divides in it,
                        // so over an integer column it would drop the fraction.
                        if (function == "avg") {
                            agg->append_param(as_double(child.value(), resource));
                            continue;
                        }
                        agg->append_param(child.value());
                    }
                    return agg;
                }
                case func_kind_t::arithmetic: {
                    if (args.size() != 2) {
                        return bad_expr("Spark arithmetic operator requires exactly 2 arguments", resource);
                    }
                    auto left = translate_expression(args.Get(0), params, resource);
                    if (left.has_error()) {
                        return left;
                    }
                    auto right = translate_expression(args.Get(1), params, resource);
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
                    auto left = translate_expression(args.Get(0), params, resource);
                    if (left.has_error()) {
                        return left;
                    }
                    auto right = translate_expression(args.Get(1), params, resource);
                    if (right.has_error()) {
                        return right;
                    }
                    return make_comparison(cls.ctype, left.value(), right.value(), resource);
                }
                case func_kind_t::logical_union: {
                    if (cls.ctype == ce::compare_type::union_not) {
                        if (args.size() != 1) {
                            return bad_expr("Spark NOT requires exactly 1 argument", resource);
                        }
                        auto child = translate_expression(args.Get(0), params, resource);
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
                        auto child = translate_expression(arg, params, resource);
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
                    auto child = translate_expression(args.Get(0), params, resource);
                    if (child.has_error()) {
                        return child;
                    }
                    // is_null/is_not_null carry no real RHS; use a NULL placeholder param,
                    // matching the SQL transformer's NullTest lowering.
                    const auto dummy = params->add_parameter(null_value(resource));
                    return ce::make_compare_expression(resource, cls.ctype, as_operand(child.value(), resource), dummy);
                }
                case func_kind_t::in_list: {
                    // Lowered as the transformer lowers `col IN (v1, v2)`: an OR of
                    // col = v, one per literal value.
                    if (args.size() < 2) {
                        return bad_expr("Spark isin requires at least one value", resource);
                    }
                    auto column = translate_expression(args.Get(0), params, resource);
                    if (column.has_error()) {
                        return column;
                    }
                    const auto key = as_operand(column.value(), resource);
                    if (!ce::is_key(key)) {
                        return unsupported("Spark isin is only supported on a column reference", resource);
                    }
                    auto any = ce::make_compare_union_expression(resource, cls.ctype);
                    for (int i = 1; i < args.size(); ++i) {
                        if (args.Get(i).expr_type_case() != sc::Expression::kLiteral) {
                            return unsupported("Spark isin is only supported with literal values", resource);
                        }
                        auto value = translate_expression(args.Get(i), params, resource);
                        if (value.has_error()) {
                            return value;
                        }
                        const auto parameter = as_operand(value.value(), resource);
                        any->append_child(ce::make_compare_expression(resource, ce::compare_type::eq, key, parameter));
                    }
                    return any;
                }
                case func_kind_t::between: {
                    // between(v, lower, upper) is v >= lower AND v <= upper. The operand
                    // is translated once per bound, so no expression node is shared.
                    if (args.size() != 3) {
                        return bad_expr("Spark between requires exactly 3 arguments", resource);
                    }
                    auto all = ce::make_compare_union_expression(resource, cls.ctype);
                    for (int bound = 1; bound <= 2; ++bound) {
                        auto operand = translate_expression(args.Get(0), params, resource);
                        if (operand.has_error()) {
                            return operand;
                        }
                        auto limit = translate_expression(args.Get(bound), params, resource);
                        if (limit.has_error()) {
                            return limit;
                        }
                        const auto type = bound == 1 ? ce::compare_type::gte : ce::compare_type::lte;
                        all->append_child(make_comparison(type, operand.value(), limit.value(), resource));
                    }
                    return all;
                }
                case func_kind_t::unary_neg: {
                    if (args.size() != 1) {
                        return bad_expr("Spark 'negative' requires exactly 1 argument", resource);
                    }
                    auto child = translate_expression(args.Get(0), params, resource);
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
                        auto child = translate_expression(arg, params, resource);
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

        expr_result translate_expression(const sc::Expression& expr,
                                         cl::parameter_node_ptr params,
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
                    auto child = translate_expression(al.expr(), params, resource);
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
                        // The output name is the expression's key: the validator, the
                        // planner and the SQL generator read it (a result alias names a
                        // table, not a column). A bare column is renamed the way the
                        // transformer renames `col AS x`: the alias is the key and the
                        // column its only parameter.
                        ce::key_t out_key{resource, alias_name};
                        if (result->group() == ce::expression_group::scalar) {
                            auto* scalar = static_cast<ce::scalar_expression_t*>(result.get());
                            if (scalar->type() == ce::scalar_type::get_field && scalar->params().empty()) {
                                out_key.set_side(scalar->key().side());
                                return ce::make_scalar_expression(resource,
                                                                  ce::scalar_type::get_field,
                                                                  out_key,
                                                                  scalar->key());
                            }
                        }
                        result->key() = out_key;
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
                    auto child = translate_expression(c.expr(), params, resource);
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
                    auto child = translate_expression(so.child(), params, resource);
                    if (child.has_error()) {
                        return child;
                    }
                    auto result = child.value();
                    ce::sort_order order = ce::sort_order::asc;
                    if (so.direction() == Expr::SortOrder::SORT_DIRECTION_DESCENDING) {
                        order = ce::sort_order::desc;
                    }
                    // Only a plain column is a sort key: the SQL generator writes ORDER BY
                    // from a key operand alone (a computed key has no name on a backend).
                    if (result->group() == ce::expression_group::scalar) {
                        auto* scalar = static_cast<ce::scalar_expression_t*>(result.get());
                        if (scalar->type() == ce::scalar_type::get_field && scalar->params().empty()) {
                            return ce::make_sort_expression(resource,
                                                            ce::key_t{scalar->key(), resource},
                                                            order,
                                                            null_order_of(so, order));
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

    } // namespace

    core::result_wrapper_t<components::logical_plan::expression_ptr>
    expression_to_plan(const sc::Expression& expr,
                       components::logical_plan::parameter_node_ptr params,
                       std::pmr::memory_resource* resource) {
        OTX_ZONE_N("spark::expression_to_plan");
        return translate_expression(expr, std::move(params), resource);
    }

} // namespace frontend::spark
