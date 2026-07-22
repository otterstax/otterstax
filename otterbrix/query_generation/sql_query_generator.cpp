// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#include "sql_query_generator.hpp"

#include <spdlog/spdlog.h>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <regex>

using namespace components::types;
using namespace components::logical_plan;
using namespace components::expressions;
using namespace components::types;

namespace {

    // Backend-aware identifier quoting (single quoting point for every
    // identifier the generator emits). Otterbrix and PostgreSQL identifiers
    // are double-quoted; MySQL and ClickHouse use backticks. The quote
    // character itself is doubled when embedded. Unknown/Mixed land in the
    // MySQL family, mirroring table_reference's default branch.
    char ident_quote_char(backend_type_t backend) {
        switch (backend) {
            case backend_type_t::PostgreSQL:
            case backend_type_t::Otterbrix:
                return '"';
            case backend_type_t::MySQL:
            case backend_type_t::ClickHouse:
            case backend_type_t::Unknown:
            case backend_type_t::Mixed:
            default:
                return '`';
        }
    }

    void quote_ident(std::stringstream& stream, std::string_view ident, backend_type_t backend) {
        const char q = ident_quote_char(backend);
        stream << q;
        for (char c : ident) {
            if (c == q) {
                stream << q;
            }
            stream << c;
        }
        stream << q;
    }

    std::string quote_ident(std::string_view ident, backend_type_t backend) {
        std::stringstream s;
        quote_ident(s, ident, backend);
        return s.str();
    }

    // Emit a (possibly multi-part) expression key. key_t carries identifier
    // parts separately in storage() (table/column or struct-member paths);
    // each part is quoted on its own — a dotted string is never quoted as one
    // identifier. A '*' part is the star token, not an identifier.
    void write_key(std::stringstream& stream, const components::expressions::key_t& key, backend_type_t backend) {
        bool dot = false;
        for (const auto& part : key.storage()) {
            if (dot) {
                stream << '.';
            }
            if (part == "*") {
                stream << '*';
            } else {
                quote_ident(stream, std::string_view{part.data(), part.size()}, backend);
            }
            dot = true;
        }
    }

    std::string key_to_string(const components::expressions::key_t& key, backend_type_t backend) {
        std::stringstream s;
        write_key(s, key, backend);
        return s.str();
    }

    template<class OStream>
    OStream& operator<<(OStream& stream, logical_type type) {
        switch (type) {
            case logical_type::BOOLEAN:
                stream << "boolean";
                break;
            case logical_type::USMALLINT:
            case logical_type::SMALLINT:
                stream << "int2";
                break;
            case logical_type::UINTEGER:
            case logical_type::INTEGER:
                stream << "int4";
                break;
            case logical_type::UBIGINT:
            case logical_type::BIGINT:
                stream << "int8";
                break;
            case logical_type::FLOAT:
                stream << "float4";
                break;
            case logical_type::DOUBLE:
                stream << "float8";
                break;
            case logical_type::BLOB:
            case logical_type::BIT:
            case logical_type::STRING_LITERAL:
                stream << "text";
                break;
            default:
                // TODO: implement other value types
                throw std::runtime_error("Encountered an unsupported type during query generation");
        }
        return stream;
    }

    // Column definition inside CREATE TABLE: quoted column name + type keyword
    // (type names are SQL keywords and stay unquoted).
    void write_column_def(std::stringstream& stream,
                          const components::types::complex_logical_type& type,
                          backend_type_t backend) {
        quote_ident(stream, type.alias(), backend);
        stream << " ";
        if (type.type() == logical_type::ARRAY) {
            // TODO: multiple dimentions array
            stream << static_cast<const components::types::array_logical_type_extension*>(type.extension())
                          ->internal_type()
                          .type();
            stream << "[";
            stream << static_cast<const components::types::array_logical_type_extension*>(type.extension())->size();
            stream << "]";
        } else {
            stream << type.type();
        }
    }

    // Write a logical value to stream with backend-specific quoting
    template<class OStream>
    void write_logical_value(OStream& stream, const components::types::logical_value_t& value, backend_type_t backend) {
        switch (value.type().type()) {
            case logical_type::NA:
                stream << "NULL";
                break;
            case logical_type::BOOLEAN:
                stream << (value.value<bool>() ? "TRUE" : "FALSE");
                break;
            case logical_type::TINYINT:
                stream << value.value<int8_t>();
                break;
            case logical_type::SMALLINT:
                stream << value.value<int16_t>();
                break;
            case logical_type::INTEGER:
                stream << value.value<int32_t>();
                break;
            case logical_type::BIGINT:
                stream << value.value<int64_t>();
                break;
            case logical_type::HUGEINT:
                stream << value.value<components::types::int128_t>();
                break;
            case logical_type::FLOAT:
                stream << value.value<float>();
                break;
            case logical_type::DOUBLE:
                stream << value.value<double>();
                break;
            case logical_type::UTINYINT:
                stream << value.value<uint8_t>();
                break;
            case logical_type::USMALLINT:
                stream << value.value<uint16_t>();
                break;
            case logical_type::UINTEGER:
                stream << value.value<uint32_t>();
                break;
            case logical_type::UBIGINT:
                stream << value.value<uint64_t>();
                break;
            case logical_type::UHUGEINT:
                stream << value.value<components::types::uint128_t>();
                break;
            case logical_type::STRING_LITERAL: {
                // PostgreSQL requires single quotes for string literals
                // MySQL accepts both, but single quotes are standard SQL
                const std::string& str = *value.value<std::string*>();
                if (backend == backend_type_t::PostgreSQL || backend == backend_type_t::ClickHouse) {
                    // For PostgreSQL/ClickHouse, use single quotes and escape any single quotes in the string
                    stream << "'";
                    for (char c : str) {
                        if (c == '\'') {
                            stream << "''"; // SQL standard escape: '' becomes '
                        } else {
                            stream << c;
                        }
                    }
                    stream << "'";
                } else {
                    // MySQL: use single quotes (standard SQL, works in all modes)
                    stream << "'" << str << "'";
                }
                break;
            }
            case logical_type::STRUCT: {
                stream << "ROW(";
                bool separator = false;
                for (const auto child_val : value.children()) {
                    if (separator) {
                        stream << ", ";
                    }
                    write_logical_value(stream, child_val, backend);
                    separator = true;
                }
                stream << ")";
                break;
            }
            case logical_type::ARRAY: {
                stream << "{";
                bool separator = false;
                for (const auto child_val : value.children()) {
                    if (separator) {
                        stream << ", ";
                    }
                    write_logical_value(stream, child_val, backend);
                    separator = true;
                }
                stream << "}";
                break;
            }
            default:
                // TODO: implement other value types
                throw std::runtime_error("Encountered an unsupported value type during query generation");
        }
    }

    // Legacy operator for backward compatibility (uses MySQL-style quoting)
    template<class OStream>
    OStream& operator<<(OStream& stream, const components::types::logical_value_t& value) {
        // Default to MySQL behavior for backward compatibility
        write_logical_value(stream, value, backend_type_t::MySQL);
        return stream;
    }

    void generate_compare_expr(std::stringstream& stream,
                               const compare_expression_ptr& expr,
                               const storage_parameters* parameters,
                               backend_type_t backend) {
        // to remove operation order deciphering incase everything in brackets
        switch (expr->type()) {
            case compare_type::union_and: {
                stream << "(";
                bool separator = false;
                for (const auto& child : expr->children()) {
                    if (separator) {
                        stream << " AND ";
                    }
                    generate_compare_expr(stream,
                                          reinterpret_cast<const compare_expression_ptr&>(child),
                                          parameters,
                                          backend);
                    separator = true;
                }
                stream << ")";
                return;
            }
            case compare_type::union_or: {
                stream << "(";
                bool separator = false;
                for (const auto& child : expr->children()) {
                    if (separator) {
                        stream << " OR ";
                    }
                    generate_compare_expr(stream,
                                          reinterpret_cast<const compare_expression_ptr&>(child),
                                          parameters,
                                          backend);
                    separator = true;
                }
                stream << ")";
                return;
            }
            case compare_type::union_not: {
                stream << "!(";
                generate_compare_expr(stream,
                                      reinterpret_cast<const compare_expression_ptr&>(expr->children().front()),
                                      parameters,
                                      backend);
                stream << ")";
                return;
            }
            default:
                break;
        }

        // left side
        if (std::holds_alternative<components::expressions::key_t>(expr->left())) {
            write_key(stream, std::get<components::expressions::key_t>(expr->left()), backend);
        } else if (std::holds_alternative<core::parameter_id_t>(expr->left())) {
            auto it = parameters->parameters.find(std::get<core::parameter_id_t>(expr->left()));
            if (it != parameters->parameters.end()) {
                stream << it->second;
            } else {
                stream << "NULL";
            }
        }
        switch (expr->type()) {
            case compare_type::eq:
                stream << " = ";
                break;
            case compare_type::ne:
                stream << " != ";
                break;
            case compare_type::gt:
                stream << " > ";
                break;
            case compare_type::lt:
                stream << " < ";
                break;
            case compare_type::gte:
                stream << " >= ";
                break;
            case compare_type::lte:
                stream << " <= ";
                break;
        }
        // right side
        if (std::holds_alternative<components::expressions::key_t>(expr->right())) {
            write_key(stream, std::get<components::expressions::key_t>(expr->right()), backend);
        } else if (std::holds_alternative<core::parameter_id_t>(expr->right())) {
            auto it = parameters->parameters.find(std::get<core::parameter_id_t>(expr->right()));
            if (it != parameters->parameters.end()) {
                stream << it->second;
            } else {
                stream << "NULL";
            }
        }
    }

    void generate_update_expr(std::stringstream& stream,
                              const update_expr_ptr& expr,
                              const storage_parameters* parameters,
                              backend_type_t backend) {
        switch (expr->type()) {
            case update_expr_type::set:
                stream << "SET ";
                write_key(stream, reinterpret_cast<const update_expr_set_ptr&>(expr)->key(), backend);
                stream << " = ";
                generate_update_expr(stream, expr->left(), parameters, backend);
                break;
            case update_expr_type::get_value:
                write_key(stream, reinterpret_cast<const update_expr_get_value_ptr&>(expr)->key(), backend);
                break;
            case update_expr_type::get_value_params: {
                auto it =
                    parameters->parameters.find(reinterpret_cast<const update_expr_get_const_value_ptr&>(expr)->id());
                if (it != parameters->parameters.end()) {
                    stream << it->second;
                } else {
                    stream << "NULL";
                }
                break;
            }
            case update_expr_type::add:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " + ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::sub:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " - ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::mult:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " * ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::div:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " / ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::mod:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " % ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::exp:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " ^ ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::sqr_root:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " |/ ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::cube_root:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " ||/ ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::factorial:
                stream << "(";
                stream << "!! ";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::abs:
                stream << "(";
                stream << "@ ";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::AND:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " & ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::OR:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " | ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::XOR:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " # ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::NOT:
                stream << "(";
                stream << "~ ";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::shift_left:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " << ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
            case update_expr_type::shift_right:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters, backend);
                stream << " >> ";
                generate_update_expr(stream, expr->right(), parameters, backend);
                stream << ")";
                break;
        }
    }

    inline std::string generate_aggregate(const aggregate_expression_ptr& agg_expr, backend_type_t backend) {
        auto fname = agg_expr->function_name();
        std::string sql_func;
        sql_func.reserve(fname.size());
        for (auto c : fname) {
            sql_func += static_cast<char>(std::toupper(c));
        }
        std::string out;
        out.reserve(sql_func.size() + 16);
        out.append(sql_func);
        out.push_back('(');
        if (agg_expr->params().empty()) {
            // parameterless aggregate (e.g. COUNT(*)) — function name itself
            out.push_back('*');
        } else if (std::holds_alternative<components::expressions::key_t>(agg_expr->params().front())) {
            out.append(key_to_string(std::get<components::expressions::key_t>(agg_expr->params().front()), backend));
        }
        out.push_back(')');
        // always alias the aggregate so the outer otterbrix plan can join
        // backend results back to the expected column name.
        out.append(" AS ");
        out.append(key_to_string(agg_expr->key(), backend));
        return out;
    }

    void generate_select(std::stringstream& stream,
                         const node_aggregate_ptr& node,
                         const storage_parameters* parameters,
                         backend_type_t backend,
                         const qualified_name_t& table_name) {
        node_select_ptr select = nullptr;
        node_group_ptr group = nullptr;
        node_match_ptr match = nullptr;
        node_sort_ptr sort = nullptr;
        node_limit_ptr limit = nullptr;
        for (const auto& child : node->children()) {
            switch (child->type()) {
                case node_type::select_t:
                    select = reinterpret_cast<const node_select_ptr&>(child);
                    break;
                case node_type::group_t:
                    group = reinterpret_cast<const node_group_ptr&>(child);
                    break;
                case node_type::match_t:
                    match = reinterpret_cast<const node_match_ptr&>(child);
                    break;
                case node_type::sort_t:
                    sort = reinterpret_cast<const node_sort_ptr&>(child);
                    break;
                case node_type::limit_t:
                    limit = reinterpret_cast<const node_limit_ptr&>(child);
                    break;
                default:
                    break;
            }
        }

        std::unordered_map<std::string, aggregate_expression_ptr> aggregate_by_alias;
        if (group) {
            for (const auto& expr : group->expressions()) {
                if (expr->group() == expression_group::aggregate) {
                    auto agg_expr = reinterpret_cast<const aggregate_expression_ptr&>(expr);
                    aggregate_by_alias.emplace(agg_expr->key().as_string(), agg_expr);
                }
            }
        }

        stream << "SELECT ";
        // fields
        {
            std::vector<std::string> fields;
            if (select) {
                for (const auto& expr : select->expressions()) {
                    if (expr->group() == expression_group::aggregate) {
                        fields.emplace_back(
                            generate_aggregate(reinterpret_cast<const aggregate_expression_ptr&>(expr), backend));
                        continue;
                    }
                    auto scalar_expr = reinterpret_cast<const scalar_expression_ptr&>(expr);

                    if (scalar_expr->type() == scalar_type::star_expand) {
                        fields.emplace_back("*");
                        continue;
                    }

                    if (scalar_expr->type() == scalar_type::get_field && scalar_expr->params().empty()) {
                        if (auto it = aggregate_by_alias.find(scalar_expr->key().as_string());
                            it != aggregate_by_alias.end()) {
                            fields.emplace_back(generate_aggregate(it->second, backend));
                            continue;
                        }
                    }

                    // plain column reference.
                    if (scalar_expr->params().empty()) {
                        fields.emplace_back(key_to_string(scalar_expr->key(), backend));
                        continue;
                    }

                    // constant / parameter.
                    if (auto param_v = scalar_expr->params().at(0);
                        std::holds_alternative<core::parameter_id_t>(param_v)) {
                        std::stringstream ss;
                        auto it = parameters->parameters.find(std::get<core::parameter_id_t>(std::move(param_v)));
                        if (it != parameters->parameters.end()) {
                            ss << it->second;
                        } else {
                            ss << "NULL";
                        }
                        fields.emplace_back(ss.str());
                    } else {
                        // Aliased column: `col AS alias`
                        fields.emplace_back(
                            key_to_string(std::get<components::expressions::key_t>(scalar_expr->params().front()),
                                          backend) +
                            " AS " + key_to_string(scalar_expr->key(), backend));
                    }
                }
            }

            // Empty select_node OR no select_node at all → SELECT *. When the
            // outer query did SELECT col1, AGG(col2), AGG(col3) — and the
            // transformer left the select_node empty (e.g. all entries were
            // pure aggregates pulled into group_node), we still need to emit
            // those aggregates so the backend computes them.
            if (fields.empty() && group) {
                for (const auto& expr : group->expressions()) {
                    if (expr->group() == expression_group::aggregate) {
                        fields.emplace_back(
                            generate_aggregate(reinterpret_cast<const aggregate_expression_ptr&>(expr), backend));
                    }
                }
            }

            if (fields.empty()) {
                stream << "*";
            } else {
                bool comma = false;
                for (const auto& f : fields) {
                    if (comma) {
                        stream << ", ";
                    }

                    stream << f;
                    comma = true;
                }
            }

            stream << " FROM ";
            stream << sql_gen::table_reference(table_name, backend);
        }
        // where
        {
            if (match) {
                stream << " WHERE ";
                generate_compare_expr(stream,
                                      reinterpret_cast<const compare_expression_ptr&>(match->expressions().front()),
                                      parameters,
                                      backend);
            }
        }
        // group by
        {
            if (group) {
                std::vector<std::string> group_by_fields;
                for (const auto& expr : group->expressions()) {
                    if (expr->group() != expression_group::scalar) {
                        continue;
                    }
                    auto scalar_expr = reinterpret_cast<const scalar_expression_ptr&>(expr);
                    // GROUP BY fields are marked with special scalar type
                    if (scalar_expr->type() != scalar_type::group_field) {
                        continue;
                    }
                    if (scalar_expr->params().empty()) {
                        group_by_fields.emplace_back(key_to_string(scalar_expr->key(), backend));
                    } else if (std::holds_alternative<components::expressions::key_t>(scalar_expr->params().front())) {
                        group_by_fields.emplace_back(
                            key_to_string(std::get<components::expressions::key_t>(scalar_expr->params().front()),
                                          backend));
                    }
                }
                if (!group_by_fields.empty()) {
                    stream << " GROUP BY ";
                    bool comma = false;
                    for (const auto& field : group_by_fields) {
                        if (comma) {
                            stream << ", ";
                        }
                        stream << field;
                        comma = true;
                    }
                }
            }
        }
        // order by
        {
            if (sort && !sort->expressions().empty()) {
                stream << " ORDER BY ";
                bool comma = false;
                for (const auto& expr : sort->expressions()) {
                    if (comma) {
                        stream << ", ";
                    }

                    auto sort_expr = reinterpret_cast<const sort_expression_ptr&>(expr);
                    write_key(stream, sort_expr->key(), backend);
                    stream << (sort_expr->order() == sort_order::desc ? " DESC" : " ASC");
                    comma = true;
                }
            }
        }
        // limit / offset — push the node_limit_t child down to the backend so a
        // remote SELECT ... LIMIT n returns n rows (unlimited sentinel is -1).
        {
            if (limit && limit->limit().limit() >= 0) {
                stream << " LIMIT " << limit->limit().limit();
                if (limit->limit().offset() > 0) {
                    stream << " OFFSET " << limit->limit().offset();
                }
            }
        }
    }

    void generate_create_collection(std::stringstream& stream,
                                    const node_create_collection_ptr& node,
                                    const qualified_name_t& name,
                                    backend_type_t backend) {
        stream << "CREATE TABLE ";
        quote_ident(stream, name.collection, backend);
        stream << " (";
        bool comma = false;
        for (const auto& type : node->schema()) {
            if (comma) {
                stream << ", ";
            }

            write_column_def(stream, type, backend);
            comma = true;
        }
        stream << ")";
    }

    // this wight cause problems with connections
    void generate_create_database(std::stringstream& stream, const qualified_name_t& name, backend_type_t backend) {
        stream << "CREATE DATABASE ";
        quote_ident(stream, name.database, backend);
    }

    void generate_create_index(std::stringstream& stream,
                               const node_create_index_ptr& node,
                               const qualified_name_t& name,
                               backend_type_t backend) {
        stream << "CREATE INDEX ";
        quote_ident(stream, node->name(), backend);
        stream << " ON " << sql_gen::table_reference(name, backend);
        stream << " (";
        bool comma = false;
        for (const auto& key : node->keys()) {
            if (comma) {
                stream << ", ";
            }

            write_key(stream, key, backend);
            comma = true;
        }
        stream << ")";
    }

    void generate_delete(std::stringstream& stream,
                         const node_delete_ptr& node,
                         const storage_parameters* parameters,
                         backend_type_t backend,
                         const otterstax::names::resolved_target_t& target) {
        node_match_ptr match = nullptr;
        for (const auto& child : node->children()) {
            if (child->type() == node_type::match_t) {
                match = reinterpret_cast<const node_match_ptr&>(child);
            }
        }
        stream << "DELETE FROM ";
        stream << sql_gen::table_reference(target.name, backend);
        if (!target.from_name.collection.empty()) {
            //! node_delete supports raw_data after using, but it is not possible to send it
            stream << " USING " << sql_gen::table_reference(target.from_name, backend);
        }
        // WHERE
        if (match) {
            stream << " WHERE ";
            generate_compare_expr(stream,
                                  reinterpret_cast<const compare_expression_ptr&>(match->expressions().front()),
                                  parameters,
                                  backend);
        }
    }

    void generate_drop_collection(std::stringstream& stream, const qualified_name_t& name, backend_type_t backend) {
        stream << "DROP TABLE ";
        quote_ident(stream, name.collection, backend);
    }

    // this wight cause problems with connections
    void generate_drop_database(std::stringstream& stream, const qualified_name_t& name, backend_type_t backend) {
        stream << "DROP DATABASE ";
        quote_ident(stream, name.database, backend);
    }

    void generate_drop_index(std::stringstream& stream,
                             const otterstax::names::resolved_target_t& target,
                             backend_type_t backend) {
        // target.name is the indexed table; target.from_name carries the index
        // (the second catalog_resolve_table of the wrapping sequence).
        stream << "DROP INDEX IF EXISTS ";
        quote_ident(stream, target.from_name.collection, backend);
        stream << " ON ";
        quote_ident(stream, target.name.collection, backend);
    }

    void generate_insert(std::stringstream& stream,
                         const node_insert_ptr& node,
                         const storage_parameters* parameters,
                         backend_type_t backend,
                         const otterstax::names::resolved_target_t& target,
                         const std::pmr::vector<external_entry_t>& batch) {
        stream << "INSERT INTO " << sql_gen::table_reference(target.name, backend) << " ";
        if (!node->key_translation().empty()) {
            stream << "(";
            bool comma = false;
            for (const auto& key : node->key_translation()) {
                if (comma) {
                    stream << ", ";
                }
                write_key(stream, key, backend);
                comma = true;
            }
            stream << ") ";
        }
        if (node->children().front()->type() == node_type::data_t) {
            sql_gen::generate_values(stream,
                                     reinterpret_cast<const node_data_ptr&>(node->children().front())->data_chunk(),
                                     backend);
        } else {
            assert(node->children().front()->type() == node_type::aggregate_t);
            // INSERT ... SELECT: the inner SELECT's table is resolved through the
            // batch targets by the child aggregate's stamped table_oid. A missing
            // or invalid oid means CatalogManager did not run / stamp this node —
            // a pipeline programming error, never something to paper over.
            const auto& child = node->children().front();
            const auto child_oid = child->table_oid();
            if (child_oid == components::catalog::INVALID_OID) {
                throw std::logic_error("generate_insert: INSERT..SELECT child aggregate has no table_oid stamped");
            }
            const qualified_name_t* child_name = nullptr;
            for (const auto& entry : batch) {
                if (entry.target.oid == child_oid) {
                    child_name = &entry.target.name;
                    break;
                }
            }
            if (!child_name) {
                throw std::logic_error(
                    "generate_insert: no batch target matches the INSERT..SELECT child aggregate's table_oid");
            }
            generate_select(stream,
                            reinterpret_cast<const node_aggregate_ptr&>(node->children().front()),
                            parameters,
                            backend,
                            *child_name);
        }
    }

    void generate_update(std::stringstream& stream,
                         const node_update_ptr& node,
                         const storage_parameters* parameters,
                         backend_type_t backend,
                         const otterstax::names::resolved_target_t& target) {
        node_match_ptr match = nullptr;
        for (const auto& child : node->children()) {
            if (child->type() == node_type::match_t) {
                match = reinterpret_cast<const node_match_ptr&>(child);
            }
        }
        stream << "UPDATE " << sql_gen::table_reference(target.name, backend) << " ";
        bool comma = false;
        for (const auto& set : node->updates()) {
            if (comma) {
                stream << ", ";
            }

            generate_update_expr(stream, set, parameters, backend);
            comma = true;
        }
        if (!target.from_name.collection.empty()) {
            stream << " FROM " << sql_gen::table_reference(target.from_name, backend);
        }
        // WHERE
        if (match) {
            stream << " WHERE ";
            generate_compare_expr(stream,
                                  reinterpret_cast<const compare_expression_ptr&>(match->expressions().front()),
                                  parameters,
                                  backend);
        }
    }

} // namespace

namespace sql_gen {

    namespace {
        // ClickHouse-only dialect fixup: postgres-style `(expr).field` tuple
        // member access doesn't parse in CH, which expects plain `expr.field`.
        std::string ch_unwrap_paren_field_access(std::string sql) {
            static const std::regex paren_field(R"(\(([a-zA-Z_][\w.]*)\)\.([a-zA-Z_]\w*))");
            for (int i = 0; i < 8; ++i) {
                std::string out = std::regex_replace(sql, paren_field, "$1.$2");
                if (out == sql)
                    break;
                sql = std::move(out);
            }
            return sql;
        }
    } // namespace

    std::string replace_qualifiers(std::string raw_sql,
                                   const std::vector<otterstax::parser::qualifier_rewrite_t>& quals,
                                   backend_type_t backend) {
        // substitute qualifiers in descending offset order so earlier slots won't shift the later ones
        std::vector<otterstax::parser::qualifier_rewrite_t> sorted = quals;
        std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) { return a.start > b.start; });
        for (const auto& q : sorted) {
            if (q.start < 0 || q.length <= 0 ||
                static_cast<size_t>(q.start) + static_cast<size_t>(q.length) > raw_sql.size()) {
                continue;
            }
            raw_sql.replace(q.start, q.length, table_reference(q.name, backend));
        }
        if (backend == backend_type_t::ClickHouse) {
            raw_sql = ch_unwrap_paren_field_access(std::move(raw_sql));
        }
        return raw_sql;
    }

    std::string table_reference(const qualified_name_t& name, backend_type_t backend) {
        std::stringstream s;
        if (name.empty()) {
            spdlog::debug("table_reference: empty name, returning NonCollectionData");
            return "NonCollectionData";
        }

        spdlog::debug("table_reference: uid={}, db={}, schema={}, table={}, backend={}",
                      name.unique_identifier,
                      name.database,
                      name.schema,
                      name.collection,
                      static_cast<int>(backend));

        switch (backend) {
            case backend_type_t::PostgreSQL:
                // PostgreSQL: schema.collection (e.g., public.products)
                // If schema is empty, use "public" as default
                quote_ident(s, name.schema.empty() ? std::string_view{"public"} : std::string_view{name.schema},
                            backend);
                s << ".";
                quote_ident(s, name.collection, backend);
                break;
            case backend_type_t::ClickHouse:
                // ClickHouse: database.collection (no schema level)
                quote_ident(s, name.database, backend);
                s << ".";
                quote_ident(s, name.collection, backend);
                break;
            case backend_type_t::MySQL:
            case backend_type_t::Unknown:
            case backend_type_t::Mixed:
            default:
                // MySQL: database.collection
                quote_ident(s, name.database, backend);
                s << ".";
                quote_ident(s, name.collection, backend);
                break;
        }
        spdlog::debug("table_reference: generated '{}'", s.str());
        return s.str();
    }

    void
    generate_values(std::stringstream& stream, const components::vector::data_chunk_t& chunk, backend_type_t backend) {
        stream << "VALUES ";
        bool comma = false;
        for (size_t i = 0; i < chunk.size(); i++) {
            if (comma) {
                stream << ", ";
            }

            stream << "(";
            for (size_t j = 0; j < chunk.column_count(); j++) {
                if (j != 0) {
                    stream << ", ";
                }

                write_logical_value(stream, chunk.value(j, i), backend);
            }
            stream << ")";
            comma = true;
        }
    }

    void generate_query(std::stringstream& stream,
                        const node_ptr& node,
                        const storage_parameters* parameters,
                        backend_type_t backend,
                        const otterstax::names::resolved_target_t& target,
                        const std::pmr::vector<external_entry_t>& batch) {
        switch (node->type()) {
            case node_type::aggregate_t:
                generate_select(stream,
                                reinterpret_cast<const node_aggregate_ptr&>(node),
                                parameters,
                                backend,
                                target.name);
                break;
            case node_type::create_collection_t:
                generate_create_collection(stream,
                                           reinterpret_cast<const node_create_collection_ptr&>(node),
                                           target.name,
                                           backend);
                break;
            case node_type::create_database_t:
                generate_create_database(stream, target.name, backend);
                break;
            case node_type::create_index_t:
                generate_create_index(stream,
                                      reinterpret_cast<const node_create_index_ptr&>(node),
                                      target.name,
                                      backend);
                break;
            case node_type::delete_t:
                generate_delete(stream, reinterpret_cast<const node_delete_ptr&>(node), parameters, backend, target);
                break;
            case node_type::drop_t: {
                switch (reinterpret_cast<const node_drop_ptr&>(node)->kind()) {
                    case drop_target_kind::collection:
                        generate_drop_collection(stream, target.name, backend);
                        break;
                    case drop_target_kind::database:
                        generate_drop_database(stream, target.name, backend);
                        break;
                    case drop_target_kind::index:
                        generate_drop_index(stream, target, backend);
                        break;
                    default:
                        // type/sequence/view/macro never reach the generator (local-only);
                        // kept as the file's throw-caught-at-actor-boundary contract (decision Q1).
                        throw std::logic_error("incorrect drop kind for generate_query: " +
                                               to_string(node->type()));
                }
                break;
            }
            case node_type::insert_t:
                generate_insert(stream,
                                reinterpret_cast<const node_insert_ptr&>(node),
                                parameters,
                                backend,
                                target,
                                batch);
                break;
            case node_type::update_t:
                generate_update(stream, reinterpret_cast<const node_update_ptr&>(node), parameters, backend, target);
                break;
            default:
                throw std::logic_error("incorrect node type for generate_query: " + to_string(node->type()));
        }
    }

    std::string generate_query(const node_ptr& node,
                               const storage_parameters* parameters,
                               backend_type_t backend,
                               const otterstax::names::resolved_target_t& target,
                               const std::pmr::vector<external_entry_t>& batch) {
        std::stringstream stream;
        generate_query(stream, node, parameters, backend, target, batch);
        stream << ";";
        return stream.str();
    }

    std::string create_database_statement(const std::string& db) {
        std::stringstream s;
        s << "CREATE DATABASE ";
        quote_ident(s, db, backend_type_t::Otterbrix);
        return s.str();
    }

    std::string drop_database_statement(const std::string& db) {
        std::stringstream s;
        s << "DROP DATABASE ";
        quote_ident(s, db, backend_type_t::Otterbrix);
        return s.str();
    }

} // namespace sql_gen