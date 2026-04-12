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
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>

using namespace components::types;
using namespace components::logical_plan;
using namespace components::expressions;
using namespace components::types;

namespace {

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

    template<class OStream>
    OStream& operator<<(OStream& stream, const components::types::complex_logical_type& type) {
        stream << type.alias() << " ";
        if (type.type() == logical_type::ARRAY) {
            // TODO: multiple dimentions array
            stream << static_cast<const components::types::array_logical_type_extension*>(type.extension())
                          ->internal_type();
            stream << "[";
            stream << static_cast<const components::types::array_logical_type_extension*>(type.extension())->size();
            stream << "]";
        } else {
            stream << type.type();
        }
        return stream;
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
                if (backend == backend_type_t::PostgreSQL) {
                    // For PostgreSQL, use single quotes and escape any single quotes in the string
                    stream << "'";
                    for (char c : str) {
                        if (c == '\'') {
                            stream << "''";  // SQL standard escape: '' becomes '
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
                               const storage_parameters* parameters) {
        // to remove operation order deciphering incase everything in brackets
        switch (expr->type()) {
            case compare_type::union_and: {
                stream << "(";
                bool separator = false;
                for (const auto& child : expr->children()) {
                    if (separator) {
                        stream << " AND ";
                    }
                    generate_compare_expr(stream, reinterpret_cast<const compare_expression_ptr&>(child), parameters);
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
                    generate_compare_expr(stream, reinterpret_cast<const compare_expression_ptr&>(child), parameters);
                    separator = true;
                }
                stream << ")";
                return;
            }
            case compare_type::union_not: {
                stream << "!(";
                generate_compare_expr(stream,
                                      reinterpret_cast<const compare_expression_ptr&>(expr->children().front()),
                                      parameters);
                stream << ")";
                return;
            }
            default:
                break;
        }

        // left side
        if (std::holds_alternative<expressions::key_t>(expr->left())) {
            stream << std::get<expressions::key_t>(expr->left()).as_string();
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
        if (std::holds_alternative<expressions::key_t>(expr->right())) {
            stream << std::get<expressions::key_t>(expr->right()).as_string();
        } else if (std::holds_alternative<core::parameter_id_t>(expr->right())) {
            auto it = parameters->parameters.find(std::get<core::parameter_id_t>(expr->right()));
            if (it != parameters->parameters.end()) {
                stream << it->second;
            } else {
                stream << "NULL";
            }
        }
    }

    void
    generate_update_expr(std::stringstream& stream, const update_expr_ptr& expr, const storage_parameters* parameters) {
        switch (expr->type()) {
            case update_expr_type::set:
                stream << "SET " << reinterpret_cast<const update_expr_set_ptr&>(expr)->key().as_string() << " = ";
                generate_update_expr(stream, expr->left(), parameters);
                break;
            case update_expr_type::get_value:
                stream << reinterpret_cast<const update_expr_get_value_ptr&>(expr)->key().as_string();
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
                generate_update_expr(stream, expr->left(), parameters);
                stream << " + ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::sub:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " - ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::mult:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " * ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::div:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " / ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::mod:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " % ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::exp:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " ^ ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::sqr_root:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " |/ ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::cube_root:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " ||/ ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::factorial:
                stream << "(";
                stream << "!! ";
                generate_update_expr(stream, expr->left(), parameters);
                stream << ")";
                break;
            case update_expr_type::abs:
                stream << "(";
                stream << "@ ";
                generate_update_expr(stream, expr->left(), parameters);
                stream << ")";
                break;
            case update_expr_type::AND:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " & ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::OR:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " | ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::XOR:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " # ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::NOT:
                stream << "(";
                stream << "~ ";
                generate_update_expr(stream, expr->left(), parameters);
                stream << ")";
                break;
            case update_expr_type::shift_left:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " << ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
            case update_expr_type::shift_right:
                stream << "(";
                generate_update_expr(stream, expr->left(), parameters);
                stream << " >> ";
                generate_update_expr(stream, expr->right(), parameters);
                stream << ")";
                break;
        }
    }

    void
    generate_select(std::stringstream& stream, const node_aggregate_ptr& node, const storage_parameters* parameters, backend_type_t backend) {
        node_group_ptr group = nullptr;
        node_match_ptr match = nullptr;
        node_sort_ptr sort = nullptr;
        for (const auto& child : node->children()) {
            if (child->type() == node_type::group_t) {
                group = reinterpret_cast<const node_group_ptr&>(child);
            } else if (child->type() == node_type::match_t) {
                match = reinterpret_cast<const node_match_ptr&>(child);
            } else if (child->type() == node_type::sort_t) {
                sort = reinterpret_cast<const node_sort_ptr&>(child);
            }
        }
        stream << "SELECT ";
        // fields
        {
            if (group) {
                std::vector<std::string> fields;
                for (const auto& expr : group->expressions()) {
                    switch (expr->group()) {
                        case expression_group::aggregate: {
                            auto agg_expr = reinterpret_cast<const aggregate_expression_ptr&>(expr);
                            auto fname = agg_expr->function_name();
                            // Convert function name to SQL uppercase
                            std::string sql_func;
                            for (auto c : fname) sql_func += static_cast<char>(std::toupper(c));
                            fields.emplace_back(sql_func + "(");
                            if (agg_expr->params().empty()) {
                                fields.back().append(agg_expr->key().as_string() + ")");
                            } else {
                                fields.back().append(
                                    std::get<components::expressions::key_t>(agg_expr->params().front()).as_string() +
                                    ") AS " + agg_expr->key().as_string());
                            }
                            break;
                        }
                        case expression_group::scalar: {
                            auto scalar_expr = reinterpret_cast<const scalar_expression_ptr&>(expr);
                            if (scalar_expr->params().empty()) {
                                fields.emplace_back(scalar_expr->key().as_string());
                            } else if (auto param_v = scalar_expr->params().at(0);
                                       std::holds_alternative<core::parameter_id_t>(param_v)) {
                                std::stringstream ss;
                                auto it =
                                    parameters->parameters.find(std::get<core::parameter_id_t>(std::move(param_v)));
                                if (it != parameters->parameters.end()) {
                                    ss << it->second;
                                } else {
                                    ss << "NULL";
                                }
                                fields.emplace_back(ss.str());
                            } else {
                                fields.emplace_back(
                                    std::get<components::expressions::key_t>(scalar_expr->params().front())
                                        .as_string() +
                                    " AS " + scalar_expr->key().as_string());
                            }
                            break;
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
            } else {
                stream << "*";
            }
            stream << " FROM ";
            stream << sql_gen::table_reference(node->collection_full_name(), backend);
        }
        // where
        {
            if (match) {
                stream << " WHERE ";
                generate_compare_expr(stream,
                                      reinterpret_cast<const compare_expression_ptr&>(match->expressions().front()),
                                      parameters);
            }
        }
        // group by
        {
            if (group) {
                std::vector<std::string> group_by_fields;
                for (const auto& expr : group->expressions()) {
                    if (expr->group() == expression_group::scalar) {
                        auto scalar_expr = reinterpret_cast<const scalar_expression_ptr&>(expr);
                        // Get the field name for GROUP BY
                        if (scalar_expr->params().empty()) {
                            group_by_fields.emplace_back(scalar_expr->key().as_string());
                        } else if (std::holds_alternative<components::expressions::key_t>(scalar_expr->params().front())) {
                            group_by_fields.emplace_back(
                                std::get<components::expressions::key_t>(scalar_expr->params().front()).as_string());
                        }
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
                    stream << sort_expr->key() << (sort_expr->order() == sort_order::desc ? " DESC" : " ASC");
                    comma = true;
                }
            }
        }
    }

    void generate_create_collection(std::stringstream& stream, const node_create_collection_ptr& node) {
        stream << "CREATE TABLE " << node->collection_full_name().collection;
        stream << " (";
        bool comma = false;
        for (const auto& type : node->schema()) {
            if (comma) {
                stream << ", ";
            }

            stream << type;
            comma = true;
        }
        stream << ")";
    }

    // this wight cause problems with connections
    void generate_create_database(std::stringstream& stream, const node_create_database_ptr& node) {
        stream << "CREATE DATABASE " << node->collection_full_name().database;
    }

    void generate_create_index(std::stringstream& stream, const node_create_index_ptr& node) {
        stream << "CREATE INDEX " << node->name() << " ON " << node->collection_full_name().to_string();
        stream << " (";
        bool comma = false;
        for (const auto& key : node->keys()) {
            if (comma) {
                stream << ", ";
            }

            stream << key.as_string();
            comma = true;
        }
        stream << ")";
    }

    void generate_delete(std::stringstream& stream, const node_delete_ptr& node, const storage_parameters* parameters, backend_type_t backend) {
        node_match_ptr match = nullptr;
        for (const auto& child : node->children()) {
            if (child->type() == node_type::match_t) {
                match = reinterpret_cast<const node_match_ptr&>(child);
            }
        }
        stream << "DELETE FROM ";
        stream << sql_gen::table_reference(node->collection_full_name(), backend);
        if (!node->collection_from().empty()) {
            //! node_delete supports raw_data after using, but it is not possible to send it
            assert(node->collection_full_name().unique_identifier.empty() ||
                   node->collection_full_name().unique_identifier == node->collection_from().unique_identifier);
            stream << " USING " << sql_gen::table_reference(node->collection_from(), backend);
        }
        // WHERE
        if (match) {
            stream << " WHERE ";
            generate_compare_expr(stream,
                                  reinterpret_cast<const compare_expression_ptr&>(match->expressions().front()),
                                  parameters);
        }
    }

    void generate_drop_collection(std::stringstream& stream, const node_drop_collection_ptr& node) {
        stream << "DROP TABLE " << node->collection_full_name().collection;
    }

    // this wight cause problems with connections
    void generate_drop_database(std::stringstream& stream, const node_drop_database_ptr& node) {
        stream << "DROP DATABASE " << node->collection_full_name().database;
    }

    void generate_drop_index(std::stringstream& stream, const node_drop_index_ptr& node) {
        stream << "DROP INDEX IF EXISTS " << node->name() << " ON " << node->collection_full_name().collection;
    }

    void generate_insert(std::stringstream& stream, const node_insert_ptr& node, const storage_parameters* parameters, backend_type_t backend) {
        stream << "INSERT INTO " << sql_gen::table_reference(node->collection_full_name(), backend) << " ";
        if (!node->key_translation().empty()) {
            stream << "(";
            bool comma = false;
            for (const auto& key : node->key_translation()) {
                if (comma) {
                    stream << ", ";
                }
                stream << key.as_string();
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
            assert(node->collection_full_name().unique_identifier ==
                   node->children().front()->collection_full_name().unique_identifier);
            generate_select(stream, reinterpret_cast<const node_aggregate_ptr&>(node->children().front()), parameters, backend);
        }
    }

    void generate_update(std::stringstream& stream, const node_update_ptr& node, const storage_parameters* parameters, backend_type_t backend) {
        node_match_ptr match = nullptr;
        for (const auto& child : node->children()) {
            if (child->type() == node_type::match_t) {
                match = reinterpret_cast<const node_match_ptr&>(child);
            }
        }
        stream << "UPDATE " << sql_gen::table_reference(node->collection_full_name(), backend) << " ";
        bool comma = false;
        for (const auto& set : node->updates()) {
            if (comma) {
                stream << ", ";
            }

            generate_update_expr(stream, set, parameters);
            comma = true;
        }
        if (!node->collection_from().empty()) {
            assert(node->collection_from().unique_identifier.empty() ||
                   node->collection_full_name().unique_identifier == node->collection_from().unique_identifier);
            stream << " FROM " << sql_gen::table_reference(node->collection_from(), backend);
        }
        // WHERE
        if (match) {
            stream << " WHERE ";
            generate_compare_expr(stream,
                                  reinterpret_cast<const compare_expression_ptr&>(match->expressions().front()),
                                  parameters);
        }
    }

} // namespace

namespace sql_gen {

    std::string table_reference(const collection_full_name_t& name, backend_type_t backend) {
        std::stringstream s;
        if (name.empty()) {
            spdlog::debug("table_reference: empty name, returning NonCollectionData");
            return "NonCollectionData";
        }

        spdlog::debug("table_reference: uid={}, db={}, schema={}, table={}, backend={}",
                      name.unique_identifier, name.database, name.schema, name.collection,
                      static_cast<int>(backend));

        switch (backend) {
            case backend_type_t::PostgreSQL:
                // PostgreSQL: schema.collection (e.g., public.products)
                // If schema is empty, use "public" as default
                if (!name.schema.empty()) {
                    s << name.schema << "." << name.collection;
                } else {
                    s << "public." << name.collection;
                }
                break;
            case backend_type_t::MySQL:
            case backend_type_t::Unknown:
            case backend_type_t::Mixed:
            default:
                // MySQL: database.collection
                s << name.database << "." << name.collection;
                break;
        }
        spdlog::debug("table_reference: generated '{}'", s.str());
        return s.str();
    }

    void generate_values(std::stringstream& stream, const components::vector::data_chunk_t& chunk, backend_type_t backend) {
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

    void generate_query(std::stringstream& stream, const node_ptr& node, const storage_parameters* parameters, backend_type_t backend) {
        switch (node->type()) {
            case node_type::aggregate_t:
                generate_select(stream, reinterpret_cast<const node_aggregate_ptr&>(node), parameters, backend);
                break;
            case node_type::create_collection_t:
                generate_create_collection(stream, reinterpret_cast<const node_create_collection_ptr&>(node));
                break;
            case node_type::create_database_t:
                generate_create_database(stream, reinterpret_cast<const node_create_database_ptr&>(node));
                break;
            case node_type::create_index_t:
                generate_create_index(stream, reinterpret_cast<const node_create_index_ptr&>(node));
                break;
            case node_type::delete_t:
                generate_delete(stream, reinterpret_cast<const node_delete_ptr&>(node), parameters, backend);
                break;
            case node_type::drop_collection_t:
                generate_drop_collection(stream, reinterpret_cast<const node_drop_collection_ptr&>(node));
                break;
            case node_type::drop_database_t:
                generate_drop_database(stream, reinterpret_cast<const node_drop_database_ptr&>(node));
                break;
            case node_type::drop_index_t:
                generate_drop_index(stream, reinterpret_cast<const node_drop_index_ptr&>(node));
                break;
            case node_type::insert_t:
                generate_insert(stream, reinterpret_cast<const node_insert_ptr&>(node), parameters, backend);
                break;
            case node_type::update_t:
                generate_update(stream, reinterpret_cast<const node_update_ptr&>(node), parameters, backend);
                break;
            default:
                throw std::logic_error("incorrect node type for generate_query: " + to_string(node->type()));
        }
    }

    std::string generate_query(const node_ptr& node, const storage_parameters* parameters, backend_type_t backend) {
        std::stringstream stream;
        generate_query(stream, node, parameters, backend);
        stream << ";";
        return stream.str();
    }

} // namespace sql_gen