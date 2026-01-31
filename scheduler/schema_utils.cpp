// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "schema_utils.hpp"

using namespace components;
using namespace components::types;

namespace {
    complex_logical_type compute_aggregate(const logical_plan::node_aggregate_t& node,
                                           logical_plan::parameter_node_t* params,
                                           cursor::cursor_t_ptr catalog,
                                           const std::pmr::map<collection_full_name_t, size_t>& dependencies) {
        if (auto it = dependencies.find(node.collection_full_name()); it != dependencies.end()) {
            if (catalog->size() > it->second && catalog->type_data()[it->second].type() == logical_type::STRUCT) {
                return schema_utils::aggregate_filter_schema(
                    static_cast<const logical_plan::node_aggregate_t&>(node),
                    params,
                    catalog::schema(node.resource(), catalog->type_data()[it->second]));
            }
        }
        return logical_type::NA;
    }

    complex_logical_type
    compute_aggregate_or_schema(const logical_plan::node_t& node,
                                logical_plan::parameter_node_t* params,
                                cursor::cursor_t_ptr catalog,
                                const std::pmr::map<collection_full_name_t, size_t>& dependencies) {
        switch (node.type()) {
            case logical_plan::node_type::unused: {
                auto& schema = static_cast<const schema_utils::schema_node_t&>(node);
                return schema.schema();
            }
            case logical_plan::node_type::aggregate_t:
                return compute_aggregate(static_cast<const logical_plan::node_aggregate_t&>(node),
                                         params,
                                         std::move(catalog),
                                         dependencies);
            default:
                return logical_type::NA;
        }
    }
} // namespace

namespace schema_utils {
    schema_node_t::schema_node_t(const collection_full_name_t& name,
                                 complex_logical_type&& schema,
                                 logical_plan::node_aggregate_t&& agg_node)
        : logical_plan::node_t(agg_node.resource(), logical_plan::node_type::unused, name)
        , schema_(std::move(schema))
        , agg_node_(new components::logical_plan::node_aggregate_t(std::move(agg_node))) {}

    const complex_logical_type& schema_node_t::schema() const { return schema_; }

    const components::logical_plan::node_aggregate_ptr schema_node_t::agg_node() { return agg_node_; }

    expressions::hash_t schema_node_t::hash_impl() const { return 0; }

    std::string schema_node_t::to_string_impl() const { return ""; }

    void schema_node_t::serialize_impl(serializer::msgpack_serializer_t*) const {}

    node_schema_ptr make_node_schema(const collection_full_name_t& name,
                                     complex_logical_type&& schema,
                                     logical_plan::node_aggregate_t&& agg_node) {
        return {new schema_node_t(name, std::move(schema), std::move(agg_node))};
    }

    complex_logical_type aggregate_filter_schema(const logical_plan::node_aggregate_t& node,
                                                 logical_plan::parameter_node_t* params,
                                                 const catalog::schema& schema) {
        auto it = std::find_if(node.children().begin(), node.children().end(), [](logical_plan::node_ptr node) {
            return node->type() == logical_plan::node_type::group_t;
        });

        if (it == node.children().end()) {
            // SELECT * case
            return schema.schema_struct();
        }

        auto& group = *it;
        std::vector<complex_logical_type> node_schema;
        node_schema.reserve(group->expressions().size());

        for (const auto& expr_ptr : group->expressions()) {
            types::complex_logical_type agg;
            switch (expr_ptr->group()) {
                case expressions::expression_group::aggregate: {
                    auto& expr = static_cast<expressions::aggregate_expression_t&>(*expr_ptr);
                    switch (expr.type()) {
                        case expressions::aggregate_type::count:
                        case expressions::aggregate_type::sum:
                        case expressions::aggregate_type::min:
                        case expressions::aggregate_type::max:
                            agg = types::logical_type::BIGINT;
                            break;
                        case expressions::aggregate_type::avg:
                            agg = types::logical_type::DOUBLE;
                            break;
                        case expressions::aggregate_type::invalid:
                            break;
                    }
                    agg.set_alias(expr.key().as_string());
                    break;
                }
                case expressions::expression_group::scalar: {
                    auto& expr = static_cast<expressions::scalar_expression_t&>(*expr_ptr);
                    if (expr.params().size()) {
                        // size is either 1 or 0
                        auto param_v = expr.params().at(0);
                        if (std::holds_alternative<core::parameter_id_t>(param_v)) {
                            auto param_id = std::get<core::parameter_id_t>(std::move(param_v));
                            if (params->parameters().parameters.size() > param_id) {
                                // param is available, will default to logical_type::NA otherwise
                                agg = get_parameter(&params->parameters(), std::get<core::parameter_id_t>(param_v))
                                          .type()
                                          .type();
                            }
                        } else if (std::holds_alternative<expressions::key_t>(param_v)) {
                            // can only be holding name of column
                            auto cur = schema.find_field(std::get<expressions::key_t>(param_v).as_string().c_str());
                            if (cur->is_success()) {
                                agg = cur->type_data().front();
                            }
                        }
                    } else {
                        // otherwise key is a name of column
                        auto cur = schema.find_field(expr.key().as_string().c_str());
                        if (cur->is_success()) {
                            agg = cur->type_data().front();
                        }
                    }
                    agg.set_alias(expr.key().as_string());
                    break;
                }
                default: {
                    continue; // do nothing
                }
            }
            node_schema.push_back(std::move(agg));
        }

        return types::complex_logical_type::create_struct(node_schema);
    }

    cursor::cursor_t_ptr compute_otterbrix_schema(const logical_plan::node_aggregate_t& node,
                                                  logical_plan::parameter_node_t* params,
                                                  cursor::cursor_t_ptr catalog,
                                                  std::pmr::map<collection_full_name_t, size_t> dependencies) {
        bool has_join = false;
        complex_logical_type schema;
        for (const auto& chld : node.children()) {
            if (chld->type() == logical_plan::node_type::join_t) {
                has_join = true;
                schema = compute_join_schema(static_cast<logical_plan::node_join_t&>(*chld),
                                             params,
                                             std::move(catalog),
                                             dependencies);
            }
        }

        // didn't include join -> get aggregate node schema
        if (!has_join) {
            schema = compute_aggregate(node, params, std::move(catalog), dependencies);
        }

        if (schema.type() == logical_type::NA) {
            return cursor::make_cursor(node.resource(),
                                       cursor::error_code_t::schema_error,
                                       "OtterBrix collection is missing in catalog " +
                                           node.collection_full_name().to_string());
        }

        return cursor::make_cursor(node.resource(), {std::move(schema)});
    }

    types::complex_logical_type compute_join_schema(const logical_plan::node_join_t& node,
                                                    logical_plan::parameter_node_t* params,
                                                    cursor::cursor_t_ptr catalog,
                                                    const std::pmr::map<collection_full_name_t, size_t>& dependencies) {
        assert(node.children().size() == 2);

        if (node.children().front()->type() == logical_plan::node_type::join_t) {
            // recursion
            auto right = compute_aggregate_or_schema(*node.children().back(), params, catalog, dependencies);
            auto left = compute_join_schema(static_cast<const logical_plan::node_join_t&>(*node.children().front()),
                                            params,
                                            catalog,
                                            dependencies);
            return merge_schemas(right, left);
        }

        // leaf
        auto right = compute_aggregate_or_schema(*node.children().front(), params, catalog, dependencies);
        auto left = compute_aggregate_or_schema(*node.children().back(), params, catalog, dependencies);
        return merge_schemas(right, left);
    }

    complex_logical_type merge_schemas(const complex_logical_type& sch1, const complex_logical_type& sch2) {
        if (sch1.type() != sch2.type() || sch1.type() != logical_type::STRUCT) {
            return logical_type::NA;
        }

        auto& struct1 = catalog::to_struct(sch1);
        auto& struct2 = catalog::to_struct(sch2);
        std::unordered_map<std::string, complex_logical_type> merged;
        for (const auto& column : struct1.child_types()) {
            merged.insert({column.alias(), column});
        }
        for (const auto& column : struct2.child_types()) {
            merged.insert({column.alias(), column});
        }

        std::vector<complex_logical_type> merged_vector;
        merged_vector.reserve(merged.size());
        for (const auto& [_, column] : merged) {
            merged_vector.push_back(column);
        }

        return complex_logical_type::create_struct(merged_vector);
    }
} // namespace schema_utils
