// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "schema_utils.hpp"

#include "utility/tracy_profiler.hpp"

#include <algorithm>

#include <core/result_wrapper.hpp>

using namespace components;
using namespace components::types;

namespace {
    complex_logical_type compute_aggregate(const logical_plan::node_aggregate_t& node,
                                           logical_plan::parameter_node_t* params,
                                           cursor::cursor_t_ptr catalog,
                                           const std::pmr::map<qualified_name_t, size_t>& dependencies) {
        const qualified_name_t lookup_key = schema_utils::agg_key(node);
        if (auto it = dependencies.find(lookup_key); it != dependencies.end()) {
            if (catalog->size() > it->second && catalog->type_data()[it->second].type() == logical_type::STRUCT) {
                const auto& fields = catalog->type_data()[it->second].child_types();
                std::pmr::vector<types::complex_logical_type> types_vec(fields.begin(), fields.end(),
                                                                        node.resource());
                return schema_utils::aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(node),
                                                             params,
                                                             types_vec);
            }
        }
        return logical_type::NA;
    }

    complex_logical_type
    compute_aggregate_or_schema(const logical_plan::node_t& node,
                                logical_plan::parameter_node_t* params,
                                cursor::cursor_t_ptr catalog,
                                const std::pmr::map<qualified_name_t, size_t>& dependencies) {
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
    schema_node_t::schema_node_t(const qualified_name_t& name,
                                 complex_logical_type&& schema,
                                 logical_plan::node_aggregate_t&& agg_node)
        : logical_plan::node_t(agg_node.resource(), logical_plan::node_type::unused)
        , name_(name)
        , schema_(std::move(schema))
        , agg_node_(new components::logical_plan::node_aggregate_t(std::move(agg_node))) {}

    schema_node_t::schema_node_t(std::pmr::memory_resource* resource,
                                 const qualified_name_t& name,
                                 std::string raw_sql,
                                 std::vector<otterstax::parser::qualifier_rewrite_t> qualifiers)
        : logical_plan::node_t(resource, logical_plan::node_type::unused)
        , name_(name)
        , schema_()
        , agg_node_(logical_plan::make_node_aggregate(resource,
                                                      core::uid_t{name.unique_identifier},
                                                      core::dbname_t{name.database},
                                                      core::relname_t{name.collection}))
        , raw_sql_(std::move(raw_sql))
        , qualifiers_(std::move(qualifiers)) {}

    const complex_logical_type& schema_node_t::schema() const { return schema_; }

    const components::logical_plan::node_aggregate_ptr schema_node_t::agg_node() { return agg_node_; }

    expressions::hash_t schema_node_t::hash_impl() const { return 0; }

    std::string schema_node_t::to_string_impl() const { return ""; }

    node_schema_ptr make_node_schema(const qualified_name_t& name,
                                     complex_logical_type&& schema,
                                     logical_plan::node_aggregate_t&& agg_node) {
        return {new schema_node_t(name, std::move(schema), std::move(agg_node))};
    }

    node_schema_ptr make_node_schema_raw(std::pmr::memory_resource* resource,
                                         const qualified_name_t& name,
                                         std::string raw_sql,
                                         std::vector<otterstax::parser::qualifier_rewrite_t> qualifiers) {
        return {new schema_node_t(resource, name, std::move(raw_sql), std::move(qualifiers))};
    }

    complex_logical_type aggregate_filter_schema(const logical_plan::node_aggregate_t& node,
                                                 logical_plan::parameter_node_t* params,
                                                 const std::pmr::vector<complex_logical_type>& schema_types) {
        OTX_ZONE_N("schema_utils::aggregate_filter_schema");
        auto it = std::find_if(node.children().begin(), node.children().end(), [](logical_plan::node_ptr node) {
            return node->type() == logical_plan::node_type::select_t;
        });

        if (it == node.children().end()) {
            // SELECT * case
            return types::complex_logical_type::create_struct("", schema_types);
        }

        std::unordered_map<std::string, std::string> aggregate_fn_by_alias;
        auto group_it = std::find_if(node.children().begin(), node.children().end(), [](logical_plan::node_ptr node) {
            return node->type() == logical_plan::node_type::group_t;
        });

        if (group_it != node.children().end()) {
            for (const auto& gexpr : (*group_it)->expressions()) {
                if (gexpr->group() == expressions::expression_group::aggregate) {
                    auto& agg_expr = static_cast<expressions::aggregate_expression_t&>(*gexpr);
                    aggregate_fn_by_alias.emplace(agg_expr.key().as_string(), agg_expr.function_name());
                }
            }
        }

        // helper: find field type by name in schema_types. Engine LIMIT-0
        // schema probes can yield alias-less columns (null type extension) —
        // alias() on those dereferences null, so they must be skipped.
        auto find_field_type = [&schema_types](const std::string& name) -> complex_logical_type {
            for (const auto& t : schema_types) {
                if (t.has_alias() && t.alias() == name) {
                    return t;
                }
            }
            return logical_type::NA;
        };

        // helper: map alias to aggregate function type
        // TODO: UDF return types
        auto type_for_aggregate_fn = [](const std::string& fn) -> complex_logical_type {
            if (fn == "count" || fn == "sum" || fn == "min" || fn == "max") {
                return types::logical_type::BIGINT;
            }
            if (fn == "avg") {
                return types::logical_type::DOUBLE;
            }
            return logical_type::NA;
        };

        auto& select = *it;
        std::pmr::vector<complex_logical_type> node_schema(node.resource());
        node_schema.reserve(select->expressions().size());

        for (const auto& expr_ptr : select->expressions()) {
            types::complex_logical_type agg;
            if (expr_ptr->group() != components::expressions::expression_group::scalar) {
                continue;
            }

            auto& expr = static_cast<expressions::scalar_expression_t&>(*expr_ptr);
            if (expr.params().size()) {
                // size is either 1 or 0
                auto param_v = expr.params().at(0);
                if (std::holds_alternative<core::parameter_id_t>(param_v)) {
                    auto param_id = std::get<core::parameter_id_t>(std::move(param_v));
                    if (params->parameters().parameters.size() > param_id) {
                        // param is available, will default to logical_type::NA otherwise
                        agg =
                            get_parameter(&params->parameters(), std::get<core::parameter_id_t>(param_v)).type().type();
                    }
                } else if (std::holds_alternative<expressions::key_t>(param_v)) {
                    agg = find_field_type(std::get<expressions::key_t>(param_v).as_string());
                }
            } else {
                auto key = expr.key().as_string();
                agg = find_field_type(key);
                if (agg == logical_type::NA) {
                    // alias-routed aggregate, resolve type via the aggregate's function name.
                    if (auto fn_it = aggregate_fn_by_alias.find(key); fn_it != aggregate_fn_by_alias.end()) {
                        agg = type_for_aggregate_fn(fn_it->second);
                    }
                }
            }

            agg.set_alias(expr.key().as_string());
            node_schema.push_back(std::move(agg));
        }

        return types::complex_logical_type::create_struct("", node_schema);
    }

    cursor::cursor_t_ptr compute_otterbrix_schema(const logical_plan::node_aggregate_t& node,
                                                  logical_plan::parameter_node_t* params,
                                                  cursor::cursor_t_ptr catalog,
                                                  std::pmr::map<qualified_name_t, size_t> dependencies) {
        OTX_ZONE_N("schema_utils::compute_otterbrix_schema");
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
                                       core::error_t(core::error_code_t::schema_error,
                                                     std::pmr::string{("OtterBrix collection is missing in catalog " +
                                                                       node.dbname().t + "." + node.relname().t)
                                                                          .c_str(),
                                                                      node.resource()}));
        }

        std::pmr::vector<types::complex_logical_type> result_types(node.resource());
        result_types.emplace_back(std::move(schema));
        return cursor::make_cursor(node.resource(), std::move(result_types));
    }

    types::complex_logical_type compute_join_schema(const logical_plan::node_join_t& node,
                                                    logical_plan::parameter_node_t* params,
                                                    cursor::cursor_t_ptr catalog,
                                                    const std::pmr::map<qualified_name_t, size_t>& dependencies) {
        OTX_ZONE_N("schema_utils::compute_join_schema");
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

        std::unordered_map<std::string, complex_logical_type> merged;
        for (const auto& column : sch1.child_types()) {
            merged.insert({column.alias(), column});
        }
        for (const auto& column : sch2.child_types()) {
            merged.insert({column.alias(), column});
        }

        std::pmr::vector<complex_logical_type> merged_vector(sch1.child_types().get_allocator());
        merged_vector.reserve(merged.size());
        for (const auto& [_, column] : merged) {
            merged_vector.push_back(column);
        }

        return complex_logical_type::create_struct("", merged_vector);
    }
} // namespace schema_utils
