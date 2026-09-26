// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "schema_utils.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/expressions/function_expression.hpp>
#include <core/result_wrapper.hpp>

#include <algorithm>

using namespace components;
using namespace components::types;

namespace {
    // The engine transformer's own output names, from
    // components/sql/transformer/impl/transfrom_common.cpp.
    constexpr std::string_view hidden_having_prefix = "__having_";
    constexpr std::string_view hidden_group_key_prefix = "__group_key_";

    // A grouping-key marker names the key the rows are grouped by, not an output
    // column. A key that is also selected gets a projected copy of its own next
    // to the marker, and it is that copy the answer carries.
    bool is_group_marker(const expressions::expression_i& expr) {
        return expr.group() == expressions::expression_group::scalar &&
               static_cast<const expressions::scalar_expression_t&>(expr).type() ==
                   expressions::scalar_type::group_field;
    }

    // Aliases the transformer invents for a grouping key hoisted into HAVING /
    // ORDER BY and for a HAVING-only aggregate. They exist only inside the
    // engine: the generated statement writes them inline where HAVING or ORDER
    // BY references them and never projects them, so they are no column of the
    // result either.
    bool is_hidden_alias(const expressions::key_t& key) {
        if (key.storage().size() != 1) {
            return false;
        }
        const auto& part = key.storage().front();
        const std::string_view name{part.data(), part.size()};
        return name.starts_with(hidden_having_prefix) || name.starts_with(hidden_group_key_prefix);
    }

    complex_logical_type compute_aggregate(const logical_plan::node_aggregate_t& node,
                                           logical_plan::parameter_node_t* params,
                                           cursor::cursor_t_ptr catalog,
                                           const std::pmr::map<qualified_name_t, size_t>& dependencies) {
        // A stub child IS this aggregate's input relation: the statement's FROM
        // is a federated sub-query, whose columns the backend described onto the
        // stub (ClickhouseManager::describe). The aggregate itself names no table
        // then — it is the transformer's unnamed derived-table wrapper — so the
        // catalog lookup below has nothing to answer with and the projection is
        // resolved against the described columns instead. A stub no backend
        // described carries no STRUCT and is left to that lookup, unchanged.
        for (const auto& child : node.children()) {
            if (child->type() != logical_plan::node_type::unused) {
                continue;
            }
            const auto& stub = static_cast<const schema_utils::schema_node_t&>(*child);
            if (stub.schema().type() != logical_type::STRUCT) {
                continue;
            }
            const auto& fields = stub.schema().child_types();
            std::pmr::vector<types::complex_logical_type> types_vec(fields.begin(), fields.end(), node.resource());
            return schema_utils::aggregate_filter_schema(node, params, types_vec);
        }

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
        , agg_node_(new components::logical_plan::node_aggregate_t(std::move(agg_node)))
        , qualifiers_(resource()) {}

    schema_node_t::schema_node_t(std::pmr::memory_resource* resource,
                                 const qualified_name_t& name,
                                 std::string_view raw_sql,
                                 const std::pmr::vector<otterstax::parser::qualifier_rewrite_t>& qualifiers)
        : logical_plan::node_t(resource, logical_plan::node_type::unused)
        , name_(name)
        , schema_()
        , agg_node_(logical_plan::make_node_aggregate(resource,
                                                      core::uid_t{name.unique_identifier},
                                                      core::dbname_t{name.database},
                                                      core::relname_t{name.collection}))
        , raw_sql_(std::pmr::string{raw_sql, resource})
        , qualifiers_(qualifiers, resource) {}

    const complex_logical_type& schema_node_t::schema() const { return schema_; }

    void schema_node_t::set_schema(complex_logical_type&& schema) { schema_ = std::move(schema); }

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
                                         std::string_view raw_sql,
                                         const std::pmr::vector<otterstax::parser::qualifier_rewrite_t>& qualifiers) {
        return {new schema_node_t(resource, name, raw_sql, qualifiers)};
    }

    complex_logical_type aggregate_filter_schema(const logical_plan::node_aggregate_t& node,
                                                 logical_plan::parameter_node_t* params,
                                                 const std::pmr::vector<complex_logical_type>& schema_types) {
        OTX_ZONE_N("schema_utils::aggregate_filter_schema");
        // The whole SELECT list lives on the group node: the transformer moves
        // every projected expression there — that is where an aggregate has to be
        // resolved — and leaves the select node holding ordinal references back
        // to those outputs. This is the same list the pushed-down statement is
        // generated from (otterbrix/query_generation), so the schema names
        // exactly the columns the backend is asked for.
        const logical_plan::node_t* group = nullptr;
        const logical_plan::node_t* select = nullptr;
        for (const auto& child : node.children()) {
            if (child->type() == logical_plan::node_type::group_t) {
                group = child.get();
            } else if (child->type() == logical_plan::node_type::select_t) {
                select = child.get();
            }
        }
        const logical_plan::node_t* projection = group != nullptr ? group : select;

        if (projection == nullptr) {
            // SELECT * case
            return types::complex_logical_type::create_struct("", schema_types);
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

        // helper: map a call to its output type
        // TODO: UDF return types
        auto type_for_call = [&find_field_type](const std::string& fn,
                                                const std::pmr::vector<expressions::param_storage>& operands)
            -> complex_logical_type {
            if (fn == "min" || fn == "max") {
                // MIN / MAX answer their argument column's type — the engine's kernels are
                // registered output_type::computed(same_type_resolver(0))
                // (components/compute/kernels/aggregate.cpp:462, :481), and every backend
                // agrees, MIN / MAX being the aggregates that promote nothing. An argument
                // this computation cannot resolve to one of the input columns (an
                // expression, a column outside the schema) leaves the output type unknown,
                // and NA is how the schema says so — the same answer a projected cast or
                // comparison gets below, rather than a BIGINT nothing promised.
                if (operands.size() == 1 && std::holds_alternative<expressions::key_t>(operands.front())) {
                    return find_field_type(std::get<expressions::key_t>(operands.front()).as_string());
                }
                return logical_type::NA;
            }
            if (fn == "count") {
                // COUNT is the one aggregate whose kernel type is fixed rather than derived
                // from its argument: UBIGINT, for COUNT(*) and COUNT(x) alike
                // (aggregate.cpp:503 and :508, output_type::fixed(UBIGINT), with
                // count_finalize at :401 writing through output.data<uint64_t>()). It is
                // also the one kernel type that holds for the backends this same
                // computation types statements for (CatalogManager::get_catalog_schema): a
                // count is an unsigned 64-bit number on MySQL (BIGINT UNSIGNED, which
                // mysql_to_chunk reads back as UBIGINT) and on ClickHouse (UInt64), and
                // PostgreSQL's signed int8 carries the same non-negative value in the same
                // width, so no wire is told a type that cannot hold what arrives.
                return types::logical_type::UBIGINT;
            }
            // SUM and AVG are deliberately NOT the engine's answer. Both kernels are
            // output_type::computed(same_type_resolver(0)) as well (aggregate.cpp:443,
            // :527): the engine sums and averages IN the argument's own type, so SUM over
            // an INTEGER column is INTEGER and AVG over one is integer division. No backend
            // agrees — MySQL's AVG is DOUBLE and its SUM of an integer column DECIMAL,
            // PostgreSQL's sum(int4) is int8 and avg(int4) numeric — and this one
            // computation has no way to tell which executor it is typing for. Narrowing
            // them to the kernel rule would trade today's mismatch on the local engine for
            // a worse one on every remote path: an INTEGER announced where a DOUBLE or an
            // int8 arrives differs in width AND kind, where COUNT above differs only in
            // signedness over the same 64-bit non-negative value. Until the type comes from
            // whoever executes the statement — a prepare-time probe like
            // ClickhouseManager::describe, or the backend threaded in here — these two stay
            // the answer the remote paths were built on.
            if (fn == "sum") {
                return types::logical_type::BIGINT;
            }
            if (fn == "avg") {
                return types::logical_type::DOUBLE;
            }
            return logical_type::NA;
        };

        std::pmr::vector<complex_logical_type> node_schema(node.resource());
        node_schema.reserve(projection->expressions().size());

        for (const auto& expr_ptr : projection->expressions()) {
            if (!expr_ptr || is_group_marker(*expr_ptr) || is_hidden_alias(expr_ptr->key())) {
                continue;
            }

            types::complex_logical_type agg;
            // A call IS the projected expression, so it is typed as the call even when its alias is
            // the name of a source column (`AVG(x) AS x`), whose type is not the call's. The
            // transformer cannot tell an aggregate from a scalar function, so a SELECT-list call is
            // routed into the group as a function call; only one it resolved for HAVING is already
            // an aggregate.
            if (expr_ptr->group() == expressions::expression_group::function) {
                const auto& call = static_cast<const expressions::function_expression_t&>(*expr_ptr);
                agg = type_for_call(call.name(), call.args());
            } else if (expr_ptr->group() == expressions::expression_group::aggregate) {
                const auto& call = static_cast<const expressions::aggregate_expression_t&>(*expr_ptr);
                agg = type_for_call(call.function_name(), call.params());
            } else if (expr_ptr->group() == expressions::expression_group::scalar) {
                const auto& expr = static_cast<const expressions::scalar_expression_t&>(*expr_ptr);
                if (expr.params().size()) {
                    // size is either 1 or 0
                    const auto& param_v = expr.params().at(0);
                    if (std::holds_alternative<core::parameter_id_t>(param_v)) {
                        auto param_id = std::get<core::parameter_id_t>(param_v);
                        if (params->parameters().parameters.size() > param_id) {
                            // param is available, will default to logical_type::NA otherwise
                            agg = get_parameter(&params->parameters(), param_id).type().type();
                        }
                    } else if (std::holds_alternative<expressions::key_t>(param_v)) {
                        agg = find_field_type(std::get<expressions::key_t>(param_v).as_string());
                    }
                } else {
                    agg = find_field_type(expr.key().as_string());
                }
            }
            // Every other projected expression — a cast, a comparison written as a value — is a
            // column of the answer just the same, under the name it is projected by. Its type is
            // none this computation can name, and NA is how the schema says so.

            agg.set_alias(expr_ptr->key().as_string());
            node_schema.push_back(std::move(agg));
        }

        if (node_schema.empty()) {
            // Only engine-internal outputs were projected, so the generated statement projects `*`:
            // the answer is the whole input schema, exactly as for a select-less aggregate.
            return types::complex_logical_type::create_struct("", schema_types);
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

        // A join node's children ARE its two inputs in SQL order: the transformer
        // appends the left side first and the right side second, and for a
        // left-deep tree the inner join node is the one it appends first
        // (components/sql/transformer/impl/transform_select.cpp, join_dfs). Since
        // merge_schemas keeps the order of what it is given, each side has to be
        // passed as the side it is.
        if (node.children().front()->type() == logical_plan::node_type::join_t) {
            // recursion: the left side is itself a join
            auto left = compute_join_schema(static_cast<const logical_plan::node_join_t&>(*node.children().front()),
                                            params,
                                            catalog,
                                            dependencies);
            auto right = compute_aggregate_or_schema(*node.children().back(), params, catalog, dependencies);
            return merge_schemas(left, right);
        }

        // leaf
        auto left = compute_aggregate_or_schema(*node.children().front(), params, catalog, dependencies);
        auto right = compute_aggregate_or_schema(*node.children().back(), params, catalog, dependencies);
        return merge_schemas(left, right);
    }

    complex_logical_type merge_schemas(const complex_logical_type& left, const complex_logical_type& right) {
        OTX_ZONE_N("schema_utils::merge_schemas");
        if (left.type() != right.type() || left.type() != logical_type::STRUCT) {
            return logical_type::NA;
        }

        std::pmr::vector<complex_logical_type> merged(left.child_types().get_allocator());
        merged.reserve(left.child_types().size() + right.child_types().size());

        // The order of the columns IS a contract with the client: it is the order
        // a RowDescription names them in, and a client decodes the row by those
        // positions. So the merge keeps the order SQL itself would name them in —
        // the left side's columns in their own order, then the right side's — and
        // never the order a hash table happened to hold them in.
        //
        // A name the merge has already taken is not taken again, and keeps the
        // type it was taken with: this merge is BY NAME (see the note on
        // compute_join_schema), so the columns of a JOIN whose sides share a name
        // are one column here while the executor answers both. That is why a
        // described shape is checked against the executed one before any row goes
        // out (postgres_resultset's same_wire_shape).
        auto take = [&merged](const std::pmr::vector<complex_logical_type>& columns) {
            for (const auto& column : columns) {
                const std::string& name = column.alias();
                const bool taken = std::any_of(merged.begin(),
                                               merged.end(),
                                               [&name](const complex_logical_type& kept) {
                                                   return kept.alias() == name;
                                               });
                if (!taken) {
                    merged.push_back(column);
                }
            }
        };
        take(left.child_types());
        take(right.child_types());

        return complex_logical_type::create_struct("", merged);
    }
} // namespace schema_utils
