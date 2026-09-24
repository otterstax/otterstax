// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/subquery_extractor.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace schema_utils {
    // Dependency-map key for an aggregate node. Both the map build
    // (Scheduler::prepare_schema) and the lookup (compute_otterbrix_schema)
    // must construct keys through this helper so they compare equal. The
    // schema part is deliberately empty on both sides: the a13 transformer
    // folds any schema qualifier into dbname, so the aggregate carries only
    // (uid, dbname, relname).
    inline qualified_name_t agg_key(const components::logical_plan::node_aggregate_t& node) {
        return qualified_name_t{node.uid().t, node.dbname().t, schema_name_t{}, node.relname().t};
    }

    // used during schema computation, replaces external nodes in main node (like node_raw_data does during execute())
    // generate query during schema analysis
    class schema_node_t final : public components::logical_plan::node_t {
    public:
        explicit schema_node_t(const qualified_name_t& name,
                               components::types::complex_logical_type&& schema,
                               components::logical_plan::node_aggregate_t&& agg_node);

        // raw_sql and qualifiers are copied into `resource`.
        schema_node_t(std::pmr::memory_resource* resource,
                      const qualified_name_t& name,
                      std::string_view raw_sql,
                      const std::pmr::vector<otterstax::parser::qualifier_rewrite_t>& qualifiers);

        const components::types::complex_logical_type& schema() const;
        // The schema a backend answered for this stub (ClickhouseManager::describe).
        // Filled in place: a raw-SQL stub must keep the text and qualifiers the
        // execute path still generates its statement from, so the node is never
        // rebuilt to carry a schema.
        void set_schema(components::types::complex_logical_type&& schema);
        const components::logical_plan::node_aggregate_ptr agg_node();

        // Qualified name of the external table/subquery this node stands in
        // for; node_t no longer carries a name, so the parser-resolved name
        // is stored here.
        const qualified_name_t& name() const noexcept { return name_; }

        bool has_raw_sql() const noexcept { return raw_sql_.has_value(); }
        const std::pmr::string& raw_sql() const noexcept { return *raw_sql_; }
        const std::pmr::vector<otterstax::parser::qualifier_rewrite_t>& qualifiers() const noexcept {
            return qualifiers_;
        }

    private:
        components::expressions::hash_t hash_impl() const final;
        std::string to_string_impl() const final;

        qualified_name_t name_;
        components::types::complex_logical_type schema_;
        components::logical_plan::node_aggregate_ptr agg_node_;
        std::optional<std::pmr::string> raw_sql_;
        std::pmr::vector<otterstax::parser::qualifier_rewrite_t> qualifiers_;
    };

    using node_schema_ptr = boost::intrusive_ptr<schema_node_t>;

    node_schema_ptr make_node_schema(const qualified_name_t& name,
                                     components::types::complex_logical_type&& schema,
                                     components::logical_plan::node_aggregate_t&& agg_node);

    node_schema_ptr make_node_schema_raw(std::pmr::memory_resource* resource,
                                         const qualified_name_t& name,
                                         std::string_view raw_sql,
                                         const std::pmr::vector<otterstax::parser::qualifier_rewrite_t>& qualifiers);

    // in terms of relational algebra - do projection, rename and aggregation of schema
    components::types::complex_logical_type
    aggregate_filter_schema(const components::logical_plan::node_aggregate_t& node,
                            components::logical_plan::parameter_node_t* params,
                            const std::pmr::vector<components::types::complex_logical_type>& schema_types);

    components::cursor::cursor_t_ptr
    compute_otterbrix_schema(const components::logical_plan::node_aggregate_t& node,
                             components::logical_plan::parameter_node_t* params,
                             components::cursor::cursor_t_ptr catalog,
                             std::pmr::map<qualified_name_t, size_t> dependencies);

    components::types::complex_logical_type
    compute_join_schema(const components::logical_plan::node_join_t& node,
                        components::logical_plan::parameter_node_t* params,
                        components::cursor::cursor_t_ptr catalog,
                        const std::pmr::map<qualified_name_t, size_t>& dependencies);

    // The two sides of a join as one schema: the left side's columns in their own
    // order, then the right side's, a name already taken not taken twice. The
    // order is what a frontend's RowDescription names and a client decodes by, so
    // the arguments are the two sides as they are — swapping them is a different
    // answer.
    components::types::complex_logical_type merge_schemas(const components::types::complex_logical_type& left,
                                                          const components::types::complex_logical_type& right);
} // namespace schema_utils
