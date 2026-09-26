// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "discovery.hpp"

#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>

using namespace components;

namespace otterstax::catalog {

    std::pmr::string
    escape_sql_literal(std::pmr::memory_resource* resource, std::string_view value, backend_type_t backend) {
        OTX_ZONE_N("catalog::escape_sql_literal");
        const bool backslash_escapes = backend == backend_type_t::MySQL || backend == backend_type_t::ClickHouse;
        std::pmr::string out{resource};
        out.reserve(value.size() + 2);
        out.push_back('\'');
        for (char c : value) {
            if (c == '\'') {
                out.push_back('\'');
            } else if (c == '\\' && backslash_escapes) {
                out.push_back('\\');
            }
            out.push_back(c);
        }
        out.push_back('\'');
        return out;
    }

    std::pmr::string
    make_list_tables_query(std::pmr::memory_resource* resource, backend_type_t backend, std::string_view scope) {
        OTX_ZONE_N("catalog::make_list_tables_query");
        std::pmr::string query{resource};
        if (backend == backend_type_t::ClickHouse) {
            query.append("SELECT name FROM system.tables WHERE database = ");
            query.append(escape_sql_literal(resource, scope, backend));
            return query;
        }
        query.append("SELECT table_name FROM information_schema.tables WHERE table_schema = ");
        query.append(escape_sql_literal(resource, scope, backend));
        query.append(" AND table_type = 'BASE TABLE';");
        return query;
    }

    std::pmr::string
    make_named_types_query(std::pmr::memory_resource* resource, std::string_view database, std::string_view table) {
        OTX_ZONE_N("catalog::make_named_types_query");
        std::pmr::string query{resource};
        query.append("SELECT name, type FROM system.columns WHERE database = ");
        query.append(escape_sql_literal(resource, database, backend_type_t::ClickHouse));
        query.append(" AND table = ");
        query.append(escape_sql_literal(resource, table, backend_type_t::ClickHouse));
        return query;
    }

    core::result_wrapper_t<std::string>
    make_schema_probe_query(std::pmr::memory_resource* resource, const qualified_name_t& name, backend_type_t backend) {
        OTX_ZONE_N("catalog::make_schema_probe_query");
        logical_plan::parameter_node_t param(resource);
        auto node = logical_plan::make_node_aggregate(resource,
                                                      core::uid_t{name.unique_identifier},
                                                      core::dbname_t{name.database},
                                                      core::relname_t{name.collection});
        node->append_child(logical_plan::make_node_match(
            resource,
            core::dbname_t{name.database},
            core::relname_t{name.collection},
            expressions::make_compare_expression(resource,
                                                 expressions::compare_type::eq,
                                                 param.add_parameter(types::logical_value_t(resource, 1)),
                                                 param.add_parameter(types::logical_value_t(resource, 0)))));

        otterstax::names::resolved_target_t probe_target{components::catalog::INVALID_OID, name, {}};
        std::pmr::vector<external_entry_t> empty_batch{resource};
        return sql_gen::generate_query(node, &param.parameters(), backend, probe_target, empty_batch, resource);
    }

    core::error_t make_discovery_error(std::pmr::memory_resource* resource,
                                       const std::pmr::vector<std::pmr::string>& failed_tables) {
        OTX_ZONE_N("catalog::make_discovery_error");
        constexpr size_t max_named = 3;
        std::pmr::string msg{resource};
        msg.append("Schema discovery failed for ");
        msg.append(std::to_string(failed_tables.size()).c_str());
        msg.append(" table(s): ");
        for (size_t i = 0; i < failed_tables.size() && i < max_named; ++i) {
            if (i != 0) {
                msg.append(", ");
            }
            msg.append(failed_tables[i]);
        }
        if (failed_tables.size() > max_named) {
            msg.append(", ...");
        }
        return core::error_t(core::error_code_t::schema_error, std::move(msg));
    }

} // namespace otterstax::catalog
