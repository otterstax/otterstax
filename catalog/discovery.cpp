// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "discovery.hpp"

#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/vector_builders.hpp>

#include <cstdint>

using namespace components;

namespace otterstax::catalog {
    std::pmr::string
    escape_sql_literal(std::pmr::memory_resource* resource, std::string_view value, backend_type_t backend) {
        OTX_ZONE_N("catalog::escape_sql_literal");
        // A PostgreSQL value holding a backslash is an escape string constant, E'...', which reads
        // the backslash as an escape whatever standard_conforming_strings says (sql_gen's
        // write_string_literal writes the same).
        const bool backslash_escapes = backend == backend_type_t::MySQL || backend == backend_type_t::ClickHouse ||
                                       value.find('\\') != std::string_view::npos;
        std::pmr::string out{resource};
        out.reserve(value.size() + 3);
        if (backslash_escapes && backend == backend_type_t::PostgreSQL) {
            out.push_back('E');
        }
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
        if (backend == backend_type_t::PostgreSQL) {
            // (schema, table): one schema, or every user schema of the database.
            query.append(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' "
                "AND ");
            if (scope.empty()) {
                query.append("table_schema NOT IN ('pg_catalog', 'information_schema') AND table_schema NOT LIKE "
                             "E'pg\\\\_toast%' AND table_schema NOT LIKE E'pg\\\\_temp%'");
            } else {
                query.append("table_schema = ");
                query.append(escape_sql_literal(resource, scope, backend));
            }
            query.append(" ORDER BY table_schema, table_name;");
            return query;
        }
        query.append("SELECT table_name FROM information_schema.tables WHERE table_schema = ");
        query.append(escape_sql_literal(resource, scope, backend));
        query.append(" AND table_type = 'BASE TABLE';");
        return query;
    }

    std::pmr::string make_list_databases_query(std::pmr::memory_resource* resource, backend_type_t backend) {
        OTX_ZONE_N("catalog::make_list_databases_query");
        if (backend == backend_type_t::ClickHouse) {
            return std::pmr::string{"SELECT name FROM system.databases WHERE name NOT IN ('system', "
                                    "'information_schema', 'INFORMATION_SCHEMA') ORDER BY name;",
                                    resource};
        }
        return std::pmr::string{"SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN "
                                "('information_schema', 'mysql', 'performance_schema', 'sys') ORDER BY schema_name;",
                                resource};
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
        auto node = logical_plan::make_node_aggregate(
            resource,
            qualified_name_t{name.connection, name.database, core::schema_t{}, name.collection});
        const auto one =
            param.add_parameter(vector::make_scalar(resource, std::int64_t{1}), vector::parameter_kind::literal);
        const auto zero =
            param.add_parameter(vector::make_scalar(resource, std::int64_t{0}), vector::parameter_kind::literal);
        node->append_child(logical_plan::make_node_match(
            resource,
            qualified_name_t{name.database, core::schema_t{}, name.collection},
            expressions::make_compare_expression(resource, expressions::compare_type::eq, one, zero)));

        sql_gen::statement_target_t probe_target{name};
        return sql_gen::generate_query(node,
                                       &param.parameters(),
                                       backend,
                                       probe_target,
                                       resource,
                                       sql_gen::generation_options_t{.validated = false});
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
        return core::error_t(core::error_code_t::undefined_object, std::move(msg));
    }
} // namespace otterstax::catalog
