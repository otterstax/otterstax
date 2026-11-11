// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/backend_type.hpp"
#include "utility/table_info.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <algorithm>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

// Query builders of schema discovery, used by the backend agents
// (otterbrix/federation/backend_agent.hpp), which run the discovery queries.
// What a discovery finds is registered in otterbrix's catalog by
// catalog/registration.hpp.
namespace otterstax::catalog {
    // Single-quoted SQL string literal for the handwritten discovery queries
    // (information_schema / system.tables / system.columns). The apostrophe is
    // doubled everywhere; MySQL (default sql_mode) and ClickHouse additionally
    // treat a backslash as an escape character inside the literal, so it is
    // doubled for them too — otherwise `\'` would close the literal early.
    std::pmr::string
    escape_sql_literal(std::pmr::memory_resource* resource, std::string_view value, backend_type_t backend);

    // Whole-database table listing per dialect — the only handwritten queries
    // besides the named-types one; every per-table probe goes through
    // sql_gen::generate_query. `scope` is the MySQL database, the PostgreSQL
    // schema or the ClickHouse database respectively. MySQL / ClickHouse answer
    // one column (the table); PostgreSQL two (table_schema, table_name), and an
    // empty `scope` lists every user schema (not pg_catalog, information_schema,
    // pg_toast*, pg_temp*).
    std::pmr::string
    make_list_tables_query(std::pmr::memory_resource* resource, backend_type_t backend, std::string_view scope);

    // The user databases of a MySQL or ClickHouse server, one column of names: every database but the server's
    // own (MySQL information_schema, mysql, performance_schema, sys; ClickHouse system, information_schema).
    std::pmr::string make_list_databases_query(std::pmr::memory_resource* resource, backend_type_t backend);

    // ClickHouse column type strings of one table (system.columns), the source
    // of the named-type overrides (Tuple(...) with field names) that the
    // native protocol's block headers do not carry.
    std::pmr::string
    make_named_types_query(std::pmr::memory_resource* resource, std::string_view database, std::string_view table);

    // Backend-dialect schema probe (SELECT * FROM <table> WHERE 1 = 0) built
    // through the regular plan-driven generator — the single quoting point.
    // The always-false predicate is two bound parameters, so no fake column
    // identifier appears in the generated SQL.
    core::result_wrapper_t<std::string>
    make_schema_probe_query(std::pmr::memory_resource* resource, const qualified_name_t& name, backend_type_t backend);

    // Per-table discovery failures are collected and folded into one hard
    // error naming the failure count and the first few tables
    // (undefined_object).
    core::error_t make_discovery_error(std::pmr::memory_resource* resource,
                                       const std::pmr::vector<std::pmr::string>& failed_tables);
} // namespace otterstax::catalog
