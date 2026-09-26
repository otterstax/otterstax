// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/parser.hpp"
#include "utility/table_info.hpp"

#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>

// Types and query builders shared by the catalog (which owns the mirror of
// the discovered tables) and the three backend integration actors (which own
// the connector managers and run the discovery queries).
namespace catalog_ext {

    enum class ConnectionType : uint8_t
    {
        MySQL = 0,
        PostgreSQL = 1,
        ClickHouse = 2
    };

    // One table discovered on a remote backend: full qualified name plus the
    // STRUCT of column types produced by the input translators.
    struct discovered_table_t {
        qualified_name_t name;
        components::types::complex_logical_type schema;
    };

    using discovered_tables_t = std::pmr::vector<discovered_table_t>;

} // namespace catalog_ext

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
    // schema or the ClickHouse database respectively.
    std::pmr::string
    make_list_tables_query(std::pmr::memory_resource* resource, backend_type_t backend, std::string_view scope);

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
    // error naming the failure count and the first few tables.
    core::error_t make_discovery_error(std::pmr::memory_resource* resource,
                                       const std::pmr::vector<std::pmr::string>& failed_tables);

} // namespace otterstax::catalog
