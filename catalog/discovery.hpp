// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/federation/provider.hpp"
#include "otterbrix/parser/parser.hpp"
#include "utility/table_info.hpp"

#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <algorithm>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

// Query builders of schema discovery, shared by the three backend
// integration classes (which own the connector managers and run the discovery
// queries) and the writers of the federation metadata (remote_metadata.hpp),
// which turn a discovery reply into rows of federation.remote_tables /
// remote_columns.
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

    // The primary key columns of the PostgreSQL tables of `schema` (of the one
    // table `table` when it is not empty): rows of (relname, attname), in key
    // order per table (pg_index.indisprimary; a partitioned table's key is its
    // parent's). MySQL needs no query — the schema probe's column definitions
    // carry PRI_KEY_FLAG — and a ClickHouse table has no key that names a row.
    std::pmr::string make_primary_keys_query(std::pmr::memory_resource* resource,
                                             std::string_view schema,
                                             std::string_view table);

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
