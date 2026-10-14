// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utility/table_info.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string_view>
#include <vector>

// What a client may list: the engine's own tables (pg_catalog.pg_class / pg_namespace /
// pg_attribute) and the remote tables the federation knows (federation.remote_tables /
// remote_columns), as one listing. The Flight SQL frontend runs the five reads below through a
// query session and hands their rows here; nothing in this file does I/O.
//
// Hidden: the system rows and namespaces, the federation metadata tables themselves
// (`federation.*`), and the Kafka bookkeeping tables (`kafka.__sources`, `kafka.<source>__offsets`).
namespace otterstax::catalog {

    // The one Flight SQL table_type OtterStax lists everything under.
    inline constexpr std::string_view table_type_name = "TABLE";

    // What a Flight SQL CommandGetTables asks of the listing, decoupled from any wire-protocol
    // library. Its filters are applied by the frontend over the listing.
    struct get_tables_command_t {
        bool include_schema = false;
    };

    // The reads, in the order their rows are passed to list_tables.
    inline constexpr std::string_view namespaces_sql = "SELECT oid, nspname FROM pg_catalog.pg_namespace;";
    inline constexpr std::string_view classes_sql =
        "SELECT oid, relname, relnamespace, relkind FROM pg_catalog.pg_class;";
    inline constexpr std::string_view attributes_sql =
        "SELECT attrelid, attname, atttypid, attnum, attisdropped, atttypspec FROM pg_catalog.pg_attribute;";
    inline constexpr std::string_view remote_tables_sql =
        "SELECT uid, database_name, schema_name, table_name, remote_database, remote_schema, remote_table, backend "
        "FROM federation.remote_tables;";
    inline constexpr std::string_view remote_columns_sql =
        "SELECT uid, database_name, schema_name, table_name, column_name, column_type, ordinal "
        "FROM federation.remote_columns;";

    using rows_t = std::pmr::vector<components::vector::data_chunk_t>;

    // True for a table no client lists: a federation metadata table, a Kafka bookkeeping table.
    bool is_internal_table(std::string_view database, std::string_view table) noexcept;

    // Every listable table, sorted by (catalog, schema, table). A local table is
    // named (database, "", table) — database "" for one created unqualified; a remote table
    // (remote database, remote schema, table) as the backend spells them, schema empty for MySQL /
    // ClickHouse. With include_schema the columns ride as a STRUCT named by the engine's column
    // names (pg_attribute's, the lower-case column_name of federation.remote_columns); `attributes`
    // and `remote_columns` are read only then. A row of a read that is not the shape above is
    // schema_error.
    core::result_wrapper_t<std::pmr::vector<table_info>> list_tables(std::pmr::memory_resource* resource,
                                                                     const get_tables_command_t& command,
                                                                     const rows_t& namespaces,
                                                                     const rows_t& classes,
                                                                     const rows_t* attributes,
                                                                     const rows_t& remote_tables,
                                                                     const rows_t* remote_columns);

} // namespace otterstax::catalog
