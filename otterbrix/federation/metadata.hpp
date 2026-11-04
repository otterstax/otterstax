// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/backend_type.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/types/types.hpp>

#include <array>
#include <cstddef>
#include <memory_resource>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// What OtterStax keeps of a remote table beside otterbrix's catalog. otterbrix keeps the table itself
// (pg_database.datconnection, pg_namespace, pg_class, pg_attribute: every name in lower case); the
// host keeps the backend's own spelling of it in an ordinary table of its own, keyed by otterbrix's
// oids, which the backend actor answers as host rows of every DDL step (backend_actor.hpp).
namespace otterstax::federation {

    // The local database of OtterStax's own tables (the Kafka objects' tables live there too).
    inline constexpr std::string_view metadata_database = "federation";
    inline constexpr std::string_view metadata_schema = "public";

    // federation.public.remote_names: the backend's spelling of every remote table and column.
    //  - a table:  (table_oid, 0, the table's name, the database (MySQL / ClickHouse) or schema
    //    (PostgreSQL) the table is in on the backend, NULL);
    //  - a column: (table_oid, attoid, the column's name, NULL, the backend's own type where the
    //    dialect reads by it — ClickHouse's system.columns type — else NULL).
    inline constexpr std::string_view remote_names_table = "remote_names";

    struct remote_names_layout {
        enum column : std::size_t
        {
            table_oid,
            attoid,
            remote_name,
            remote_container,
            remote_type,
            count
        };
        static constexpr std::array<std::string_view, count> names{"table_oid",
                                                                   "attoid",
                                                                   "remote_name",
                                                                   "remote_container",
                                                                   "remote_type"};
    };

    // ASCII lower case: otterbrix folds an unquoted identifier so, and every name OtterStax registers
    // in its catalog is written so.
    std::string fold_identifier(std::string_view name);

    // The backend's own spelling of a table.
    struct remote_table_name_t {
        std::string database;
        std::string schema;
        std::string table;
    };

    // A remote table's backend spelling as the generator names a table: database.table for MySQL
    // and ClickHouse, schema.table for PostgreSQL (its connection is bound to one database).
    qualified_name_t generator_name(backend_type_t kind, const remote_table_name_t& table);

    // One table a discovery found on a backend: its full name in the backend's spelling
    // (connection, database, schema — PostgreSQL's; empty for MySQL / ClickHouse — and table), the
    // STRUCT of its column types as the input translators produce them (each field named in the
    // backend's spelling) and, for ClickHouse, the system.columns type of each column by that name:
    // what a read of the column is held to (ch_to_chunk's declared types).
    struct discovered_table_t {
        qualified_name_t name;
        components::types::complex_logical_type schema;
        std::unordered_map<std::string, std::string> remote_types{};
    };

    using discovered_tables_t = std::pmr::vector<discovered_table_t>;

} // namespace otterstax::federation
