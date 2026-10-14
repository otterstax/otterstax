// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/backend_type.hpp"

#include <components/base/collection_full_name.hpp>

#include <array>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

// The layout of the host metadata OtterStax keeps in ordinary engine tables: the remote tables
// discovered on the servers of config.yaml and their columns. The name resolution hook reads
// federation.remote_columns (need / decide); catalog/remote_metadata.hpp builds the plans that write
// both (startup: backends::publish_federation_metadata; a rediscovery: the query session). Both
// sides take every name and column position from here.
namespace otterstax::federation {

    // The spelling stored in the `backend` columns: "mysql", "postgresql", "clickhouse".
    std::string_view backend_kind_name(backend_type_t kind) noexcept;
    std::optional<backend_type_t> parse_backend_kind(std::string_view name) noexcept;

    inline constexpr std::string_view metadata_database = "federation";
    inline constexpr std::string_view remote_tables_table = "remote_tables";
    inline constexpr std::string_view remote_columns_table = "remote_columns";

    // federation.remote_tables: one row per discovered table. The key columns (uid .. table_name) are
    // the engine-facing name (table_key_t); remote_* keep the backend's own spelling.
    struct remote_tables_layout {
        enum column : std::size_t
        {
            uid,
            database_name,
            schema_name,
            table_name,
            remote_database,
            remote_schema,
            remote_table,
            backend,
            count
        };
        static constexpr std::array<std::string_view, count> names{"uid",
                                                                   "database_name",
                                                                   "schema_name",
                                                                   "table_name",
                                                                   "remote_database",
                                                                   "remote_schema",
                                                                   "remote_table",
                                                                   "backend"};
    };

    // federation.remote_columns: one row per column of a discovered table. column_name is what the
    // engine binds (lower case, as an unquoted reference is folded); remote_column is the backend's
    // spelling, column_type the engine type (otterbrix's type spec), ordinal its 1-based position,
    // key_position its 1-based position in the table's primary key (0: not a key column; the
    // storage changes a row the engine picked by these columns). The table's remote spelling is
    // repeated on every row, so ONE read of this table is everything decide needs to build a
    // storage (need issues one read per unresolved name).
    struct remote_columns_layout {
        enum column : std::size_t
        {
            uid,
            database_name,
            schema_name,
            table_name,
            column_name,
            remote_column,
            column_type,
            ordinal,
            backend,
            remote_database,
            remote_schema,
            remote_table,
            key_position,
            count
        };
        static constexpr std::array<std::string_view, count> names{"uid",
                                                                   "database_name",
                                                                   "schema_name",
                                                                   "table_name",
                                                                   "column_name",
                                                                   "remote_column",
                                                                   "column_type",
                                                                   "ordinal",
                                                                   "backend",
                                                                   "remote_database",
                                                                   "remote_schema",
                                                                   "remote_table",
                                                                   "key_position"};
    };

    // ASCII lower case: the engine folds an unquoted identifier so, and the metadata keys are stored so.
    std::string fold_identifier(std::string_view name);

    // The engine-facing key of a remote table, lower case. A MySQL or ClickHouse table is named by
    // its database (the schema slot stays empty); a PostgreSQL table by its schema (the database
    // slot stays empty: a connection reaches one database, so the slot cannot select another).
    struct table_key_t {
        std::string uid;
        std::string database;
        std::string schema;
        std::string table;

        bool operator==(const table_key_t&) const = default;
    };

    table_key_t make_table_key(backend_type_t kind,
                               std::string_view uid,
                               std::string_view database,
                               std::string_view schema,
                               std::string_view table);

    // The backend's own spelling of a table, as discovery answered it.
    struct remote_table_name_t {
        std::string database;
        std::string schema;
        std::string table;
    };

    // A remote table's backend spelling as the generator names a table: database.table for MySQL
    // and ClickHouse, schema.table for PostgreSQL (its connection is bound to one database).
    qualified_name_t generator_name(backend_type_t kind, const remote_table_name_t& table);

} // namespace otterstax::federation
