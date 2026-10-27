// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "integration/backend.hpp"

#include <memory_resource>
#include <string>
#include <string_view>

namespace db {

    // MySQL decodes every answer from its own column definitions: no metadata to keep.
    struct no_metadata_t {};

    struct mysql_traits;
    using MySQLBackend = backend_t<mysql_traits>;

    struct mysql_traits {
        static constexpr backend_type_t type = backend_type_t::MySQL;
        static constexpr const char* name = "MySQL";
        static constexpr std::string_view log_tag = logger_tag::SQL_CONNECTION_MANAGER;
        using metadata_t = no_metadata_t;

        static core::error_t check(const metadata_t&, std::string_view, std::pmr::memory_resource*) {
            return core::error_t::no_error();
        }

        // Every slot is a rows statement: mysql_to_chunk turns a DML's OK packet into the
        // affected-row carrier itself.
        static conn::statement_t slot_statement(components::logical_plan::node_type,
                                                std::pmr::string sql,
                                                const metadata_t&,
                                                std::string_view,
                                                std::string_view) {
            return conn::statement_t::rows(std::move(sql));
        }
        static core::result_wrapper_t<components::vector::data_chunk_t>
        slot_chunk(std::pmr::memory_resource* resource,
                   components::logical_plan::node_type slot,
                   conn::statement_result_t answer,
                   std::string_view uid);
        // The probe's zero-row result set is read by mysql_to_struct — the STRUCT of the very
        // types mysql_to_chunk gives execute's rows — so discover, describe and execute cannot
        // disagree about a column's type.
        static conn::statement_t
        probe_statement(std::pmr::string probe, const metadata_t&, std::string_view, std::string_view) {
            return conn::statement_t::schema(std::move(probe));
        }
        static constexpr std::string_view no_header = "MySQL answered the prepare probe without column metadata: ";

        // Empty `scope.collection` → every base table of `scope.database` (information_schema
        // listing, then one probe per table); non-empty → that single table. A per-table probe
        // failure fails the whole discovery.
        static actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
        discover(MySQLBackend& backend, qualified_name_t scope);
    };

    extern template class backend_t<mysql_traits>;

} // namespace db
