// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "integration/backend.hpp"
#include "utility/string_hash.hpp"

#include <memory_resource>
#include <shared_mutex>
#include <string>
#include <string_view>

namespace db {

    // Named-type overrides of discovered ClickHouse tables: uid -> table -> the column types
    // system.columns answered (the native protocol's headers lack named Tuple field names). Discovery
    // writes it, every Worker's backend reads it. The lock only orders a test's re-discovery.
    class ch_metadata_t {
    public:
        explicit ch_metadata_t(std::pmr::memory_resource* resource)
            : tables_(resource) {}

        void set(std::string_view uid, std::string_view table, conn::named_types_ptr types);
        conn::named_types_ptr find(std::string_view uid, std::string_view table) const;

    private:
        mutable std::shared_mutex mutex_;
        // Both levels on the registry's resource: an inner map takes it from the outer one.
        otterstax::pmr_string_map_t<otterstax::pmr_string_map_t<conn::named_types_ptr>> tables_;
    };

    struct clickhouse_traits;
    using ClickhouseBackend = backend_t<clickhouse_traits>;

    struct clickhouse_traits {
        static constexpr backend_type_t type = backend_type_t::ClickHouse;
        static constexpr const char* name = "ClickHouse";
        static constexpr std::string_view log_tag = logger_tag::CH_CONNECTION_MANAGER;
        using metadata_t = ch_metadata_t;

        static core::error_t check(const metadata_t&, std::string_view, std::pmr::memory_resource*) {
            return core::error_t::no_error();
        }

        // A DML streams no block. Only an INSERT's count is real: its written-row progress
        // (materialized views add theirs; async_insert reports none). A lightweight DELETE and
        // ALTER ... UPDATE are mutations outside the statement's pipeline: they report 0.
        static conn::statement_t slot_statement(components::logical_plan::node_type slot,
                                                std::pmr::string sql,
                                                const metadata_t& metadata,
                                                std::string_view uid,
                                                std::string_view table);
        static core::result_wrapper_t<components::vector::data_chunk_t>
        slot_chunk(std::pmr::memory_resource* resource,
                   components::logical_plan::node_type slot,
                   conn::statement_result_t answer,
                   std::string_view uid);
        // The prepare probe's header goes through ch_to_struct under the slot's table's named
        // types, as execute's blocks go through ch_to_chunk, so the prepared column types are
        // the executed chunk's by construction.
        static conn::statement_t probe_statement(std::pmr::string probe,
                                                 const metadata_t& metadata,
                                                 std::string_view uid,
                                                 std::string_view table);
        static constexpr std::string_view no_header = "ClickHouse answered the prepare probe without a header block: ";

        // `scope.database` is the database to probe (empty is missing_field); empty `scope.collection`
        // means every table, otherwise that one. Named types (system.columns) are fetched first and a
        // failure fails the table, so no table is registered with degraded column types.
        static actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
        discover(ClickhouseBackend& backend, qualified_name_t scope);
    };

    extern template class backend_t<clickhouse_traits>;

} // namespace db
