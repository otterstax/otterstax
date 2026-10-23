// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "integration/backend.hpp"

#include <memory_resource>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace db {

    // Named-type overrides of every discovered ClickHouse table: uid → table → the column
    // types system.columns answered (a named Tuple's field names, which the native protocol's
    // block headers do not carry). Discovery writes it — at startup, through the catalog's
    // backend — and every Worker's backend reads it for each statement. A table without an
    // entry (a DDL target, never probed) has no overrides. The lock only orders a
    // re-discovery a test may run against those reads.
    class ch_metadata_t {
    public:
        void set(const std::string& uid, const std::string& table, conn::named_types_ptr types);
        conn::named_types_ptr find(const std::string& uid, const std::string& table) const;

    private:
        mutable std::shared_mutex mutex_;
        std::unordered_map<std::string, std::unordered_map<std::string, conn::named_types_ptr>> tables_;
    };

    struct clickhouse_traits;
    using ClickhouseBackend = backend_t<clickhouse_traits>;

    struct clickhouse_traits {
        static constexpr backend_type_t type = backend_type_t::ClickHouse;
        static constexpr const char* name = "ClickHouse";
        static constexpr std::string_view log_tag = logger_tag::CH_CONNECTION_MANAGER;
        using metadata_t = ch_metadata_t;

        static core::error_t check(const metadata_t&, const std::string&, std::pmr::memory_resource*) {
            return core::error_t::no_error();
        }

        // A DML streams no block. Only an INSERT's count is real: its written-row progress
        // (materialized views add theirs; async_insert reports none). A lightweight DELETE and
        // ALTER ... UPDATE are mutations outside the statement's pipeline: they report 0.
        static conn::statement_t slot_statement(components::logical_plan::node_type slot,
                                                std::string sql,
                                                const metadata_t& metadata,
                                                const std::string& uid,
                                                const std::string& table);
        static core::result_wrapper_t<components::vector::data_chunk_t>
        slot_chunk(std::pmr::memory_resource* resource,
                   components::logical_plan::node_type slot,
                   conn::statement_result_t answer,
                   const std::string& uid);
        // The prepare probe's header goes through ch_to_struct under the slot's table's named
        // types, as execute's blocks go through ch_to_chunk, so the prepared column types are
        // the executed chunk's by construction.
        static conn::statement_t probe_statement(std::string probe,
                                                 const metadata_t& metadata,
                                                 const std::string& uid,
                                                 const std::string& table);
        static std::string no_header(const std::string& probe) {
            return "ClickHouse answered the prepare probe without a header block: " + probe;
        }

        // `scope.database` is the ClickHouse database the probes run against (empty is
        // missing_field); empty `scope.collection` → every table of it (system.tables, then one
        // probe per table), otherwise that table. Each table's named types (system.columns)
        // are fetched first — a failed metadata query fails the table, so it is never
        // registered with degraded column types — and the probe's STRUCT is built with them.
        static actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
        discover(ClickhouseBackend& backend, qualified_name_t scope);
    };

    extern template class backend_t<clickhouse_traits>;

} // namespace db
