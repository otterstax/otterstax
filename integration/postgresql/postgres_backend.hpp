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

    // The ENUM types of every discovered PostgreSQL connection, by uid: what pg_to_chunk and
    // pg_to_struct decode an enum column with. Refreshed by every discovery of the uid — a
    // failed pg_enum query fails the discovery — and read by every Worker's backend for each
    // statement. A uid without an entry never went through discovery and is do_not_exists.
    // The lock only orders a re-discovery a test may run against those reads.
    class pg_metadata_t {
    public:
        void set(const std::string& uid, conn::pg_enums_ptr enums);
        conn::pg_enums_ptr find(const std::string& uid) const;

    private:
        mutable std::shared_mutex mutex_;
        std::unordered_map<std::string, conn::pg_enums_ptr> enums_;
    };

    struct postgres_traits;
    using PostgresBackend = backend_t<postgres_traits>;

    struct postgres_traits {
        static constexpr backend_type_t type = backend_type_t::PostgreSQL;
        static constexpr const char* name = "PostgreSQL";
        static constexpr std::string_view log_tag = logger_tag::PG_CONNECTION_MANAGER;
        using metadata_t = pg_metadata_t;

        static core::error_t check(const metadata_t& metadata, const std::string& uid, std::pmr::memory_resource* resource);

        // Every slot is a rows statement: pg_to_chunk turns a DML's command tag into the
        // affected-row carrier itself.
        static conn::statement_t slot_statement(components::logical_plan::node_type,
                                                std::string sql,
                                                const metadata_t& metadata,
                                                const std::string& uid,
                                                const std::string&) {
            return conn::statement_t::rows(std::move(sql), metadata.find(uid));
        }
        static core::result_wrapper_t<components::vector::data_chunk_t>
        slot_chunk(std::pmr::memory_resource* resource,
                   components::logical_plan::node_type slot,
                   conn::statement_result_t answer,
                   const std::string& uid);
        // The probe's answer goes through pg_to_struct under the connection's ENUM map — the
        // translator pg_to_chunk types tuples with.
        static conn::statement_t
        probe_statement(std::string probe, const metadata_t& metadata, const std::string& uid, const std::string&) {
            return conn::statement_t::schema(std::move(probe), metadata.find(uid));
        }
        static std::string no_header(const std::string& probe) {
            return "PostgreSQL answered the prepare probe without columns: " + probe;
        }

        // `scope.schema` is required (missing_field otherwise). The ENUM map is fetched first,
        // then empty `scope.collection` → every base table of the schema (information_schema
        // listing, then one probe per table), non-empty → that table.
        static actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
        discover(PostgresBackend& backend, qualified_name_t scope);
    };

    extern template class backend_t<postgres_traits>;

} // namespace db
