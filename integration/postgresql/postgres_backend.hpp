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

    // The ENUM types of every discovered PostgreSQL connection, by uid, for pg_to_chunk and pg_to_struct.
    // Every discovery of the uid refreshes it (a failed pg_enum query fails the discovery) and every
    // Worker's backend reads it. A uid without an entry never went through discovery: do_not_exists.
    class pg_metadata_t {
    public:
        explicit pg_metadata_t(std::pmr::memory_resource* resource)
            : enums_(resource) {}

        void set(std::string_view uid, conn::pg_enums_ptr enums);
        conn::pg_enums_ptr find(std::string_view uid) const;

    private:
        mutable std::shared_mutex mutex_;
        otterstax::pmr_string_map_t<conn::pg_enums_ptr> enums_;
    };

    struct postgres_traits;
    using PostgresBackend = backend_t<postgres_traits>;

    struct postgres_traits {
        static constexpr backend_type_t type = backend_type_t::PostgreSQL;
        static constexpr const char* name = "PostgreSQL";
        static constexpr std::string_view log_tag = logger_tag::PG_CONNECTION_MANAGER;
        using metadata_t = pg_metadata_t;

        static core::error_t check(const metadata_t& metadata, std::string_view uid, std::pmr::memory_resource* resource);

        // Every slot is a rows statement: pg_to_chunk turns a DML's command tag into the
        // affected-row carrier itself.
        static conn::statement_t slot_statement(components::logical_plan::node_type,
                                                std::pmr::string sql,
                                                const metadata_t& metadata,
                                                std::string_view uid,
                                                std::string_view) {
            return conn::statement_t::rows(std::move(sql), metadata.find(uid));
        }
        static core::result_wrapper_t<components::vector::data_chunk_t>
        slot_chunk(std::pmr::memory_resource* resource,
                   components::logical_plan::node_type slot,
                   conn::statement_result_t answer,
                   std::string_view uid);
        // The probe's answer goes through pg_to_struct under the connection's ENUM map — the
        // translator pg_to_chunk types tuples with.
        static conn::statement_t
        probe_statement(std::pmr::string probe, const metadata_t& metadata, std::string_view uid, std::string_view) {
            return conn::statement_t::schema(std::move(probe), metadata.find(uid));
        }
        static constexpr std::string_view no_header = "PostgreSQL answered the prepare probe without columns: ";

        // `scope.schema` is required (missing_field otherwise). The ENUM map is fetched first,
        // then empty `scope.collection` → every base table of the schema (information_schema
        // listing, then one probe per table), non-empty → that table.
        static actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
        discover(PostgresBackend& backend, qualified_name_t scope);
    };

    extern template class backend_t<postgres_traits>;

} // namespace db
