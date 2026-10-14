// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "catalog/discovery.hpp"
#include "connectors/postgresql/manager.hpp"
#include "integration/statement_executor.hpp"
#include "otterbrix/federation/provider.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <core/result_wrapper.hpp>

#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace db {

    // The PostgreSQL backend: the sole driver of the pg::ConnectorManager after startup. Same shape
    // as db::MySQLManager (integration/sql/connection_manager.hpp): the federation's backend_t —
    // `discover` and `execute_ddl` on the calling thread, `scan` / `write` on the statements pool
    // (statement_executor.hpp), the rows translated by tsl::pg_to_chunk under the uid's ENUM labels.
    class PostgressManager final : public otterstax::federation::backend_t {
    public:
        // `statements` runs the federation's statements (checkout + wait); it must keep running until
        // this object is destroyed (statement_executor.hpp).
        PostgressManager(std::pmr::memory_resource* res,
                         pg::ConnectorManager* connector_manager,
                         statement_executor_t::pool_t* statements);
        ~PostgressManager() override;

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Schema discovery: `scope` carries the database and schema the probes run against (a
        // missing schema is missing_field); empty `scope.collection` → every base table of that
        // schema, non-empty → that single table. Refreshes the connection's ENUM oid map first — a
        // failed metadata query fails the discovery, so a table is never recorded with degraded
        // column types — and reads the tables' primary keys (one pg_index query for the scope).
        // One discovery runs at a time (mutex_).
        core::result_wrapper_t<otterstax::federation::discovered_tables_t> discover(const qualified_name_t& scope) override;

        // One DDL statement on `uid`, already in PostgreSQL's dialect; the connector's error, code kept.
        core::error_t execute_ddl(const std::string& uid, const std::string& sql) override;

        actor_zeta::unique_future<core::error_t>
        scan(std::pmr::memory_resource* resource,
             otterstax::federation::scan_request_t request,
             std::shared_ptr<otterstax::federation::scan_rows_t> rows) override;
        actor_zeta::unique_future<otterstax::federation::write_result_t>
        write(std::pmr::memory_resource* resource, otterstax::federation::write_request_t request) override;

    private:
        std::pmr::memory_resource* resource_;
        // Non-owning: the remote backends module owns it and outlives this object.
        pg::ConnectorManager* connector_manager_;
        log_t log_;
        // Serializes discover: a discovery publishes the ENUM labels it read after its queries, so
        // two at once could leave the older reading in enums_.
        OTX_LOCKABLE_N(std::mutex, mutex_, "PostgressManager::mutex");
        // ENUM type oid → labels per connection uid, the metadata pg_to_chunk needs to decode enum
        // columns. Written by discover, read by scan (the backends pool's jobs): under enums_mutex_.
        // Every uid that went through discovery has an entry (possibly empty); a scan treats a
        // missing one as an error.
        mutable std::shared_mutex enums_mutex_;
        std::unordered_map<std::string, tsl::pg_enum_oid_map> enums_;
        // The last data member: destroyed first, it waits for the statements still queued.
        statement_executor_t statements_;

        std::optional<tsl::pg_enum_oid_map> enums_of(const std::string& uid) const;

        // Runs the pg_enum query on the connection and returns the decoded map
        // as a value; nothing is cached before the outcome is known.
        core::result_wrapper_t<tsl::pg_enum_oid_map> fetch_enum_types(const std::string& uuid);

        // The primary key columns of the tables of `schema` (of `table` alone when it is not
        // empty), by table name, in key order.
        core::result_wrapper_t<std::unordered_map<std::string, std::vector<std::string>>>
        fetch_primary_keys(const std::string& uuid, const std::string& schema, const std::string& table);
    };
} // namespace db
