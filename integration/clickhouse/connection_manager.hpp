// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "catalog/discovery.hpp"
#include "connectors/clickhouse/manager.hpp"
#include "integration/statement_executor.hpp"
#include "otterbrix/federation/provider.hpp"
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
#include <vector>

namespace db {

    // The ClickHouse backend: the sole driver of the ch::ConnectorManager after startup. Same shape
    // as db::MySQLManager (integration/sql/connection_manager.hpp): the federation's backend_t —
    // `discover` and `execute_ddl` on the calling thread, `scan` / `write` on the statements pool
    // (statement_executor.hpp), each on a connection of the uid's pool (ch::ConnectorManager), the
    // rows translated by tsl::ch_to_chunk under the table's named types (system.columns). A
    // write reports ClickHouse's written_rows for an INSERT and 0 for a mutation (ALTER TABLE ...
    // UPDATE, lightweight DELETE), which reports nothing.
    class ClickhouseManager final : public otterstax::federation::backend_t {
    public:
        // `statements` runs the federation's statements (checkout + wait); it must keep running until
        // this object is destroyed (statement_executor.hpp).
        ClickhouseManager(std::pmr::memory_resource* res,
                          ch::ConnectorManager* connector_manager,
                          statement_executor_t::pool_t* statements);
        ~ClickhouseManager() override;

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Schema discovery: empty `scope.collection` → every table of `scope.database`
        // (system.tables), non-empty → that single table. Each table's named types
        // (system.columns) are fetched first and kept for the scans; the probe's STRUCT is built with
        // them, so the recorded schema and the rows a scan decodes agree on named tuple columns.
        // One discovery runs at a time (mutex_).
        core::result_wrapper_t<otterstax::federation::discovered_tables_t> discover(const qualified_name_t& scope) override;

        // One DDL statement on `uid`, already in ClickHouse's dialect; the connector's error, code kept.
        core::error_t execute_ddl(const std::string& uid, const std::string& sql) override;

        actor_zeta::unique_future<core::error_t>
        scan(std::pmr::memory_resource* resource,
             otterstax::federation::scan_request_t request,
             std::shared_ptr<otterstax::federation::scan_rows_t> rows) override;
        actor_zeta::unique_future<otterstax::federation::write_result_t>
        write(std::pmr::memory_resource* resource, otterstax::federation::write_request_t request) override;

    private:
        using named_types_t = std::unordered_map<std::string, std::string>;

        std::pmr::memory_resource* resource_;
        // Non-owning: the remote backends module owns it and outlives this object.
        ch::ConnectorManager* connector_manager_;
        log_t log_;
        // Serializes discover: a discovery publishes the named types it read after its query, so two
        // at once could leave the older reading in named_types_.
        OTX_LOCKABLE_N(std::mutex, mutex_, "ClickhouseManager::mutex");
        // column → system.columns type per (uid, database, table), replaced on rediscovery: one table
        // name may be a table of several databases of one connection. Written by discover, read by
        // scan (the backends pool's jobs): under named_types_mutex_.
        mutable std::shared_mutex named_types_mutex_;
        std::unordered_map<std::string,
                           std::unordered_map<std::string, std::unordered_map<std::string, named_types_t>>>
            named_types_;
        // The last data member: destroyed first, it waits for the statements still queued.
        statement_executor_t statements_;

        core::result_wrapper_t<named_types_t>
        fetch_named_types(const std::string& uuid, const std::string& database, const std::string& table);
        // Per result column of `request`, by position: the system.columns type of the table column it
        // reads as it is (scan_request_t::columns); nullopt for a computed column and for a table that
        // went through no discovery.
        std::vector<std::optional<std::string>>
        declared_types_for(const otterstax::federation::scan_request_t& request) const;
    };
} // namespace db
