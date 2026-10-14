// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "catalog/discovery.hpp"
#include "connectors/mysql/manager.hpp"
#include "integration/statement_executor.hpp"
#include "otterbrix/federation/provider.hpp"
#include "utility/session.hpp"

#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>

namespace db {

    // The rows an UPDATE matched, out of the info of its OK packet. MySQL's affected-row count of an
    // UPDATE is the rows it CHANGED (Boost.MySQL never sets CLIENT_FOUND_ROWS), while the engine,
    // PostgreSQL and the row-by-row sink count the rows the statement matched (an UPDATE that sets a
    // value to itself still counts its row). The info is the server's ER_UPDATE_INFO message —
    // "Rows matched: N  Changed: M  Warnings: W" on MySQL and MariaDB alike — in the session's
    // lc_messages language, so it is read by its numbers, not its words: exactly three, the first
    // being the rows matched. nullopt for any other text.
    std::optional<std::uint64_t> mysql_update_rows_matched(std::string_view info);

    // The MySQL backend: the sole driver of the mysql::ConnectorManager after startup, and the
    // federation's backend_t (otterbrix/federation/provider.hpp) for every MySQL connection:
    //
    //  - `discover`, the schema discovery of a scope, called by the remote backends module at startup
    //    and by a query session's rediscovery; `execute_ddl`, one remote DDL statement of a query
    //    session (CREATE / DROP TABLE, CREATE / DROP INDEX on an alias). Both run on the calling
    //    thread until the backend answered.
    //  - `scan` and `write`, called by the engine's executors for the operators of a remote table.
    //    Each starts its statement on the statements pool (statement_executor.hpp: the runtime's
    //    backends pool) — the connection checkout and the wait for the answer happen there, never on
    //    the executor — translates the rows with tsl::mysql_to_chunk on the connector's io thread and
    //    fulfils the operator's future.
    //
    // Thread-safe: the state every member reads (the connector manager's registry) is read-only
    // after startup, and each statement checks a connection of its own out of the uid's pool.
    class MySQLManager final : public otterstax::federation::backend_t {
    public:
        // `statements` runs the federation's statements (checkout + wait); it must keep running until
        // this object is destroyed (statement_executor.hpp).
        MySQLManager(std::pmr::memory_resource* res,
                     mysql::ConnectorManager* connector_manager,
                     statement_executor_t::pool_t* statements);
        // Makes the connector manager refuse new statements and abort the running ones
        // (mysql::ConnectorManager::shutdown), then waits for the statements still queued.
        ~MySQLManager() override;

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Schema discovery: empty `scope.collection` → every base table of `scope.database`
        // (information_schema listing, then one probe per table); non-empty → that single table.
        // Each table's primary key comes with its probe (the column definitions' PRI_KEY_FLAG).
        // A per-table probe failure fails the whole discovery, and so does a connection whose
        // sql_mode has NO_BACKSLASH_ESCAPES (the generated literals escape a backslash).
        core::result_wrapper_t<otterstax::federation::discovered_tables_t> discover(const qualified_name_t& scope) override;

        // One DDL statement on `uid`, already in MySQL's dialect; the connector's error, code kept.
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
        mysql::ConnectorManager* connector_manager_;
        log_t log_;
        // Last: destroyed first, it waits for the statements still queued.
        statement_executor_t statements_;
    };
} // namespace db
