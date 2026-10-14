// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "connector.hpp"
#include "utility/tracy_profiler.hpp"

#include <atomic>
#include <chrono>
#include <concepts>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "connectors/api_connections/pg_connection_config.hpp"
#include "connectors/common/connection_pool.hpp"
#include "connectors/common/pooled_query.hpp"
#include "utility/thread_pool_manager.hpp"
#include "utility/wait_barrier.hpp"

#include <components/expressions/compare_expression.hpp>

namespace pg {

    namespace asio = boost::asio;

    std::unique_ptr<pg::IConnector>
    make_pg_connector(std::pmr::memory_resource* resource, connect_params params, std::string alias);

    using connection_pool = conn::connection_pool_t<pg::IConnector>;


    // Every alias owns a pool of `pool_size` connections (addConnection; 1 by
    // default). A statement checks a connection out for its whole run, so
    // statements on one alias never overlap on one connection; with every
    // connection busy the next statement waits for one, up to the checkout
    // timeout.
    class ConnectorManager {
    public:
        // `resource` comes first and has no default: it owns every core::error_t
        // message this manager marshals off the io thread, and a trailing defaulted
        // parameter would let existing call sites keep compiling while silently
        // picking the wrong arena. `io_threads` sizes the manager's own asio pool,
        // the threads its statements run on (service.runtime.connector_io, sized by
        // remote_backends_t).
        ConnectorManager(std::pmr::memory_resource* resource, connector_factory make_connector, size_t io_threads);
        thread_pool_status status() const noexcept;
        void start();
        void stop();

        // Refuses every later statement (executeQuery answers io_error at once), closes the pools —
        // a statement waiting for a free connection wakes up refused — and aborts the statements
        // running (IConnector::cancel on every connection checked out). The owning integration backend
        // calls it from its destructor, before it waits for the statements it posted: so a statement
        // the backend never answers does not hold the shutdown. Idempotent, any thread, never blocks
        // on a backend.
        void shutdown() noexcept;

        // Opens the connector(s) of `uuid`. With removeConnection, the only
        // writes to the connection registry; both run on the startup thread
        // before the first statement reaches the backend: from then on the
        // registry is read-only, which is what lets executeQuery look it up
        // without a lock. Schema discovery is the owning integration backend's
        // (`discover`), run by the remote backends module right after.
        // `pool_size` connections are opened (each through the factory); if any of
        // them cannot be opened nothing is kept.
        // A pool_size of 0 is invalid_parameter.
        [[nodiscard]] core::result_wrapper_t<std::string> addConnection(connect_params connection_param,
                                                                        const std::string& uuid,
                                                                        std::size_t pool_size = 1);
        [[nodiscard]] core::result_wrapper_t<std::string>
        addConnection(conn::api_server::PgConnectionParams connection_param);

        // Closes and forgets `uuid`'s connection(s): a connection whose startup
        // discovery failed is not kept. Startup thread only, before any statement
        // (see addConnection); a uuid that is not registered is ignored.
        void removeConnection(const std::string& uuid);

        // Every failure — the checks below, a connector error and anything the
        // driver or handler throws on the io thread — comes back through the
        // returned future as a value. This function does not throw.
        template<typename Callable>
        requires std::invocable<Callable, PGresult*>
            otterstax::query_future_t<std::invoke_result_t<Callable, PGresult*>>
            executeQuery(const std::string& uuid, std::string_view query, Callable handler) {
            OTX_ZONE_N("pg::ConnectorManager::executeQuery");
            using result_t = std::invoke_result_t<Callable, PGresult*>;
            // A query spawned on a pool that is not running never completes: nothing
            // drives the io_context, so the future would wait forever.
            if (stopping_.load(std::memory_order_acquire)) {
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::io_error,
                    std::pmr::string{"[ConnectorManager::executeQuery] the connector manager is shutting down",
                                     resource_}));
            }
            if (thread_pool_manager_.status() != thread_pool_status::RUNNING) {
                log_->error("[PgConnectorManager::executeQuery] Connector thread pool is not running");
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::io_error,
                    std::pmr::string{"[PgConnectorManager::executeQuery] Connector thread pool is not running",
                                     resource_}));
            }
            auto pool = connections_.find(uuid);
            if (pool == connections_.end()) {
                log_->error("[PgConnectorManager::executeQuery] Invalid connection uuid: {}", uuid);
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::do_not_exists,
                    std::pmr::string{("[PgConnectorManager::executeQuery] Invalid connection uuid: " + uuid).c_str(),
                                     resource_}));
            }
            // Exclusive use of one connection for this statement. Waits while every
            // connection of the alias is busy; the health check (ping, reconnect)
            // runs on the checked-out connection, so it no longer races a statement
            // running on it. A transient reconnect failure fails THIS query only and
            // leaves the connection registered: the next statement may find the
            // backend reachable again.
            auto checked_out = checkout_(uuid, *pool->second);
            if (checked_out.has_error()) {
                return otterstax::make_failed_future<result_t>(core::error_on(resource_, checked_out.error()));
            }
            try {
                return otterstax::spawn_marshaled<result_t>(
                    thread_pool_manager_.ctx(),
                    conn::run_on_checked_out<result_t, PGresult*>(std::move(checked_out.value()),
                                                                  query,
                                                                  std::move(handler)),
                    resource_);
            } catch (const std::exception& e) {
                // run_on_checked_out's coroutine frame, with the handler and the
                // checked-out connection moved into it, is created here, on the
                // caller's thread, before anything reaches the io thread: a throw from
                // that construction is the only one that can bypass the marshalling,
                // so it is converted on the spot (the connection goes back to the pool
                // with the by-value argument).
                return otterstax::make_failed_future<result_t>(
                    core::error_t(core::error_code_t::io_error, std::pmr::string{e.what(), resource_}));
            } catch (...) {
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::io_error,
                    std::pmr::string{"[PgConnectorManager::executeQuery] unknown error spawning the query",
                                     resource_}));
            }
        }


        // How long a statement waits for a free connection of its uid.
        void setCheckoutTimeout(std::chrono::milliseconds timeout) noexcept;
        std::chrono::milliseconds checkoutTimeout() const noexcept;

        size_t totalConnections() const noexcept;
        bool hasConnection(const std::string& uuid) const noexcept;
        // Connections in the uid's pool / of them not checked out (0 for an unknown uid).
        size_t poolSize(const std::string& uuid) const noexcept;
        size_t idleConnections(const std::string& uuid) const;

    private:
        // A connection of `uuid` checked out of `pool` and ready for a statement:
        // waits up to the checkout timeout for a free one (io_error after it),
        // refuses a Closed one (io_error) and reconnects one whose ping fails.
        core::result_wrapper_t<connection_pool::handle_t> checkout_(const std::string& uuid, connection_pool& pool);

        std::pmr::memory_resource* resource_;
        log_t log_;
        thread_pool_manager thread_pool_manager_;
        connector_factory make_connector_;
        // Driven by exactly one backend (db::PostgressManager) after startup; no
        // per-connection metadata lives here — the ENUM oid map a query needs
        // is state of that backend, filled by its discovery.
        // Written only by addConnection (startup thread, before any query): the
        // map itself is read-only afterwards, and each pool synchronizes its own
        // checkouts.
        std::unordered_map<std::string, std::unique_ptr<connection_pool>> connections_;
        std::atomic<std::chrono::milliseconds::rep> checkout_timeout_ms_{conn::default_checkout_timeout.count()};
        std::atomic<bool> stopping_{false};
    };
} // namespace pg
