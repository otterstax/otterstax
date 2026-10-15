// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "connector.hpp"
#include "utility/tracy_profiler.hpp"

#include <concepts>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "../api_connections/connection_config.hpp"
#include "utility/thread_pool_manager.hpp"
#include "utility/session.hpp"
#include "utility/wait_barrier.hpp"

#include <components/expressions/compare_expression.hpp>

namespace mysql {

    namespace bm = boost::mysql;
    namespace asio = boost::asio;

    std::unique_ptr<mysql::IConnector> make_mysql_connector(std::pmr::memory_resource* resource,
                                                            asio::io_context& io_ctx,
                                                            bm::connect_params params,
                                                            std::string alias);

    class ConnectorManager {
    public:
        // `resource` comes first and has no default: it owns every core::error_t
        // message this manager marshals off the io thread, and a trailing defaulted
        // parameter would let existing call sites keep compiling while silently
        // picking the wrong arena.
        ConnectorManager(std::pmr::memory_resource* resource,
                         actor_zeta::address_t catalog_manager,
                         connector_factory make_connector,
                         size_t pool_size = std::thread::hardware_concurrency());
        thread_pool_status status() const noexcept;
        void start();
        void stop();

        // Opens the connector and registers its schema with the catalog; a
        // connection whose schema registration fails is not kept. This is the
        // only write to the connection registry, and it runs on the startup
        // thread before the first query message reaches the owning actor: from
        // then on the registry is read-only, which is what lets executeQuery
        // look it up without a lock.
        [[nodiscard]] core::result_wrapper_t<std::string> addConnection(bm::connect_params connection_param,
                                                                        const std::string& uuid);
        [[nodiscard]] core::result_wrapper_t<std::string>
        addConnection(conn::api_server::ConnectionParams connection_param);

        // Driven only by the integration actor, after its preceding queries
        // have drained. Each transaction owns a separate physical connection.
        core::error_t begin_transaction(session_id session, const std::string& alias);
        core::error_t finish_transaction(session_id session, bool commit);
        core::result_wrapper_t<std::unique_ptr<ConnectorManager>> fork_transaction(session_id session, const std::string& alias);

        // Every failure — the checks below, a connector error and anything the
        // driver or handler throws on the io thread — comes back through the
        // returned future as a value. This function does not throw.
        template<typename Callable>
        requires std::invocable<Callable, const boost::mysql::results&>
            otterstax::query_future_t<std::invoke_result_t<Callable, const boost::mysql::results&>>
            executeQuery(const std::string& uuid, std::string_view query, Callable handler,
                         std::optional<session_id> transaction = std::nullopt) {
            OTX_ZONE_N("mysql::ConnectorManager::executeQuery");
            using result_t = std::invoke_result_t<Callable, const boost::mysql::results&>;
            // A query spawned on a pool that is not running never completes: nothing
            // drives the io_context, so the future would wait forever.
            if (thread_pool_manager_.status() != thread_pool_status::RUNNING) {
                log_->error("[ConnectorManager::executeQuery] Connector thread pool is not running");
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::io_error,
                    std::pmr::string{"[ConnectorManager::executeQuery] Connector thread pool is not running",
                                     resource_}));
            }
            IConnector* selected = nullptr;
            if (transaction) {
                auto active = transactions_.find(*transaction);
                if (active == transactions_.end() || active->second.alias != uuid) {
                    return otterstax::make_failed_future<result_t>(core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"No transaction connection for this session and alias", resource_}));
                }
                selected = active->second.connector.get();
            } else {
                auto conn = connections_.find(uuid);
                if (conn == connections_.end()) {
                    return otterstax::make_failed_future<result_t>(core::error_t(
                        core::error_code_t::do_not_exists,
                        std::pmr::string{"Invalid connection uuid: " + uuid, resource_}));
                }
                selected = conn->second.get();
            }
            if (selected->status() == Status::Closed) {
                log_->error("[ConnectorManager::executeQuery] Connector is not connected");
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::io_error,
                    std::pmr::string{"[ConnectorManager::executeQuery] Connector is not connected", resource_}));
            }
            // Health probes and reconnect can discard an aborted transaction.
            // Transaction queries must use the original socket or fail.
            if (!transaction && !selected->isConnected()) {
                // A transient reconnect failure fails THIS query only and leaves the
                // connection registered: unregistering it here would drop the uid's
                // external database from the ENGINE catalog while other sessions'
                // in-flight plans still reference it, and they would then die with a
                // misleading "database does not exist".
                if (auto err = selected->tryReconnect(); err.contains_error()) {
                    return otterstax::make_failed_future<result_t>(std::move(err));
                }
            }
            try {
                return otterstax::spawn_marshaled<result_t>(
                    thread_pool_manager_.ctx(),
                    otterstax::run_with_owned_handler<result_t, const boost::mysql::results&>(*selected,
                                                                                              query,
                                                                                              std::move(handler)),
                    resource_);
            } catch (const std::exception& e) {
                // run_with_owned_handler's coroutine frame, with the handler moved
                // into it, is created here, on the caller's thread, before anything
                // reaches the io thread: a throw from that construction is the only
                // one that can bypass the marshalling, so it is converted on the spot.
                return otterstax::make_failed_future<result_t>(
                    core::error_t(core::error_code_t::io_error, std::pmr::string{e.what(), resource_}));
            } catch (...) {
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::io_error,
                    std::pmr::string{"[ConnectorManager::executeQuery] unknown error spawning the query", resource_}));
            }
        }

        size_t totalConnections() const noexcept;
        bool hasConnection(const std::string& uuid) const noexcept;

    private:
        struct transaction_connection {
            std::string alias;
            std::unique_ptr<IConnector> connector;
        };
        core::error_t control(IConnector& connector, const char* command);
        core::error_t open_transaction(session_id session, const std::string& alias, bm::connect_params params);
        // No in-flight query may outlive an entry: the owning actor drains
        // execute/describe before processing transaction completion.
        std::pmr::memory_resource* resource_;
        log_t log_;
        thread_pool_manager thread_pool_manager_;
        actor_zeta::address_t catalog_manager_;
        connector_factory make_connector_;
        // Driven by exactly one actor (db::MySQLManager) after startup; no
        // per-connection metadata lives here — what a query needs beyond the
        // connector itself is state of that actor.
        std::unordered_map<std::string, std::unique_ptr<mysql::IConnector>> connections_;
        std::unordered_map<session_id, transaction_connection> transactions_;
    };
} // namespace mysql
