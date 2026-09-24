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

#include "connectors/api_connections/ch_connection_config.hpp"
#include "utility/thread_pool_manager.hpp"
#include "utility/wait_barrier.hpp"

#include <components/expressions/compare_expression.hpp>

namespace ch {

    namespace asio = boost::asio;

    std::unique_ptr<ch::IConnector>
    make_ch_connector(std::pmr::memory_resource* resource, connect_params params, std::string alias);

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
        [[nodiscard]] core::result_wrapper_t<std::string> addConnection(connect_params connection_param,
                                                                        const std::string& uuid);
        [[nodiscard]] core::result_wrapper_t<std::string>
        addConnection(conn::api_server::ChConnectionParams connection_param);

        // Every failure — the checks below, a connector error and anything the
        // driver or handler throws on the io thread — comes back through the
        // returned future as a value. This function does not throw.
        template<typename Callable>
        requires std::invocable<Callable, const select_result_t&>
            otterstax::query_future_t<std::invoke_result_t<Callable, const select_result_t&>>
            executeQuery(const std::string& uuid, std::string_view query, Callable handler) {
            OTX_ZONE_N("ch::ConnectorManager::executeQuery");
            using result_t = std::invoke_result_t<Callable, const select_result_t&>;
            // A query spawned on a pool that is not running never completes: nothing
            // drives the io_context, so the future would wait forever.
            if (thread_pool_manager_.status() != thread_pool_status::RUNNING) {
                log_->error("[ChConnectorManager::executeQuery] Connector thread pool is not running");
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::io_error,
                    std::pmr::string{"[ChConnectorManager::executeQuery] Connector thread pool is not running",
                                     resource_}));
            }
            auto conn = connections_.find(uuid);
            if (conn == connections_.end()) {
                log_->error("[ChConnectorManager::executeQuery] Invalid connection uuid: {}", uuid);
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::do_not_exists,
                    std::pmr::string{("[ChConnectorManager::executeQuery] Invalid connection uuid: " + uuid).c_str(),
                                     resource_}));
            }
            if (conn->second->status() == Status::Closed) {
                log_->error("[ChConnectorManager::executeQuery] Connector is not connected");
                return otterstax::make_failed_future<result_t>(core::error_t(
                    core::error_code_t::io_error,
                    std::pmr::string{"[ChConnectorManager::executeQuery] Connector is not connected", resource_}));
            }
            if (!conn->second->isConnected()) {
                // A transient reconnect failure fails THIS query only and leaves the
                // connection registered: unregistering it here would drop the uid's
                // external database from the ENGINE catalog while other sessions'
                // in-flight plans still reference it, and they would then die with a
                // misleading "database does not exist".
                if (auto err = conn->second->tryReconnect(); err.contains_error()) {
                    return otterstax::make_failed_future<result_t>(std::move(err));
                }
            }
            try {
                return otterstax::spawn_marshaled<result_t>(
                    thread_pool_manager_.ctx(),
                    otterstax::run_with_owned_handler<result_t, const select_result_t&>(*conn->second,
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
                    std::pmr::string{"[ChConnectorManager::executeQuery] unknown error spawning the query",
                                     resource_}));
            }
        }

        size_t totalConnections() const noexcept;
        bool hasConnection(const std::string& uuid) const noexcept;

    private:
        std::pmr::memory_resource* resource_;
        log_t log_;
        thread_pool_manager thread_pool_manager_;
        actor_zeta::address_t catalog_manager_;
        connector_factory make_connector_;
        // Driven by exactly one actor (db::ClickhouseManager) after startup; no
        // per-table metadata lives here — the named-type overrides a query
        // needs are state of that actor, filled by its discovery handler.
        std::unordered_map<std::string, std::unique_ptr<ch::IConnector>> connections_;
    };
} // namespace ch
