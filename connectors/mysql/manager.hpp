// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "connector.hpp"
#include "utility/tracy_profiler.hpp"

#include <concepts>
#include <coroutine>
#include <exception>
#include <functional>
#include <thread>

#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "../api_connections/connection_config.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/thread_pool_manager.hpp"
#include "utility/wait_barrier.hpp"

#include <components/expressions/compare_expression.hpp>

namespace mysql {

    namespace bm = boost::mysql;
    namespace asio = boost::asio;
    using asio::awaitable;
    using asio::co_spawn;
    using asio::use_awaitable;

    std::unique_ptr<mysql::IConnector>
    make_mysql_connector(asio::io_context& io_ctx, bm::connect_params params, std::string alias);

    class ConnectorManager {
    public:
        ConnectorManager(actor_zeta::address_t catalog_manager,
                         connector_factory make_connector = make_mysql_connector,
                         size_t pool_size = std::thread::hardware_concurrency());
        thread_pool_status status() const noexcept;
        void start();
        void stop();

        // TODO add query for adding and removing connections
        // TODO this is not thread safe!!!
        std::string addConnection(bm::connect_params connection_param, const std::string& uuid);
        std::string addConnection(conn::api_server::ConnectionParams connection_param);
        void removeConnection(const std::string& uuid);

        template<typename Callable>
        requires std::invocable<Callable, const boost::mysql::results&>
            std::future<query_outcome<std::invoke_result_t<Callable, const boost::mysql::results&>>>
            executeQuery(const std::string& uuid, std::string_view query, Callable handler) {
            using result_t = std::invoke_result_t<Callable, const boost::mysql::results&>;
            OTX_ZONE_N("mysql::ConnectorManager::executeQuery");
            auto conn = connections_.find(uuid);
            if (conn == connections_.end()) {
                log_->error("[ConnectorManager::executeQuery] Invalid connection uuid: {}", uuid);
                throw std::runtime_error("[ConnectorManager::executeQuery]  Invalid connection uuid: " + uuid);
            }
            if (conn->second->status() == Status::Closed) {
                log_->error("[ConnectorManager::executeQuery] Connector is not connected");
                throw std::runtime_error("[ConnectorManager::executeQuery]  Connector is not connected\n");
            }
            if (!conn->second->isConnected()) {
                try {
                    conn->second->tryReconnect();
                } catch (const std::exception& e) {
                    notify_connection_removed(uuid);
                    throw std::runtime_error("Failed to reconnect. Error message: " + std::string(e.what()));
                }
            }
            // Marshal any connector error into a value ON the io thread — never ship a live
            // std::exception across the io->consumer boundary (boost.asio use_future captures +
            // destroys the exception_ptr on the io thread, racing future.get(); TSAN race under
            // boost 1.88). The consumer rethrows from the copied string on its own thread.
            auto guarded = [](awaitable<result_t> inner) -> awaitable<query_outcome<result_t>> {
                try {
                    co_return query_outcome<result_t>{co_await std::move(inner), {}};
                } catch (const std::exception& e) {
                    co_return query_outcome<result_t>{result_t{}, std::string{e.what()}};
                }
            }(conn->second->runQuery(query, handler));
            return co_spawn(thread_pool_manager_.ctx(), std::move(guarded), asio::use_future);
        }

        size_t totalConnections() const noexcept;
        std::optional<bm::connect_params> conn_params(const std::string& uuid) const;
        bool hasConnection(const std::string& uuid) const noexcept;

    private:
        void notify_connection_removed(const std::string& uuid);

        log_t log_;
        thread_pool_manager thread_pool_manager_;
        actor_zeta::address_t catalog_manager_;
        connector_factory make_connector_;
        std::unordered_map<std::string, std::unique_ptr<mysql::IConnector>> connections_;
    };
} // namespace mysql
