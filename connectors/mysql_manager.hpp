// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "mysql_connector.hpp"

#include <concepts>
#include <coroutine>
#include <exception>
#include <functional>
#include <iostream>
#include <thread>

#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "../routes/catalog_manager.hpp"
#include "../utility/cv_wrapper.hpp"
#include "../utility/thread_pool_manager.hpp"
#include "http_server/connection_config.hpp"

#include <components/expressions/compare_expression.hpp>

namespace mysqlc {

    namespace mysql = boost::mysql;
    namespace asio = boost::asio;
    using asio::awaitable;
    using asio::co_spawn;
    using asio::use_awaitable;

    std::unique_ptr<mysqlc::IConnector>
    make_mysql_connector(asio::io_context& io_ctx, mysql::connect_params params, std::string alias);

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
        std::string addConnection(mysql::connect_params connection_param, const std::string& uuid);
        std::string addConnection(http_server::ConnectionParams connection_param);
        void removeConnection(const std::string& uuid);

        template<typename Callable>
        requires std::invocable<Callable, const boost::mysql::results&>
            std::future<std::invoke_result_t<Callable, const boost::mysql::results&>>
            executeQuery(const std::string& uuid, std::string_view query, Callable handler) {
            auto conn = connections_.find(uuid);
            if (conn == connections_.end()) {
                std::cerr << "[ConnectorManager::executeQuery]  Invalid connection uuid: " << uuid << "\n";
                throw std::runtime_error("[ConnectorManager::executeQuery]  Invalid connection uuid: " + uuid);
            }
            if (conn->second->status() == Status::Closed) {
                std::cerr << "[ConnectorManager::executeQuery]  Connector is not connected\n";
                throw std::runtime_error("[ConnectorManager::executeQuery]  Connector is not connected\n");
            }
            if (!conn->second->isConnected()) {
                try {
                    conn->second->tryReconnect();
                } catch (const std::exception& e) {
                    actor_zeta::send(catalog_manager_->address(),
                                     catalog_manager_->address(),
                                     catalog_manager::handler_id(catalog_manager::route::remove_connection_schema),
                                     uuid);
                    throw std::runtime_error("Failed to reconnect. Error message: " + std::string(e.what()));
                }
            }
            return co_spawn(thread_pool_manager_.ctx(), conn->second->runQuery(query, handler), asio::use_future);
        }

        size_t totalConnections() const noexcept;
        std::optional<mysql::connect_params> conn_params(const std::string& uuid) const;
        bool hasConnection(const std::string& uuid) const noexcept;

    private:
        thread_pool_manager thread_pool_manager_;
        actor_zeta::address_t catalog_manager_;
        connector_factory make_connector_;
        std::unordered_map<std::string, std::unique_ptr<mysqlc::IConnector>> connections_;
    };
} // namespace mysqlc