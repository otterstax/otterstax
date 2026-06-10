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

#include "connectors/api_connections/pg_connection_config.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/thread_pool_manager.hpp"

#include <components/expressions/compare_expression.hpp>

namespace pg {

    namespace asio = boost::asio;
    using asio::awaitable;
    using asio::co_spawn;
    using asio::use_awaitable;

    std::unique_ptr<pg::IConnector> make_pg_connector(connect_params params, std::string alias);

    class ConnectorManager {
    public:
        ConnectorManager(actor_zeta::address_t catalog_manager,
                         connector_factory make_connector = make_pg_connector,
                         size_t pool_size = std::thread::hardware_concurrency());
        thread_pool_status status() const noexcept;
        void start();
        void stop();

        std::string addConnection(connect_params connection_param, const std::string& uuid);
        std::string addConnection(http_server::PgConnectionParams connection_param);
        void removeConnection(const std::string& uuid);

        template<typename Callable>
        requires std::invocable<Callable, PGresult*> std::future<std::invoke_result_t<Callable, PGresult*>>
        executeQuery(const std::string& uuid, std::string_view query, Callable handler) {
            OTX_ZONE_N("pg::ConnectorManager::executeQuery");
            auto conn = connections_.find(uuid);
            if (conn == connections_.end()) {
                log_->error("[PgConnectorManager::executeQuery] Invalid connection uuid: {}", uuid);
                throw std::runtime_error("[PgConnectorManager::executeQuery] Invalid connection uuid: " + uuid);
            }
            if (conn->second->status() == Status::Closed) {
                log_->error("[PgConnectorManager::executeQuery] Connector is not connected");
                throw std::runtime_error("[PgConnectorManager::executeQuery] Connector is not connected\n");
            }
            if (!conn->second->isConnected()) {
                try {
                    conn->second->tryReconnect();
                } catch (const std::exception& e) {
                    notify_connection_removed(uuid);
                    throw std::runtime_error("Failed to reconnect. Error message: " + std::string(e.what()));
                }
            }
            return co_spawn(thread_pool_manager_.ctx(), conn->second->runQuery(query, handler), asio::use_future);
        }

        size_t totalConnections() const noexcept;
        std::optional<connect_params> conn_params(const std::string& uuid) const;
        bool hasConnection(const std::string& uuid) const noexcept;

        void fetch_enum_types(const std::string& uuid);
        tsl::pg_enum_oid_map enums_for(const std::string& uuid) const;

    private:
        void notify_connection_removed(const std::string& uuid);

        log_t log_;
        thread_pool_manager thread_pool_manager_;
        actor_zeta::address_t catalog_manager_;
        connector_factory make_connector_;
        std::unordered_map<std::string, std::unique_ptr<pg::IConnector>> connections_;
        std::unordered_map<std::string, tsl::pg_enum_oid_map> enums_;
    };
} // namespace pg
