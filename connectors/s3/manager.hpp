// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "connector.hpp"

#include <concepts>
#include <coroutine>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "routes/catalog_manager.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/thread_pool_manager.hpp"

namespace s3c {

    namespace asio = boost::asio;
    using asio::co_spawn;

    std::unique_ptr<s3c::IConnector>
    make_s3_connector(connect_params params, std::string alias);

    class ConnectorManager {
    public:
        ConnectorManager(actor_zeta::address_t catalog_manager,
                         connector_factory make_connector = make_s3_connector,
                         size_t pool_size = std::thread::hardware_concurrency());
        thread_pool_status status() const noexcept;
        void start();
        void stop();

        std::string addConnection(connect_params connection_param);
        void removeConnection(const std::string& uuid);

        template<typename Callable>
        requires std::invocable<Callable, const S3Data&>
            std::future<std::invoke_result_t<Callable, const S3Data&>>
            executeQuery(const std::string& uuid, std::string_view query, Callable handler) {
            auto conn = connections_.find(uuid);
            if (conn == connections_.end()) {
                log_->error("[S3ConnectorManager::executeQuery] Invalid connection uuid: {}", uuid);
                throw std::runtime_error("[S3ConnectorManager::executeQuery] Invalid connection uuid: " + uuid);
            }
            if (conn->second->isClosed()) {
                log_->error("[S3ConnectorManager::executeQuery] Connector is closed");
                throw std::runtime_error("[S3ConnectorManager::executeQuery] Connector is closed");
            }
            if (!conn->second->isConnected()) {
                conn->second->tryReconnect();
            }
            return co_spawn(thread_pool_manager_.ctx(), conn->second->runQuery(query, handler), asio::use_future);
        }

        size_t totalConnections() const noexcept;
        bool hasConnection(const std::string& uuid) const noexcept;

    private:
        log_t log_;
        thread_pool_manager thread_pool_manager_;
        actor_zeta::address_t catalog_manager_;
        connector_factory make_connector_;
        std::unordered_map<std::string, std::unique_ptr<s3c::IConnector>> connections_;
    };

} // namespace s3c
