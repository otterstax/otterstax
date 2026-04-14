// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "utility/logger.hpp"

namespace filec {

    std::unique_ptr<filec::IConnector> make_file_connector(connect_params params, std::string alias) {
        return std::make_unique<filec::FileConnector>(std::move(params), std::move(alias));
    }

    ConnectorManager::ConnectorManager(actor_zeta::address_t catalog_manager,
                                       connector_factory make_connector,
                                       size_t pool_size)
        : log_(get_logger(logger_tag::CONNECTOR_MANAGER))
        , thread_pool_manager_(pool_size)
        , catalog_manager_(catalog_manager)
        , make_connector_(make_connector) {
    }

    thread_pool_status ConnectorManager::status() const noexcept { return thread_pool_manager_.status(); }
    void ConnectorManager::start() { thread_pool_manager_.start(); }
    void ConnectorManager::stop() { thread_pool_manager_.stop(); }

    std::string ConnectorManager::addConnection(connect_params connection_param) {
        std::string uuid = connection_param.alias;
        try {
            log_->debug("[FileConnectorManager] Adding file connection: {} -> {}", uuid, connection_param.path);
            connections_[uuid] = make_connector_(connection_param, uuid);
            connections_[uuid]->connect();

            collection_full_name_t name(uuid, uuid, "", uuid);
            actor_zeta::send(catalog_manager_->address(),
                             catalog_manager_->address(),
                             catalog_manager::handler_id(catalog_manager::route::add_connection_schema),
                             std::move(name));
            return uuid;
        } catch (const std::exception& e) {
            log_->error("[FileConnectorManager] Error: {}", e.what());
            if (connections_.contains(uuid)) {
                connections_[uuid]->close();
                connections_.erase(uuid);
            }
            throw std::runtime_error("Add file connection error: " + std::string(e.what()));
        }
    }

    void ConnectorManager::removeConnection(const std::string& uuid) {
        auto conn = connections_.find(uuid);
        if (conn == connections_.end()) {
            throw std::runtime_error("[FileConnectorManager] Invalid connection uuid: " + uuid);
        }
        conn->second->close();
        connections_.erase(uuid);
        actor_zeta::send(catalog_manager_->address(),
                         catalog_manager_->address(),
                         catalog_manager::handler_id(catalog_manager::route::remove_connection_schema),
                         uuid);
    }

    size_t ConnectorManager::totalConnections() const noexcept { return connections_.size(); }
    bool ConnectorManager::hasConnection(const std::string& uuid) const noexcept { return connections_.contains(uuid); }

} // namespace filec
