// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "catalog/catalog_manager.hpp"
#include "utility/connection_uid.hpp"
#include "utility/logger.hpp"

#include <tuple>

using namespace components;

namespace chc {

    std::unique_ptr<chc::IConnector> make_ch_connector(connect_params params, std::string alias) {
        return std::make_unique<chc::Connector>(std::move(params), std::move(alias));
    }

    ConnectorManager::ConnectorManager(actor_zeta::address_t catalog_manager,
                                       connector_factory make_connector,
                                       size_t pool_size)
        : log_(get_logger(logger_tag::CONNECTOR_MANAGER))
        , thread_pool_manager_(pool_size)
        , catalog_manager_(catalog_manager)
        , make_connector_(make_connector) {
        assert(log_.is_valid());
    }

    thread_pool_status ConnectorManager::status() const noexcept { return thread_pool_manager_.status(); }

    void ConnectorManager::start() { thread_pool_manager_.start(); }

    void ConnectorManager::stop() { thread_pool_manager_.stop(); }

    std::string ConnectorManager::addConnection(connect_params connection_param, const std::string& uuid) {
        try {
            std::string addr = connection_param.host + ":" + std::to_string(connection_param.port);

            log_->debug("Try add ClickHouse connection with uuid: {}", uuid);
            connections_[uuid] = make_connector_(connection_param, uuid);
            connections_[uuid]->connect();

            // ClickHouse: no schema level, use database.table
            collection_full_name_t name(uuid, connection_param.database, "", connection_param.table);
            log_->debug("Creating collection_full_name: uid={}, db={}, schema={}, table={}",
                        name.unique_identifier,
                        name.database,
                        name.schema,
                        name.collection);
            std::ignore = actor_zeta::send(catalog_manager_, &mysqlc::CatalogManager::add_connection_schema, std::move(name));
            return uuid;
        } catch (const std::exception& e) {
            log_->error("ClickHouse Error: {}", e.what());
            if (connections_.contains(uuid)) {
                connections_[uuid]->close();
                connections_.erase(uuid);
            }
            throw std::runtime_error("Add ClickHouse connection error: " + std::string(e.what()));
        }
    }

    std::string ConnectorManager::addConnection(http_server::ChConnectionParams connection_param) {
        connect_params params;
        log_->debug("Try add ClickHouse connection with alias: {}", connection_param.alias);
        log_->debug("Host: {}", connection_param.host);

        params.host = connection_param.host;
        if (!connection_param.port.empty()) {
            log_->debug("Port: {}", connection_param.port);
            params.port = static_cast<uint16_t>(std::stoi(connection_param.port));
        }
        params.username = connection_param.username;
        params.password = connection_param.password;
        params.database = connection_param.database;
        params.table = connection_param.table;

        log_->debug("Database: {}", params.database);
        log_->debug("Table: {}", params.table);

        return addConnection(params, connection_param.alias);
    }

    void ConnectorManager::removeConnection(const std::string& uuid) {
        auto conn = connections_.find(uuid);
        if (conn == connections_.end()) {
            log_->error("Invalid connection uuid: {}", uuid);
            throw std::runtime_error("Invalid connection uuid: " + uuid);
        }
        conn->second->close();
        connections_.erase(uuid);
        notify_connection_removed(uuid);
    }

    size_t ConnectorManager::totalConnections() const noexcept { return connections_.size(); }

    std::optional<connect_params> ConnectorManager::conn_params(const std::string& uuid) const {
        auto conn = connections_.find(uuid);
        if (conn == connections_.end()) {
            return std::nullopt;
        }
        return conn->second->params();
    }

    bool ConnectorManager::hasConnection(const std::string& uuid) const noexcept { return connections_.contains(uuid); }

    void ConnectorManager::notify_connection_removed(const std::string& uuid) {
        std::ignore = actor_zeta::send(catalog_manager_, &mysqlc::CatalogManager::remove_connection_schema, uuid);
    }
} // namespace chc
