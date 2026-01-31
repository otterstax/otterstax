// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_manager.hpp"
#include "utility/connection_uid.hpp"
#include "utility/logger.hpp"

using namespace components;

namespace mysqlc {

    std::unique_ptr<mysqlc::IConnector>
    make_mysql_connector(asio::io_context& io_ctx, mysql::connect_params params, std::string alias) {
        return std::make_unique<mysqlc::Connector>(io_ctx, std::move(params), std::move(alias));
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

    // TODO add query for adding and removing connections
    // TODO this is not thread safe!!!
    std::string ConnectorManager::addConnection(mysql::connect_params connection_param, const std::string& uuid) {
        try {
            std::string addr = std::string(connection_param.server_address.hostname()) + ":" +
                               std::to_string(connection_param.server_address.port());

            log_->debug("Try add connection with uuid: {}", uuid);
            connections_[uuid] = make_connector_(thread_pool_manager_.ctx(), connection_param, uuid);
            connections_[uuid]->connect();

            collection_full_name_t name(connection_param.database, uuid, uuid); // treat uuid as schema
            actor_zeta::send(catalog_manager_->address(),
                             catalog_manager_->address(),
                             catalog_manager::handler_id(catalog_manager::route::add_connection_schema),
                             std::move(name));
            return uuid;
        } catch (const boost::mysql::error_with_diagnostics& e) {
            log_->error("MySQL error occurred - Error code: {}, Message: {}, Diagnostics: {}",
                        e.code().value(),
                        e.what(),
                        e.get_diagnostics().server_message());
            connections_[uuid]->close();
            connections_.erase(uuid);
            throw std::runtime_error("Add connection asio error: " + std::string(e.what()));
        } catch (const std::exception& e) {
            log_->error("Error: {}", e.what());
            connections_[uuid]->close();
            connections_.erase(uuid);
            throw std::runtime_error("Add connection common error: " + std::string(e.what()));
        }
    }

    // TODO not threadsafe!!!
    std::string ConnectorManager::addConnection(http_server::ConnectionParams connection_param) {
        boost::mysql::connect_params params;
        log_->debug("Try add connection with alias: {}", connection_param.alias);
        log_->debug("Host: {}", connection_param.host);
        if (!connection_param.port.empty()) {
            log_->debug("Port: {}", connection_param.port);
            params.server_address.emplace_host_and_port(connection_param.host, std::stoi(connection_param.port));
        } else {
            params.server_address.emplace_host_and_port(connection_param.host);
        }
        params.username = connection_param.username;
        params.password = connection_param.password;
        params.database = connection_param.database;
        return addConnection(params, connection_param.alias);
    }

    void ConnectorManager::removeConnection(const std::string& uuid) {
        auto conn = connections_.find(uuid);
        if (conn == connections_.end()) {
            log_->error("Invalid connection uuid: {}", uuid);
            throw std::runtime_error("Invalid connection uuid: : " + uuid);
        }
        conn->second->close();
        connections_.erase(uuid);
        actor_zeta::send(catalog_manager_->address(),
                         catalog_manager_->address(),
                         catalog_manager::handler_id(catalog_manager::route::remove_connection_schema),
                         uuid);
    }

    size_t ConnectorManager::totalConnections() const noexcept { return connections_.size(); }

    std::optional<mysql::connect_params> ConnectorManager::conn_params(const std::string& uuid) const {
        auto conn = connections_.find(uuid);
        if (conn == connections_.end()) {
            return std::nullopt;
        }
        return conn->second->params();
    }

    bool ConnectorManager::hasConnection(const std::string& uuid) const noexcept { return connections_.contains(uuid); }
} // namespace mysqlc