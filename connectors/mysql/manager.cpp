// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "catalog/catalog_manager.hpp"
#include "utility/connection_uid.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <tuple>

using namespace components;

namespace mysql {

    std::unique_ptr<mysql::IConnector>
    make_mysql_connector(asio::io_context& io_ctx, bm::connect_params params, std::string alias) {
        return std::make_unique<mysql::Connector>(io_ctx, std::move(params), std::move(alias));
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
    std::string ConnectorManager::addConnection(bm::connect_params connection_param, const std::string& uuid) {
        OTX_ZONE_N("mysql::ConnectorManager::addConnection");
        try {
            std::string addr = std::string(connection_param.server_address.hostname()) + ":" +
                               std::to_string(connection_param.server_address.port());

            log_->debug("Try add connection with uuid: {}", uuid);
            connections_[uuid] = make_connector_(thread_pool_manager_.ctx(), connection_param, uuid);
            connections_[uuid]->connect();

            // Empty collection → CatalogManager discovers every table of the
            // configured database via information_schema (unified discovery
            // contract; the alias is NOT a table name).
            qualified_name_t name(uuid, connection_param.database, "", "");
            std::ignore = actor_zeta::send(catalog_manager_, &mysql::CatalogManager::add_connection_schema, std::move(name));
            return uuid;
        } catch (const boost::mysql::error_with_diagnostics& e) {
            log_->error("MySQL error occurred - Error code: {}, Message: {}, Diagnostics: {}",
                        e.code().value(),
                        e.what(),
                        e.get_diagnostics().server_message());
            // make_connector_()/connect() may throw before a live connector is
            // stored (operator[] then leaves a null entry). Guard the close() so
            // a failed add logs + rethrows instead of dereferencing null — the
            // caller (register_connections) then retries per connection_retry.
            if (auto it = connections_.find(uuid); it != connections_.end()) {
                if (it->second) {
                    it->second->close();
                }
                connections_.erase(it);
            }
            throw std::runtime_error("Add connection asio error: " + std::string(e.what()));
        } catch (const std::exception& e) {
            log_->error("Error: {}", e.what());
            if (auto it = connections_.find(uuid); it != connections_.end()) {
                if (it->second) {
                    it->second->close();
                }
                connections_.erase(it);
            }
            throw std::runtime_error("Add connection common error: " + std::string(e.what()));
        }
    }

    // TODO not threadsafe!!!
    std::string ConnectorManager::addConnection(conn::api_server::ConnectionParams connection_param) {
        OTX_ZONE_N("mysql::ConnectorManager::addConnection(http)");
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
        OTX_ZONE_N("mysql::ConnectorManager::removeConnection");
        auto conn = connections_.find(uuid);
        if (conn == connections_.end()) {
            log_->error("Invalid connection uuid: {}", uuid);
            throw std::runtime_error("Invalid connection uuid: : " + uuid);
        }
        conn->second->close();
        connections_.erase(uuid);
        notify_connection_removed(uuid);
    }

    size_t ConnectorManager::totalConnections() const noexcept { return connections_.size(); }

    std::optional<bm::connect_params> ConnectorManager::conn_params(const std::string& uuid) const {
        auto conn = connections_.find(uuid);
        if (conn == connections_.end()) {
            return std::nullopt;
        }
        return conn->second->params();
    }

    bool ConnectorManager::hasConnection(const std::string& uuid) const noexcept { return connections_.contains(uuid); }

    void ConnectorManager::notify_connection_removed(const std::string& uuid) {
        std::ignore = actor_zeta::send(catalog_manager_, &mysql::CatalogManager::remove_connection_schema, uuid);
    }
} // namespace mysql
