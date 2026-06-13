// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "catalog/catalog_manager.hpp"
#include "utility/connection_uid.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <tuple>

using namespace components;

namespace ch {

    std::unique_ptr<ch::IConnector> make_ch_connector(connect_params params, std::string alias) {
        return std::make_unique<ch::Connector>(std::move(params), std::move(alias));
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
        OTX_ZONE_N("ch::ConnectorManager::addConnection");
        try {
            std::string addr = connection_param.host + ":" + std::to_string(connection_param.port);

            log_->debug("Try add ClickHouse connection with uuid: {}", uuid);
            connections_[uuid] = make_connector_(connection_param, uuid);
            connections_[uuid]->connect();

            // ClickHouse: no schema level, use database.table
            qualified_name_t name(uuid, connection_param.database, "", connection_param.table);
            log_->debug("Creating collection_full_name: uid={}, db={}, schema={}, table={}",
                        name.unique_identifier,
                        name.database,
                        name.schema,
                        name.collection);
            std::ignore = actor_zeta::send(catalog_manager_, &mysql::CatalogManager::add_connection_schema, std::move(name));
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
        OTX_ZONE_N("ch::ConnectorManager::addConnection(http)");
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
        OTX_ZONE_N("ch::ConnectorManager::removeConnection");
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

    void ConnectorManager::fetch_named_types(const std::string& uuid,
                                             const std::string& database,
                                             const std::string& table) {
        OTX_ZONE_N("ch::ConnectorManager::fetch_named_types");
        auto conn = connections_.find(uuid);
        if (conn == connections_.end()) {
            log_->warn("fetch_named_types: no connection for uid={}", uuid);
            return;
        }

        std::string query =
            "SELECT name, type FROM system.columns WHERE database = '" + database + "' AND table = '" + table + "'";
        auto handler = [this, uuid, table](const std::vector<clickhouse::Block>& blocks) -> int {
            auto& slot = named_types_[uuid][table];
            for (const auto& block : blocks) {
                if (block.GetColumnCount() < 2 || block.GetRowCount() == 0)
                    continue;
                auto name_col = block[0]->As<clickhouse::ColumnString>();
                auto type_col = block[1]->As<clickhouse::ColumnString>();
                if (!name_col || !type_col) {
                    log_->warn("fetch_named_types: unexpected system.columns shape");
                    continue;
                }
                for (size_t i = 0; i < block.GetRowCount(); ++i) {
                    std::string col_name(name_col->At(i));
                    std::string col_type(type_col->At(i));
                    log_->debug("fetch_named_types: {}.{}.{} -> {}", uuid, table, col_name, col_type);
                    slot.emplace(std::move(col_name), std::move(col_type));
                }
            }
            return 0;
        };

        try {
            auto fut = co_spawn(thread_pool_manager_.ctx(), conn->second->runQuery(query, handler), asio::use_future);
            fut.get();
        } catch (const std::exception& e) {
            log_->error("fetch_named_types failed for {}.{}: {}", database, table, e.what());
        }
    }

    std::unordered_map<std::string, std::string> ConnectorManager::named_types_for(const std::string& uuid,
                                                                                   const std::string& table) const {
        auto u_it = named_types_.find(uuid);
        if (u_it == named_types_.end())
            return {};
        auto t_it = u_it->second.find(table);
        if (t_it == u_it->second.end())
            return {};
        return t_it->second;
    }

    void ConnectorManager::notify_connection_removed(const std::string& uuid) {
        std::ignore = actor_zeta::send(catalog_manager_, &mysql::CatalogManager::remove_connection_schema, uuid);
    }
} // namespace ch
