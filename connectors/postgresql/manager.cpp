// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "catalog/catalog_manager.hpp"
#include "utility/connection_uid.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <tuple>

using namespace components;

namespace pg {

    std::unique_ptr<pg::IConnector> make_pg_connector(connect_params params, std::string alias) {
        return std::make_unique<pg::Connector>(std::move(params), std::move(alias));
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
        OTX_ZONE_N("pg::ConnectorManager::addConnection");
        try {
            std::string addr = connection_param.host + ":" + std::to_string(connection_param.port);

            log_->debug("Try add PostgreSQL connection with uuid: {}", uuid);
            connections_[uuid] = make_connector_(connection_param, uuid);
            connections_[uuid]->connect();

            // For PostgreSQL: use schema.table format
            // collection_full_name_t(database, schema, collection) -> database.schema, with unique_identifier=uuid
            // We need to store: uuid=alias, database, schema, table
            collection_full_name_t name(uuid,
                                        connection_param.database,
                                        connection_param.schema.empty() ? "public" : connection_param.schema,
                                        connection_param.table);
            log_->debug("Creating collection_full_name: uid={}, db={}, schema={}, table={}",
                        name.unique_identifier,
                        name.database,
                        name.schema,
                        name.collection);
            std::ignore = actor_zeta::send(catalog_manager_, &mysql::CatalogManager::add_connection_schema, std::move(name));
            return uuid;
        } catch (const std::exception& e) {
            log_->error("PostgreSQL Error: {}", e.what());
            if (connections_.contains(uuid)) {
                connections_[uuid]->close();
                connections_.erase(uuid);
            }
            throw std::runtime_error("Add PostgreSQL connection error: " + std::string(e.what()));
        }
    }

    std::string ConnectorManager::addConnection(http_server::PgConnectionParams connection_param) {
        OTX_ZONE_N("pg::ConnectorManager::addConnection(http)");
        connect_params params;
        log_->debug("Try add PostgreSQL connection with alias: {}", connection_param.alias);
        log_->debug("Host: {}", connection_param.host);

        params.host = connection_param.host;
        if (!connection_param.port.empty()) {
            log_->debug("Port: {}", connection_param.port);
            params.port = static_cast<uint16_t>(std::stoi(connection_param.port));
        }
        params.username = connection_param.username;
        params.password = connection_param.password;
        params.database = connection_param.database;
        params.schema = connection_param.schema.empty() ? "public" : connection_param.schema;
        params.table = connection_param.table;

        log_->debug("Schema: {}, Table: {}", params.schema, params.table);

        return addConnection(params, connection_param.alias);
    }

    void ConnectorManager::removeConnection(const std::string& uuid) {
        OTX_ZONE_N("pg::ConnectorManager::removeConnection");
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

    void ConnectorManager::fetch_enum_types(const std::string& uuid) {
        OTX_ZONE_N("pg::ConnectorManager::fetch_enum_types");
        const std::string query =
            "SELECT t.oid, t.typname, e.enumlabel "
            "FROM pg_type t "
            "JOIN pg_enum e ON e.enumtypid = t.oid "
            "WHERE t.typtype = 'e' "
            "ORDER BY t.oid, e.enumsortorder;";

        auto handler = [this, uuid](PGresult* result) -> otterstax::asio_error_t {
            if (!result) {
                return otterstax::asio_error_t{};
            }
            tsl::pg_enum_oid_map& map = enums_[uuid];
            int nrows = PQntuples(result);
            for (int i = 0; i < nrows; ++i) {
                const char* oid_str = PQgetvalue(result, i, 0);
                const char* typname = PQgetvalue(result, i, 1);
                const char* enumlabel = PQgetvalue(result, i, 2);
                if (!oid_str || !typname || !enumlabel) {
                    continue;
                }
                unsigned int oid = static_cast<unsigned int>(std::stoul(oid_str));
                auto& desc = map[oid];
                if (desc.typname.empty()) {
                    desc.typname = typname;
                }
                desc.values.emplace_back(enumlabel);
            }
            log_->info("fetch_enum_types: cached {} ENUM types for uuid={}", map.size(), uuid);
            for (const auto& [oid, desc] : map) {
                std::string joined;
                for (const auto& v : desc.values) {
                    if (!joined.empty()) {
                        joined += ',';
                    }
                    joined += v;
                }
                log_->debug("  enum oid={} typname={} values=[{}]", oid, desc.typname, joined);
            }
            return otterstax::asio_error_t{};
        };

        try {
            auto fut = executeQuery(uuid, query, handler);
            (void) fut.get();
        } catch (const std::exception& e) {
            log_->warn("fetch_enum_types: query failed for uuid={}: {}", uuid, e.what());
        }
    }

    tsl::pg_enum_oid_map ConnectorManager::enums_for(const std::string& uuid) const {
        auto it = enums_.find(uuid);
        if (it == enums_.end()) {
            return {};
        }
        return it->second;
    }

    void ConnectorManager::notify_connection_removed(const std::string& uuid) {
        std::ignore = actor_zeta::send(catalog_manager_, &mysql::CatalogManager::remove_connection_schema, uuid);
        enums_.erase(uuid);
    }
} // namespace pg
