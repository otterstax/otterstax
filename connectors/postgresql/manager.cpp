// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "catalog/catalog_manager.hpp"
#include "utility/connection_uid.hpp"
#include "utility/logger.hpp"
#include "utility/parse_port.hpp"
#include "utility/settled_future.hpp"
#include "utility/tracy_profiler.hpp"


using namespace components;

namespace pg {

    std::unique_ptr<pg::IConnector>
    make_pg_connector(std::pmr::memory_resource* resource, connect_params params, std::string alias) {
        return std::make_unique<pg::Connector>(resource, std::move(params), std::move(alias));
    }

    ConnectorManager::ConnectorManager(std::pmr::memory_resource* resource,
                                       actor_zeta::address_t catalog_manager,
                                       connector_factory make_connector,
                                       size_t pool_size)
        : resource_(resource)
        , log_(get_logger(logger_tag::CONNECTOR_MANAGER))
        , thread_pool_manager_(pool_size)
        , catalog_manager_(catalog_manager)
        , make_connector_(make_connector) {
        assert(log_.is_valid());
        assert(resource_ != nullptr);
        assert(make_connector_ != nullptr);
    }

    thread_pool_status ConnectorManager::status() const noexcept { return thread_pool_manager_.status(); }

    void ConnectorManager::start() { thread_pool_manager_.start(); }

    void ConnectorManager::stop() { thread_pool_manager_.stop(); }

    core::result_wrapper_t<std::string>
    ConnectorManager::addConnection(connect_params connection_param, const std::string& uuid) {
        OTX_ZONE_N("pg::ConnectorManager::addConnection");
        // The catalog's discovery queries run on the pool's io_context; without a
        // running pool they would wait forever.
        if (thread_pool_manager_.status() != thread_pool_status::RUNNING) {
            log_->error("Add PostgreSQL connection {} failed: connector thread pool is not running", uuid);
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"[PgConnectorManager::addConnection] Connector thread pool is not running",
                                 resource_});
        }

        log_->debug("Try add PostgreSQL connection with uuid: {}", uuid);
        auto connector = make_connector_(resource_, connection_param, uuid);
        if (auto err = connector->connect(); err.contains_error()) {
            log_->error("Add PostgreSQL connection {} failed: {}", uuid, err.what);
            return std::move(err);
        }
        connections_[uuid] = std::move(connector);

        // For PostgreSQL: use schema.table format
        // qualified_name_t(database, schema, collection) -> database.schema, with unique_identifier=uuid
        // We need to store: uuid=alias, database, schema, table
        qualified_name_t name(uuid,
                                    connection_param.database,
                                    connection_param.schema.empty() ? "public" : connection_param.schema,
                                    connection_param.table);
        log_->debug("Creating collection_full_name: uid={}, db={}, schema={}, table={}",
                    name.unique_identifier,
                    name.database,
                    name.schema,
                    name.collection);
        // The catalog answers synchronously (running the probes through the
        // PostgreSQL backend actor that owns this manager), and a connection
        // whose schema could not be registered is unusable for federated SQL,
        // so it is closed and dropped here.
        auto [needs_sched, registered] = actor_zeta::send(catalog_manager_,
                                                          &mysql::CatalogManager::add_connection_schema,
                                                          std::move(name),
                                                          catalog_ext::ConnectionType::PostgreSQL);
        if (auto err = otterstax::take_settled_error(std::move(registered), resource_); err.contains_error()) {
            log_->error("Add PostgreSQL connection {} failed: schema registration: {}", uuid, err.what);
            connections_[uuid]->close();
            connections_.erase(uuid);
            return std::move(err);
        }
        return uuid;
    }

    core::result_wrapper_t<std::string>
    ConnectorManager::addConnection(conn::api_server::PgConnectionParams connection_param) {
        OTX_ZONE_N("pg::ConnectorManager::addConnection(http)");
        connect_params params;
        log_->debug("Try add PostgreSQL connection with alias: {}", connection_param.alias);
        log_->debug("Host: {}", connection_param.host);

        params.host = connection_param.host;
        if (!connection_param.port.empty()) {
            log_->debug("Port: {}", connection_param.port);
            auto port = otterstax::parse_port(connection_param.port, resource_);
            if (port.has_error()) {
                log_->error("Add PostgreSQL connection {} failed: {}", connection_param.alias, port.error().what);
                return port.convert_error<std::string>();
            }
            params.port = port.value();
        }
        params.username = connection_param.username;
        params.password = connection_param.password;
        params.database = connection_param.database;
        params.schema = connection_param.schema.empty() ? "public" : connection_param.schema;
        params.table = connection_param.table;

        log_->debug("Schema: {}, Table: {}", params.schema, params.table);

        return addConnection(params, connection_param.alias);
    }

    size_t ConnectorManager::totalConnections() const noexcept { return connections_.size(); }

    bool ConnectorManager::hasConnection(const std::string& uuid) const noexcept { return connections_.contains(uuid); }
} // namespace pg
