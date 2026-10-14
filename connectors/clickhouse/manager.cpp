// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "utility/connection_uid.hpp"
#include "utility/logger.hpp"
#include "utility/parse_port.hpp"
#include "utility/tracy_profiler.hpp"


using namespace components;

namespace ch {

    std::unique_ptr<ch::IConnector>
    make_ch_connector(std::pmr::memory_resource* resource, connect_params params, std::string alias) {
        return std::make_unique<ch::Connector>(resource, std::move(params), std::move(alias));
    }

    ConnectorManager::ConnectorManager(std::pmr::memory_resource* resource,
                                       connector_factory make_connector,
                                       size_t io_threads)
        : resource_(resource)
        , log_(get_logger(logger_tag::CONNECTOR_MANAGER))
        , thread_pool_manager_(io_threads)
        , make_connector_(make_connector) {
        assert(log_.is_valid());
        assert(resource_ != nullptr);
        assert(make_connector_ != nullptr);
    }

    thread_pool_status ConnectorManager::status() const noexcept { return thread_pool_manager_.status(); }

    void ConnectorManager::start() { thread_pool_manager_.start(); }

    void ConnectorManager::stop() { thread_pool_manager_.stop(); }

    void ConnectorManager::shutdown() noexcept {
        OTX_ZONE_N("ch::ConnectorManager::shutdown");
        if (stopping_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        // The registry is read-only after startup (addConnection / removeConnection), so walking it
        // here races nothing; each pool locks its own state.
        for (auto& [uuid, pool] : connections_) {
            pool->close();
            pool->for_each_in_use([](IConnector& connector) { connector.cancel(); });
        }
    }

    core::result_wrapper_t<std::string>
    ConnectorManager::addConnection(connect_params connection_param, const std::string& uuid, std::size_t pool_size) {
        OTX_ZONE_N("ch::ConnectorManager::addConnection");
        if (pool_size == 0) {
            log_->error("Add ClickHouse connection {} failed: pool_size must be at least 1", uuid);
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{("[ChConnectorManager::addConnection] pool_size of '" + uuid + "' must be at least 1")
                                     .c_str(),
                                 resource_});
        }
        // The discovery queries run on the pool's io_context; without a
        // running pool they would wait forever.
        if (thread_pool_manager_.status() != thread_pool_status::RUNNING) {
            log_->error("Add ClickHouse connection {} failed: connector thread pool is not running", uuid);
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"[ChConnectorManager::addConnection] Connector thread pool is not running",
                                 resource_});
        }

        log_->debug("Try add ClickHouse connection with uuid: {} (pool_size {})", uuid, pool_size);
        std::vector<std::unique_ptr<ch::IConnector>> connectors;
        connectors.reserve(pool_size);
        for (std::size_t i = 0; i < pool_size; ++i) {
            auto connector = make_connector_(resource_, connection_param, uuid);
            if (auto err = connector->connect(); err.contains_error()) {
                log_->error("Add ClickHouse connection {} failed (connection {} of {}): {}",
                            uuid,
                            i + 1,
                            pool_size,
                            err.what);
                for (auto& opened : connectors) {
                    opened->close();
                }
                return std::move(err);
            }
            connectors.push_back(std::move(connector));
        }
        connections_[uuid] = std::make_unique<connection_pool>(std::move(connectors));
        return uuid;
    }

    core::result_wrapper_t<std::string>
    ConnectorManager::addConnection(conn::api_server::ChConnectionParams connection_param) {
        OTX_ZONE_N("ch::ConnectorManager::addConnection(params)");
        connect_params params;
        log_->debug("Try add ClickHouse connection with alias: {}", connection_param.alias);
        log_->debug("Host: {}", connection_param.host);

        params.host = connection_param.host;
        if (!connection_param.port.empty()) {
            log_->debug("Port: {}", connection_param.port);
            auto port = otterstax::parse_port(connection_param.port, resource_);
            if (port.has_error()) {
                log_->error("Add ClickHouse connection {} failed: {}", connection_param.alias, port.error().what);
                return port.convert_error<std::string>();
            }
            params.port = port.value();
        }
        params.username = connection_param.username;
        params.password = connection_param.password;
        params.database = connection_param.database;
        params.table = connection_param.table;

        log_->debug("Database: {}", params.database);
        log_->debug("Table: {}", params.table);

        return addConnection(params, connection_param.alias, connection_param.pool_size);
    }

    core::result_wrapper_t<connection_pool::handle_t> ConnectorManager::checkout_(const std::string& uuid,
                                                                                 connection_pool& pool) {
        OTX_ZONE_N("ch::ConnectorManager::checkout");
        auto checked_out = pool.checkout_for(checkoutTimeout());
        if (!checked_out) {
            std::string what = "[ChConnectorManager::checkout] no free connection of '" + uuid + "' within " +
                               std::to_string(checkoutTimeout().count()) + " ms (pool_size " +
                               std::to_string(pool.size()) + ")";
            log_->error(what);
            return core::error_t(core::error_code_t::io_error, std::pmr::string{what.c_str(), resource_});
        }
        connection_pool::handle_t handle = std::move(*checked_out);
        if (handle->status() == Status::Closed) {
            log_->error("[ChConnectorManager::executeQuery] Connector is not connected");
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"[ChConnectorManager::executeQuery] Connector is not connected", resource_});
        }
        if (!handle->isConnected()) {
            if (auto err = handle->tryReconnect(); err.contains_error()) {
                return std::move(err);
            }
        }
        return std::move(handle);
    }

    void ConnectorManager::setCheckoutTimeout(std::chrono::milliseconds timeout) noexcept {
        checkout_timeout_ms_.store(timeout.count(), std::memory_order_relaxed);
    }

    std::chrono::milliseconds ConnectorManager::checkoutTimeout() const noexcept {
        return std::chrono::milliseconds{checkout_timeout_ms_.load(std::memory_order_relaxed)};
    }

    size_t ConnectorManager::totalConnections() const noexcept { return connections_.size(); }

    bool ConnectorManager::hasConnection(const std::string& uuid) const noexcept { return connections_.contains(uuid); }

    size_t ConnectorManager::poolSize(const std::string& uuid) const noexcept {
        auto pool = connections_.find(uuid);
        return pool == connections_.end() ? 0 : pool->second->size();
    }

    size_t ConnectorManager::idleConnections(const std::string& uuid) const {
        auto pool = connections_.find(uuid);
        return pool == connections_.end() ? 0 : pool->second->idle();
    }

    void ConnectorManager::removeConnection(const std::string& uuid) {
        OTX_ZONE_N("ch::ConnectorManager::removeConnection");
        auto it = connections_.find(uuid);
        if (it == connections_.end()) {
            return;
        }
        it->second->for_each_idle([](ch::IConnector& connector) { connector.close(); });
        connections_.erase(it);
    }

} // namespace ch
