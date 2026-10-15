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

namespace mysql {

    std::unique_ptr<mysql::IConnector> make_mysql_connector(std::pmr::memory_resource* resource,
                                                            asio::io_context& io_ctx,
                                                            bm::connect_params params,
                                                            std::string alias) {
        return std::make_unique<mysql::Connector>(resource, io_ctx, std::move(params), std::move(alias));
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
    ConnectorManager::addConnection(bm::connect_params connection_param, const std::string& uuid) {
        OTX_ZONE_N("mysql::ConnectorManager::addConnection");
        // connect() completes on the pool's io_context; without a running pool it
        // would wait forever, and so would the catalog's discovery queries.
        if (thread_pool_manager_.status() != thread_pool_status::RUNNING) {
            log_->error("Add connection {} failed: connector thread pool is not running", uuid);
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"[ConnectorManager::addConnection] Connector thread pool is not running",
                                 resource_});
        }

        log_->debug("Try add connection with uuid: {}", uuid);
        auto connector = make_connector_(resource_, thread_pool_manager_.ctx(), connection_param, uuid);
        if (auto err = connector->connect(); err.contains_error()) {
            log_->error("Add connection {} failed: {}", uuid, err.what);
            return std::move(err);
        }
        connections_[uuid] = std::move(connector);

        // Empty collection → the catalog discovers every table of the
        // configured database via information_schema (unified discovery
        // contract; the alias is NOT a table name), running the probes through
        // the MySQL backend actor that owns this manager. The catalog answers
        // synchronously, and a connection whose schema could not be registered
        // is unusable for federated SQL, so it is closed and dropped here.
        qualified_name_t name(uuid, connection_param.database, "", "");
        auto [needs_sched, registered] = actor_zeta::send(catalog_manager_,
                                                          &mysql::CatalogManager::add_connection_schema,
                                                          std::move(name),
                                                          catalog_ext::ConnectionType::MySQL);
        if (auto err = otterstax::take_settled_error(std::move(registered), resource_); err.contains_error()) {
            log_->error("Add connection {} failed: schema registration: {}", uuid, err.what);
            connections_[uuid]->close();
            connections_.erase(uuid);
            return std::move(err);
        }
        return uuid;
    }

    core::result_wrapper_t<std::string>
    ConnectorManager::addConnection(conn::api_server::ConnectionParams connection_param) {
        OTX_ZONE_N("mysql::ConnectorManager::addConnection(http)");
        boost::mysql::connect_params params;
        log_->debug("Try add connection with alias: {}", connection_param.alias);
        log_->debug("Host: {}", connection_param.host);
        if (!connection_param.port.empty()) {
            log_->debug("Port: {}", connection_param.port);
            auto port = otterstax::parse_port(connection_param.port, resource_);
            if (port.has_error()) {
                log_->error("Add connection {} failed: {}", connection_param.alias, port.error().what);
                return port.convert_error<std::string>();
            }
            params.server_address.emplace_host_and_port(connection_param.host, port.value());
        } else {
            params.server_address.emplace_host_and_port(connection_param.host);
        }
        params.username = connection_param.username;
        params.password = connection_param.password;
        params.database = connection_param.database;
        return addConnection(params, connection_param.alias);
    }


    core::error_t ConnectorManager::control(IConnector& connector, const char* command) {
        if (thread_pool_manager_.status() != thread_pool_status::RUNNING) {
            return core::error_t{core::error_code_t::io_error,
                std::pmr::string{"Connector thread pool is not running", resource_}};
        }
        try {
            return otterstax::spawn_marshaled<otterstax::asio_error_t>(
                thread_pool_manager_.ctx(),
                otterstax::run_with_owned_handler<otterstax::asio_error_t, const boost::mysql::results&>(
                    connector, command, []( const boost::mysql::results&) { return otterstax::asio_error_t{}; }), resource_).get();
        } catch (const std::exception& e) {
            return core::error_t{core::error_code_t::io_error, std::pmr::string{e.what(), resource_}};
        }
    }

    core::error_t ConnectorManager::begin_transaction(session_id session, const std::string& alias) {
        if (thread_pool_manager_.status() != thread_pool_status::RUNNING) {
            return core::error_t{core::error_code_t::io_error,
                std::pmr::string{"Connector thread pool is not running", resource_}};
        }
        if (transactions_.contains(session)) {
            return core::error_t{core::error_code_t::invalid_parameter,
                std::pmr::string{"Transaction connection already exists", resource_}};
        }
        auto base = connections_.find(alias);
        if (base == connections_.end()) {
            return core::error_t{core::error_code_t::do_not_exists,
                std::pmr::string{"Unknown transaction connection alias", resource_}};
        }
        return open_transaction(session, alias, base->second->params());
    }

    core::result_wrapper_t<std::unique_ptr<ConnectorManager>>
    ConnectorManager::fork_transaction(session_id session, const std::string& alias) {
        auto base = connections_.find(alias);
        if (base == connections_.end()) {
            return core::error_t{core::error_code_t::do_not_exists,
                std::pmr::string{"Unknown transaction connection alias", resource_}};
        }
        auto child = std::make_unique<ConnectorManager>(resource_, actor_zeta::address_t::empty_address(), make_connector_, 1);
        child->start();
        if (auto error = child->open_transaction(session, alias, base->second->params()); error.contains_error()) {
            return error;
        }
        return child;
    }

    core::error_t ConnectorManager::open_transaction(session_id session, const std::string& alias, bm::connect_params params) {
        try {
            auto connector = make_connector_(resource_, thread_pool_manager_.ctx(), std::move(params), alias);
            if (auto err = connector->connect(); err.contains_error()) return err;
            if (auto err = control(*connector, "START TRANSACTION"); err.contains_error()) {
                connector->close();
                return err;
            }
            transactions_.emplace(session, transaction_connection{alias, std::move(connector)});
            return core::error_t::no_error();
        } catch (const std::exception& e) {
            return core::error_t{core::error_code_t::io_error, std::pmr::string{e.what(), resource_}};
        }
    }

    core::error_t ConnectorManager::finish_transaction(session_id session, bool commit) {
        auto active = transactions_.extract(session);
        if (active.empty()) {
            // Rollback also cleans up a failed begin or an already aborted block.
            if (!commit) return core::error_t::no_error();
            return core::error_t{core::error_code_t::invalid_parameter,
                std::pmr::string{"Transaction connection does not exist", resource_}};
        }
        auto outcome = control(*active.mapped().connector, commit ? "COMMIT" : "ROLLBACK");
        // A transport failure during COMMIT has an unknown outcome. Never retry
        // it, reconnect, or return this socket to the autocommit connection.
        active.mapped().connector->close();
        return outcome;
    }

    size_t ConnectorManager::totalConnections() const noexcept { return connections_.size(); }

    bool ConnectorManager::hasConnection(const std::string& uuid) const noexcept {
        if (connections_.contains(uuid)) return true;
        for (const auto& [session, transaction] : transactions_) {
            if (transaction.alias == uuid) return true;
        }
        return false;
    }
} // namespace mysql
