// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "component_manager.hpp"
#include "utility/logger.hpp"

ComponentManager::ComponentManager(const configuration::config& config)
    : otterbrix_(otterbrix::make_otterbrix(config))
    , resource_(otterbrix_->dispatcher()->resource())
    , log_path_(config.log.path.c_str()) {
    initialize_all_loggers(log_path_);

    // To test otterbrix create some tables
    assert(resource_ != nullptr && "memory resource must not be null");

    catalog_manager_ = actor_zeta::spawn_supervisor<mysqlc::CatalogManager>(resource_);
    assert(catalog_manager_ != nullptr && "catalog manager must not be null");

    db_connector_manager_ = std::make_shared<mysqlc::ConnectorManager>(catalog_manager_->address());
    catalog_manager_->set_mysql_connector_manager(db_connector_manager_); // cyclic dependency

    pg_connector_manager_ = std::make_shared<pgc::ConnectorManager>(catalog_manager_->address());
    catalog_manager_->set_pg_connector_manager(pg_connector_manager_);

    ch_connector_manager_ = std::make_shared<chc::ConnectorManager>(catalog_manager_->address());
    catalog_manager_->set_ch_connector_manager(ch_connector_manager_);

    otterbrix_manager_ =
        actor_zeta::spawn_supervisor<db_conn::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_));
    assert(otterbrix_manager_ != nullptr && "otterbrix manager must not be null");

    sql_connection_manager_ =
        actor_zeta::spawn_supervisor<db_conn::SqlConnectionManager>(resource_, db_connector_manager_);
    assert(sql_connection_manager_ != nullptr && "sql connection manager must not be null");

    pg_connection_manager_ =
        actor_zeta::spawn_supervisor<db_conn::PgConnectionManager>(resource_, pg_connector_manager_);
    assert(pg_connection_manager_ != nullptr && "pg connection manager must not be null");

    ch_connection_manager_actor_ =
        actor_zeta::spawn_supervisor<db_conn::ChConnectionManager>(resource_, ch_connector_manager_);
    assert(ch_connection_manager_actor_ != nullptr && "ch connection manager must not be null");

    file_connector_manager_ = std::make_shared<filec::ConnectorManager>(catalog_manager_->address());
    catalog_manager_->set_file_connector_manager(file_connector_manager_);

    file_connection_manager_actor_ =
        actor_zeta::spawn_supervisor<db_conn::FileConnectionManager>(resource_, file_connector_manager_);
    assert(file_connection_manager_actor_ != nullptr && "file connection manager must not be null");

    scheduler_ = actor_zeta::spawn_supervisor<Scheduler>(resource_,
                                                         make_parser(resource_),
                                                         sql_connection_manager_->address(),
                                                         pg_connection_manager_->address(),
                                                         ch_connection_manager_actor_->address(),
                                                         file_connection_manager_actor_->address(),
                                                         otterbrix_manager_->address(),
                                                         catalog_manager_->address());

    assert(scheduler_ != nullptr && "scheduler must not be null");

    // Start connector managers
    db_connector_manager_->start();
    pg_connector_manager_->start();
    ch_connector_manager_->start();
    file_connector_manager_->start();
}

std::pmr::memory_resource* ComponentManager::getResource() {
    assert(resource_);
    return resource_;
}

std::string ComponentManager::getLogPath() { return log_path_; }

std::shared_ptr<mysqlc::ConnectorManager> ComponentManager::db_connection_manager() const { return db_connector_manager_; }

std::shared_ptr<pgc::ConnectorManager> ComponentManager::pg_connection_manager() const { return pg_connector_manager_; }

std::shared_ptr<chc::ConnectorManager> ComponentManager::ch_connection_manager() const { return ch_connector_manager_; }

actor_zeta::address_t ComponentManager::scheduler_address() const { return scheduler_->address(); }

actor_zeta::address_t ComponentManager::catalog_address() const { return catalog_manager_->address(); }

actor_zeta::address_t ComponentManager::otterbrix_manager_address() const { return otterbrix_manager_->address(); }

actor_zeta::address_t ComponentManager::sql_connection_manager_address() const { return sql_connection_manager_->address(); }

actor_zeta::address_t ComponentManager::pg_connection_manager_address() const { return pg_connection_manager_->address(); }
