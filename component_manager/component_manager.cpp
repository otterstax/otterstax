// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "component_manager.hpp"

ComponentManager::ComponentManager(const configuration::config& config)
    : otterbrix_(otterbrix::make_otterbrix(config))
    , resource_(otterbrix_->dispatcher()->resource()) {
    // To test otterbrix create some tables
    assert(resource_ != nullptr && "memory resource must not be null");

    catalog_manager_ = actor_zeta::spawn_supervisor<mysqlc::CatalogManager>(resource_);
    assert(catalog_manager_ != nullptr && "catalog manager must not be null");

    db_connector_manager_ = std::make_shared<mysqlc::ConnectorManager>(catalog_manager_->address());
    catalog_manager_->set_connector_manager(db_connector_manager_); // cyclic dependency

    otterbrix_manager_ =
        actor_zeta::spawn_supervisor<db_conn::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_));
    assert(otterbrix_manager_ != nullptr && "otterbrix manager must not be null");

    sql_connection_manager_ =
        actor_zeta::spawn_supervisor<db_conn::SqlConnectionManager>(resource_, db_connector_manager_);
    assert(sql_connection_manager_ != nullptr && "sql connection manager must not be null");

    scheduler_ = actor_zeta::spawn_supervisor<Scheduler>(resource_,
                                                         make_parser(resource_),
                                                         sql_connection_manager_->address(),
                                                         otterbrix_manager_->address(),
                                                         catalog_manager_->address());

    assert(scheduler_ != nullptr && "scheduler must not be null");

    // Start connector manager
    db_connector_manager_->start();
}

std::pmr::memory_resource* ComponentManager::getResource() {
    assert(resource_);
    return resource_;
}

std::shared_ptr<mysqlc::ConnectorManager> ComponentManager::db_connection_manager() const { return db_connector_manager_; }

actor_zeta::address_t ComponentManager::scheduler_address() const { return scheduler_->address(); }

actor_zeta::address_t ComponentManager::catalog_address() const { return catalog_manager_->address(); }

actor_zeta::address_t ComponentManager::otterbrix_manager_address() const { return otterbrix_manager_->address(); }

actor_zeta::address_t ComponentManager::sql_connection_manager_address() const { return sql_connection_manager_->address(); }