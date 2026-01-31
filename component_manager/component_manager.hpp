// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/catalog_manager.hpp"
#include "connectors/mysql_manager.hpp"
#include "db_integration/otterbrix/otterbrix_manager.hpp"
#include "db_integration/sql/connection_manager.hpp"
#include "scheduler/scheduler.hpp"

#include <actor-zeta.hpp>
#include <otterbrix/otterbrix.hpp>

#include <memory_resource>

#include <memory>

class ComponentManager {
public:
    explicit ComponentManager(const configuration::config& config);
    std::pmr::memory_resource* getResource();
    std::string getLogPath();
    std::shared_ptr<mysqlc::ConnectorManager> db_connection_manager() const;
    actor_zeta::address_t scheduler_address() const;
    actor_zeta::address_t catalog_address() const;
    actor_zeta::address_t otterbrix_manager_address() const;
    actor_zeta::address_t sql_connection_manager_address() const;
private:
    otterbrix::otterbrix_ptr otterbrix_{nullptr};
    std::pmr::memory_resource* resource_{nullptr};
    std::string log_path_;
    std::shared_ptr<mysqlc::ConnectorManager> db_connector_manager_{nullptr};
    std::unique_ptr<mysqlc::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    std::unique_ptr<db_conn::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    std::unique_ptr<db_conn::SqlConnectionManager, actor_zeta::pmr::deleter_t> sql_connection_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_{nullptr,
                                                                      actor_zeta::pmr::deleter_t{getResource()}};
};