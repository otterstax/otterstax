// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/catalog_manager.hpp"
#include "connectors/mysql/manager.hpp"
#include "connectors/postgresql/manager.hpp"
#include "connectors/clickhouse/manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "scheduler/scheduler.hpp"

#include <actor-zeta.hpp>
#include <otterbrix/otterbrix.hpp>

#include <memory_resource>

#include <memory>

class ComponentManager {
public:
    explicit ComponentManager(const configuration::config& config);
    ~ComponentManager();
    std::pmr::memory_resource* getResource();
    std::string getLogPath();
    std::shared_ptr<mysql::ConnectorManager> db_connection_manager() const;
    std::shared_ptr<pg::ConnectorManager> pg_connection_manager() const;
    std::shared_ptr<ch::ConnectorManager> ch_connection_manager() const;
    actor_zeta::address_t scheduler_address() const;
    actor_zeta::address_t catalog_address() const;
    actor_zeta::address_t otterbrix_manager_address() const;
    actor_zeta::address_t sql_connection_manager_address() const;
    actor_zeta::address_t pg_connection_manager_address() const;
private:
    otterbrix::otterbrix_ptr otterbrix_{nullptr};
    std::pmr::memory_resource* resource_{nullptr};
    std::string log_path_;
    std::shared_ptr<mysql::ConnectorManager> db_connector_manager_{nullptr};
    std::shared_ptr<pg::ConnectorManager> pg_connector_manager_{nullptr};
    std::shared_ptr<ch::ConnectorManager> ch_connector_manager_{nullptr};
    std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> sql_connection_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> pg_connection_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    std::unique_ptr<db::ClickhouseManager, actor_zeta::pmr::deleter_t> ch_connection_manager_actor_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    // The work-sharing thread pool that drives the Worker actors. Declared before
    // scheduler_ so it outlives it: scheduler_ (workers + event loop) is destroyed
    // first, the (already-stopped) pool after. Stopped in ~ComponentManager before
    // any actor is destroyed.
    std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_{nullptr};
    std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_{nullptr,
                                                                      actor_zeta::pmr::deleter_t{getResource()}};
};