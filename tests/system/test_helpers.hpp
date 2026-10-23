// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Helpers shared by the system tests that build an actor graph by hand rather
// than through scheduler_stack_owner: polling a cross-actor future from a plain
// thread, a default-config engine, and the backends over mock drivers wired the
// way ComponentManager wires the real ones.

#pragma once

#include "../mock/mock_driver.hpp"
#include "catalog/catalog_manager.hpp"
#include "integration/backends.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "utility/logger.hpp"
#include "utility/settled_future.hpp"

#include <otterbrix/otterbrix.hpp>

#include <catch2/catch_all.hpp>

#include <chrono>
#include <memory>
#include <memory_resource>
#include <string>
#include <thread>
#include <utility>

namespace otterstax::test {

    // Polls `future` every 10 ms until it is ready or `timeout` has passed and
    // reports whether it settled. It asserts nothing, so a thread other than the
    // test thread may call it (Catch2 assertions are not thread-safe).
    template<typename Future>
    bool poll_until_ready(Future& future, std::chrono::milliseconds timeout) {
        using namespace std::chrono_literals;
        for (auto waited = 0ms; waited < timeout && !future.is_ready(); waited += 10ms) {
            std::this_thread::sleep_for(10ms);
        }
        return future.is_ready();
    }

    // Test-thread form: the future must settle within 10 s.
    template<typename Future>
    void wait_until_ready(Future& future) {
        using namespace std::chrono_literals;
        REQUIRE(poll_until_ready(future, 10s));
    }

    // A default-config engine (no data directory), for tests that never persist
    // anything — unlike scheduler_stack.hpp's init_test_otterbrix.
    inline db::otterbrix_engine_ptr init_default_test_otterbrix() {
        auto config = configuration::config::default_config();
        initialize_all_loggers(config.log.path.string());
        return db::make_otterbrix_engine(std::move(config));
    }

    // What ComponentManager::register_connections does for one connection: the
    // alias's pool opens, then the catalog discovers its tables (`scope`), and a
    // connection whose discovery fails is dropped again. Answers the first
    // failure.
    inline core::error_t register_backend(std::pmr::memory_resource* resource,
                                          conn::connection_pool_t& pool,
                                          actor_zeta::address_t catalog,
                                          const std::string& alias,
                                          std::unique_ptr<conn::IDriver> driver,
                                          qualified_name_t scope,
                                          catalog_ext::ConnectionType type,
                                          conn::pool_config_t config = {}) {
        if (auto err = pool.add(alias, std::move(driver), config); err.contains_error()) {
            return err;
        }
        auto [needs_sched, registered] =
            actor_zeta::send(catalog, &mysql::CatalogManager::add_connection_schema, std::move(scope), type);
        auto err = otterstax::take_settled_error(std::move(registered), resource);
        if (err.contains_error()) {
            pool.drop(alias);
        }
        return err;
    }

    // The connection pools and decode metadata of the three backend types — what
    // the catalog and every Worker build their backends over — with the catalog's
    // own backends wired in (unless `wire_catalog` is false: a catalog without
    // backends refuses every registration). Declare it after the catalog and
    // before the Scheduler: it outlives the Workers and is gone before the
    // catalog.
    class mock_backends {
    public:
        mock_backends(std::pmr::memory_resource* resource, mysql::CatalogManager& catalog, bool wire_catalog = true)
            : resource_(resource)
            , catalog_(catalog.address())
            , registry_(resource)
            , catalog_backends_(resource, registry_) {
            if (wire_catalog) {
                catalog.set_backends(
                    {&catalog_backends_.mysql, &catalog_backends_.postgresql, &catalog_backends_.clickhouse});
            }
        }

        mock_backends(const mock_backends&) = delete;
        mock_backends& operator=(const mock_backends&) = delete;

        // A MySQL connection over a mock driver. The whole database is discovered and
        // the mock lists no table, so registration succeeds with none; a table a
        // statement references is then registered lazily on its first query.
        core::error_t add_mysql(const std::string& alias, mock_config config = {}, const std::string& database = "db") {
            return add(backend_type_t::MySQL, alias, mock(alias, config), qualified_name_t(alias, database, "", ""));
        }
        core::error_t add_postgresql(const std::string& alias,
                                     const std::string& database,
                                     const std::string& schema,
                                     const std::string& table,
                                     mock_config config = {}) {
            return add(backend_type_t::PostgreSQL,
                       alias,
                       mock(alias, config),
                       qualified_name_t(alias, database, schema, table));
        }
        core::error_t add_clickhouse(const std::string& alias,
                                     const std::string& database,
                                     const std::string& table,
                                     mock_config config = {}) {
            return add(backend_type_t::ClickHouse, alias, mock(alias, config), qualified_name_t(alias, database, "", table));
        }

        // Any driver, registered as register_connections registers one.
        core::error_t add(backend_type_t type,
                          const std::string& alias,
                          std::unique_ptr<conn::IDriver> driver,
                          qualified_name_t scope,
                          conn::pool_config_t config = {}) {
            switch (type) {
                case backend_type_t::MySQL:
                    return register_backend(resource_, registry_.mysql, catalog_, alias, std::move(driver),
                                            std::move(scope), catalog_ext::ConnectionType::MySQL, config);
                case backend_type_t::PostgreSQL:
                    return register_backend(resource_, registry_.postgresql, catalog_, alias, std::move(driver),
                                            std::move(scope), catalog_ext::ConnectionType::PostgreSQL, config);
                case backend_type_t::ClickHouse:
                    return register_backend(resource_, registry_.clickhouse, catalog_, alias, std::move(driver),
                                            std::move(scope), catalog_ext::ConnectionType::ClickHouse, config);
                default:
                    break;
            }
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{"mock_backends: MySQL, PostgreSQL and ClickHouse only", resource_});
        }

        db::backend_registry_t& registry() noexcept { return registry_; }
        // The catalog's backends; a test may drive one directly (otterstax::test::drive).
        db::backend_set_t& backends() noexcept { return catalog_backends_; }

    private:
        std::unique_ptr<conn::IDriver> mock(const std::string& alias, mock_config config) const {
            if (config.resource == nullptr) {
                config.resource = resource_;
            }
            return std::make_unique<mock_driver_t>(std::move(config), alias);
        }

        std::pmr::memory_resource* resource_;
        actor_zeta::address_t catalog_;
        db::backend_registry_t registry_;
        db::backend_set_t catalog_backends_;
    };

} // namespace otterstax::test
