// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// #include "db_integration/nosql/connection_manager.hpp"
#include "catalog/catalog_manager.hpp"
#include "db_integration/otterbrix/otterbrix_manager.hpp"
#include "db_integration/sql/connection_manager.hpp"
#include "scheduler/scheduler.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/parser.hpp"
#include "../mock/sql_db_connector.hpp"

#include "utility/logger.hpp"

#include <actor-zeta.hpp>
#include <otterbrix/otterbrix.hpp>

#include <catch2/catch.hpp>
#include <chrono>

namespace {
    otterbrix::otterbrix_ptr init_otterbrix() {
        auto config = configuration::config::default_config();

        auto log_path = config.log.path.string();
        initialize_all_loggers(log_path);

        return otterbrix::make_otterbrix(std::move(config));
    }
} // namespace

TEST_CASE("base test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = std::pmr::get_default_resource(); // no ottterbrix processing, use default for easier mock
    assert(resource);

    auto catalog_manager = actor_zeta::spawn_supervisor<mysqlc::CatalogManager>(resource);
    auto conn_manager =
        std::make_shared<mysqlc::ConnectorManager>(catalog_manager->address(), make_mysql_mock_connector);

    conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager =
        actor_zeta::spawn_supervisor<db_conn::OtterbrixManager>(resource,
                                                                std::make_unique<SimpleMockOtterbrixManager>());
    auto sql_conn_manager = actor_zeta::spawn_supervisor<db_conn::SqlConnectionManager>(resource, conn_manager);

    auto scheduler = actor_zeta::spawn_supervisor<Scheduler>(resource,
                                                             std::make_unique<SimpleMockParser>(),
                                                             sql_conn_manager->address(),
                                                             otterbrix_manager->address(),
                                                             catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_id id; // Use session_id type for consistency
    auto shared_data = create_cv_wrapper(flight_data(resource));
    // Register shared data in scheduler
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    actor_zeta::send(scheduler->address(),
                     scheduler->address(),
                     scheduler::handler_id(scheduler::route::execute),
                     id.hash(),
                     shared_data,
                     sql);
    shared_data->wait_for(5000ms);
    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;
    REQUIRE(shared_data->status() == cv_wrapper::Status::Ok);
    REQUIRE(shared_data->result.chunk.size() == 2);
}

TEST_CASE("Error in connector test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto catalog_manager = actor_zeta::spawn_supervisor<mysqlc::CatalogManager>(resource);
    auto conn_manager =
        std::make_shared<mysqlc::ConnectorManager>(catalog_manager->address(), make_mysql_mock_connector_throw);

    conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager =
        actor_zeta::spawn_supervisor<db_conn::OtterbrixManager>(resource,
                                                                std::make_unique<SimpleMockOtterbrixManager>());
    auto sql_conn_manager = actor_zeta::spawn_supervisor<db_conn::SqlConnectionManager>(resource, conn_manager);

    auto scheduler = actor_zeta::spawn_supervisor<Scheduler>(resource,
                                                             std::make_unique<SimpleMockParser>(),
                                                             sql_conn_manager->address(),
                                                             otterbrix_manager->address(),
                                                             catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto shared_data = create_cv_wrapper(flight_data(resource));
    // Register shared data in scheduler
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    actor_zeta::send(scheduler->address(),
                     scheduler->address(),
                     scheduler::handler_id(scheduler::route::execute),
                     id,
                     shared_data,
                     sql);
    shared_data->wait_for(5000ms);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(shared_data->status() == cv_wrapper::Status::Error);
    REQUIRE(shared_data->error_message() == "MockConnector: exception in runQuery");
    REQUIRE(shared_data->result.chunk.empty() == true);
}

TEST_CASE("Error in otterbrix test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto catalog_manager = actor_zeta::spawn_supervisor<mysqlc::CatalogManager>(resource);
    auto conn_manager =
        std::make_shared<mysqlc::ConnectorManager>(catalog_manager->address(), make_mysql_mock_connector);

    conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager = actor_zeta::spawn_supervisor<db_conn::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.can_throw = true}));
    auto sql_conn_manager = actor_zeta::spawn_supervisor<db_conn::SqlConnectionManager>(resource, conn_manager);

    auto scheduler = actor_zeta::spawn_supervisor<Scheduler>(resource,
                                                             std::make_unique<SimpleMockParser>(),
                                                             sql_conn_manager->address(),
                                                             otterbrix_manager->address(),
                                                             catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto shared_data = create_cv_wrapper(flight_data(resource));
    // Register shared data in scheduler
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    actor_zeta::send(scheduler->address(),
                     scheduler->address(),
                     scheduler::handler_id(scheduler::route::execute),
                     id,
                     shared_data,
                     sql);
    shared_data->wait_for(5000ms);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(shared_data->status() == cv_wrapper::Status::Error);
    REQUIRE(shared_data->error_message() == "SimpleMockOtterbrixManager: exception in execute_plan");
    REQUIRE(shared_data->result.chunk.empty() == true);
}

TEST_CASE("Error in scheduler test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto catalog_manager = actor_zeta::spawn_supervisor<mysqlc::CatalogManager>(resource);
    auto conn_manager =
        std::make_shared<mysqlc::ConnectorManager>(catalog_manager->address(), make_mysql_mock_connector);

    conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager =
        actor_zeta::spawn_supervisor<db_conn::OtterbrixManager>(resource,
                                                                std::make_unique<SimpleMockOtterbrixManager>());
    auto sql_conn_manager = actor_zeta::spawn_supervisor<db_conn::SqlConnectionManager>(resource, conn_manager);

    auto scheduler =
        actor_zeta::spawn_supervisor<Scheduler>(resource,
                                                std::make_unique<SimpleMockParser>(mock_config{.can_throw = true}),
                                                sql_conn_manager->address(),
                                                otterbrix_manager->address(),
                                                catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto shared_data = create_cv_wrapper(flight_data(resource));
    // Register shared data in scheduler
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    actor_zeta::send(scheduler->address(),
                     scheduler->address(),
                     scheduler::handler_id(scheduler::route::execute),
                     id,
                     shared_data,
                     sql);
    shared_data->wait_for(5000ms);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(shared_data->status() == cv_wrapper::Status::Error);
    REQUIRE(shared_data->error_message() == "SimpleMockParser: exception in parse");
    REQUIRE(shared_data->result.chunk.empty() == true);
}

TEST_CASE("Error in otterbrix + sql connector test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto catalog_manager = actor_zeta::spawn_supervisor<mysqlc::CatalogManager>(resource);
    auto conn_manager =
        std::make_shared<mysqlc::ConnectorManager>(catalog_manager->address(), make_mysql_mock_connector_throw);

    conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager = actor_zeta::spawn_supervisor<db_conn::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.can_throw = true}));
    auto sql_conn_manager = actor_zeta::spawn_supervisor<db_conn::SqlConnectionManager>(resource, conn_manager);

    auto scheduler = actor_zeta::spawn_supervisor<Scheduler>(resource,
                                                             std::make_unique<SimpleMockParser>(),
                                                             sql_conn_manager->address(),
                                                             otterbrix_manager->address(),
                                                             catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto shared_data = create_cv_wrapper(flight_data(resource));
    // Register shared data in scheduler
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    actor_zeta::send(scheduler->address(),
                     scheduler->address(),
                     scheduler::handler_id(scheduler::route::execute),
                     id,
                     shared_data,
                     sql);
    shared_data->wait_for(5000ms);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(shared_data->status() == cv_wrapper::Status::Error);
    REQUIRE(shared_data->error_message() == "MockConnector: exception in runQuery");
    REQUIRE(shared_data->result.chunk.empty() == true);
}

TEST_CASE("return empty test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto catalog_manager = actor_zeta::spawn_supervisor<mysqlc::CatalogManager>(resource);
    auto conn_manager =
        std::make_shared<mysqlc::ConnectorManager>(catalog_manager->address(), make_mysql_mock_connector);

    conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager = actor_zeta::spawn_supervisor<db_conn::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.return_empty = true}));
    auto sql_conn_manager = actor_zeta::spawn_supervisor<db_conn::SqlConnectionManager>(resource, conn_manager);

    auto scheduler = actor_zeta::spawn_supervisor<Scheduler>(resource,
                                                             std::make_unique<SimpleMockParser>(),
                                                             sql_conn_manager->address(),
                                                             otterbrix_manager->address(),
                                                             catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto shared_data = create_cv_wrapper(flight_data(resource));
    // Register shared data in scheduler
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    actor_zeta::send(scheduler->address(),
                     scheduler->address(),
                     scheduler::handler_id(scheduler::route::execute),
                     id,
                     shared_data,
                     sql);
    shared_data->wait_for(5000ms);
    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;
    REQUIRE(shared_data->status() == cv_wrapper::Status::Empty);
    REQUIRE(shared_data->result.chunk.empty() == true);
}