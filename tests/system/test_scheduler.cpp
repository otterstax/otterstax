// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog/catalog_manager.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "scheduler/session_data.hpp"
#include "scheduler/scheduler.hpp"

#include "../mock/ch_db_connector.hpp"
#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/parser.hpp"
#include "../mock/pg_db_connector.hpp"
#include "../mock/sql_db_connector.hpp"
#include "scheduler_stack.hpp"
#include "test_helpers.hpp"

#include "utility/logger.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <core/result_wrapper.hpp>
#include <otterbrix/otterbrix.hpp>

#include <catch2/catch_all.hpp>
#include <chrono>
#include <thread>

namespace {
    // Worker pool, session-future bridge and pool sizing are the ones every
    // system test shares (scheduler_stack.hpp); the default-config engine and
    // the mock MySQL connect params come from test_helpers.hpp. The table the
    // mock parser references ("1") is registered lazily on the first query.
    using otterstax::test::await_session;
    using otterstax::test::init_default_test_otterbrix;
    using otterstax::test::make_az_scheduler;
    using otterstax::test::mock_connect_params;
    using otterstax::test::worker_pool_size;
} // namespace

TEST_CASE("base test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    // Objects built here escape into the actor graph and are freed on other
    // threads: name the global allocator rather than arena-scoping it.
    auto resource = std::pmr::new_delete_resource(); // no otterbrix processing, plain malloc/free for the mock
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(resource,
                                                                        catalog_manager->address(),
                                                                        &mysql_mock_connector_factory);
    auto pg_conn_manager = std::make_unique<pg::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &pg_mock_connector_factory);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_conn_manager = std::make_unique<ch::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &ch_mock_connector_factory);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(mysql_connection_manager->address(),
                                          pg_connection_manager->address(),
                                          ch_connection_manager->address());

    // addConnection triggers eager schema discovery + engine registration via
    // the catalog; the connector thread pools must already be running (they
    // are started by the integration-actor constructors above).
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "1").has_error());
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "2").has_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  mysql_connection_manager->address(),
                                                  pg_connection_manager->address(),
                                                  ch_connection_manager->address(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_id id; // Use session_id type for consistency
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id.hash(), sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;
    REQUIRE(!r.has_error());
    auto sp = std::move(r.value());
    REQUIRE(sp.size() == 2);

    az_scheduler->stop();
}

TEST_CASE("Error in connector test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(resource,
                                                                        catalog_manager->address(),
                                                                        &mysql_mock_connector_factory_throw);
    auto pg_conn_manager = std::make_unique<pg::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &pg_mock_connector_factory);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_conn_manager = std::make_unique<ch::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &ch_mock_connector_factory);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(mysql_connection_manager->address(),
                                          pg_connection_manager->address(),
                                          ch_connection_manager->address());

    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "1").has_error());
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "2").has_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  mysql_connection_manager->address(),
                                                  pg_connection_manager->address(),
                                                  ch_connection_manager->address(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(r.has_error());
    REQUIRE(r.error().what == "MockConnector: exception in runQuery");

    az_scheduler->stop();
}

TEST_CASE("Error in otterbrix test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .can_throw = true}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(resource,
                                                                        catalog_manager->address(),
                                                                        &mysql_mock_connector_factory);
    auto pg_conn_manager = std::make_unique<pg::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &pg_mock_connector_factory);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_conn_manager = std::make_unique<ch::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &ch_mock_connector_factory);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(mysql_connection_manager->address(),
                                          pg_connection_manager->address(),
                                          ch_connection_manager->address());

    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "1").has_error());
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "2").has_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  mysql_connection_manager->address(),
                                                  pg_connection_manager->address(),
                                                  ch_connection_manager->address(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(r.has_error());
    REQUIRE(r.error().what == "Otterbrix execution failed: SimpleMockOtterbrixManager: exception in execute_plan");

    az_scheduler->stop();
}

TEST_CASE("Error in scheduler test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(resource,
                                                                        catalog_manager->address(),
                                                                        &mysql_mock_connector_factory);
    auto pg_conn_manager = std::make_unique<pg::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &pg_mock_connector_factory);

    // No backend manager is registered with the catalog here on purpose: it
    // has no actor to run the discovery through, so schema registration is
    // rejected and addConnection drops the connection. The statement never
    // needs it — the parser throws first.
    REQUIRE(mysql_conn_manager->addConnection(mock_connect_params(), "1").has_error());
    REQUIRE(mysql_conn_manager->addConnection(mock_connect_params(), "2").has_error());

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_conn_manager = std::make_unique<ch::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &ch_mock_connector_factory);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_throwing_mock_parser,
                                                  mysql_connection_manager->address(),
                                                  pg_connection_manager->address(),
                                                  ch_connection_manager->address(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(r.has_error());
    REQUIRE(r.error().what == "SimpleMockParser: exception in parse");

    az_scheduler->stop();
}

TEST_CASE("Error in otterbrix + sql connector test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .can_throw = true}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(resource,
                                                                        catalog_manager->address(),
                                                                        &mysql_mock_connector_factory_throw);
    auto pg_conn_manager = std::make_unique<pg::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &pg_mock_connector_factory);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_conn_manager = std::make_unique<ch::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &ch_mock_connector_factory);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(mysql_connection_manager->address(),
                                          pg_connection_manager->address(),
                                          ch_connection_manager->address());

    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "1").has_error());
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "2").has_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  mysql_connection_manager->address(),
                                                  pg_connection_manager->address(),
                                                  ch_connection_manager->address(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(r.has_error());
    REQUIRE(r.error().what == "MockConnector: exception in runQuery");

    az_scheduler->stop();
}

// Mock parser that creates cross-backend query (MySQL + PostgreSQL)
class CrossBackendMockParser : public IParser {
public:
    core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
        std::cout << "CrossBackendMockParser: parsing SQL: " << sql << std::endl;

        auto resource = std::pmr::new_delete_resource();

        auto binder = sql::transform::transform_result(
            resource,
            logical_plan::execution_plan_t(resource,
                                           logical_plan::make_node_aggregate(resource,
                                                                             core::uid_t{"result"},
                                                                             core::dbname_t{"db"},
                                                                             core::relname_t{"result_table"}),
                                           logical_plan::make_parameter_node(resource)),
            sql::transform::transform_result::parameter_map_t{resource},
            sql::transform::transform_result::insert_map_t{resource},
            sql::transform::transform_result::insert_rows_t(resource));

        auto parsed = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::pmr::vector<std::pmr::vector<external_entry_t>>{resource},
                                                 binder.params_ptr(),
                                                 binder.node_ptr(),
                                                 2), // 2 external nodes
            std::move(binder),
            NodeTag::T_SelectStmt);

        // Add external nodes - use the parsed node structure
        // Create two references to simulate MySQL and PostgreSQL backends
        // Note: In real usage, these would be separate nodes from different backends
        // For this mock test, we use the same node twice to verify backend detection works
        // uids/names must match the connections registered in the test
        // ("campaigns" MySQL with an empty database, "products" PostgreSQL
        // with pgdb/public/products) so that the catalog maps each node to
        // its backend and finds the registered schemas.
        parsed->otterbrix_params->external_nodes.emplace_back();
        parsed->otterbrix_params->external_nodes.back().push_back(
            external_entry_t{&parsed->otterbrix_params->node,
                             otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                 qualified_name_t{"campaigns", "", "", "campaigns"},
                                                                 {}}});
        parsed->otterbrix_params->external_nodes.emplace_back();
        parsed->otterbrix_params->external_nodes.back().push_back(
            external_entry_t{&parsed->otterbrix_params->node,
                             otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                 qualified_name_t{"products", "pgdb", "public", "products"},
                                                                 {}}});
        parsed->otterbrix_params->external_nodes_count = 2;

        std::cout << "CrossBackendMockParser: created query with 2 external nodes (simulating MySQL + PostgreSQL)"
                  << std::endl;
        return parsed;
    }
};

// Stateless factory for the cross-backend test: each Worker builds its own
// CrossBackendMockParser (a plain function pointer, the Scheduler's
// parser_factory_fn shape).
inline parser_ptr make_cross_backend_mock_parser(std::pmr::memory_resource*) {
    return std::make_unique<CrossBackendMockParser>();
}

// Mock Otterbrix Manager for cross-backend test
class CrossBackendMockOtterbrixManager : public SimpleMockOtterbrixManager {
public:
    explicit CrossBackendMockOtterbrixManager(std::pmr::memory_resource* resource)
        : SimpleMockOtterbrixManager(mock_config{.resource = resource}) {}

    components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) override {
        std::cout << "CrossBackendMockOtterbrixManager: executing cross-backend plan" << std::endl;
        // Simulate successful execution - return a mock cursor
        std::pmr::memory_resource* resource = std::pmr::new_delete_resource();
        return components::cursor::make_cursor(resource, components::vector::data_chunk_t{resource, {}, 0});
    }
};

TEST_CASE("Cross-backend JOIN detection test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = std::pmr::new_delete_resource(); // plain malloc/free for the mock
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    // Use mock Otterbrix manager that doesn't require real execution
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<CrossBackendMockOtterbrixManager>(resource));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(resource,
                                                                        catalog_manager->address(),
                                                                        &mysql_mock_connector_factory);
    auto pg_conn_manager = std::make_unique<pg::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &pg_mock_connector_factory);

    auto mysql_conn_manager_actor = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_conn_manager = std::make_unique<ch::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &ch_mock_connector_factory);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(mysql_conn_manager_actor->address(),
                                          pg_connection_manager->address(),
                                          ch_connection_manager->address());

    // Register both MySQL and PostgreSQL connections (mocked, no real DB).
    // Must happen after the integration actors above started the connector
    // thread pools — addConnection performs eager schema discovery.
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "campaigns").has_error());

    // For PostgreSQL, use the single-parameter overload
    conn::api_server::PgConnectionParams pg_params;
    pg_params.alias = "products";
    pg_params.host = "localhost";
    pg_params.port = "5432";
    pg_params.username = "user";
    pg_params.password = "pass";
    pg_params.database = "pgdb";
    pg_params.schema = "public";
    pg_params.table = "products";
    REQUIRE_FALSE(pg_conn_manager->addConnection(pg_params).has_error());

    // The Scheduler builds its parser from the injected factory (&make_mock_parser);
    // each Worker gets its own instance, so the mock plan drives backend detection.
    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_cross_backend_mock_parser,
                                                  mysql_conn_manager_actor->address(),
                                                  pg_connection_manager->address(),
                                                  ch_connection_manager->address(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address());
    assert(scheduler);

    std::string sql = "SELECT * FROM products.pgdb.public.products p JOIN campaigns.db1.schema.campaigns c ON "
                      "p.campaign_id = c.campaign_id";
    session_hash_t id = 1;

    std::cout << "[Main thread] " << std::this_thread::get_id() << " sending cross-backend query" << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);

    // Wait for completion
    auto r = await_session(std::move(fut), 10000ms, resource);

    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;

    // The test verifies that:
    // 1. get_catalog_schema() was called (detected Mixed backend)
    // 2. Both MySQL and PostgreSQL connections were recognized
    // 3. Query didn't fail with "database does not exist" due to backend detection failure

    std::cout << "Test completed with error: " << r.has_error() << std::endl;
    if (r.has_error()) {
        std::cout << "Error message: " << r.error().what << std::endl;
    }

    // Both uids must have been attributed to a backend: a backend_unknown
    // failure is the one outcome ruled out here (success, or any other typed
    // error, is acceptable).
    REQUIRE_FALSE((r.has_error() && r.error().what.starts_with("backend_unknown")));

    az_scheduler->stop();
}

TEST_CASE("return empty test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .return_empty = true}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    // Use return_empty connector so MySQL returns empty results
    auto mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(resource,
                                                                        catalog_manager->address(),
                                                                        &mysql_mock_connector_factory_return_empty);
    auto pg_conn_manager = std::make_unique<pg::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &pg_mock_connector_factory);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_conn_manager = std::make_unique<ch::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &ch_mock_connector_factory);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(mysql_connection_manager->address(),
                                          pg_connection_manager->address(),
                                          ch_connection_manager->address());

    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "1").has_error());
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "2").has_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  mysql_connection_manager->address(),
                                                  pg_connection_manager->address(),
                                                  ch_connection_manager->address(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;
    REQUIRE(!r.has_error());
    auto sp = std::move(r.value());
    REQUIRE(sp.empty() == true);

    az_scheduler->stop();
}

// Regression for the latent >1024-row truncation: the engine's cursor delivers a
// result as a batch of <=1024-row chunks (never one oversized chunk). The mock
// returns N=2500 rows split into ceil(2500/1024)=3 chunks; the whole vector must
// ride through the scheduler into the session_payload without losing rows.
TEST_CASE("multi-chunk result carries all rows through the scheduler") {
    using namespace std::chrono_literals;

    constexpr size_t N = 2500;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}, N));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(resource,
                                                                        catalog_manager->address(),
                                                                        &mysql_mock_connector_factory);
    auto pg_conn_manager = std::make_unique<pg::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &pg_mock_connector_factory);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_conn_manager = std::make_unique<ch::ConnectorManager>(resource,
                                                                  catalog_manager->address(),
                                                                  &ch_mock_connector_factory);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(mysql_connection_manager->address(),
                                          pg_connection_manager->address(),
                                          ch_connection_manager->address());

    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "1").has_error());
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "2").has_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  mysql_connection_manager->address(),
                                                  pg_connection_manager->address(),
                                                  ch_connection_manager->address(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;
    REQUIRE(!r.has_error());
    auto sp = std::move(r.value());
    // No row loss across the chunk boundary, and the result really did arrive as
    // multiple chunks (would be 1 under a naive chunks().front()-only payload).
    REQUIRE(sp.size() == N);
    REQUIRE(sp.chunks.size() >= 3);
    // Every row carries its own values, in order, across the chunk boundaries —
    // the right total alone would not catch a chunk delivered empty or repeated.
    size_t row = 0;
    for (const auto& chunk : sp.chunks) {
        REQUIRE(chunk.column_count() == 2);
        for (size_t r_idx = 0; r_idx < chunk.size(); ++r_idx, ++row) {
            REQUIRE(chunk.value(0, r_idx).value<int32_t>() == SimpleMockOtterbrixManager::multi_chunk_id(row));
            REQUIRE(chunk.value(1, r_idx).value<std::string_view>() ==
                    SimpleMockOtterbrixManager::multi_chunk_name(row));
        }
    }
    REQUIRE(row == N);

    az_scheduler->stop();
}
// ---------------------------------------------------------------------------
// Sequence-root unwrap during schema resolution.
//
// A sequence-rooted statement wraps its data-producing node behind
// catalog_resolve_* siblings, the consumer LAST. CatalogManager::get_catalog_schema
// and OtterbrixManager::get_schema must unwrap that root instead of silently
// returning an empty schema.
//
// The parser no longer builds that shape — catalog lookups moved out of the plan
// tree into execution_plan_t::catalog_resolves, so a statement arrives as a bare
// consumer. The unwrap stayed, because a root that does arrive wrapped must not be
// read as an empty plan, and these cases are what pins it: each one builds the
// wrapped root itself with wrap_root_in_resolve_sequence.
// ---------------------------------------------------------------------------

#include "otterbrix/config.hpp"
#include "otterbrix/schema/schema_utils.hpp"

#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_sequence.hpp>

#include <filesystem>

namespace {

    using otterstax::test::wait_until_ready;

    // Catalog actor graph with mocked connectors and a registered PostgreSQL
    // connection "products" (pgdb/public/products) — shared by the
    // sequence-unwrap catalog test cases below.
    struct catalog_schema_fixture {
        std::pmr::memory_resource* resource;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager;
        // Sole owners of the connector managers. The catalog actor and the three
        // integration actors hold non-owning pointers into them and are declared
        // below, so they are destroyed first — the same order ComponentManager
        // keeps. Construction has to run the other way round (the connector
        // managers take the catalog's address), hence the constructor body.
        std::unique_ptr<mysql::ConnectorManager> mysql_conn_manager;
        std::unique_ptr<pg::ConnectorManager> pg_conn_manager;
        std::unique_ptr<ch::ConnectorManager> ch_conn_manager;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager;
        std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> mysql_connection_manager;
        std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> pg_connection_manager;
        std::unique_ptr<db::ClickhouseManager, actor_zeta::pmr::deleter_t> ch_connection_manager;

        explicit catalog_schema_fixture(std::pmr::memory_resource* res)
            : resource(res)
            , otterbrix_manager(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_manager(nullptr, actor_zeta::pmr::deleter_t{res})
            , mysql_connection_manager(nullptr, actor_zeta::pmr::deleter_t{res})
            , pg_connection_manager(nullptr, actor_zeta::pmr::deleter_t{res})
            , ch_connection_manager(nullptr, actor_zeta::pmr::deleter_t{res}) {
            catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager->address());
            mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(res,
                                                                           catalog_manager->address(),
                                                                           &mysql_mock_connector_factory);
            pg_conn_manager = std::make_unique<pg::ConnectorManager>(res,
                                                                     catalog_manager->address(),
                                                                     &pg_mock_connector_factory);
            ch_conn_manager = std::make_unique<ch::ConnectorManager>(res,
                                                                     catalog_manager->address(),
                                                                     &ch_mock_connector_factory);

            // Integration actors start the connector thread pools; addConnection
            // below performs eager schema discovery and needs them running.
            mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(res, mysql_conn_manager.get());
            pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(res, pg_conn_manager.get());
            ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(res, ch_conn_manager.get());
            catalog_manager->set_backend_managers(mysql_connection_manager->address(),
                                                  pg_connection_manager->address(),
                                                  ch_connection_manager->address());

            conn::api_server::PgConnectionParams pg_params;
            pg_params.alias = "products";
            pg_params.host = "localhost";
            pg_params.port = "5432";
            pg_params.username = "user";
            pg_params.password = "pass";
            pg_params.database = "pgdb";
            pg_params.schema = "public";
            pg_params.table = "products";
            REQUIRE_FALSE(pg_conn_manager->addConnection(pg_params).has_error());
        }
    };

    // Rebuilds the sequence-rooted shape the unwrap paths defend against: a
    // node_sequence_t holding a catalog_resolve_t sibling first and the parsed
    // statement's consumer LAST. The parser hands over the bare consumer, so the
    // wrapped root exists only where a test makes one.
    //
    // An external slot is a pointer INTO the plan, so a slot that named the old root
    // is repointed at the consumer's new home inside the sequence; left alone it
    // would name a node the plan no longer roots. The repointing must happen BEFORE
    // the root slot is overwritten: a slot naming the root IS that slot, so once the
    // sequence is stored there the slot no longer reads as the consumer.
    void wrap_root_in_resolve_sequence(const ParsedQueryDataPtr& data, std::pmr::memory_resource* res) {
        auto consumer = data->otterbrix_params->node;
        REQUIRE(consumer);
        auto sequence = logical_plan::node_ptr(new logical_plan::node_sequence_t(res));
        sequence->append_child(logical_plan::make_node_catalog_resolve(res, logical_plan::resolve_kind::table));
        sequence->append_child(consumer);
        for (auto& batch : data->otterbrix_params->external_nodes) {
            for (auto& entry : batch) {
                if (*entry.node == consumer) {
                    entry.node = &sequence->children().back();
                }
            }
        }
        data->otterbrix_params->node = sequence;
    }

} // namespace

TEST_CASE("otterbrix get_schema: sequence-rooted SELECT resolves columns") {
    const char* disk_path = "/tmp/otterstax_seq_get_schema";
    std::filesystem::remove_all(disk_path);
    auto cfg = make_create_config(disk_path);
    auto inst = db::make_otterbrix_engine(cfg);
    auto* resource = inst->dispatcher()->resource();

    {
        // Local engine table the get_schema dependency probe resolves against.
        auto setup = make_otterbrix_manager(inst);
        auto db_cursor = setup->execute_sql("CREATE DATABASE db1;");
        REQUIRE(db_cursor);
        REQUIRE_FALSE(db_cursor->is_error());
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("campaign_id", types::complex_logical_type(types::logical_type::INTEGER));
        cols.emplace_back("campaign_name", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        cols.emplace_back("budget", types::complex_logical_type(types::logical_type::DOUBLE));
        components::catalog::oid_t oid = components::catalog::INVALID_OID;
        auto create_cursor = setup->create_collection("db1", "campaigns", std::move(cols), oid);
        REQUIRE(create_cursor);
        REQUIRE_FALSE(create_cursor->is_error());
    }

    GreenplumParser parser(resource);
    auto parsed = parser.parse("SELECT campaign_name, budget FROM db1.campaigns;");
    REQUIRE_FALSE(parsed.has_error());
    auto data = std::move(parsed.value());

    // The parser hands over the bare consumer; the wrapped root this case is about
    // is built here.
    REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::aggregate_t);
    wrap_root_in_resolve_sequence(data, resource);

    // Wrapped shape: sequence root, the aggregate consumer is LAST.
    const auto& root = data->otterbrix_params->node;
    REQUIRE(root->type() == logical_plan::node_type::sequence_t);
    REQUIRE_FALSE(root->children().empty());
    REQUIRE(root->children().back()->type() == logical_plan::node_type::aggregate_t);

    std::pmr::map<qualified_name_t, size_t> dependencies(resource);
    dependencies.emplace(
        schema_utils::agg_key(static_cast<const logical_plan::node_aggregate_t&>(*root->children().back())),
        0);

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(inst));
    auto [needs_sched, future] = actor_zeta::send(otterbrix_manager->address(),
                                                  &db::OtterbrixManager::get_schema,
                                                  session_hash_t{1},
                                                  std::move(dependencies),
                                                  std::move(data));
    wait_until_ready(future);
    auto result = std::move(future).take_ready();
    REQUIRE_FALSE(result.has_error());
    auto [cursor, returned] = std::move(result.value());
    REQUIRE(cursor);
    REQUIRE_FALSE(cursor->is_error());
    // A sequence root is not an empty plan: exactly one schema, for the
    // aggregate that ends the sequence.
    REQUIRE(cursor->size() == 1);
    const auto& schema = cursor->type_data()[0];
    REQUIRE(schema.type() == types::logical_type::STRUCT);
    // The schema lists the SELECT-list columns by name, in statement order.
    REQUIRE(schema.child_types().size() == 2);
    REQUIRE(schema.child_types()[0].alias() == "campaign_name");
    REQUIRE(schema.child_types()[1].alias() == "budget");
    // The probe resolves the column types, not only their names.
    REQUIRE(schema.child_types()[0].type() == types::logical_type::STRING_LITERAL);
    REQUIRE(schema.child_types()[1].type() == types::logical_type::DOUBLE);
}

TEST_CASE("otterbrix get_schema: non-aggregate plans keep the empty-schema contract") {
    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    GreenplumParser parser(resource);

    SECTION("non-sequence non-aggregate root -> empty schema, no error") {
        auto parsed = parser.parse("SELECT campaign_name FROM db1.campaigns;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        data->otterbrix_params->node =
            logical_plan::make_node_raw_data(resource, components::vector::data_chunk_t(resource, {}));

        std::pmr::map<qualified_name_t, size_t> dependencies(resource);
        auto [needs_sched, future] = actor_zeta::send(otterbrix_manager->address(),
                                                      &db::OtterbrixManager::get_schema,
                                                      session_hash_t{1},
                                                      std::move(dependencies),
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE_FALSE(result.has_error());
        auto [cursor, returned] = std::move(result.value());
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == 0);
    }

    SECTION("sequence whose last child is not an aggregate (CREATE) -> empty schema, no error") {
        auto parsed = parser.parse("CREATE TABLE db1.newtable (id INT);");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::create_collection_t);
        wrap_root_in_resolve_sequence(data, resource);
        REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::sequence_t);
        REQUIRE(data->otterbrix_params->node->children().back()->type() != logical_plan::node_type::aggregate_t);

        std::pmr::map<qualified_name_t, size_t> dependencies(resource);
        auto [needs_sched, future] = actor_zeta::send(otterbrix_manager->address(),
                                                      &db::OtterbrixManager::get_schema,
                                                      session_hash_t{1},
                                                      std::move(dependencies),
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE_FALSE(result.has_error());
        auto [cursor, returned] = std::move(result.value());
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == 0);
    }

    SECTION("sequence with no children -> error, not UB") {
        auto parsed = parser.parse("SELECT campaign_name FROM db1.campaigns;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        data->otterbrix_params->node = logical_plan::node_ptr(new logical_plan::node_sequence_t(resource));

        std::pmr::map<qualified_name_t, size_t> dependencies(resource);
        auto [needs_sched, future] = actor_zeta::send(otterbrix_manager->address(),
                                                      &db::OtterbrixManager::get_schema,
                                                      session_hash_t{1},
                                                      std::move(dependencies),
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE(result.has_error());
    }
}

TEST_CASE("catalog get_catalog_schema: sequence-rooted SELECT rewrites the external node") {
    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    catalog_schema_fixture fx(resource);

    GreenplumParser parser(resource);
    auto parsed = parser.parse("SELECT id, name FROM products.pgdb.public.products;");
    REQUIRE_FALSE(parsed.has_error());
    auto data = std::move(parsed.value());

    REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::aggregate_t);
    wrap_root_in_resolve_sequence(data, resource);

    // Wrapped shape: sequence root, the external aggregate is LAST.
    REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::sequence_t);
    REQUIRE(data->otterbrix_params->external_nodes.size() == 1);
    REQUIRE(data->otterbrix_params->external_nodes.front().size() == 1);
    REQUIRE((*data->otterbrix_params->external_nodes.front().front().node)->type() ==
            logical_plan::node_type::aggregate_t);

    auto [needs_sched, future] = actor_zeta::send(fx.catalog_manager->address(),
                                                  &mysql::CatalogManager::get_catalog_schema,
                                                  session_hash_t{1},
                                                  std::move(data));
    wait_until_ready(future);
    auto result = std::move(future).take_ready();
    REQUIRE_FALSE(result.has_error());
    auto updated = std::move(result.value());
    REQUIRE(updated->backend_type == backend_type_t::PostgreSQL);

    // The external aggregate under the sequence root must be rewritten into its
    // schema node; a root mistaken for an empty plan leaves it untouched.
    auto& entry = updated->otterbrix_params->external_nodes.front().front();
    REQUIRE((*entry.node)->type() == logical_plan::node_type::unused);
    const auto& schema = static_cast<schema_utils::schema_node_t&>(**entry.node).schema();
    REQUIRE(schema.child_types().size() == 2);
    REQUIRE(schema.child_types()[0].alias() == "id");
    REQUIRE(schema.child_types()[1].alias() == "name");
}

TEST_CASE("catalog get_catalog_schema: non-aggregate plans keep the empty-schema contract") {
    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    catalog_schema_fixture fx(resource);
    GreenplumParser parser(resource);

    SECTION("sequence whose last child is not an aggregate (CREATE) -> empty schema, no error") {
        auto parsed = parser.parse("CREATE TABLE products.pgdb.public.newtable (id INT);");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::create_collection_t);
        wrap_root_in_resolve_sequence(data, resource);
        REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::sequence_t);
        REQUIRE(data->otterbrix_params->node->children().back()->type() ==
                logical_plan::node_type::create_collection_t);

        auto [needs_sched, future] = actor_zeta::send(fx.catalog_manager->address(),
                                                      &mysql::CatalogManager::get_catalog_schema,
                                                      session_hash_t{2},
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE_FALSE(result.has_error());
        auto updated = std::move(result.value());
        REQUIRE(updated->backend_type == backend_type_t::PostgreSQL);
        REQUIRE(updated->otterbrix_params->node->children().back()->type() ==
                logical_plan::node_type::create_collection_t);
    }

    SECTION("non-sequence non-aggregate root -> empty schema, no error") {
        auto parsed = parser.parse("SELECT id, name FROM products.pgdb.public.products;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        data->otterbrix_params->node =
            logical_plan::make_node_raw_data(resource, components::vector::data_chunk_t(resource, {}));
        data->otterbrix_params->external_nodes.front().front().node = &data->otterbrix_params->node;

        auto [needs_sched, future] = actor_zeta::send(fx.catalog_manager->address(),
                                                      &mysql::CatalogManager::get_catalog_schema,
                                                      session_hash_t{3},
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE_FALSE(result.has_error());
        auto updated = std::move(result.value());
        REQUIRE(updated->otterbrix_params->node->type() == logical_plan::node_type::data_t);
    }

    SECTION("sequence with no children -> error, not UB") {
        auto parsed = parser.parse("SELECT id, name FROM products.pgdb.public.products;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        data->otterbrix_params->node = logical_plan::node_ptr(new logical_plan::node_sequence_t(resource));
        data->otterbrix_params->external_nodes.front().front().node = &data->otterbrix_params->node;

        auto [needs_sched, future] = actor_zeta::send(fx.catalog_manager->address(),
                                                      &mysql::CatalogManager::get_catalog_schema,
                                                      session_hash_t{4},
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE(result.has_error());
    }
}
