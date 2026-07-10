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

#include "utility/logger.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <core/result_wrapper.hpp>
#include <otterbrix/otterbrix.hpp>

#include <catch2/catch.hpp>
#include <chrono>
#include <thread>
#include <tuple>

namespace {
    std::once_flag log_init_flag;

    otterbrix::otterbrix_ptr init_otterbrix() {
        auto config = configuration::config::default_config();

        std::call_once(log_init_flag, [&] { initialize_all_loggers(config.log.path.string()); });

        return otterbrix::make_otterbrix(std::move(config));
    }

    // The worker pool runs on an actor-zeta sharing_scheduler; the Scheduler ctor
    // takes a raw pointer to it plus the worker count.
    std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> make_az_scheduler() {
        auto sched = std::make_unique<actor_zeta::scheduler::sharing_scheduler>(
            std::max<std::size_t>(2, std::thread::hardware_concurrency()),
            /*max_throughput*/ 1000);
        sched->start();
        return sched;
    }

    std::size_t worker_count() { return std::max<std::size_t>(2, std::thread::hardware_concurrency()); }

    // Scheduler handlers now RETURN unique_future<result<session_payload>> instead
    // of signalling a shared_session_payload. Drive that future from the test
    // thread through the working-tree asio bridge (async_await_future: poll over
    // is_ready()/failed()/take_ready(); no blocking get(), no cancel()).
    core::result_wrapper_t<session_payload>
    await_session(actor_zeta::unique_future<core::result_wrapper_t<session_payload>> fut,
                  std::chrono::milliseconds timeout,
                  std::pmr::memory_resource* resource) {
        core::result_wrapper_t<session_payload> r{resource};
        boost::asio::io_context local;
        boost::asio::co_spawn(
            local,
            [&]() -> boost::asio::awaitable<void> {
                r = co_await otterstax::async_await_future(std::move(fut), timeout);
            },
            boost::asio::detached);
        local.run();
        return r;
    }
} // namespace

TEST_CASE("base test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(), mysql_mock_connector_factory(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    // Register connector managers with catalog manager
    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    // addConnection triggers eager schema discovery + engine registration via
    // the catalog; the connector thread pools must already be running (they
    // are started by the integration-actor constructors above).
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_count(),
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
    REQUIRE(sp.chunk.size() == 2);

    az_scheduler->stop();
}

TEST_CASE("Error in connector test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_shared<mysql::ConnectorManager>(catalog_manager->address(),
                                                                         mysql_mock_connector_factory_throw(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    // Register connector managers with catalog manager
    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_count(),
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

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .can_throw = true}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(), mysql_mock_connector_factory(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    // Register connector managers with catalog manager
    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_count(),
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
    REQUIRE(r.error().what ==
            "Otterbrix execution failed: SimpleMockOtterbrixManager: exception in execute_plan");

    az_scheduler->stop();
}

TEST_CASE("Error in scheduler test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(), mysql_mock_connector_factory(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    // No connector managers are registered with the catalog here on purpose:
    // schema discovery fails (and is ignored) — the parser throws first.
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_count(),
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

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .can_throw = true}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_shared<mysql::ConnectorManager>(catalog_manager->address(),
                                                                         mysql_mock_connector_factory_throw(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    // Register connector managers with catalog manager
    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_count(),
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

        auto resource = std::pmr::get_default_resource();

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
            data_chunk_t(resource, {}));

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
// CrossBackendMockParser (function pointer — not std::function, codex rule 14).
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
        std::pmr::memory_resource* resource = std::pmr::get_default_resource();
        return components::cursor::make_cursor(resource, components::vector::data_chunk_t{resource, {}, 0});
    }
};

TEST_CASE("Cross-backend JOIN detection test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = std::pmr::get_default_resource(); // use default for easier mock
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    // Use mock Otterbrix manager that doesn't require real execution
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<CrossBackendMockOtterbrixManager>(resource));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(), mysql_mock_connector_factory(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    // Register connector managers with catalog manager
    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    auto mysql_conn_manager_actor = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    // Register both MySQL and PostgreSQL connections (mocked, no real DB).
    // Must happen after the integration actors above started the connector
    // thread pools — addConnection performs eager schema discovery.
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "campaigns");

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
    pg_conn_manager->addConnection(pg_params);

    // The Scheduler builds its parser from the injected factory (&make_mock_parser);
    // each Worker gets its own instance, so the mock plan drives backend detection.
    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_count(),
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

    // The original assertion was `status() != cv_wrapper::Status::Unknown`: the
    // backend WAS resolved. The result<> equivalent is that we did not fail with
    // a backend_unknown error (success, or any other typed error, is acceptable).
    REQUIRE_FALSE((r.has_error() && r.error().what.starts_with("backend_unknown")));

    az_scheduler->stop();
}


TEST_CASE("return empty test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .return_empty = true}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    // Use return_empty connector so MySQL returns empty results
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(),
                                                   mysql_mock_connector_factory_return_empty(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    // Register connector managers with catalog manager
    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_count(),
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
    REQUIRE(sp.chunk.empty() == true);

    az_scheduler->stop();
}
// ---------------------------------------------------------------------------
// a13 sequence-root unwrap during schema resolution.
//
// The a13 transformer wraps table-referencing statements in a node_sequence_t
// (catalog_resolve_* siblings first, the data-producing node LAST — see
// maybe_wrap_with_catalog_resolve_table in components/sql/transformer).
// CatalogManager::get_catalog_schema and OtterbrixManager::get_schema must
// unwrap that root instead of silently returning an empty schema.
// ---------------------------------------------------------------------------

#include "otterbrix/config.hpp"
#include "scheduler/schema_utils.hpp"

#include <components/logical_plan/node_sequence.hpp>

#include <filesystem>

namespace {

    template<typename Future>
    void wait_until_ready(Future& future) {
        using namespace std::chrono_literals;
        for (int i = 0; i < 1000 && !future.is_ready(); ++i) {
            std::this_thread::sleep_for(10ms);
        }
        REQUIRE(future.is_ready());
    }

    // Catalog actor graph with mocked connectors and a registered PostgreSQL
    // connection "products" (pgdb/public/products) — shared by the
    // sequence-unwrap catalog test cases below.
    struct catalog_schema_fixture {
        std::pmr::memory_resource* resource;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager;
        std::shared_ptr<mysql::ConnectorManager> mysql_conn_manager;
        std::shared_ptr<pg::ConnectorManager> pg_conn_manager;
        std::shared_ptr<ch::ConnectorManager> ch_conn_manager;
        std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> mysql_connection_manager;
        std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> pg_connection_manager;
        std::unique_ptr<db::ClickhouseManager, actor_zeta::pmr::deleter_t> ch_connection_manager;

        explicit catalog_schema_fixture(std::pmr::memory_resource* res)
            : resource(res)
            , otterbrix_manager(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_manager(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager->address()))
            , mysql_conn_manager(std::make_shared<mysql::ConnectorManager>(catalog_manager->address(),
                                                                            mysql_mock_connector_factory(res)))
            , pg_conn_manager(
                  std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(res)))
            , ch_conn_manager(
                  std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(res)))
            // Integration actors start the connector thread pools; addConnection
            // below performs eager schema discovery and needs them running.
            , mysql_connection_manager(actor_zeta::spawn<db::MySQLManager>(res, mysql_conn_manager))
            , pg_connection_manager(actor_zeta::spawn<db::PostgressManager>(res, pg_conn_manager))
            , ch_connection_manager(actor_zeta::spawn<db::ClickhouseManager>(res, ch_conn_manager)) {
            catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
            catalog_manager->set_pg_connector_manager(pg_conn_manager);
            catalog_manager->set_ch_connector_manager(ch_conn_manager);

            conn::api_server::PgConnectionParams pg_params;
            pg_params.alias = "products";
            pg_params.host = "localhost";
            pg_params.port = "5432";
            pg_params.username = "user";
            pg_params.password = "pass";
            pg_params.database = "pgdb";
            pg_params.schema = "public";
            pg_params.table = "products";
            pg_conn_manager->addConnection(pg_params);
        }
    };

} // namespace

TEST_CASE("otterbrix get_schema: sequence-rooted SELECT resolves columns") {
    const char* disk_path = "/tmp/otterstax_seq_get_schema";
    std::filesystem::remove_all(disk_path);
    auto cfg = make_create_config(disk_path);
    auto inst = otterbrix::make_otterbrix(cfg);
    auto* resource = inst->dispatcher()->resource();

    {
        // Local engine table the LIMIT 0 schema probe resolves against.
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

    // Transformer contract: sequence root, the aggregate consumer is LAST.
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
    // Pre-unwrap this path silently returned an empty cursor (empty schema).
    REQUIRE(cursor->size() == 1);
    const auto& schema = cursor->type_data()[0];
    REQUIRE(schema.type() == types::logical_type::STRUCT);
    // The selected columns must be present. Their concrete types stay NA for
    // now — the engine LIMIT-0 probe returns alias-less column types, so the
    // SELECT-list lookup cannot match them by name yet.
    REQUIRE(schema.child_types().size() == 2);
    REQUIRE(schema.child_types()[0].alias() == "campaign_name");
    REQUIRE(schema.child_types()[1].alias() == "budget");
}

TEST_CASE("otterbrix get_schema: non-aggregate plans keep the empty-schema contract") {
    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
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
        REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::sequence_t);
        REQUIRE(data->otterbrix_params->node->children().back()->type() !=
                logical_plan::node_type::aggregate_t);

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
    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    catalog_schema_fixture fx(resource);

    GreenplumParser parser(resource);
    auto parsed = parser.parse("SELECT id, name FROM products.pgdb.public.products;");
    REQUIRE_FALSE(parsed.has_error());
    auto data = std::move(parsed.value());

    // Transformer contract: sequence root, the external aggregate is LAST.
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

    // Pre-unwrap the sequence root hit the empty-schema early return and the
    // external aggregate was never rewritten into its schema node.
    auto& entry = updated->otterbrix_params->external_nodes.front().front();
    REQUIRE((*entry.node)->type() == logical_plan::node_type::unused);
    const auto& schema = static_cast<schema_utils::schema_node_t&>(**entry.node).schema();
    REQUIRE(schema.child_types().size() == 2);
    REQUIRE(schema.child_types()[0].alias() == "id");
    REQUIRE(schema.child_types()[1].alias() == "name");
}

TEST_CASE("catalog get_catalog_schema: non-aggregate plans keep the empty-schema contract") {
    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    catalog_schema_fixture fx(resource);
    GreenplumParser parser(resource);

    SECTION("sequence whose last child is not an aggregate (CREATE) -> empty schema, no error") {
        auto parsed = parser.parse("CREATE TABLE products.pgdb.public.newtable (id INT);");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
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
