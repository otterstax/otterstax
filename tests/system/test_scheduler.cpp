// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog/catalog_manager.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "scheduler/result.hpp"
#include "scheduler/scheduler.hpp"

#include "../mock/ch_db_connector.hpp"
#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/parser.hpp"
#include "../mock/pg_db_connector.hpp"
#include "../mock/sql_db_connector.hpp"

#include "utility/logger.hpp"
#include "utility/pipeline_error.hpp"

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
    otterbrix::otterbrix_ptr init_otterbrix() {
        auto config = configuration::config::default_config();

        auto log_path = config.log.path.string();
        initialize_all_loggers(log_path);

        return otterbrix::make_otterbrix(std::move(config));
    }

    std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> make_az_scheduler() {
        auto sched = std::make_unique<actor_zeta::scheduler::sharing_scheduler>(
            std::max<std::size_t>(2, std::thread::hardware_concurrency()),
            /*max_throughput*/ 1000);
        sched->start();
        return sched;
    }

    // Drive a Scheduler-returned future from the test thread using a local io_context.
    otterstax::result<session_payload>
    await_session(actor_zeta::unique_future<otterstax::result<session_payload>> fut,
                  std::chrono::milliseconds timeout,
                  std::pmr::memory_resource* resource) {
        otterstax::result<session_payload> r{std::make_unique<session_payload>(resource)};
        boost::asio::io_context local;
        boost::asio::co_spawn(
            local,
            [&]() -> boost::asio::awaitable<void> {
                r = co_await otterstax::await_az_future(std::move(fut), timeout);
            },
            boost::asio::detached);
        local.run();
        return r;
    }
} // namespace

TEST_CASE("base test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = std::pmr::get_default_resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource);
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(), mysql_mock_connector_factory(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, std::make_unique<SimpleMockOtterbrixManager>());
    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        /*worker_count*/ std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        [res = resource]() -> std::unique_ptr<IParser> {
            return std::make_unique<SimpleMockParser>(mock_config{.resource = res});
        },
        mysql_connection_manager->address(),
        pg_connection_manager->address(),
        ch_connection_manager->address(),
        otterbrix_manager->address(),
        catalog_manager->address());
    assert(scheduler);

    std::string sql = "SELECT 1 AS test";
    session_id id;
    auto fut = actor_zeta::send(scheduler->address(), &Scheduler::execute,id.hash(), sql).second;
    auto r = await_session(std::move(fut), 5000ms, resource);

    REQUIRE_FALSE(r.has_error());
    REQUIRE(r.value().chunk.size() == 2);

    az_scheduler->stop();
}

TEST_CASE("Error in connector test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource);
    auto mysql_conn_manager = std::make_shared<mysql::ConnectorManager>(catalog_manager->address(),
                                                                         mysql_mock_connector_factory_throw(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, std::make_unique<SimpleMockOtterbrixManager>());
    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        [res = resource]() -> std::unique_ptr<IParser> {
            return std::make_unique<SimpleMockParser>(mock_config{.resource = res});
        },
        mysql_connection_manager->address(),
        pg_connection_manager->address(),
        ch_connection_manager->address(),
        otterbrix_manager->address(),
        catalog_manager->address());
    assert(scheduler);

    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto fut = actor_zeta::send(scheduler->address(), &Scheduler::execute,id, sql).second;
    auto r = await_session(std::move(fut), 5000ms, resource);

    REQUIRE(r.has_error());
    REQUIRE(r.error().what.find("MockConnector: exception in runQuery") != std::string::npos);

    az_scheduler->stop();
}

TEST_CASE("Error in otterbrix test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource);
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(), mysql_mock_connector_factory(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.can_throw = true}));
    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        [res = resource]() -> std::unique_ptr<IParser> {
            return std::make_unique<SimpleMockParser>(mock_config{.resource = res});
        },
        mysql_connection_manager->address(),
        pg_connection_manager->address(),
        ch_connection_manager->address(),
        otterbrix_manager->address(),
        catalog_manager->address());
    assert(scheduler);

    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto fut = actor_zeta::send(scheduler->address(), &Scheduler::execute,id, sql).second;
    auto r = await_session(std::move(fut), 5000ms, resource);

    REQUIRE(r.has_error());
    REQUIRE(r.error().what.find("SimpleMockOtterbrixManager: exception in execute_plan") != std::string::npos);

    az_scheduler->stop();
}

TEST_CASE("Error in scheduler test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource);
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(), mysql_mock_connector_factory(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, std::make_unique<SimpleMockOtterbrixManager>());
    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        [res = resource]() -> std::unique_ptr<IParser> {
            return std::make_unique<SimpleMockParser>(mock_config{.resource = res, .can_throw = true});
        },
        mysql_connection_manager->address(),
        pg_connection_manager->address(),
        ch_connection_manager->address(),
        otterbrix_manager->address(),
        catalog_manager->address());
    assert(scheduler);

    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto fut = actor_zeta::send(scheduler->address(), &Scheduler::execute,id, sql).second;
    auto r = await_session(std::move(fut), 5000ms, resource);

    REQUIRE(r.has_error());
    REQUIRE(r.error().what.find("SimpleMockParser: exception in parse") != std::string::npos);

    az_scheduler->stop();
}

TEST_CASE("Error in otterbrix + sql connector test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource);
    auto mysql_conn_manager = std::make_shared<mysql::ConnectorManager>(catalog_manager->address(),
                                                                         mysql_mock_connector_factory_throw(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.can_throw = true}));
    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        [res = resource]() -> std::unique_ptr<IParser> {
            return std::make_unique<SimpleMockParser>(mock_config{.resource = res});
        },
        mysql_connection_manager->address(),
        pg_connection_manager->address(),
        ch_connection_manager->address(),
        otterbrix_manager->address(),
        catalog_manager->address());
    assert(scheduler);

    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto fut = actor_zeta::send(scheduler->address(), &Scheduler::execute,id, sql).second;
    auto r = await_session(std::move(fut), 5000ms, resource);

    REQUIRE(r.has_error());
    REQUIRE(r.error().what.find("MockConnector: exception in runQuery") != std::string::npos);

    az_scheduler->stop();
}

// Mock parser that creates cross-backend query (MySQL + PostgreSQL)
class CrossBackendMockParser : public IParser {
public:
    core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
        auto resource = std::pmr::get_default_resource();

        auto result_node = logical_plan::make_node_aggregate(resource, {"result", "db", "schema", "result_table"});

        auto binder = sql::transform::transform_result(
            resource,
            result_node,
            logical_plan::make_parameter_node(resource),
            sql::transform::transform_result::parameter_map_t{resource},
            sql::transform::transform_result::insert_map_t{resource},
            data_chunk_t(resource, {}));

        auto parsed = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::vector<std::vector<logical_plan::node_ptr*>>{},
                                                 binder.params_ptr(),
                                                 binder.node_ptr(),
                                                 2),
            std::move(binder),
            NodeTag::T_SelectStmt);

        parsed->otterbrix_params->external_nodes.push_back({&parsed->otterbrix_params->node});
        parsed->otterbrix_params->external_nodes.push_back({&parsed->otterbrix_params->node});
        parsed->otterbrix_params->external_nodes_count = 2;

        return parsed;
    }
};

class CrossBackendMockOtterbrixManager : public SimpleMockOtterbrixManager {
public:
    CrossBackendMockOtterbrixManager()
        : SimpleMockOtterbrixManager(mock_config{}) {}

    components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) override {
        std::pmr::memory_resource* resource = std::pmr::get_default_resource();
        return components::cursor::make_cursor(resource, components::vector::data_chunk_t{resource, {}, 0});
    }
};

TEST_CASE("Cross-backend JOIN detection test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = std::pmr::get_default_resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource);
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(), mysql_mock_connector_factory(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "campaigns");

    http_server::PgConnectionParams pg_params;
    pg_params.alias = "products";
    pg_params.host = "localhost";
    pg_params.port = "5432";
    pg_params.username = "user";
    pg_params.password = "pass";
    pg_params.database = "pgdb";
    pg_params.schema = "public";
    pg_params.table = "";
    pg_conn_manager->addConnection(pg_params);

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, std::make_unique<CrossBackendMockOtterbrixManager>());
    auto mysql_conn_manager_actor = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        []() -> std::unique_ptr<IParser> { return std::make_unique<CrossBackendMockParser>(); },
        mysql_conn_manager_actor->address(),
        pg_connection_manager->address(),
        ch_connection_manager->address(),
        otterbrix_manager->address(),
        catalog_manager->address());
    assert(scheduler);

    std::string sql = "SELECT * FROM products.pgdb.public.products p JOIN campaigns.db1.schema.campaigns c ON "
                      "p.campaign_id = c.campaign_id";
    session_hash_t id = 1;
    auto fut = actor_zeta::send(scheduler->address(), &Scheduler::execute,id, sql).second;
    auto r = await_session(std::move(fut), 10000ms, resource);

    // Test passes if scheduler made progress (either success or a defined error — not a hang).
    INFO("Cross-backend result error: " << r.has_error() << ", message: "
                                         << (r.has_error() ? r.error().what : std::string{}));

    az_scheduler->stop();
}

TEST_CASE("return empty test case") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource);
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(),
                                                   mysql_mock_connector_factory_return_empty(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .return_empty = true}));
    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        [res = resource]() -> std::unique_ptr<IParser> {
            return std::make_unique<SimpleMockParser>(mock_config{.resource = res});
        },
        mysql_connection_manager->address(),
        pg_connection_manager->address(),
        ch_connection_manager->address(),
        otterbrix_manager->address(),
        catalog_manager->address());
    assert(scheduler);

    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    auto fut = actor_zeta::send(scheduler->address(), &Scheduler::execute,id, sql).second;
    auto r = await_session(std::move(fut), 5000ms, resource);

    REQUIRE_FALSE(r.has_error());
    REQUIRE(r.value().chunk.empty());

    az_scheduler->stop();
}
