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
#include "utility/wait_barrier.hpp"

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

#include <catch2/catch_all.hpp>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

    otterbrix::otterbrix_ptr init_otterbrix() {
        auto config = configuration::config::default_config();
        auto log_path = config.log.path.string();
        initialize_all_loggers(log_path);
        return otterbrix::make_otterbrix(std::move(config));
    }

    auto mysql_mock_connector_factory_slow(std::pmr::memory_resource* resource,
                                           std::chrono::milliseconds wait_time) {
        return [resource, wait_time](boost::asio::io_context&,
                                     boost::mysql::connect_params,
                                     std::string alias) {
            return std::make_unique<mysql::MockConnector>(
                mock_config{.resource = resource, .wait_time = wait_time},
                std::move(alias));
        };
    }

    std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> make_az_scheduler() {
        auto sched = std::make_unique<actor_zeta::scheduler::sharing_scheduler>(
            std::max<std::size_t>(2, std::thread::hardware_concurrency()),
            /*max_throughput*/ 1000);
        sched->start();
        return sched;
    }

    // Drive a Scheduler-returned actor-zeta future from a test thread. The
    // working-tree bridge (frontend/common/asio_future_bridge.hpp) exposes
    // async_await_future, an exception-free poll over the 1.2.0 future API
    // (is_ready()/failed()/take_ready() — no blocking get(), no cancel()).
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

// Spawns the full scheduler + connection managers stack with mock connectors and
// fires N parallel Scheduler::execute() messages. Validates that N parallel
// sessions don't hang and each returns a typed result.
TEST_CASE("scheduler handles N parallel sessions without hanging") {
    using namespace std::chrono_literals;

    constexpr size_t N = 32;
    constexpr auto WAIT_TIMEOUT = 60000ms;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    REQUIRE(resource != nullptr);

    auto az_scheduler = make_az_scheduler();

    // OtterbrixManager must exist before the CatalogManager: the current
    // CatalogManager ctor takes the OtterbrixManager address (OID-centric
    // catalog registration).
    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource,
                                                std::make_unique<SimpleMockOtterbrixManager>(
                                                    mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(),
                                                  mysql_mock_connector_factory(resource));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager);
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager);
    auto ch_conn_manager =
        std::make_shared<ch::ConnectorManager>(catalog_manager->address(), ch_mock_connector_factory(resource));
    catalog_manager->set_ch_connector_manager(ch_conn_manager);
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager);

    // The current Scheduler ctor takes (resource, az_scheduler, worker_count,
    // parser_factory, 5 actor addresses + s3 + file); each Worker builds its
    // own parser from the injected factory — tests pass &make_mock_parser. The
    // s3/file managers are empty here because the mock pipeline does not exercise
    // CREATE EXTERNAL TABLE / COPY ... TO statements.
    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        &make_mock_parser,
        mysql_connection_manager->address(),
        pg_connection_manager->address(),
        ch_connection_manager->address(),
        otterbrix_manager->address(),
        catalog_manager->address(),
        actor_zeta::address_t::empty_address(),
        actor_zeta::address_t::empty_address());
    REQUIRE(scheduler != nullptr);

    const std::string sql = "SELECT 1 AS test";

    std::vector<actor_zeta::unique_future<core::result_wrapper_t<session_payload>>> futs;
    futs.reserve(N);

    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < N; ++i) {
        futs.push_back(actor_zeta::send(scheduler->address(),
                                        &Scheduler::execute,
                                        static_cast<session_hash_t>(i + 1),
                                        sql)
                           .second);
    }

    std::vector<core::result_wrapper_t<session_payload>> results;
    results.reserve(N);
    for (size_t i = 0; i < N; ++i) {
        results.push_back(await_session(std::move(futs[i]), WAIT_TIMEOUT, resource));
    }
    auto end = std::chrono::steady_clock::now();

    for (size_t i = 0; i < N; ++i) {
        INFO("session " << i);
        REQUIRE_FALSE(results[i].has_error());
        REQUIRE(results[i].value().size() == 2);
    }

    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    WARN("Scheduler N=" << N << " parallel sessions completed in " << duration_ms << " ms");
    REQUIRE(duration_ms < WAIT_TIMEOUT.count());

    az_scheduler->stop();
}

// Measures wall-clock cost of multiple parallel sessions against a deliberately
// slow MySQL mock connector. The backend manager's enqueue_impl mutex
// (integration/sql/connection_manager.cpp) plus the synchronous
// QueryHandleWaiter::wait (utility/wait_barrier.hpp) still serialise the
// sessions inside the backend — this test documents that baseline.
TEST_CASE("slow MySQL connector does not starve other sessions") {
    using namespace std::chrono_literals;

    constexpr auto SLOW_WAIT = 300ms;
    constexpr size_t SESSIONS = 4;
    constexpr auto WAIT_TIMEOUT = 30000ms;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    REQUIRE(resource != nullptr);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource,
                                                std::make_unique<SimpleMockOtterbrixManager>(
                                                    mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager =
        std::make_shared<mysql::ConnectorManager>(catalog_manager->address(),
                                                  mysql_mock_connector_factory_slow(resource, SLOW_WAIT));
    auto pg_conn_manager =
        std::make_shared<pg::ConnectorManager>(catalog_manager->address(), pg_mock_connector_factory(resource));

    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);

    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "1");
    mysql_conn_manager->addConnection(boost::mysql::connect_params{}, "2");

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
        &make_mock_parser,
        mysql_connection_manager->address(),
        pg_connection_manager->address(),
        ch_connection_manager->address(),
        otterbrix_manager->address(),
        catalog_manager->address(),
        actor_zeta::address_t::empty_address(),
        actor_zeta::address_t::empty_address());
    REQUIRE(scheduler != nullptr);

    // Baseline: time for a single session.
    auto baseline_start = std::chrono::steady_clock::now();
    {
        auto fut = actor_zeta::send(scheduler->address(),
                                    &Scheduler::execute,
                                    static_cast<session_hash_t>(1000),
                                    std::string("SELECT 1"))
                       .second;
        auto r = await_session(std::move(fut), WAIT_TIMEOUT, resource);
        REQUIRE_FALSE(r.has_error());
    }
    auto baseline_end = std::chrono::steady_clock::now();
    auto baseline_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(baseline_end - baseline_start).count();

    // SESSIONS in parallel.
    std::vector<actor_zeta::unique_future<core::result_wrapper_t<session_payload>>> futs;
    futs.reserve(SESSIONS);
    auto parallel_start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < SESSIONS; ++i) {
        futs.push_back(actor_zeta::send(scheduler->address(),
                                        &Scheduler::execute,
                                        static_cast<session_hash_t>(i + 1),
                                        std::string("SELECT 1"))
                           .second);
    }
    std::vector<core::result_wrapper_t<session_payload>> results;
    results.reserve(SESSIONS);
    for (size_t i = 0; i < SESSIONS; ++i) {
        results.push_back(await_session(std::move(futs[i]), WAIT_TIMEOUT, resource));
    }
    auto parallel_end = std::chrono::steady_clock::now();
    auto parallel_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(parallel_end - parallel_start).count();

    for (size_t i = 0; i < SESSIONS; ++i) {
        REQUIRE_FALSE(results[i].has_error());
    }

    double ratio = baseline_ms > 0 ? static_cast<double>(parallel_ms) / static_cast<double>(baseline_ms) : 0.0;
    WARN("baseline=" << baseline_ms << "ms, parallel(" << SESSIONS << ")=" << parallel_ms
                     << "ms, ratio=" << ratio);

    REQUIRE(parallel_ms < WAIT_TIMEOUT.count());

    az_scheduler->stop();
}

// QueryHandleWaiter (utility/wait_barrier.hpp) iterates futures with .get(),
// which must propagate exceptions rather than block forever.
TEST_CASE("QueryHandleWaiter propagates future exceptions") {
    QueryHandleWaiter<int> waiter;

    std::promise<int> good;
    std::promise<int> bad;
    waiter.futures.push_back(good.get_future());
    waiter.futures.push_back(bad.get_future());

    good.set_value(42);
    bad.set_exception(std::make_exception_ptr(std::runtime_error("simulated DB failure")));

    REQUIRE_THROWS_AS(waiter.wait(), std::runtime_error);
}
