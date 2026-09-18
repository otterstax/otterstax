// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog/catalog_manager.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "scheduler/session_data.hpp"
#include "scheduler/scheduler.hpp"
#include "scheduler_stack.hpp"
#include "test_helpers.hpp"
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
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <components/logical_plan/node_data.hpp>
#include <core/result_wrapper.hpp>
#include <otterbrix/otterbrix.hpp>

#include <catch2/catch_all.hpp>
#include <chrono>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
    // Worker pool, session-future bridge and pool sizing are the ones every
    // system test shares (scheduler_stack.hpp); the default-config engine, the
    // mock MySQL connect params and the future polling come from test_helpers.hpp.
    using otterstax::test::await_session;
    using otterstax::test::init_default_test_otterbrix;
    using otterstax::test::make_az_scheduler;
    using otterstax::test::mock_connect_params;
    using otterstax::test::poll_until_ready;
    using otterstax::test::worker_pool_size;

    // connector_factory is a plain function pointer, so the delay is a
    // namespace constant rather than captured state.
    constexpr auto SLOW_WAIT = std::chrono::milliseconds(300);

    std::unique_ptr<mysql::IConnector> mysql_mock_connector_factory_slow(std::pmr::memory_resource* resource,
                                                                        boost::asio::io_context&,
                                                                        boost::mysql::connect_params,
                                                                        std::string alias) {
        return std::make_unique<mysql::MockConnector>(mock_config{.resource = resource, .wait_time = SLOW_WAIT},
                                                      std::move(alias));
    }

    // How long a test thread polls a cross-actor future; poll_until_ready
    // asserts nothing, so it is safe off the main thread.
    constexpr auto THREAD_POLL_TIMEOUT = std::chrono::seconds(30);

    // What a test thread records about one message it sent: Catch2 assertions
    // are not thread-safe, so the verdicts are checked after the join.
    struct thread_verdict {
        bool ok{false};
        std::string what;
        size_t rows{0};
    };

    // The single external slot of the mock parser's statement, rewritten as a
    // schema node carrying raw backend SQL — what the catalog produces for a
    // SELECT — so a backend actor's execute runs it against uid "1".
    ParsedQueryDataPtr raw_select_statement(std::pmr::memory_resource* resource) {
        SimpleMockParser parser(mock_config{.resource = resource});
        auto parsed = parser.parse("SELECT 1");
        if (parsed.has_error()) {
            return nullptr;
        }
        auto data = std::move(parsed.value());
        auto& slot = data->otterbrix_params->external_nodes.front().front();
        *slot.node = schema_utils::make_node_schema_raw(resource, slot.target.name, "SELECT 1", {});
        return data;
    }

    const data_chunk_t* fetched_slot(const ParsedQueryDataPtr& data) {
        const auto& slot = data->otterbrix_params->external_nodes.front().front();
        if ((*slot.node)->type() != components::logical_plan::node_type::data_t) {
            return nullptr;
        }
        return &static_cast<const components::logical_plan::node_data_t&>(**slot.node).data_chunk();
    }

    // ClickHouse connector for the concurrency case: every metadata query
    // (named types, schema probe) is answered with a 0-row header block, every
    // data query with no blocks.
    clickhouse::Block ch_header_block() {
        clickhouse::Block block;
        block.AppendColumn("id", std::make_shared<clickhouse::ColumnInt32>());
        block.AppendColumn("name", std::make_shared<clickhouse::ColumnString>());
        return block;
    }

    class ch_header_connector final : public ch::IConnector {
    public:
        ch_header_connector(ch::connect_params params, std::string alias)
            : params_(std::move(params))
            , alias_(std::move(alias)) {}

        ch::Status status() const noexcept override { return ch::Status::Connected; }
        ch::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler)
            override {
            ch::select_result_t outcome;
            co_return handler(outcome);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)>) override {
            co_return int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t probe;
            probe.blocks.push_back(ch_header_block());
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(probe));
        }

    private:
        ch::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<ch::IConnector>
    ch_header_connector_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        return std::make_unique<ch_header_connector>(std::move(params), std::move(alias));
    }

} // namespace

// Spawns the full scheduler + connection managers stack with mock connectors and
// fires N parallel Scheduler::execute() messages. Validates that N parallel
// sessions don't hang and each returns a typed result.
TEST_CASE("scheduler handles N parallel sessions without hanging") {
    using namespace std::chrono_literals;

    constexpr size_t N = 32;
    constexpr auto WAIT_TIMEOUT = 60000ms;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
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
        std::make_unique<mysql::ConnectorManager>(resource, catalog_manager->address(), &mysql_mock_connector_factory);
    auto pg_conn_manager =
        std::make_unique<pg::ConnectorManager>(resource, catalog_manager->address(), &pg_mock_connector_factory);
    auto ch_conn_manager =
        std::make_unique<ch::ConnectorManager>(resource, catalog_manager->address(), &ch_mock_connector_factory);

    // The integration actors start the connector thread pools and are the
    // catalog's route to a backend; addConnection needs both.
    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(mysql_connection_manager->address(),
                                          pg_connection_manager->address(),
                                          ch_connection_manager->address());

    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "1").has_error());
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "2").has_error());

    // The current Scheduler ctor takes (resource, az_scheduler, worker_count,
    // parser_factory, 5 actor addresses + s3 + file); each Worker builds its
    // own parser from the injected factory — tests pass &make_mock_parser. The
    // s3/file managers are empty here because the mock pipeline does not exercise
    // CREATE EXTERNAL TABLE / COPY ... TO statements.
    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
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

    constexpr size_t SESSIONS = 4;
    constexpr auto WAIT_TIMEOUT = 30000ms;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    REQUIRE(resource != nullptr);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource,
                                                std::make_unique<SimpleMockOtterbrixManager>(
                                                    mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto mysql_conn_manager = std::make_unique<mysql::ConnectorManager>(resource,
                                                                        catalog_manager->address(),
                                                                        &mysql_mock_connector_factory_slow);
    auto pg_conn_manager =
        std::make_unique<pg::ConnectorManager>(resource, catalog_manager->address(), &pg_mock_connector_factory);
    auto ch_conn_manager =
        std::make_unique<ch::ConnectorManager>(resource, catalog_manager->address(), &ch_mock_connector_factory);

    auto mysql_connection_manager = actor_zeta::spawn<db::MySQLManager>(resource, mysql_conn_manager.get());
    auto pg_connection_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    auto ch_connection_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(mysql_connection_manager->address(),
                                          pg_connection_manager->address(),
                                          ch_connection_manager->address());

    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "1").has_error());
    REQUIRE_FALSE(mysql_conn_manager->addConnection(mock_connect_params(), "2").has_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
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

// The PostgreSQL actor's ENUM cache is written by discovery (the catalog's
// add_connection_schema, re-run here for an already registered table) and read
// by execute. Both are messages to the same actor, so senders on different
// threads are serialised by its mailbox and the io-thread query only hands a
// value back — the invariant ThreadSanitizer checks while the two interleave.
TEST_CASE("PostgressManager: parallel execute and re-discovery share no unsynchronised state") {
    constexpr size_t THREADS = 8;
    constexpr size_t ROUNDS = 6;

    auto* resource = std::pmr::new_delete_resource();
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto pg_conn_manager =
        std::make_unique<pg::ConnectorManager>(resource, catalog_manager->address(), &pg_mock_connector_factory, 2);
    auto pg_manager = actor_zeta::spawn<db::PostgressManager>(resource, pg_conn_manager.get());
    catalog_manager->set_backend_managers(actor_zeta::address_t::empty_address(),
                                          pg_manager->address(),
                                          actor_zeta::address_t::empty_address());

    // uid "1" is what the mock parser stamps on its external node.
    conn::api_server::PgConnectionParams pg_params;
    pg_params.alias = "1";
    pg_params.host = "localhost";
    pg_params.port = "5432";
    pg_params.username = "user";
    pg_params.password = "pass";
    pg_params.database = "pgdb";
    pg_params.schema = "public";
    pg_params.table = "t";
    REQUIRE_FALSE(pg_conn_manager->addConnection(pg_params).has_error());

    std::vector<thread_verdict> verdicts(THREADS * ROUNDS);
    {
        std::vector<std::jthread> threads;
        threads.reserve(THREADS);
        for (size_t t = 0; t < THREADS; ++t) {
            threads.emplace_back([&, t] {
                for (size_t r = 0; r < ROUNDS; ++r) {
                    auto& verdict = verdicts[t * ROUNDS + r];
                    if (t % 2 == 0) {
                        auto data = raw_select_statement(resource);
                        if (!data) {
                            verdict.what = "mock parse failed";
                            continue;
                        }
                        auto [needs_sched, future] = actor_zeta::send(pg_manager->address(),
                                                                      &db::PostgressManager::execute,
                                                                      static_cast<session_hash_t>(t * ROUNDS + r + 1),
                                                                      std::move(data));
                        if (!poll_until_ready(future, THREAD_POLL_TIMEOUT)) {
                            verdict.what = "execute did not settle";
                            continue;
                        }
                        auto result = std::move(future).take_ready();
                        if (result.has_error()) {
                            verdict.what = std::string{result.error().what.c_str()};
                            continue;
                        }
                        const auto* rows = fetched_slot(result.value());
                        if (rows == nullptr) {
                            verdict.what = "slot was not fetched";
                            continue;
                        }
                        verdict.rows = rows->size();
                        verdict.ok = true;
                    } else {
                        auto [needs_sched, future] =
                            actor_zeta::send(catalog_manager->address(),
                                             &mysql::CatalogManager::add_connection_schema,
                                             qualified_name_t{"1", "pgdb", "public", "t"},
                                             catalog_ext::ConnectionType::PostgreSQL);
                        if (!poll_until_ready(future, THREAD_POLL_TIMEOUT)) {
                            verdict.what = "add_connection_schema did not settle";
                            continue;
                        }
                        auto err = std::move(future).take_ready();
                        if (err.contains_error()) {
                            verdict.what = std::string{err.what.c_str()};
                            continue;
                        }
                        verdict.ok = true;
                    }
                }
            });
        }
    }

    for (size_t t = 0; t < THREADS; ++t) {
        for (size_t r = 0; r < ROUNDS; ++r) {
            const auto& verdict = verdicts[t * ROUNDS + r];
            INFO("thread " << t << " round " << r << ": " << verdict.what);
            REQUIRE(verdict.ok);
            if (t % 2 == 0) {
                REQUIRE(verdict.rows == 2);
            }
        }
    }
}

// Same shape for the ClickHouse actor: discovery rewrites the per-table
// named-type overrides that execute copies into its converter.
TEST_CASE("ClickhouseManager: parallel execute and re-discovery share no unsynchronised state") {
    constexpr size_t THREADS = 8;
    constexpr size_t ROUNDS = 6;

    auto* resource = std::pmr::new_delete_resource();
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto ch_conn_manager =
        std::make_unique<ch::ConnectorManager>(resource, catalog_manager->address(), &ch_header_connector_factory, 2);
    auto ch_manager = actor_zeta::spawn<db::ClickhouseManager>(resource, ch_conn_manager.get());
    catalog_manager->set_backend_managers(actor_zeta::address_t::empty_address(),
                                          actor_zeta::address_t::empty_address(),
                                          ch_manager->address());

    ch::connect_params params;
    params.host = "localhost";
    params.database = "ev";
    params.table = "1";
    REQUIRE_FALSE(ch_conn_manager->addConnection(params, "1").has_error());

    std::vector<thread_verdict> verdicts(THREADS * ROUNDS);
    {
        std::vector<std::jthread> threads;
        threads.reserve(THREADS);
        for (size_t t = 0; t < THREADS; ++t) {
            threads.emplace_back([&, t] {
                for (size_t r = 0; r < ROUNDS; ++r) {
                    auto& verdict = verdicts[t * ROUNDS + r];
                    if (t % 2 == 0) {
                        auto data = raw_select_statement(resource);
                        if (!data) {
                            verdict.what = "mock parse failed";
                            continue;
                        }
                        auto [needs_sched, future] = actor_zeta::send(ch_manager->address(),
                                                                      &db::ClickhouseManager::execute,
                                                                      static_cast<session_hash_t>(t * ROUNDS + r + 1),
                                                                      std::move(data));
                        if (!poll_until_ready(future, THREAD_POLL_TIMEOUT)) {
                            verdict.what = "execute did not settle";
                            continue;
                        }
                        auto result = std::move(future).take_ready();
                        if (result.has_error()) {
                            verdict.what = std::string{result.error().what.c_str()};
                            continue;
                        }
                        if (fetched_slot(result.value()) == nullptr) {
                            verdict.what = "slot was not fetched";
                            continue;
                        }
                        verdict.ok = true;
                    } else {
                        auto [needs_sched, future] =
                            actor_zeta::send(catalog_manager->address(),
                                             &mysql::CatalogManager::add_connection_schema,
                                             qualified_name_t{"1", "ev", "", "1"},
                                             catalog_ext::ConnectionType::ClickHouse);
                        if (!poll_until_ready(future, THREAD_POLL_TIMEOUT)) {
                            verdict.what = "add_connection_schema did not settle";
                            continue;
                        }
                        auto err = std::move(future).take_ready();
                        if (err.contains_error()) {
                            verdict.what = std::string{err.what.c_str()};
                            continue;
                        }
                        verdict.ok = true;
                    }
                }
            });
        }
    }

    for (size_t t = 0; t < THREADS; ++t) {
        for (size_t r = 0; r < ROUNDS; ++r) {
            const auto& verdict = verdicts[t * ROUNDS + r];
            INFO("thread " << t << " round " << r << ": " << verdict.what);
            REQUIRE(verdict.ok);
        }
    }
}

// QueryHandleWaiter (utility/wait_barrier.hpp) iterates futures with .get(),
// which must surface a failure rather than block forever.
TEST_CASE("QueryHandleWaiter propagates future errors") {
    auto* resource = std::pmr::new_delete_resource();
    otterstax::QueryHandleWaiter<int> waiter{resource};

    std::promise<core::result_wrapper_t<int>> good;
    std::promise<core::result_wrapper_t<int>> bad;
    waiter.futures.push_back(good.get_future());
    waiter.futures.push_back(bad.get_future());

    // Connector errors arrive as a value (core::error_t inside result_wrapper_t),
    // not a future exception (utility/wait_barrier.hpp — avoids the boost.asio
    // use_future TSAN race); wait() hands that error back on the consumer thread
    // instead of throwing, which is why the result is [[nodiscard]].
    good.set_value(42);
    bad.set_value(core::error_t(core::error_code_t::io_error, std::pmr::string{"simulated DB failure", resource}));

    auto barrier = waiter.wait();
    REQUIRE(barrier.has_error());
    REQUIRE(std::string{barrier.error().what.c_str()} == "simulated DB failure");
}
