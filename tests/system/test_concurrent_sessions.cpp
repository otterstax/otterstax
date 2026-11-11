// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "backends/remote_backends.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "session/session_payload.hpp"
#include "session_stack.hpp"
#include "test_helpers.hpp"
#include "utility/wait_barrier.hpp"

#include "../mock/ch_db_connector.hpp"
#include "../mock/mock_config.hpp"
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

#include <catch2/catch_all.hpp>
#include <chrono>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
    // The stack and the session-future bridge are the ones every system test
    // shares (session_stack.hpp); the mock MySQL connect params and the future
    // polling come from test_helpers.hpp.
    using otterstax::session::query_session_t;
    using otterstax::test::await_session;
    using otterstax::test::poll_until_ready;
    using otterstax::test::session_stack_owner;
    using otterstax::test::stack_options_t;
    using otterstax::test::stmt;

    // connector_factory is a plain function pointer, so the delay is a
    // namespace constant rather than captured state.
    constexpr auto SLOW_WAIT = std::chrono::milliseconds(300);

    std::unique_ptr<mysql::IConnector> mysql_mock_connector_factory_slow(std::pmr::memory_resource* resource,
                                                                         boost::asio::io_context&,
                                                                         boost::mysql::connect_params params,
                                                                         std::string alias) {
        return std::make_unique<mysql::MockConnector>(mock_config{.resource = resource, .wait_time = SLOW_WAIT},
                                                      std::move(alias),
                                                      std::move(params));
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

    // ClickHouse connector for the concurrency case: system.columns answers
    // (id Int32, name String), the schema probe a 0-row header block of them,
    // every data query no blocks.
    clickhouse::Block ch_header_block() {
        clickhouse::Block block;
        block.AppendColumn("id", std::make_shared<clickhouse::ColumnInt32>());
        block.AppendColumn("name", std::make_shared<clickhouse::ColumnString>());
        return block;
    }

    clickhouse::Block ch_named_types_block() {
        auto names = std::make_shared<clickhouse::ColumnString>();
        auto types = std::make_shared<clickhouse::ColumnString>();
        names->Append("id");
        types->Append("Int32");
        names->Append("name");
        types->Append("String");
        clickhouse::Block block;
        block.AppendColumn("name", names);
        block.AppendColumn("type", types);
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

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>> runQuery(
            std::string_view,
            otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler) override {
            // The table's columns, no row.
            ch::select_result_t outcome;
            outcome.blocks.push_back(ch_header_block());
            co_return handler(outcome);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)>) override {
            co_return int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t probe;
            probe.blocks.push_back(query.find("system.columns") != std::string_view::npos ? ch_named_types_block()
                                                                                          : ch_header_block());
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

// The whole stack with mock connectors: N connections, each its own query
// session, fire one statement each before any is awaited. N parallel sessions
// don't hang and each returns a typed result.
TEST_CASE("N parallel connections run without hanging") {
    using namespace std::chrono_literals;

    constexpr size_t N = 32;
    constexpr auto WAIT_TIMEOUT = 60000ms;

    stack_options_t options;
    options.parser_factory = &make_mock_parser;
    options.connections.mysql = otterstax::test::mock_mysql({"1", "2"});
    session_stack_owner owner("/tmp/test_concurrent_sessions", options);
    auto* resource = owner.engine().resource();

    std::vector<actor_zeta::address_t> connections;
    connections.reserve(N);
    for (size_t i = 0; i < N; ++i) {
        connections.push_back(owner.open_connection());
    }

    const std::string sql = "SELECT 1 AS test";
    std::vector<actor_zeta::unique_future<core::result_wrapper_t<session_payload>>> futs;
    futs.reserve(N);

    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < N; ++i) {
        futs.push_back(actor_zeta::send(connections[i], &query_session_t::execute, stmt(i + 1), sql).second);
    }

    std::vector<core::result_wrapper_t<session_payload>> results;
    results.reserve(N);
    for (size_t i = 0; i < N; ++i) {
        results.push_back(await_session(std::move(futs[i]), WAIT_TIMEOUT, resource));
    }
    auto end = std::chrono::steady_clock::now();

    for (size_t i = 0; i < N; ++i) {
        INFO("connection " << i);
        REQUIRE_FALSE(results[i].has_error());
        REQUIRE(results[i].value().size() == 2);
    }

    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    WARN("N=" << N << " parallel connections completed in " << duration_ms << " ms");
    REQUIRE(duration_ms < WAIT_TIMEOUT.count());
}

// Measures wall-clock cost of parallel connections against a deliberately slow
// MySQL mock connector: the federation's reads run on the backend's statement
// workers, off the engine's executors — this test documents the ratio.
TEST_CASE("slow MySQL connector does not starve other connections") {
    using namespace std::chrono_literals;

    constexpr size_t SESSIONS = 4;
    constexpr auto WAIT_TIMEOUT = 30000ms;

    stack_options_t options;
    options.parser_factory = &make_mock_parser;
    options.connectors.mysql = &mysql_mock_connector_factory_slow;
    options.connections.mysql = otterstax::test::mock_mysql({"1", "2"});
    session_stack_owner owner("/tmp/test_concurrent_slow", options);
    auto* resource = owner.engine().resource();

    // Baseline: time for a single statement.
    auto baseline_start = std::chrono::steady_clock::now();
    {
        auto fut =
            actor_zeta::send(owner.stack().session, &query_session_t::execute, stmt(1000), std::string("SELECT 1"))
                .second;
        auto r = await_session(std::move(fut), WAIT_TIMEOUT, resource);
        REQUIRE_FALSE(r.has_error());
    }
    auto baseline_end = std::chrono::steady_clock::now();
    auto baseline_ms = std::chrono::duration_cast<std::chrono::milliseconds>(baseline_end - baseline_start).count();

    // SESSIONS connections in parallel.
    std::vector<actor_zeta::address_t> connections;
    for (size_t i = 0; i < SESSIONS; ++i) {
        connections.push_back(owner.open_connection());
    }
    std::vector<actor_zeta::unique_future<core::result_wrapper_t<session_payload>>> futs;
    futs.reserve(SESSIONS);
    auto parallel_start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < SESSIONS; ++i) {
        futs.push_back(
            actor_zeta::send(connections[i], &query_session_t::execute, stmt(i + 1), std::string("SELECT 1")).second);
    }
    std::vector<core::result_wrapper_t<session_payload>> results;
    results.reserve(SESSIONS);
    for (size_t i = 0; i < SESSIONS; ++i) {
        results.push_back(await_session(std::move(futs[i]), WAIT_TIMEOUT, resource));
    }
    auto parallel_end = std::chrono::steady_clock::now();
    auto parallel_ms = std::chrono::duration_cast<std::chrono::milliseconds>(parallel_end - parallel_start).count();

    for (size_t i = 0; i < SESSIONS; ++i) {
        REQUIRE_FALSE(results[i].has_error());
    }

    double ratio = baseline_ms > 0 ? static_cast<double>(parallel_ms) / static_cast<double>(baseline_ms) : 0.0;
    WARN("baseline=" << baseline_ms << "ms, parallel(" << SESSIONS << ")=" << parallel_ms << "ms, ratio=" << ratio);

    REQUIRE(parallel_ms < WAIT_TIMEOUT.count());
}

namespace {
    namespace fed = otterstax::federation;

    // The remote backends alone over `connectors`, the connections of
    // `connections` opened (no engine). `runtime` lends its backends pool and
    // must outlive the module.
    std::unique_ptr<otterstax::backends::remote_backends_t>
    open_remote(std::pmr::memory_resource* resource,
                otterstax::runtime::runtime_t& runtime,
                otterstax::backends::connector_factories_t connectors,
                const config::ConnectionsConfig& connections) {
        auto remote = std::make_unique<otterstax::backends::remote_backends_t>(
            resource,
            otterstax::backends::remote_backends_options_t{.connectors = connectors, .runtime = &runtime});
        auto opened = remote->open(connections, config::ConnectionRetryConfig{});
        INFO(opened.what.c_str());
        REQUIRE_FALSE(opened.contains_error());
        REQUIRE(remote->discovered().size() == 1);
        return remote;
    }

    constexpr fed::oid_t table_oid = 100;

    // The startup discovery's table (id, name) handed to the connection's actor, as the
    // registration does once the engine is open (oid `table_oid`).
    fed::backend_actor_t& actor_with_table(otterstax::backends::remote_backends_t& remote,
                                           const std::string& alias,
                                           const std::string& container) {
        auto* actor = remote.actor(alias);
        REQUIRE(actor != nullptr);
        const auto& discovered = remote.discovered().front();
        REQUIRE(discovered.discovered);
        REQUIRE(discovered.tables.size() == 1);
        const auto& table = discovered.tables.front();
        fed::registered_table_t registered{table_oid,
                                           qualified_name_t{core::connection_name_t{alias},
                                                            core::dbname_t{"d"},
                                                            core::schema_t{"public"},
                                                            core::relname_t{table.name.collection.t}},
                                           container,
                                           table.name.collection.t,
                                           {}};
        fed::oid_t attoid = 1;
        for (const auto& column : table.schema.child_types()) {
            const auto type = table.remote_types.find(column.alias());
            registered.columns.push_back(
                fed::registered_column_t{attoid++,
                                         column.alias(),
                                         column,
                                         column.alias(),
                                         type == table.remote_types.end() ? std::string{} : type->second});
        }
        actor->load_tables({std::move(registered)});
        return *actor;
    }

    // One read of the table through the connection's actor (otterbrix's storage_fetch_next_batch,
    // an autocommit statement), polled from the calling thread: the rows it answered, or the error.
    thread_verdict scan_once(fed::backend_actor_t& actor) {
        thread_verdict verdict;
        auto [needs_sched, future] = actor_zeta::send(&actor,
                                                      &fed::backend_actor_t::storage_fetch_next_batch,
                                                      components::session::session_id_t{},
                                                      table_oid,
                                                      std::uint64_t{0},
                                                      std::int64_t{-1},
                                                      std::vector<size_t>{},
                                                      components::table::transaction_data{1, 0});
        static_cast<void>(needs_sched);
        if (!poll_until_ready(future, THREAD_POLL_TIMEOUT)) {
            verdict.what = "read did not settle";
            return verdict;
        }
        auto page = std::move(future).take_ready();
        if (page.has_error()) {
            verdict.what = std::string{page.error().what.c_str()};
            return verdict;
        }
        verdict.rows = page.value().batch ? page.value().batch->size() : 0;
        verdict.ok = true;
        return verdict;
    }

    // One discovery through the connection's actor (its discover message).
    thread_verdict discover_once(fed::backend_actor_t& actor, const qualified_name_t& scope) {
        thread_verdict verdict;
        auto [needs_sched, future] = actor_zeta::send(&actor, &fed::backend_actor_t::discover, scope);
        static_cast<void>(needs_sched);
        if (!poll_until_ready(future, THREAD_POLL_TIMEOUT)) {
            verdict.what = "discovery did not settle";
            return verdict;
        }
        auto reply = std::move(future).take_ready();
        if (reply.has_error()) {
            verdict.what = std::string{reply.error().what.c_str()};
            return verdict;
        }
        verdict.ok = true;
        return verdict;
    }
} // namespace

// A PostgreSQL connection's actor: reads and re-discoveries sent from many threads at once run on
// its agents (an agent's ENUM labels read again after a discovery) — the invariant ThreadSanitizer
// checks.
TEST_CASE("PostgreSQL actor: parallel reads and re-discovery share no unsynchronised state") {
    constexpr size_t THREADS = 8;
    constexpr size_t ROUNDS = 6;

    std::pmr::synchronized_pool_resource resource(std::pmr::new_delete_resource());
    config::ConnectionsConfig connections;
    connections.postgresql.push_back(config::PgConnectionDesc{.alias = "1",
                                                              .host = "localhost",
                                                              .port = "5432",
                                                              .username = "user",
                                                              .password = "pass",
                                                              .database = "pgdb",
                                                              .schema = "public",
                                                              .table = "t"});
    otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
    auto remote = open_remote(&resource, runtime, otterstax::test::mock_connectors(), connections);
    auto& actor = actor_with_table(*remote, "1", "public");

    std::vector<thread_verdict> verdicts(THREADS * ROUNDS);
    {
        std::vector<std::jthread> threads;
        threads.reserve(THREADS);
        for (size_t t = 0; t < THREADS; ++t) {
            threads.emplace_back([&, t] {
                for (size_t r = 0; r < ROUNDS; ++r) {
                    verdicts[t * ROUNDS + r] = t % 2 == 0 ? scan_once(actor)
                                                          : discover_once(actor,
                                                                          qualified_name_t{core::connection_name_t{"1"},
                                                                                           core::dbname_t{"pgdb"},
                                                                                           core::schema_t{"public"},
                                                                                           core::relname_t{"t"}});
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

// Same shape for a ClickHouse connection: a read holds its columns to their system.columns types.
TEST_CASE("ClickHouse actor: parallel reads and re-discovery share no unsynchronised state") {
    constexpr size_t THREADS = 8;
    constexpr size_t ROUNDS = 6;

    std::pmr::synchronized_pool_resource resource(std::pmr::new_delete_resource());
    config::ConnectionsConfig connections;
    connections.clickhouse.push_back(
        config::ChConnectionDesc{.alias = "1", .host = "localhost", .database = "ev", .table = "t"});
    auto connectors = otterstax::test::mock_connectors();
    connectors.clickhouse = &ch_header_connector_factory;
    otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
    auto remote = open_remote(&resource, runtime, connectors, connections);
    auto& actor = actor_with_table(*remote, "1", "ev");

    std::vector<thread_verdict> verdicts(THREADS * ROUNDS);
    {
        std::vector<std::jthread> threads;
        threads.reserve(THREADS);
        for (size_t t = 0; t < THREADS; ++t) {
            threads.emplace_back([&, t] {
                for (size_t r = 0; r < ROUNDS; ++r) {
                    verdicts[t * ROUNDS + r] = t % 2 == 0 ? scan_once(actor)
                                                          : discover_once(actor,
                                                                          qualified_name_t{core::connection_name_t{"1"},
                                                                                           core::dbname_t{"ev"},
                                                                                           core::schema_t{},
                                                                                           core::relname_t{"t"}});
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
