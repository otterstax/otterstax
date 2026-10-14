// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The ClickHouse ConnectorManager over its per-alias connection pool, with a
// stub connector injected through the factory, driven by its integration backend
// (db::ClickhouseManager, whose scans run on the runtime's backends pool):
//   * addConnection opens pool_size connections; 0 is invalid_parameter;
//   * two scans of one alias at once (a JOIN of two tables of one server) never
//     run on one connection at the same time: with pool_size 2 they run side by
//     side on two connections, with pool_size 1 one waits for the other.

#include "connectors/clickhouse/manager.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "runtime/runtime.hpp"
#include "tests/system/session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <actor-zeta.hpp>
#include <clickhouse/columns/numeric.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {

    using namespace std::chrono_literals;
    using components::vector::data_chunk_t;

    // What the stub connectors did, process-wide: the factory is a plain function pointer.
    struct ch_recorder_t {
        std::atomic<int> next_id{0};
        std::atomic<int> in_flight{0};
        std::atomic<int> max_in_flight{0};
        // A query that started on a connection already running one.
        std::atomic<int> shared_use{0};
        std::atomic<int> connects{0};
        std::mutex mutex;
        std::set<int> connections_used;

        void reset() {
            next_id = 0;
            in_flight = 0;
            max_in_flight = 0;
            shared_use = 0;
            connects = 0;
            std::lock_guard lock(mutex);
            connections_used.clear();
        }
    };

    ch_recorder_t& ch_recorder() {
        static ch_recorder_t instance;
        return instance;
    }

    // One clickhouse::Client stand-in: a query takes 200 ms and answers one Int64 row; it notes a
    // second query started on it while the first still runs.
    class ch_stub final : public ch::IConnector {
    public:
        ch_stub(ch::connect_params params, std::string alias)
            : params_(std::move(params))
            , alias_(std::move(alias))
            , id_(ch_recorder().next_id++) {}

        ch::Status status() const noexcept override { return status_; }
        ch::connect_params params() const noexcept override { return params_; }
        void close() override { status_ = ch::Status::Closed; }
        core::error_t connect() override {
            ++ch_recorder().connects;
            status_ = ch::Status::Connected;
            return core::error_t::no_error();
        }
        bool isConnected() override { return status_ == ch::Status::Connected; }
        core::error_t tryReconnect() override { return connect(); }
        bool isClosed() const noexcept override { return status_ == ch::Status::Closed; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler) override {
            auto result = run();
            co_return otterstax::as_query_result<std::unique_ptr<data_chunk_t>>(handler(result));
        }
        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)> handler) override {
            auto result = run();
            co_return otterstax::as_query_result<int64_t>(handler(result));
        }
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            auto result = run();
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result));
        }

    private:
        ch::select_result_t run() {
            auto& recorder = ch_recorder();
            if (busy_.exchange(true)) {
                ++recorder.shared_use;
            }
            {
                std::lock_guard lock(recorder.mutex);
                recorder.connections_used.insert(id_);
            }
            const int now = recorder.in_flight.fetch_add(1) + 1;
            int seen = recorder.max_in_flight.load();
            while (now > seen && !recorder.max_in_flight.compare_exchange_weak(seen, now)) {
            }
            std::this_thread::sleep_for(200ms);
            ch::select_result_t result;
            auto id = std::make_shared<clickhouse::ColumnInt64>();
            id->Append(id_);
            clickhouse::Block block;
            block.AppendColumn("id", id);
            result.blocks.push_back(std::move(block));
            recorder.in_flight.fetch_sub(1);
            busy_.store(false);
            return result;
        }

        ch::connect_params params_;
        std::string alias_;
        int id_;
        ch::Status status_{ch::Status::Created};
        std::atomic<bool> busy_{false};
    };

    std::unique_ptr<ch::IConnector>
    make_ch_stub(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        return std::make_unique<ch_stub>(std::move(params), std::move(alias));
    }

    // The manager and its backend over a runtime whose backends pool has room for two statements.
    struct ch_stack {
        std::pmr::synchronized_pool_resource resource;
        otterstax::runtime::runtime_t runtime{[] {
            auto config = otterstax::test::test_runtime_config();
            config.backends = config::PoolConfig{2};
            return config;
        }()};
        std::unique_ptr<ch::ConnectorManager> manager;
        std::unique_ptr<db::ClickhouseManager> backend;

        ch_stack()
            : manager(std::make_unique<ch::ConnectorManager>(&resource, &make_ch_stub, 2))
            , backend(std::make_unique<db::ClickhouseManager>(&resource, manager.get(), runtime.backends_scheduler())) {
            ch_recorder().reset();
        }

        void add(const std::string& alias, std::size_t pool_size) {
            auto added = manager->addConnection(conn::api_server::ChConnectionParams{.alias = alias,
                                                                                     .host = "localhost",
                                                                                     .port = "",
                                                                                     .username = "default",
                                                                                     .password = "",
                                                                                     .database = "ev",
                                                                                     .table = "",
                                                                                     .pool_size = pool_size});
            REQUIRE_FALSE(added.has_error());
        }

        // Two scans of `alias` started at once, as a JOIN of two of its tables starts them; both
        // must answer one row.
        void two_scans(const std::string& alias) {
            std::pmr::synchronized_pool_resource promises;
            auto first_rows = std::make_shared<otterstax::federation::scan_rows_t>();
            auto second_rows = std::make_shared<otterstax::federation::scan_rows_t>();
            auto first = backend->scan(&promises,
                                     otterstax::federation::scan_request_t{alias, "SELECT `id` FROM `ev`.`a`", {}},
                                     first_rows);
            auto second = backend->scan(&promises,
                                      otterstax::federation::scan_request_t{alias, "SELECT `id` FROM `ev`.`b`", {}},
                                      second_rows);
            const auto deadline = std::chrono::steady_clock::now() + 10s;
            while ((!first.is_ready() || !second.is_ready()) && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(5ms);
            }
            REQUIRE(first.is_ready());
            REQUIRE(second.is_ready());
            auto first_error = std::move(first).take_ready();
            auto second_error = std::move(second).take_ready();
            INFO(first_error.what.c_str() << " / " << second_error.what.c_str());
            REQUIRE_FALSE(first_error.contains_error());
            REQUIRE_FALSE(second_error.contains_error());
            REQUIRE(first_rows->chunks.size() == 1);
            REQUIRE(second_rows->chunks.size() == 1);
            CHECK(first_rows->chunks.front().size() == 1);
            CHECK(second_rows->chunks.front().size() == 1);
        }
    };

} // namespace

TEST_CASE("pooled clickhouse: addConnection opens pool_size connections and 0 is invalid_parameter") {
    ch_stack stack;
    stack.add("three", 3);
    CHECK(stack.manager->poolSize("three") == 3);
    CHECK(stack.manager->idleConnections("three") == 3);
    CHECK(ch_recorder().connects == 3);

    auto zero = stack.manager->addConnection(
        conn::api_server::ChConnectionParams{.alias = "zero", .host = "localhost", .database = "ev", .pool_size = 0});
    REQUIRE(zero.has_error());
    CHECK(zero.error().type == core::error_code_t::invalid_parameter);
    CHECK_FALSE(stack.manager->hasConnection("zero"));
}

TEST_CASE("pooled clickhouse: two scans of one alias never share a connection") {
    SECTION("pool_size 2: side by side on two connections") {
        ch_stack stack;
        stack.add("ch", 2);
        stack.two_scans("ch");
        CHECK(ch_recorder().shared_use == 0);
        CHECK(ch_recorder().max_in_flight == 2);
        CHECK(ch_recorder().connections_used.size() == 2);
        CHECK(stack.manager->idleConnections("ch") == 2);
    }
    SECTION("pool_size 1: one after the other on the one connection") {
        ch_stack stack;
        stack.add("ch", 1);
        stack.two_scans("ch");
        CHECK(ch_recorder().shared_use == 0);
        CHECK(ch_recorder().max_in_flight == 1);
        CHECK(stack.manager->idleConnections("ch") == 1);
    }
}
