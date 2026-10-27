// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connectors/common/connection_pool.hpp"
#include "tests/mock/drive.hpp"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using otterstax::test::drive;

namespace {

    // Shared by every driver a pool clones from one prototype: what the test scripts and
    // what it observes.
    struct script_t {
        std::atomic<int> connects{0};
        std::atomic<int> executes{0};
        std::atomic<int> pings{0};
        std::atomic<int> closes{0};
        std::atomic<int> in_flight{0};
        std::atomic<int> max_in_flight{0};
        std::atomic<bool> fail_connect{false};
        std::chrono::milliseconds delay{0};
        std::atomic<int> connect_delay_ms{0}; // how long connect() takes
    };

    class mock_driver_t final : public conn::IDriver {
    public:
        mock_driver_t(std::pmr::memory_resource* resource, std::shared_ptr<script_t> script)
            : resource_(resource)
            , script_(std::move(script)) {}

        core::error_t connect() override {
            ++script_->connects;
            if (const int ms = script_->connect_delay_ms; ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            }
            if (script_->fail_connect) {
                return core::error_t(core::error_code_t::io_error, std::pmr::string{"refused", resource_});
            }
            return core::error_t::no_error();
        }

        conn::statement_outcome_t execute(const conn::statement_t& statement) override {
            const int now = ++script_->in_flight;
            int seen = script_->max_in_flight;
            while (now > seen && !script_->max_in_flight.compare_exchange_weak(seen, now)) {
            }
            if (script_->delay.count() > 0) {
                std::this_thread::sleep_for(script_->delay);
            }
            ++script_->executes;
            --script_->in_flight;
            conn::statement_result_t result;
            result.affected_rows = statement.sql.size();
            return result;
        }

        core::error_t ping() override {
            ++script_->pings;
            return core::error_t::no_error();
        }

        void close() noexcept override { ++script_->closes; }

        std::unique_ptr<conn::IDriver> clone_disconnected() const override {
            return std::make_unique<mock_driver_t>(resource_, script_);
        }

    private:
        std::pmr::memory_resource* resource_;
        std::shared_ptr<script_t> script_;
    };

    conn::pool_config_t config(std::size_t min_size, std::size_t max_size) {
        conn::pool_config_t c;
        c.min_size = min_size;
        c.max_size = max_size;
        return c;
    }

    struct fixture_t {
        std::pmr::synchronized_pool_resource resource;
        std::shared_ptr<script_t> script = std::make_shared<script_t>();
        conn::connection_pool_t pool{&resource};

        void add(const std::string& alias, conn::pool_config_t cfg) {
            auto added = pool.add(alias, std::make_unique<mock_driver_t>(&resource, script), cfg);
            REQUIRE_FALSE(added.contains_error());
        }

        conn::statement_outcome_t run(const std::string& alias, std::string sql) {
            return drive(&resource, pool.execute(alias, conn::statement_t::rows(std::pmr::string(sql, &resource))));
        }
    };

    // A plain object (not an actor) whose coroutines await pool statements through a
    // nested call — the shape db::backend_t has inside a Worker or the catalog.
    struct nested_caller_t {
        std::pmr::memory_resource* resource_;
        conn::connection_pool_t* pool;

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        actor_zeta::unique_future<core::result_wrapper_t<uint64_t>> inner(std::string alias) {
            auto first = co_await pool->execute(alias, conn::statement_t::rows("abc"));
            if (first.has_error()) {
                co_return core::error_on(resource(), first.error());
            }
            auto second = co_await pool->execute(alias, conn::statement_t::rows("defgh"));
            if (second.has_error()) {
                co_return core::error_on(resource(), second.error());
            }
            co_return first.value().affected_rows + second.value().affected_rows;
        }

        actor_zeta::unique_future<core::result_wrapper_t<uint64_t>> outer(std::string alias) {
            auto a = co_await inner(alias);
            if (a.has_error()) {
                co_return core::error_on(resource(), a.error());
            }
            auto b = co_await inner(alias);
            if (b.has_error()) {
                co_return core::error_on(resource(), b.error());
            }
            co_return a.value() + b.value();
        }
    };

} // namespace

TEST_CASE("connection_pool: a statement runs and its answer comes back through the future") {
    fixture_t f;
    f.add("a", config(1, 1));
    auto outcome = f.run("a", "SELECT 1");
    REQUIRE_FALSE(outcome.has_error());
    CHECK(outcome.value().affected_rows == 8);
    CHECK(f.script->executes == 1);
    CHECK(f.script->connects == 1);
}

TEST_CASE("connection_pool: an unknown alias is do_not_exists") {
    fixture_t f;
    auto outcome = f.run("missing", "SELECT 1");
    REQUIRE(outcome.has_error());
    CHECK(outcome.error().type == core::error_code_t::do_not_exists);
}

TEST_CASE("connection_pool: a burst grows the pool to max_size and runs statements side by side") {
    fixture_t f;
    f.script->delay = 200ms;
    f.add("a", config(1, 4));
    std::vector<conn::statement_future_t> futures;
    const auto started = std::chrono::steady_clock::now();
    for (int i = 0; i < 8; ++i) {
        futures.push_back(f.pool.execute("a", conn::statement_t::rows("SELECT 1")));
    }
    for (auto& future : futures) {
        auto outcome = drive(&f.resource, std::move(future));
        REQUIRE_FALSE(outcome.has_error());
    }
    const auto elapsed = std::chrono::steady_clock::now() - started;
    CHECK(f.script->executes == 8);
    CHECK(f.script->max_in_flight > 1);
    CHECK(f.script->max_in_flight <= 4);
    CHECK(f.pool.find("a")->open_sessions() <= 4);
    // Eight 200 ms statements one at a time would take 1.6 s.
    CHECK(elapsed < 1400ms);
}

TEST_CASE("connection_pool: connections above min_size close after idle_timeout") {
    fixture_t f;
    f.script->delay = 100ms;
    auto cfg = config(1, 4);
    cfg.idle_timeout = 200ms;
    f.add("a", cfg);
    std::vector<conn::statement_future_t> futures;
    for (int i = 0; i < 6; ++i) {
        futures.push_back(f.pool.execute("a", conn::statement_t::rows("SELECT 1")));
    }
    for (auto& future : futures) {
        REQUIRE_FALSE(drive(&f.resource, std::move(future)).has_error());
    }
    REQUIRE(f.pool.find("a")->open_sessions() > 1);
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (f.pool.find("a")->open_sessions() > 1 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(50ms);
    }
    CHECK(f.pool.find("a")->open_sessions() == 1);
    CHECK(f.script->closes >= 1);
    // The floor keeps serving.
    CHECK_FALSE(f.run("a", "SELECT 1").has_error());
}

// The last open session must not close for idleness while a second one is still connecting:
// if that connect then fails, the pool would have no session and nothing to grow it again.
TEST_CASE("connection_pool: a connect in flight does not count toward min_size") {
    fixture_t f;
    f.script->delay = 50ms;
    auto cfg = config(1, 2);
    cfg.idle_timeout = 100ms;
    cfg.connect_backoff = 10s;
    cfg.queue_timeout = 2s;
    f.add("a", cfg);
    f.script->connect_delay_ms = 600; // the grown session connects for longer than the idle timeout
    f.script->fail_connect = true;
    std::vector<conn::statement_future_t> futures;
    for (int i = 0; i < 2; ++i) {
        futures.push_back(f.pool.execute("a", conn::statement_t::rows("SELECT 1")));
    }
    for (auto& future : futures) {
        REQUIRE_FALSE(drive(&f.resource, std::move(future)).has_error());
    }
    std::this_thread::sleep_for(900ms); // the open session idles past its timeout, the connect fails
    CHECK(f.pool.find("a")->open_sessions() == 1);
    CHECK_FALSE(f.run("a", "SELECT 1").has_error());
}

TEST_CASE("connection_pool: a failed connect backs off and the open connection serves the queue") {
    fixture_t f;
    f.script->delay = 50ms;
    auto cfg = config(1, 4);
    cfg.connect_backoff = 10s;
    f.add("a", cfg);
    f.script->fail_connect = true;
    std::vector<conn::statement_future_t> futures;
    for (int i = 0; i < 4; ++i) {
        futures.push_back(f.pool.execute("a", conn::statement_t::rows("SELECT 1")));
    }
    for (auto& future : futures) {
        REQUIRE_FALSE(drive(&f.resource, std::move(future)).has_error());
    }
    CHECK(f.script->executes == 4);
    // The startup connect, then at most one growth attempt inside the backoff window.
    CHECK(f.script->connects <= 2);
    CHECK(f.pool.find("a")->open_sessions() == 1);
}

TEST_CASE("connection_pool: a connection idle past check_after_idle is pinged before use") {
    fixture_t f;
    auto cfg = config(1, 1);
    cfg.check_after_idle = 100ms;
    f.add("a", cfg);
    REQUIRE_FALSE(f.run("a", "SELECT 1").has_error());
    REQUIRE_FALSE(f.run("a", "SELECT 1").has_error());
    CHECK(f.script->pings == 0);
    std::this_thread::sleep_for(250ms);
    REQUIRE_FALSE(f.run("a", "SELECT 1").has_error());
    CHECK(f.script->pings == 1);
}

TEST_CASE("connection_pool: a request that waited past queue_timeout is refused") {
    fixture_t f;
    f.script->delay = 300ms;
    auto cfg = config(1, 1);
    cfg.queue_timeout = 100ms;
    f.add("a", cfg);
    auto first = f.pool.execute("a", conn::statement_t::rows("SELECT 1"));
    auto second = f.pool.execute("a", conn::statement_t::rows("SELECT 2"));
    CHECK_FALSE(drive(&f.resource, std::move(first)).has_error());
    auto refused = drive(&f.resource, std::move(second));
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::io_error);
    CHECK(f.script->executes == 1);
}

// The refusal comes when queue_timeout is up, not when a session frees up: a request queued
// behind a long statement on the only connection is answered (and its awaiter woken) while
// that statement still runs.
TEST_CASE("connection_pool: a request queued behind a busy pool is refused at queue_timeout") {
    fixture_t f;
    f.script->delay = 1500ms;
    auto cfg = config(1, 1);
    cfg.queue_timeout = 100ms;
    f.add("a", cfg);
    auto first = f.pool.execute("a", conn::statement_t::rows("SELECT 1"));
    std::atomic<int> wakes{0};
    const auto queued = std::chrono::steady_clock::now();
    auto second = f.pool.execute("a", conn::statement_t::rows("SELECT 2"), [&] { ++wakes; });

    for (int i = 0; i < 100 && !second.is_ready(); ++i) {
        std::this_thread::sleep_for(10ms);
    }
    const auto waited = std::chrono::steady_clock::now() - queued;
    REQUIRE(second.is_ready());
    CHECK(waited < 1000ms); // well before the running statement finishes
    CHECK(f.script->executes == 0);
    auto refused = std::move(second).take_ready();
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::io_error);
    for (int i = 0; i < 100 && wakes.load() == 0; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    CHECK(wakes.load() == 1);
    CHECK_FALSE(drive(&f.resource, std::move(first), 5s).has_error());
}

TEST_CASE("connection_pool: shutdown answers every queued request") {
    std::pmr::synchronized_pool_resource resource;
    auto script = std::make_shared<script_t>();
    script->delay = 300ms;
    std::vector<conn::statement_future_t> futures;
    {
        conn::connection_pool_t pool{&resource};
        REQUIRE_FALSE(pool.add("a", std::make_unique<mock_driver_t>(&resource, script), config(1, 1)).contains_error());
        for (int i = 0; i < 3; ++i) {
            futures.push_back(pool.execute("a", conn::statement_t::rows("SELECT 1")));
        }
        std::this_thread::sleep_for(50ms); // the first statement is running
    }
    int answered = 0;
    int refused = 0;
    for (auto& future : futures) {
        auto outcome = drive(&resource, std::move(future), 5s);
        outcome.has_error() ? ++refused : ++answered;
    }
    CHECK(answered == 1);
    CHECK(refused == 2);
}

TEST_CASE("connection_pool: a duplicate alias or a failed first connect is not registered") {
    fixture_t f;
    f.add("a", config(1, 1));
    auto duplicate = f.pool.add("a", std::make_unique<mock_driver_t>(&f.resource, f.script), config(1, 1));
    CHECK(duplicate.type == core::error_code_t::invalid_parameter);

    f.script->fail_connect = true;
    auto refused = f.pool.add("b", std::make_unique<mock_driver_t>(&f.resource, f.script), config(1, 1));
    CHECK(refused.type == core::error_code_t::io_error);
    CHECK_FALSE(f.pool.contains("b"));
}

TEST_CASE("connection_pool: nested non-actor coroutines resume as pool threads answer") {
    fixture_t f;
    f.script->delay = 20ms;
    f.add("a", config(1, 2));
    nested_caller_t caller{&f.resource, &f.pool};
    auto outcome = drive(&f.resource, caller.outer("a"));
    REQUIRE_FALSE(outcome.has_error());
    // Two inner calls, each "abc" (3) + "defgh" (5).
    CHECK(outcome.value() == 16);
    CHECK(f.script->executes == 4);
}

TEST_CASE("connection_pool: the waker is called once the answer is settled and its promise released") {
    fixture_t f;
    f.script->delay = 50ms;
    f.add("a", config(1, 1));

    // Set before the statement can be answered (the driver sleeps first); read on the pool
    // thread when the waker runs.
    std::atomic<const conn::statement_future_t*> awaited{nullptr};
    std::atomic<int> wakes{0};
    std::atomic<bool> settled_when_woken{false};
    auto future = f.pool.execute("a", conn::statement_t::rows("abc"), [&] {
        const auto* seen = awaited.load();
        settled_when_woken = seen != nullptr && seen->is_ready();
        ++wakes;
    });
    awaited = &future;

    for (int i = 0; i < 500 && wakes.load() == 0; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    CHECK(wakes.load() == 1);
    CHECK(settled_when_woken.load());
    REQUIRE(future.is_ready());
    auto outcome = std::move(future).take_ready();
    REQUIRE_FALSE(outcome.has_error());
    CHECK(outcome.value().affected_rows == 3);
}

TEST_CASE("connection_pool: an unknown alias is answered at once and wakes nobody") {
    fixture_t f;
    std::atomic<int> wakes{0};
    auto future = f.pool.execute("nobody", conn::statement_t::rows("x"), [&] { ++wakes; });
    REQUIRE(future.is_ready());
    auto outcome = drive(&f.resource, std::move(future));
    REQUIRE(outcome.has_error());
    CHECK(outcome.error().type == core::error_code_t::do_not_exists);
    CHECK(wakes.load() == 0);
}
