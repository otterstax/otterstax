// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The mysql / pg ConnectorManagers over their per-alias connection pools, with
// recording stub connectors injected through the factory, each driven by its
// integration backend (the stack test_connector_manager uses):
//   * addConnection opens pool_size connections; 0 is invalid_parameter;
//   * executeQuery checks a connection out for the statement: with pool_size 1
//     statements on one alias never overlap, with pool_size 2 two run at once;
//   * a statement waits for a free connection up to the checkout timeout.

#include "connectors/mysql/manager.hpp"
#include "connectors/postgresql/manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "integration/statement_executor.hpp"
#include "tests/mock/sql_db_connector.hpp"
#include "tests/system/session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <actor-zeta.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <optional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

    using namespace std::chrono_literals;
    using components::vector::data_chunk_t;

    // What every stub connector did, shared through a process-wide instance: the
    // factories are plain function pointers and cannot carry state.
    struct recorder_t {
        std::mutex mutex;
        std::vector<std::pair<int, std::string>> events; // (connector id, statement / "connect" / "close")
        std::atomic<int> next_id{0};
        std::atomic<int> in_flight{0};
        std::atomic<int> max_in_flight{0};
        std::chrono::milliseconds data_delay{0};
        // A data query blocks its io thread until the connector is cancelled (a backend that
        // never answers), up to a 30 s safety deadline.
        std::atomic<bool> block_data{false};

        void reset() {
            std::lock_guard lock(mutex);
            events.clear();
            next_id = 0;
            in_flight = 0;
            max_in_flight = 0;
            data_delay = 0ms;
            block_data = false;
        }
        void record(int id, std::string_view what) {
            std::lock_guard lock(mutex);
            events.emplace_back(id, std::string(what));
        }
        int count(std::string_view what) {
            std::lock_guard lock(mutex);
            int n = 0;
            for (const auto& event : events) {
                n += event.second == what ? 1 : 0;
            }
            return n;
        }
        // A data query: tracks how many run at once on this backend.
        void enter() {
            const int now = in_flight.fetch_add(1) + 1;
            int seen = max_in_flight.load();
            while (now > seen && !max_in_flight.compare_exchange_weak(seen, now)) {
            }
        }
        void leave() { in_flight.fetch_sub(1); }
    };

    recorder_t& recorder() {
        static recorder_t instance;
        return instance;
    }

    // ---------------------------------------------------------------- MySQL stub

    class mysql_stub final : public mysql::IConnector {
    public:
        explicit mysql_stub(std::string alias)
            : alias_(std::move(alias))
            , id_(recorder().next_id++) {}

        mysql::Status status() const noexcept override { return status_; }
        boost::mysql::connect_params params() const noexcept override { return {}; }
        void close() override {
            recorder().record(id_, "close");
            status_ = mysql::Status::Closed;
        }
        core::error_t connect() override {
            recorder().record(id_, "connect");
            status_ = mysql::Status::Connected;
            return core::error_t::no_error();
        }
        bool isConnected() override { return status_ == mysql::Status::Connected; }
        core::error_t tryReconnect() override {
            recorder().record(id_, "reconnect");
            status_ = mysql::Status::Connected;
            return core::error_t::no_error();
        }
        bool isClosed() const noexcept override { return status_ == mysql::Status::Closed; }
        std::string alias() const noexcept override { return alias_; }

        void cancel() noexcept override {
            recorder().record(id_, "cancel");
            cancelled_ = true;
        }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)>) override {
            recorder().record(id_, query);
            recorder().enter();
            std::this_thread::sleep_for(recorder().data_delay);
            const auto deadline = std::chrono::steady_clock::now() + 30s;
            while (recorder().block_data && !cancelled_ && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(1ms);
            }
            recorder().leave();
            if (cancelled_) {
                co_return core::error_t(core::error_code_t::io_error, std::pmr::string{"stub statement cancelled"});
            }
            co_return std::unique_ptr<data_chunk_t>{};
        }
        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(const boost::mysql::results&)>) override {
            recorder().record(id_, query);
            co_return int64_t{1};
        }
        // Discovery.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            recorder().record(id_, query);
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(mysql::mock_ok_results()));
        }

        int id() const noexcept { return id_; }

    private:
        std::string alias_;
        int id_;
        mysql::Status status_{mysql::Status::Created};
        std::atomic<bool> cancelled_{false};
    };

    std::unique_ptr<mysql::IConnector> make_mysql_stub(std::pmr::memory_resource*,
                                                       boost::asio::io_context&,
                                                       boost::mysql::connect_params,
                                                       std::string alias) {
        return std::make_unique<mysql_stub>(std::move(alias));
    }

    // ------------------------------------------------------------------- PG stub

    class pg_stub final : public pg::IConnector {
    public:
        pg_stub(std::string alias, pg::connect_params params)
            : alias_(std::move(alias))
            , params_(std::move(params))
            , id_(recorder().next_id++) {}

        pg::Status status() const noexcept override { return status_; }
        pg::connect_params params() const noexcept override { return params_; }
        void close() override {
            recorder().record(id_, "close");
            status_ = pg::Status::Closed;
        }
        core::error_t connect() override {
            recorder().record(id_, "connect");
            status_ = pg::Status::Connected;
            return core::error_t::no_error();
        }
        bool isConnected() override { return status_ == pg::Status::Connected; }
        core::error_t tryReconnect() override {
            recorder().record(id_, "reconnect");
            status_ = pg::Status::Connected;
            return core::error_t::no_error();
        }
        bool isClosed() const noexcept override { return status_ == pg::Status::Closed; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query, otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)>) override {
            recorder().record(id_, query);
            recorder().enter();
            std::this_thread::sleep_for(recorder().data_delay);
            recorder().leave();
            co_return std::unique_ptr<data_chunk_t>{};
        }
        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            recorder().record(id_, query);
            co_return int64_t{1};
        }
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query, otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            recorder().record(id_, query);
            std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                 &PQclear);
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
        }

    private:
        std::string alias_;
        pg::connect_params params_;
        int id_;
        pg::Status status_{pg::Status::Created};
    };

    std::unique_ptr<pg::IConnector>
    make_pg_stub(std::pmr::memory_resource*, pg::connect_params params, std::string alias) {
        return std::make_unique<pg_stub>(std::move(alias), std::move(params));
    }

    // ------------------------------------------------------------------- stacks

    // The backend's ConnectorManager and its integration backend (the manager's
    // sole driver, whose constructor starts the io pool).
    struct mysql_stack {
        std::pmr::memory_resource* resource{std::pmr::new_delete_resource()};
        // The backends pool the backend's statements run on; outlives the backend.
        otterstax::runtime::runtime_t runtime{otterstax::test::test_runtime_config()};
        std::unique_ptr<mysql::ConnectorManager> manager;
        std::unique_ptr<db::MySQLManager> backend;

        mysql_stack()
            : manager(std::make_unique<mysql::ConnectorManager>(resource, &make_mysql_stub, 4))
            , backend(std::make_unique<db::MySQLManager>(resource, manager.get(), runtime.backends_scheduler())) {
            recorder().reset();
            REQUIRE(manager->status() == thread_pool_status::RUNNING);
        }

        void add(const std::string& alias, std::size_t pool_size) {
            auto added = manager->addConnection(conn::api_server::ConnectionParams{.alias = alias,
                                                                                   .host = "localhost",
                                                                                   .port = "",
                                                                                   .username = "user",
                                                                                   .password = "pass",
                                                                                   .database = "db",
                                                                                   .table = "",
                                                                                   .pool_size = pool_size});
            REQUIRE_FALSE(added.has_error());
        }
    };

    struct pg_stack {
        std::pmr::memory_resource* resource{std::pmr::new_delete_resource()};
        // The backends pool the backend's statements run on; outlives the backend.
        otterstax::runtime::runtime_t runtime{otterstax::test::test_runtime_config()};
        std::unique_ptr<pg::ConnectorManager> manager;
        std::unique_ptr<db::PostgressManager> backend;

        pg_stack()
            : manager(std::make_unique<pg::ConnectorManager>(resource, &make_pg_stub, 4))
            , backend(std::make_unique<db::PostgressManager>(resource, manager.get(), runtime.backends_scheduler())) {
            recorder().reset();
            REQUIRE(manager->status() == thread_pool_status::RUNNING);
        }

        void add(const std::string& alias, std::size_t pool_size) {
            auto added = manager->addConnection(conn::api_server::PgConnectionParams{.alias = alias,
                                                                                     .host = "localhost",
                                                                                     .port = "",
                                                                                     .username = "user",
                                                                                     .password = "pass",
                                                                                     .database = "db",
                                                                                     .schema = "public",
                                                                                     .table = "",
                                                                                     .pool_size = pool_size});
            REQUIRE_FALSE(added.has_error());
        }
    };

    std::unique_ptr<data_chunk_t> no_rows_mysql(const boost::mysql::results&) { return nullptr; }
    std::unique_ptr<data_chunk_t> no_rows_pg(PGresult*) { return nullptr; }

    // Runs `n` statements on `alias` from `n` threads at once; every outcome must
    // be a success.
    template<typename Manager, typename Handler>
    void run_concurrently(Manager& manager, const std::string& alias, int n, Handler handler) {
        std::vector<std::thread> threads;
        std::atomic<int> failures{0};
        for (int i = 0; i < n; ++i) {
            threads.emplace_back([&, i] {
                const std::string query = "SELECT " + std::to_string(i);
                auto outcome = manager.executeQuery(alias, query, handler).get();
                if (outcome.has_error()) {
                    ++failures;
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        REQUIRE(failures == 0);
    }

} // namespace

TEST_CASE("pooled mysql: addConnection opens pool_size connections and 0 is invalid_parameter") {
    mysql_stack stack;
    stack.add("three", 3);
    REQUIRE(stack.manager->poolSize("three") == 3);
    REQUIRE(stack.manager->idleConnections("three") == 3);
    REQUIRE(stack.manager->totalConnections() == 1);
    REQUIRE(recorder().count("connect") == 3);

    auto zero = stack.manager->addConnection(conn::api_server::ConnectionParams{.alias = "zero",
                                                                                .host = "localhost",
                                                                                .username = "user",
                                                                                .database = "db",
                                                                                .pool_size = 0});
    REQUIRE(zero.has_error());
    REQUIRE(zero.error().type == core::error_code_t::invalid_parameter);
    REQUIRE_FALSE(stack.manager->hasConnection("zero"));

    // The default stays one connection per alias.
    stack.add("default", 1);
    REQUIRE(stack.manager->poolSize("default") == 1);
    REQUIRE(stack.manager->poolSize("unknown") == 0);
}

TEST_CASE("pooled mysql: with pool_size 1 statements on one alias never overlap") {
    mysql_stack stack;
    stack.add("one", 1);
    recorder().data_delay = 50ms;
    run_concurrently(*stack.manager, "one", 4, &no_rows_mysql);
    REQUIRE(recorder().max_in_flight == 1);
    REQUIRE(stack.manager->idleConnections("one") == 1);
}

TEST_CASE("pooled mysql: with pool_size 2 two statements run at once and never three") {
    mysql_stack stack;
    stack.add("two", 2);
    recorder().data_delay = 200ms;
    run_concurrently(*stack.manager, "two", 4, &no_rows_mysql);
    REQUIRE(recorder().max_in_flight == 2);
    REQUIRE(stack.manager->idleConnections("two") == 2);
}

TEST_CASE("pooled mysql: a statement waits for a free connection up to the checkout timeout") {
    mysql_stack stack;
    stack.add("busy", 1);
    stack.manager->setCheckoutTimeout(50ms);
    recorder().data_delay = 500ms;

    auto slow = stack.manager->executeQuery("busy", "SELECT slow", &no_rows_mysql);
    std::this_thread::sleep_for(50ms); // the slow statement holds the only connection
    const auto started = std::chrono::steady_clock::now();
    auto starved = stack.manager->executeQuery("busy", "SELECT starved", &no_rows_mysql).get();
    REQUIRE(std::chrono::steady_clock::now() - started >= 50ms);
    REQUIRE(starved.has_error());
    REQUIRE(starved.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{starved.error().what.c_str()}.find("no free connection of 'busy'") != std::string::npos);
    REQUIRE_FALSE(slow.get().has_error());

    // Once the connection is back, the next statement runs.
    REQUIRE_FALSE(stack.manager->executeQuery("busy", "SELECT after", &no_rows_mysql).get().has_error());
}

TEST_CASE("pooled pg: addConnection opens pool_size connections") {
    pg_stack stack;
    stack.add("pg3", 3);
    REQUIRE(stack.manager->poolSize("pg3") == 3);
    REQUIRE(stack.manager->idleConnections("pg3") == 3);
    REQUIRE(recorder().count("connect") == 3);
}

TEST_CASE("pooled pg: with pool_size 1 statements on one alias never overlap") {
    pg_stack stack;
    stack.add("one", 1);
    recorder().data_delay = 50ms;
    run_concurrently(*stack.manager, "one", 4, &no_rows_pg);
    REQUIRE(recorder().max_in_flight == 1);
}

TEST_CASE("pooled pg: with pool_size 2 two statements run at once and never three") {
    pg_stack stack;
    stack.add("two", 2);
    recorder().data_delay = 200ms;
    run_concurrently(*stack.manager, "two", 4, &no_rows_pg);
    REQUIRE(recorder().max_in_flight == 2);
}

// ── shutdown: a statement the backend never answers does not hold it ──────────

namespace {

    // Waits up to 5 s for `condition`.
    template<typename Condition>
    bool eventually(Condition condition) {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (!condition()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(1ms);
        }
        return true;
    }

} // namespace

TEST_CASE("pooled mysql: shutdown aborts the running statement and refuses the next ones") {
    mysql_stack stack;
    stack.add("one", 1);
    recorder().block_data = true;
    auto running = std::async(std::launch::async, [&stack] {
        return stack.manager->executeQuery("one", "SELECT hang", &no_rows_mysql).get();
    });
    REQUIRE(eventually([] { return recorder().in_flight.load() == 1; }));

    const auto started = std::chrono::steady_clock::now();
    stack.manager->shutdown();
    REQUIRE(running.wait_for(5s) == std::future_status::ready);
    auto outcome = running.get();
    CHECK(std::chrono::steady_clock::now() - started < 5s);
    REQUIRE(outcome.has_error());
    CHECK(std::string{outcome.error().what.c_str()} == "stub statement cancelled");
    CHECK(recorder().count("cancel") == 1);

    auto refused = stack.manager->executeQuery("one", "SELECT 1", &no_rows_mysql).get();
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::io_error);
    CHECK(std::string{refused.error().what.c_str()}.find("shutting down") != std::string::npos);
    // Idempotent.
    stack.manager->shutdown();
    CHECK(recorder().count("cancel") == 1);
}

TEST_CASE("pooled mysql: destroying the backend aborts a scan the backend never answers and the one queued behind it") {
    mysql_stack stack;
    stack.add("one", 1);
    recorder().block_data = true;
    auto* resource = std::pmr::new_delete_resource();
    auto first_rows = std::make_shared<otterstax::federation::scan_rows_t>();
    auto second_rows = std::make_shared<otterstax::federation::scan_rows_t>();
    auto first = stack.backend->scan(resource,
                                           otterstax::federation::scan_request_t{"one", "SELECT hang", {}},
                                           first_rows);
    REQUIRE(eventually([] { return recorder().in_flight.load() == 1; }));
    // Waits for the one connection, which the first scan holds.
    auto second = stack.backend->scan(resource,
                                            otterstax::federation::scan_request_t{"one", "SELECT queued", {}},
                                            second_rows);

    const auto started = std::chrono::steady_clock::now();
    stack.backend.reset();
    CHECK(std::chrono::steady_clock::now() - started < 5s);
    REQUIRE(first.is_ready());
    REQUIRE(second.is_ready());
    CHECK(std::move(first).take_ready().contains_error());
    CHECK(std::move(second).take_ready().contains_error());
}

// ── statement_executor_t ──────────────────────────────────────────────────────

TEST_CASE("statement executor: destruction waits for every job posted") {
    otterstax::runtime::runtime_t runtime{otterstax::test::test_runtime_config()};
    std::atomic<int> done{0};
    {
        db::statement_executor_t executor(runtime.backends_scheduler());
        for (int i = 0; i < 16; ++i) {
            executor.post([&done] {
                std::this_thread::sleep_for(10ms);
                done.fetch_add(1);
            });
        }
    }
    CHECK(done.load() == 16);
}

TEST_CASE("statement executor: a job that throws is contained and its promise still settles") {
    otterstax::runtime::runtime_t runtime{otterstax::test::test_runtime_config()};
    auto* resource = std::pmr::new_delete_resource();
    actor_zeta::promise<core::error_t> promise(resource);
    auto future = promise.get_future();
    std::atomic<bool> later_job_ran{false};
    {
        db::statement_executor_t executor(runtime.backends_scheduler());
        executor.post([owned = std::move(promise)]() mutable { throw std::runtime_error("job failed"); });
        executor.post([&later_job_ran] { later_job_ran = true; });
    }
    // The throw neither terminated the process nor left the destructor waiting: both jobs count
    // as finished, and the promise the throwing job owned settled its future as failed.
    CHECK(later_job_ran.load());
    REQUIRE(future.is_ready());
    CHECK(future.failed());
}

TEST_CASE("statement executor: answer_of turns a throw into the job's own answer") {
    const auto answered = db::answer_of([]() -> int { throw std::runtime_error("boom"); },
                                        [](std::string_view what) { return static_cast<int>(what.size()); });
    CHECK(answered == 4);
    CHECK(db::answer_of([] { return 7; }, [](std::string_view) { return 0; }) == 7);
}

// ── the rows an UPDATE matched, out of the OK packet's info ───────────────────

TEST_CASE("mysql_update_rows_matched: the first of the three numbers whatever the language") {
    // MySQL and MariaDB, English (ER_UPDATE_INFO).
    CHECK(db::mysql_update_rows_matched("Rows matched: 3  Changed: 1  Warnings: 0") == std::optional<std::uint64_t>{3});
    CHECK(db::mysql_update_rows_matched("Rows matched: 0  Changed: 0  Warnings: 0") == std::optional<std::uint64_t>{0});
    // The same message under another lc_messages.
    CHECK(db::mysql_update_rows_matched("Datensätze gefunden: 12  Geändert: 0  Warnungen: 0") ==
          std::optional<std::uint64_t>{12});
    CHECK(db::mysql_update_rows_matched("Записей найдено: 2  Изменено: 2  Предупреждений: 0") ==
          std::optional<std::uint64_t>{2});
    CHECK(db::mysql_update_rows_matched("Rows matched: 18446744073709551615  Changed: 0  Warnings: 0") ==
          std::optional<std::uint64_t>{18446744073709551615ULL});
    // Anything but three numbers is not that message.
    CHECK_FALSE(db::mysql_update_rows_matched("").has_value());
    CHECK_FALSE(db::mysql_update_rows_matched("Rows matched: 1  Changed: 1").has_value());
    CHECK_FALSE(db::mysql_update_rows_matched("Records: 1  Duplicates: 0  Warnings: 0  Extra: 4").has_value());
    CHECK_FALSE(db::mysql_update_rows_matched("Rows matched: 99999999999999999999  Changed: 0  Warnings: 0").has_value());
}
