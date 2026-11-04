// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// What only a query session owns: its life (make_session_services()
// sets every address a session sends to; close() frees an idle session at once
// and a busy one when its statement in flight has finished, refusing what was
// queued behind it),
// one statement at a time across real suspensions (the engine on its own pools,
// a slow backend on the backends pool), and the connection's engine session:
// BEGIN / COMMIT / ROLLBACK / SAVEPOINT over engine tables with PostgreSQL
// semantics, isolated per connection, the status the engine stamps on every
// answer, SET in the engine, and close_session when the session closes.

#include "../system/session_stack.hpp"
#include "../mock/cell.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/parser.hpp"
#include "../mock/sql_db_connector.hpp"

#include <catch2/catch_all.hpp>

#include <sys/resource.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <optional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using otterstax::session::query_session_t;
using otterstax::test::await_session;
using otterstax::test::run_session_or_fail;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::session_stack_owner;
using otterstax::test::stack_options_t;
using otterstax::test::stmt;

namespace {

    using namespace std::chrono_literals;
    using future_t = actor_zeta::unique_future<core::result_wrapper_t<session_payload>>;

    constexpr const char* kSlow = "SLOW REMOTE SELECT";
    constexpr auto kSlowFor = std::chrono::milliseconds(800);

    // A remote SELECT whose backend answers after kSlowFor: the session awaits
    // the MySQL backend actor while the mock sleeps on its agent's backends-pool thread.
    std::unique_ptr<mysql::IConnector> slow_mysql_factory(std::pmr::memory_resource* resource,
                                                          boost::asio::io_context&,
                                                          boost::mysql::connect_params,
                                                          std::string alias) {
        return std::make_unique<mysql::MockConnector>(mock_config{.resource = resource, .wait_time = kSlowFor},
                                                      std::move(alias));
    }

    // kSlow parses to the mock parser's remote SELECT over uid "1"; any other
    // text goes to the real parser.
    class slow_or_real_parser final : public IParser {
    public:
        explicit slow_or_real_parser(std::pmr::memory_resource* resource)
            : mock_(make_mock_parser(resource))
            , real_(make_parser(resource)) {}

        core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
            return sql == kSlow ? mock_->parse(sql) : real_->parse(sql);
        }

    private:
        parser_ptr mock_;
        parser_ptr real_;
    };

    parser_ptr make_slow_or_real_parser(std::pmr::memory_resource* resource) {
        return std::make_unique<slow_or_real_parser>(resource);
    }

    stack_options_t slow_options() {
        stack_options_t options;
        options.parser_factory = &make_slow_or_real_parser;
        options.connectors.mysql = &slow_mysql_factory;
        options.connections.mysql = otterstax::test::mock_mysql({"1", "2"});
        return options;
    }

    // A backend statement held open until the test lets it go: the data path
    // reports that it started, then waits for release() (bounded, so a broken
    // test cannot hang the run). What "a statement is in flight" means, without
    // guessing it from a sleep.
    struct backend_gate_t {
        std::mutex mutex;
        std::condition_variable changed;
        bool started{false};
        bool open{false};

        void reset() {
            std::lock_guard lock(mutex);
            started = false;
            open = false;
        }
        void enter() {
            std::unique_lock lock(mutex);
            started = true;
            changed.notify_all();
            changed.wait_for(lock, 10s, [this] { return open; });
        }
        bool wait_started(std::chrono::milliseconds timeout) {
            std::unique_lock lock(mutex);
            return changed.wait_for(lock, timeout, [this] { return started; });
        }
        void release() {
            std::lock_guard lock(mutex);
            open = true;
            changed.notify_all();
        }
    };

    backend_gate_t& backend_gate() {
        static backend_gate_t gate;
        return gate;
    }

    class gated_connector final : public mysql::MockConnector {
    public:
        using mysql::MockConnector::MockConnector;
        using mysql::MockConnector::runQuery;

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<components::vector::data_chunk_t>(const boost::mysql::results&)>)
            override {
            backend_gate().enter();
            co_return std::make_unique<components::vector::data_chunk_t>(get_chunk());
        }
    };

    std::unique_ptr<mysql::IConnector> gated_mysql_factory(std::pmr::memory_resource* resource,
                                                           boost::asio::io_context&,
                                                           boost::mysql::connect_params,
                                                           std::string alias) {
        return std::make_unique<gated_connector>(mock_config{.resource = resource}, std::move(alias));
    }

    future_t send_sql(const actor_zeta::address_t& session, session_hash_t id, const std::string& sql) {
        return actor_zeta::send(session, &query_session_t::execute, stmt(id), sql).second;
    }

    uint64_t count_of(const core::result_wrapper_t<session_payload>& r) {
        INFO((r.has_error() ? r.error().what.c_str() : ""));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().size() == 1);
        for (const auto& chunk : r.value().chunks) {
            if (chunk.size() != 0) {
                return otterstax::test::cell(chunk, 0, 0).value<uint64_t>();
            }
        }
        FAIL("no row");
        return 0;
    }

    uint64_t count_rows(const session_stack& s, session_hash_t id, const std::string& table) {
        return count_of(run_session_sql_payload(s, id, "SELECT count(*) FROM " + table + ";"));
    }

    bool wait_for_live(otterstax::session::session_factory_t& sessions, size_t live, std::chrono::milliseconds timeout) {
        for (auto waited = 0ms; waited < timeout; waited += 10ms) {
            if (sessions.live_sessions() == live) {
                return true;
            }
            std::this_thread::sleep_for(10ms);
        }
        return sessions.live_sessions() == live;
    }

    void require_error_containing(const core::result_wrapper_t<session_payload>& r, std::string_view text) {
        REQUIRE(r.has_error());
        INFO(r.error().what.c_str());
        CHECK(std::string_view{r.error().what.c_str()}.find(text) != std::string_view::npos);
    }

} // namespace

TEST_CASE("session services: make_session_services sets every address") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_services"));
    auto services = otterstax::session::make_session_services(owner.engine().resource(),
                                                              owner.engine().dispatcher(),
                                                              owner.s3_address(),
                                                              owner.file_address(),
                                                              owner.kafka_address(),
                                                              owner.kafka_objects(),
                                                              owner.remote().connections());
    REQUIRE_FALSE(services.has_error());
    CHECK(services.value().engine_dispatcher == owner.engine().dispatcher());
    CHECK(services.value().s3 == owner.s3_address());
    CHECK(services.value().file == owner.file_address());
    CHECK(services.value().kafka == owner.kafka_address());
}

// An empty address would be dereferenced by the first send to it: each one is
// refused as invalid_parameter naming it.
TEST_CASE("session services: make_session_services refuses each empty address") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_services_empty"));
    const auto empty = actor_zeta::address_t::empty_address();
    const char* names[] = {"engine dispatcher", "s3 manager", "file manager", "kafka manager"};
    for (int missing = 0; missing < 4; ++missing) {
        INFO(names[missing]);
        auto services = otterstax::session::make_session_services(owner.engine().resource(),
                                                                  missing == 0 ? empty : owner.engine().dispatcher(),
                                                                  missing == 1 ? empty : owner.s3_address(),
                                                                  missing == 2 ? empty : owner.file_address(),
                                                                  missing == 3 ? empty : owner.kafka_address(),
                                                                  owner.kafka_objects(),
                                                                  owner.remote().connections());
        REQUIRE(services.has_error());
        CHECK(services.error().type == core::error_code_t::invalid_parameter);
        CHECK(std::string{services.error().what.c_str()} ==
              std::string{"session services: the "} + names[missing] + " address is empty");
    }
}

// The registration of a missed table needs the backend actor of every
// configured connection.
TEST_CASE("session services: connections without the backend actor are refused") {
    stack_options_t options;
    options.connections.mysql = otterstax::test::mock_mysql({"orders"});
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_services_actor"), options);
    REQUIRE_FALSE(owner.remote().connections().empty());
    auto without_actor = owner.remote().connections();
    without_actor.front().actor = nullptr;
    auto services = otterstax::session::make_session_services(owner.engine().resource(),
                                                              owner.engine().dispatcher(),
                                                              owner.s3_address(),
                                                              owner.file_address(),
                                                              owner.kafka_address(),
                                                              owner.kafka_objects(),
                                                              without_actor);
    REQUIRE(services.has_error());
    CHECK(services.error().type == core::error_code_t::invalid_parameter);
    auto with_actor = otterstax::session::make_session_services(owner.engine().resource(),
                                                                owner.engine().dispatcher(),
                                                                owner.s3_address(),
                                                                owner.file_address(),
                                                                owner.kafka_address(),
                                                                owner.kafka_objects(),
                                                                owner.remote().connections());
    REQUIRE_FALSE(with_actor.has_error());
    REQUIRE(with_actor.value().aliases.size() == 1);
    CHECK(with_actor.value().aliases.front() == "orders");
}

TEST_CASE("session close: an idle session frees itself at once") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_close_idle"));
    auto& sessions = owner.sessions();
    const size_t before = sessions.live_sessions();

    auto session = owner.open_session();
    REQUIRE(sessions.live_sessions() == before + 1);
    run_session_or_fail(owner.stack().on(session->address()), 1, "CREATE DATABASE idle_close;");
    session.reset();
    // Nothing of it was queued or running: close() freed it on this thread.
    CHECK(sessions.live_sessions() == before);
}

TEST_CASE("session close: a statement in flight finishes and what was queued behind it is refused") {
    auto options = slow_options();
    options.connectors.mysql = &gated_mysql_factory;
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_close_busy"), options);
    auto& sessions = owner.sessions();
    const size_t before = sessions.live_sessions();

    backend_gate().reset();
    auto session = owner.open_session();
    const auto address = session->address();
    auto slow = send_sql(address, 1, kSlow);
    auto queued = send_sql(address, 2, "CREATE DATABASE never_created;");
    // The backend reports the slow statement started; it is held there until
    // released, so the close below happens with it in flight.
    REQUIRE(backend_gate().wait_started(10000ms));
    REQUIRE_FALSE(slow.is_ready());
    session.reset();
    CHECK(sessions.live_sessions() == before + 1);
    backend_gate().release();

    auto slow_result = await_session(std::move(slow), 10000ms, owner.engine().resource());
    INFO((slow_result.has_error() ? slow_result.error().what.c_str() : ""));
    REQUIRE_FALSE(slow_result.has_error());
    CHECK(slow_result.value().size() == 2);

    auto queued_result = await_session(std::move(queued), 10000ms, owner.engine().resource());
    require_error_containing(queued_result, "the connection is closed");

    // The last job ended: the session freed itself.
    CHECK(wait_for_live(sessions, before, 5000ms));
    auto probe = owner.engine().sql("SELECT * FROM never_created.t;");
    REQUIRE(probe);
    CHECK(probe->is_error());
}

namespace {

    constexpr const char* kRemoteSelect = "SELECT * FROM orders.db.public.\"table\";";

    // One MySQL connection "orders" of one agent (pool_size 1): every block that reads it holds
    // that agent until it ends.
    stack_options_t one_agent_options() {
        stack_options_t options;
        options.connections.mysql = otterstax::test::mock_mysql({"orders"});
        options.connections.mysql.front().pool_size = 1;
        return options;
    }

    otterstax::federation::backend_actor_t& orders_actor(session_stack_owner& owner) {
        for (const auto& connection : owner.remote().connections()) {
            if (connection.name == "orders") {
                REQUIRE(connection.actor != nullptr);
                return *connection.actor;
            }
        }
        FAIL("no connection orders");
        throw;
    }

    bool wait_until(const std::function<bool()>& done, std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!done()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(1ms);
        }
        return true;
    }

} // namespace

// A client that closes its connection inside a block (a driver with autocommit off: its first
// SELECT opened the block) frees the backend connection the block held at once. The engine's own
// ROLLBACK of the closed session is a statement for one of its executors (four, each running one
// statement at a time), and here every executor runs a statement of another block waiting for that
// very connection: the agent comes back only because the session's close reaches the backend actor
// itself, never behind those statements.
TEST_CASE("session close: a closed block's backend connection serves the blocks waiting for it at once") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_close_pin"), one_agent_options());
    auto& actor = orders_actor(owner);
    actor.set_checkout_timeout(30s);
    auto holder = owner.open_session();
    const auto h = owner.stack().on(holder->address());
    run_session_or_fail(h, 1, "BEGIN;");
    run_session_or_fail(h, 2, kRemoteSelect);
    REQUIRE(actor.pinned() == 1);

    // More blocks wait for the agent than the engine has executors: their BEGINs first, then
    // their SELECTs one after another, so the SELECTs take every executor.
    constexpr std::size_t blocks = 6;
    std::vector<otterstax::session::session_ptr> waiting;
    std::vector<std::optional<future_t>> selects(blocks);
    for (std::size_t i = 0; i < blocks; ++i) {
        waiting.push_back(owner.open_session());
        run_session_or_fail(owner.stack().on(waiting.back()->address()), 1, "BEGIN;");
    }
    for (std::size_t i = 0; i < blocks; ++i) {
        selects[i].emplace(send_sql(waiting[i]->address(), 2, kRemoteSelect));
    }
    // Four of them reached the actor's queue, one per executor of the engine; the others wait for an
    // executor.
    REQUIRE(wait_until([&] { return actor.waiting() >= 4; }, 10000ms));

    const auto started = std::chrono::steady_clock::now();
    holder.reset();
    // Each block gets the agent in turn, as the one before it closes.
    std::size_t answered = 0;
    const bool all = wait_until(
        [&] {
            for (std::size_t i = 0; i < blocks; ++i) {
                if (selects[i] && selects[i]->is_ready()) {
                    auto result = await_session(std::move(*selects[i]), 1000ms, owner.engine().resource());
                    selects[i].reset();
                    INFO((result.has_error() ? result.error().what.c_str() : ""));
                    CHECK_FALSE(result.has_error());
                    waiting[i].reset();
                    ++answered;
                }
            }
            return answered == blocks;
        },
        10000ms);
    INFO("answered " << answered << " of " << blocks << " after "
                     << std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count() << " s");
    REQUIRE(all);
    CHECK(wait_until([&] { return actor.pinned() == 0 && actor.waiting() == 0; }, 5000ms));
}

// A session closed while its block's first statement still waits in the queue for an agent: the
// statement leaves the queue at once with the connection's close, and holds nothing after it.
TEST_CASE("session close: a statement waiting for a backend connection leaves the queue with the close") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_close_waiter"), one_agent_options());
    auto& actor = orders_actor(owner);
    actor.set_checkout_timeout(30s);
    auto holder = owner.open_session();
    const auto h = owner.stack().on(holder->address());
    run_session_or_fail(h, 1, "BEGIN;");
    run_session_or_fail(h, 2, kRemoteSelect);
    REQUIRE(actor.pinned() == 1);

    auto waiter = owner.open_session();
    run_session_or_fail(owner.stack().on(waiter->address()), 1, "BEGIN;");
    auto select = send_sql(waiter->address(), 2, kRemoteSelect);
    REQUIRE(wait_until([&] { return actor.waiting() == 1; }, 10000ms));
    waiter.reset();
    auto result = await_session(std::move(select), 5000ms, owner.engine().resource());
    require_error_containing(result, "the connection is closed");
    CHECK(actor.waiting() == 0);
    // The holder still has its agent; the waiter took none.
    CHECK(actor.pinned() == 1);
    run_session_or_fail(h, 3, "COMMIT;");
    CHECK(wait_until([&] { return actor.pinned() == 0; }, 5000ms));
}

// Hidden until a settled future wakes the actor that awaits it: a
// statement held on its backend for a second must leave the process idle. In
// actor-zeta 2.0.0 a cooperative actor suspended on a co_await is requeued on
// every pass of its pool (completion is flag-only), so today both the session
// on the sessions pool and the engine's executor on the exec pool spin: about
// 3.1 CPU-seconds per second of waiting on an 18-core machine (otterbrix
// 61aa43d6), against 0.03 with nothing in flight. Asserts the correct
// behaviour; take the dot off once a settled future wakes the actor that
// awaits it.
TEST_CASE("session wait: a statement held on its backend does not spin the pools", "[.engine-defect-otb-notify]") {
    auto options = slow_options();
    options.connectors.mysql = &gated_mysql_factory;
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_wait_idle"), options);
    const auto cpu_seconds = [] {
        rusage usage{};
        getrusage(RUSAGE_SELF, &usage);
        return static_cast<double>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
               static_cast<double>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
    };

    backend_gate().reset();
    auto session = owner.open_session();
    auto held = send_sql(session->address(), 1, kSlow);
    REQUIRE(backend_gate().wait_started(10000ms));
    const double before = cpu_seconds();
    std::this_thread::sleep_for(1000ms);
    const double spent = cpu_seconds() - before;
    backend_gate().release();

    auto result = await_session(std::move(held), 10000ms, owner.engine().resource());
    INFO((result.has_error() ? result.error().what.c_str() : ""));
    REQUIRE_FALSE(result.has_error());
    INFO("CPU-seconds spent while the statement waited 1 s: " << spent);
    CHECK(spent < 0.5);
}

// Every statement is sent before any answer is read. The engine runs on its
// own pools and the slow statement on the backends pool, so the session
// really suspends at each of them; a statement taken off the mailbox early, or
// a wakeup lost, shows up as a count that is not the number of INSERTs sent
// before it, or as a future that never settles.
TEST_CASE("session order: one statement at a time across real suspensions") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_order_suspend"), slow_options());
    const auto s = owner.stack();
    run_session_or_fail(s, 1, "CREATE SCHEMA ord;");
    run_session_or_fail(s, 2, "CREATE TABLE ord.t (id bigint);");

    constexpr size_t kRounds = 12;
    std::vector<future_t> inserts;
    std::vector<future_t> counts;
    std::vector<future_t> slows;
    session_hash_t id = 10;
    for (size_t round = 0; round < kRounds; ++round) {
        if (round % 4 == 0) {
            slows.push_back(send_sql(s.session, id++, kSlow));
        }
        inserts.push_back(send_sql(s.session, id++, "INSERT INTO ord.t (id) VALUES (" + std::to_string(round) + ");"));
        counts.push_back(send_sql(s.session, id++, "SELECT count(*) FROM ord.t;"));
    }

    for (size_t round = 0; round < kRounds; ++round) {
        INFO("round " << round);
        auto counted = await_session(std::move(counts[round]), 30000ms, s.resource);
        CHECK(count_of(counted) == round + 1);
        auto inserted = await_session(std::move(inserts[round]), 1000ms, s.resource);
        REQUIRE_FALSE(inserted.has_error());
        CHECK(inserted.value().affected_rows == 1);
    }
    for (auto& slow : slows) {
        auto r = await_session(std::move(slow), 1000ms, s.resource);
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value().size() == 2);
    }
}

TEST_CASE("session transaction: ROLLBACK discards and COMMIT keeps the block's writes") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn"));
    const auto s = owner.stack();
    run_session_or_fail(s, 1, "CREATE SCHEMA txn;");
    run_session_or_fail(s, 2, "CREATE TABLE txn.t (id bigint);");

    run_session_or_fail(s, 3, "BEGIN;");
    run_session_or_fail(s, 4, "INSERT INTO txn.t (id) VALUES (1), (2);");
    CHECK(count_rows(s, 5, "txn.t") == 2);
    run_session_or_fail(s, 6, "ROLLBACK;");
    CHECK(count_rows(s, 7, "txn.t") == 0);

    run_session_or_fail(s, 8, "BEGIN;");
    run_session_or_fail(s, 9, "INSERT INTO txn.t (id) VALUES (3);");
    run_session_or_fail(s, 10, "COMMIT;");
    CHECK(count_rows(s, 11, "txn.t") == 1);
}

TEST_CASE("session transaction: an error aborts the block until ROLLBACK") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_abort"));
    const auto s = owner.stack();
    run_session_or_fail(s, 1, "CREATE SCHEMA txa;");
    run_session_or_fail(s, 2, "CREATE TABLE txa.t (id bigint);");

    SECTION("an engine error, then ROLLBACK") {
        run_session_or_fail(s, 3, "BEGIN;");
        run_session_or_fail(s, 4, "INSERT INTO txa.t (id) VALUES (1);");
        REQUIRE(run_session_sql_payload(s, 5, "SELECT * FROM txa.nowhere;").has_error());
        auto refused = run_session_sql_payload(s, 6, "SELECT count(*) FROM txa.t;");
        require_error_containing(refused, "only ROLLBACK");
        CHECK(refused.error().type == core::error_code_t::transaction_finalized);
        run_session_or_fail(s, 7, "ROLLBACK;");
        CHECK(count_rows(s, 8, "txa.t") == 0);
    }
    SECTION("an error before the engine (parse), then COMMIT ends it as a rollback") {
        run_session_or_fail(s, 3, "BEGIN;");
        run_session_or_fail(s, 4, "INSERT INTO txa.t (id) VALUES (1);");
        auto parse = run_session_sql_payload(s, 5, "SELEC broken;");
        REQUIRE(parse.has_error());
        CHECK(parse.error().type == core::error_code_t::sql_parse_error);
        require_error_containing(run_session_sql_payload(s, 6, "INSERT INTO txa.t (id) VALUES (2);"),
                                 "only ROLLBACK");
        // The engine answers the COMMIT of a failed block with success and
        // rolls it back; the session tags it ROLLBACK, as PostgreSQL does.
        auto commit = run_session_sql_payload(s, 7, "COMMIT;");
        REQUIRE_FALSE(commit.has_error());
        CHECK(commit.value().transaction == otterstax::session::transaction_command_t::rollback);
        // The block is over: statements run again, and nothing of it was kept.
        CHECK(count_rows(s, 8, "txa.t") == 0);
    }
}

TEST_CASE("session transaction: two connections are isolated") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_iso"));
    const auto a = owner.stack();
    const auto b = a.on(owner.open_connection());
    run_session_or_fail(a, 1, "CREATE SCHEMA iso;");
    run_session_or_fail(a, 2, "CREATE TABLE iso.t (id bigint);");

    run_session_or_fail(a, 3, "BEGIN;");
    run_session_or_fail(a, 4, "INSERT INTO iso.t (id) VALUES (1), (2), (3);");
    CHECK(count_rows(a, 5, "iso.t") == 3);
    // Uncommitted rows of A are not B's, and B is not inside A's block.
    CHECK(count_rows(b, 100, "iso.t") == 0);
    run_session_or_fail(b, 101, "INSERT INTO iso.t (id) VALUES (10);");
    run_session_or_fail(a, 6, "COMMIT;");
    CHECK(count_rows(b, 102, "iso.t") == 4);

    // An error in A's block does not touch B.
    run_session_or_fail(a, 7, "BEGIN;");
    REQUIRE(run_session_sql_payload(a, 8, "SELECT * FROM iso.nowhere;").has_error());
    CHECK(count_rows(b, 103, "iso.t") == 4);
    run_session_or_fail(a, 9, "ROLLBACK;");
}

// Clients that run with autocommit off on the driver side (psycopg2,
// mysql-connector) send COMMIT / ROLLBACK with no block open. The engine
// answers them as PostgreSQL does: no error, nothing to end.
TEST_CASE("session transaction: COMMIT and ROLLBACK outside a block are accepted") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_none"));
    const auto s = owner.stack();
    for (const char* sql : {"COMMIT;", "ROLLBACK;"}) {
        auto r = run_session_sql_payload(s, 1, sql);
        INFO(sql << ": " << (r.has_error() ? r.error().what.c_str() : ""));
        CHECK_FALSE(r.has_error());
    }
    run_session_or_fail(s, 2, "CREATE DATABASE txnone;");
}

// ─── Where the engine stamped the session (transaction_status) ──────────────

using otterstax::session::transaction_command_t;
using status_t = components::session::transaction_status_t;

TEST_CASE("transaction status: the session reads the engine's status on every answer") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_state"));
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());
    run_session_or_fail(s, 1, "CREATE SCHEMA txs;");
    run_session_or_fail(s, 2, "CREATE TABLE txs.t (id bigint);");
    CHECK(session->transaction_status() == status_t::idle);

    auto begun = run_session_sql_payload(s, 3, "BEGIN;");
    REQUIRE_FALSE(begun.has_error());
    CHECK(begun.value().transaction == transaction_command_t::begin);
    CHECK(session->transaction_status() == status_t::in_transaction);

    auto inserted = run_session_sql_payload(s, 4, "INSERT INTO txs.t (id) VALUES (1);");
    REQUIRE_FALSE(inserted.has_error());
    CHECK(inserted.value().transaction == transaction_command_t::none);
    CHECK(session->transaction_status() == status_t::in_transaction);

    SECTION("an engine error") {
        REQUIRE(run_session_sql_payload(s, 5, "SELECT * FROM txs.nowhere;").has_error());
    }
    SECTION("a prepare the engine refuses") {
        REQUIRE(otterstax::test::prepare_session_sql(s, 5, "SELECT nowhere FROM txs.t;").has_error());
    }
    SECTION("a prepare that does not parse") {
        REQUIRE(otterstax::test::prepare_session_sql(s, 5, "SELEKT id FROM txs.t;").has_error());
    }
    SECTION("a failure the frontend reports") {
        auto [ns, fut] = actor_zeta::send(s.session,
                                          &query_session_t::fail_transaction,
                                          core::error_code_t::protocol_violation,
                                          std::string{"the client sent a malformed message"});
        auto failed = await_session(std::move(fut), 10000ms, s.resource);
        require_error_containing(failed, "malformed message");
    }
    CHECK(session->transaction_status() == status_t::failed);
    auto refused = run_session_sql_payload(s, 6, "INSERT INTO txs.t (id) VALUES (2);");
    require_error_containing(refused, "only ROLLBACK");
    CHECK(refused.error().type == core::error_code_t::transaction_finalized);
    CHECK(session->transaction_status() == status_t::failed);

    auto commit = run_session_sql_payload(s, 7, "COMMIT;");
    REQUIRE_FALSE(commit.has_error());
    CHECK(commit.value().transaction == transaction_command_t::rollback);
    CHECK(session->transaction_status() == status_t::idle);
    CHECK(count_rows(s, 8, "txs.t") == 0);

    auto rolled = run_session_sql_payload(s, 9, "ROLLBACK;");
    REQUIRE_FALSE(rolled.has_error());
    CHECK(rolled.value().transaction == transaction_command_t::rollback);
    CHECK(session->transaction_status() == status_t::idle);
}

TEST_CASE("transaction status: SAVEPOINT, RELEASE and ROLLBACK TO run on the engine") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_savepoint"));
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());
    const auto other = owner.stack();
    run_session_or_fail(s, 1, "CREATE SCHEMA txsp;");
    run_session_or_fail(s, 2, "CREATE TABLE txsp.t (id bigint);");

    run_session_or_fail(s, 3, "BEGIN;");
    run_session_or_fail(s, 4, "INSERT INTO txsp.t (id) VALUES (1);");
    auto saved = run_session_sql_payload(s, 5, "SAVEPOINT sp;");
    REQUIRE_FALSE(saved.has_error());
    CHECK(saved.value().transaction == transaction_command_t::savepoint);
    run_session_or_fail(s, 6, "INSERT INTO txsp.t (id) VALUES (2);");
    // An error after the savepoint fails the block; ROLLBACK TO takes it back.
    REQUIRE(run_session_sql_payload(s, 7, "SELECT * FROM txsp.nowhere;").has_error());
    CHECK(session->transaction_status() == status_t::failed);
    auto back = run_session_sql_payload(s, 8, "ROLLBACK TO SAVEPOINT sp;");
    INFO((back.has_error() ? back.error().what.c_str() : "ok"));
    REQUIRE_FALSE(back.has_error());
    CHECK(back.value().transaction == transaction_command_t::rollback_to);
    CHECK(session->transaction_status() == status_t::in_transaction);
    auto released = run_session_sql_payload(s, 9, "RELEASE SAVEPOINT sp;");
    REQUIRE_FALSE(released.has_error());
    CHECK(released.value().transaction == transaction_command_t::release);
    run_session_or_fail(s, 10, "COMMIT;");
    // The row written before the savepoint is kept, the one after it is not.
    CHECK(count_rows(other, 11, "txsp.t") == 1);
}

// SET goes to the engine as written: the engine keeps it for its session
// alone (PostgreSQL 18's rules), and refuses a parameter it does not know.
TEST_CASE("settings: a SET of one session changes no other session") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_set_isolated"));
    auto a = owner.open_session();
    auto b = owner.open_session();
    const auto sa = owner.stack().on(a->address());
    const auto sb = owner.stack().on(b->address());
    const auto sc = owner.stack();
    run_session_or_fail(sa, 1, "CREATE SCHEMA sets;");
    run_session_or_fail(sa, 2, "CREATE TABLE sets.t (id bigint);");

    auto set = run_session_sql_payload(sa, 3, "SET autocommit TO off;");
    INFO((set.has_error() ? set.error().what.c_str() : "ok"));
    REQUIRE_FALSE(set.has_error());
    CHECK(set.value().column_count() == 0);
    CHECK(set.value().tag == T_VariableSetStmt);

    // a is out of autocommit: its INSERT opens a block the others do not see.
    run_session_or_fail(sa, 4, "INSERT INTO sets.t (id) VALUES (2);");
    CHECK(a->transaction_status() == status_t::in_transaction);
    CHECK(count_rows(sc, 5, "sets.t") == 0);
    // b is still in autocommit: its INSERT commits on its own, seen at once.
    run_session_or_fail(sb, 6, "INSERT INTO sets.t (id) VALUES (1);");
    CHECK(b->transaction_status() == status_t::idle);
    CHECK(count_rows(sc, 7, "sets.t") == 1);
    run_session_or_fail(sa, 8, "COMMIT;");
    CHECK(count_rows(sc, 9, "sets.t") == 2);
}

TEST_CASE("settings: a SET of a parameter the engine does not know is the engine's error") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_set_unknown"));
    const auto s = owner.stack();
    for (const char* sql : {"SET no_such_parameter TO '1';", "SET client_encoding = 'utf8';"}) {
        INFO(sql);
        auto refused = run_session_sql_payload(s, 1, sql);
        REQUIRE(refused.has_error());
    }
    // Prepared: the same refusal at prepare.
    REQUIRE(otterstax::test::prepare_session_sql(s, 2, "SET no_such_parameter TO '1';").has_error());
}

TEST_CASE("settings: a SET inside a block keeps it open and is refused in a failed one") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_set_block"));
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());
    const auto other = owner.stack();
    run_session_or_fail(s, 1, "CREATE SCHEMA setb;");
    run_session_or_fail(s, 2, "CREATE TABLE setb.t (id bigint);");

    run_session_or_fail(s, 3, "BEGIN;");
    run_session_or_fail(s, 4, "INSERT INTO setb.t (id) VALUES (1);");
    run_session_or_fail(s, 6, "SET timezone TO 'UTC';");
    CHECK(session->transaction_status() == status_t::in_transaction);
    run_session_or_fail(s, 7, "COMMIT;");
    CHECK(count_rows(other, 8, "setb.t") == 1);

    // In a failed block a SET is refused as the engine refuses everything
    // there, and the block stays failed.
    run_session_or_fail(s, 9, "BEGIN;");
    REQUIRE(run_session_sql_payload(s, 10, "SELECT * FROM setb.nowhere;").has_error());
    auto refused = run_session_sql_payload(s, 11, "SET timezone TO 'UTC';");
    require_error_containing(refused, "only ROLLBACK");
    CHECK(session->transaction_status() == status_t::failed);
    run_session_or_fail(s, 12, "ROLLBACK;");
}

// close() ends the engine session (manager_dispatcher_t::close_session): an
// open block rolls back and the session's own state — its SQL PREPAREs among
// it — goes, whether the session was inside a block or not.
TEST_CASE("transaction status: a session closed inside a block rolls it back") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_close"));
    auto& sessions = owner.sessions();
    const size_t baseline = sessions.live_sessions();
    const auto other = owner.stack();
    run_session_or_fail(other, 1, "CREATE SCHEMA txc;");
    run_session_or_fail(other, 2, "CREATE TABLE txc.t (id bigint);");

    auto session = owner.open_session();
    const auto engine_session = session->engine_session();
    const auto s = owner.stack().on(session->address());
    run_session_or_fail(s, 1, "BEGIN;");
    run_session_or_fail(s, 2, "INSERT INTO txc.t (id) VALUES (1), (2);");
    REQUIRE(session->transaction_status() == status_t::in_transaction);
    session.reset();
    REQUIRE(wait_for_live(sessions, baseline, 10000ms));
    // Not visible from another session — which MVCC would answer for a block
    // left open too, so it proves nothing about the rollback by itself.
    CHECK(count_rows(other, 3, "txc.t") == 0);
    // The proof: the closed connection's engine session is a new, empty one.
    // A COMMIT sent on it now commits nothing; had the block been left with
    // the engine, this COMMIT would publish the two rows.
    auto commit = otterstax::engine::sql_plan(owner.engine().resource(), "COMMIT;");
    REQUIRE_FALSE(commit.has_error());
    auto committed = owner.engine().engine().execute_plan_blocking(engine_session, std::move(commit.value()));
    REQUIRE(committed);
    CHECK(count_rows(other, 4, "txc.t") == 0);
}

TEST_CASE("transaction status: an idle session closed takes its engine state with it") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_close_idle"));
    auto& sessions = owner.sessions();
    const size_t baseline = sessions.live_sessions();
    auto session = owner.open_session();
    const auto engine_session = session->engine_session();
    const auto s = owner.stack().on(session->address());
    run_session_or_fail(s, 1, "PREPARE q AS SELECT 1;");
    run_session_or_fail(s, 2, "EXECUTE q;");
    REQUIRE(session->transaction_status() == status_t::idle);
    session.reset();
    REQUIRE(wait_for_live(sessions, baseline, 10000ms));
    // The engine dropped the session's prepared statement with the session.
    auto execute = otterstax::engine::sql_plan(owner.engine().resource(), "EXECUTE q;");
    REQUIRE_FALSE(execute.has_error());
    auto executed = owner.engine().engine().execute_plan_blocking(engine_session, std::move(execute.value()));
    REQUIRE(executed);
    CHECK(executed->is_error());
}

// A statement over a connection's table in a failed block is refused by the
// engine, as everything is there.
TEST_CASE("transaction status: a statement over a connection's table is refused in a failed block") {
    stack_options_t options;
    options.connections.mysql = otterstax::test::mock_mysql({"orders"});
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_remote"), options);
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());

    run_session_or_fail(s, 1, "BEGIN;");
    REQUIRE(run_session_sql_payload(s, 2, "SELECT * FROM txr_nowhere.t;").has_error());
    REQUIRE(session->transaction_status() == status_t::failed);
    for (const char* sql : {"UPDATE orders.db.public.\"table\" SET name = 'x' WHERE id = 1;",
                            "CREATE TABLE orders.db.public.created_in_failed_block (id bigint);"}) {
        INFO(sql);
        auto refused = run_session_sql_payload(s, 3, sql);
        require_error_containing(refused, "only ROLLBACK");
        CHECK(refused.error().type == core::error_code_t::transaction_finalized);
        CHECK(session->transaction_status() == status_t::failed);
    }
    run_session_or_fail(s, 4, "ROLLBACK;");
}
