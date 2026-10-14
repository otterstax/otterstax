// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// What only a query session owns: its life (make_session_services()
// sets every address a session sends to; close() frees an idle session at once
// and a busy one when its statement in flight has finished, refusing what was
// queued behind it),
// one statement at a time across real suspensions (the engine on its own pools,
// a slow backend on the connector pool), and the connection's engine session:
// BEGIN / COMMIT / ROLLBACK over engine tables with PostgreSQL semantics,
// isolated per connection.

#include "../system/session_stack.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/parser.hpp"
#include "../mock/sql_db_connector.hpp"

#include <catch2/catch_all.hpp>

#include <sys/resource.h>

#include <chrono>
#include <condition_variable>
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
    // the MySQL integration backend while the mock sleeps on the connector pool.
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
                return chunk.value(0, 0).value<uint64_t>();
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
                                                              owner.kafka_address());
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
                                                                  missing == 3 ? empty : owner.kafka_address());
        REQUIRE(services.has_error());
        CHECK(services.error().type == core::error_code_t::invalid_parameter);
        CHECK(std::string{services.error().what.c_str()} ==
              std::string{"session services: the "} + names[missing] + " address is empty");
    }
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

// Hidden until OTB-notify (docs/proposals/otterbrix-embedding.md §4.13): a
// statement held on its backend for a second must leave the process idle. In
// actor-zeta 2.0.0 a cooperative actor suspended on a co_await is requeued on
// every pass of its pool (completion is flag-only), so today both the session
// on the sessions pool and the engine's executor on the exec pool spin: about
// 3.4 CPU-seconds per second of waiting on an 18-core machine, against 0.03
// with nothing in flight. Asserts the correct behaviour; take the dot off once
// a settled future wakes the actor that awaits it.
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
// own pools and the slow statement on the connector pool, so the session
// really suspends at each of them; a statement taken off the mailbox early, or
// a wakeup lost, shows up as a count that is not the number of INSERTs sent
// before it, or as a future that never settles.
TEST_CASE("session order: one statement at a time across real suspensions") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_order_suspend"), slow_options());
    const auto s = owner.stack();
    run_session_or_fail(s, 1, "CREATE DATABASE ord;");
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
    run_session_or_fail(s, 1, "CREATE DATABASE txn;");
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
    run_session_or_fail(s, 1, "CREATE DATABASE txa;");
    run_session_or_fail(s, 2, "CREATE TABLE txa.t (id bigint);");

    SECTION("an engine error, then ROLLBACK") {
        run_session_or_fail(s, 3, "BEGIN;");
        run_session_or_fail(s, 4, "INSERT INTO txa.t (id) VALUES (1);");
        REQUIRE(run_session_sql_payload(s, 5, "SELECT * FROM txa.nowhere;").has_error());
        auto refused = run_session_sql_payload(s, 6, "SELECT count(*) FROM txa.t;");
        require_error_containing(refused, "only ROLLBACK is accepted");
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
                                 "only ROLLBACK is accepted");
        auto commit = run_session_sql_payload(s, 7, "COMMIT;");
        require_error_containing(commit, "nothing was committed");
        CHECK(commit.error().type == core::error_code_t::transaction_finalized);
        // The block is over: statements run again, and nothing of it was kept.
        CHECK(count_rows(s, 8, "txa.t") == 0);
    }
}

TEST_CASE("session transaction: two connections are isolated") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_iso"));
    const auto a = owner.stack();
    const auto b = a.on(owner.open_connection());
    run_session_or_fail(a, 1, "CREATE DATABASE iso;");
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

// ─── The block the session follows (transaction_state) ──────────────────────

using otterstax::session::next_transaction_state;
using otterstax::session::transaction_command_t;
using otterstax::session::transaction_state_t;

TEST_CASE("transaction state: the transitions mirror the engine dispatcher") {
    using enum transaction_state_t;
    constexpr auto none = core::error_code_t::none;
    constexpr auto finalized = core::error_code_t::transaction_finalized;
    constexpr auto other = core::error_code_t::table_not_exists;
    constexpr auto io = core::error_code_t::io_error;
    // BEGIN opens a block; a stray BEGIN inside one keeps it.
    STATIC_REQUIRE(next_transaction_state(idle, transaction_command_t::begin, true, none) == in_block);
    STATIC_REQUIRE(next_transaction_state(in_block, transaction_command_t::begin, true, none) == in_block);
    // COMMIT / ROLLBACK end it, whatever it was; COMMIT of a failed block ends
    // it as the rollback the engine answers transaction_finalized for.
    STATIC_REQUIRE(next_transaction_state(in_block, transaction_command_t::commit, true, none) == idle);
    STATIC_REQUIRE(next_transaction_state(failed, transaction_command_t::commit, false, finalized) == idle);
    STATIC_REQUIRE(next_transaction_state(failed, transaction_command_t::rollback, true, none) == idle);
    STATIC_REQUIRE(next_transaction_state(idle, transaction_command_t::commit, true, none) == idle);
    // A COMMIT that fails on its own (the WAL marker, the index commit) has
    // already drained the transaction in the engine: the session is idle there,
    // so it is idle here — never failed, which would refuse the remote DDL /
    // DML, COPY and Kafka statements and show E on the PG wire until the next
    // statement.
    STATIC_REQUIRE(next_transaction_state(in_block, transaction_command_t::commit, false, io) == idle);
    STATIC_REQUIRE(next_transaction_state(in_block, transaction_command_t::commit, false, other) == idle);
    STATIC_REQUIRE(next_transaction_state(failed, transaction_command_t::commit, false, io) == idle);
    // An error fails an open block, and only an open one.
    STATIC_REQUIRE(next_transaction_state(in_block, transaction_command_t::none, false, other) == failed);
    STATIC_REQUIRE(next_transaction_state(idle, transaction_command_t::none, false, other) == idle);
    // A refusal as transaction_finalized is a failed block, whatever was believed.
    STATIC_REQUIRE(next_transaction_state(failed, transaction_command_t::none, false, finalized) == failed);
    STATIC_REQUIRE(next_transaction_state(idle, transaction_command_t::none, false, finalized) == failed);
    STATIC_REQUIRE(next_transaction_state(failed, transaction_command_t::begin, false, finalized) == failed);
    // A statement that ran proves there is no failed block.
    STATIC_REQUIRE(next_transaction_state(failed, transaction_command_t::none, true, none) == idle);
    STATIC_REQUIRE(next_transaction_state(in_block, transaction_command_t::none, true, none) == in_block);
}

TEST_CASE("transaction state: the session follows BEGIN, a failure, the refusals and COMMIT") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_state"));
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());
    run_session_or_fail(s, 1, "CREATE DATABASE txs;");
    run_session_or_fail(s, 2, "CREATE TABLE txs.t (id bigint);");
    CHECK(session->transaction_state() == transaction_state_t::idle);

    auto begun = run_session_sql_payload(s, 3, "BEGIN;");
    REQUIRE_FALSE(begun.has_error());
    CHECK(begun.value().transaction == transaction_command_t::begin);
    CHECK(session->transaction_state() == transaction_state_t::in_block);

    auto inserted = run_session_sql_payload(s, 4, "INSERT INTO txs.t (id) VALUES (1);");
    REQUIRE_FALSE(inserted.has_error());
    CHECK(inserted.value().transaction == transaction_command_t::none);
    CHECK(session->transaction_state() == transaction_state_t::in_block);

    SECTION("an engine error") {
        REQUIRE(run_session_sql_payload(s, 5, "SELECT * FROM txs.nowhere;").has_error());
    }
    SECTION("a prepare the engine refuses") {
        REQUIRE(otterstax::test::prepare_session_sql(s, 5, "SELECT nowhere FROM txs.t;").has_error());
    }
    SECTION("a failure the frontend reports") {
        auto [ns, fut] = actor_zeta::send(s.session,
                                          &query_session_t::fail_transaction,
                                          core::error_code_t::other_error,
                                          std::string{"the client sent a malformed message"});
        auto failed = await_session(std::move(fut), 10000ms, s.resource);
        require_error_containing(failed, "malformed message");
    }
    CHECK(session->transaction_state() == transaction_state_t::failed);
    require_error_containing(run_session_sql_payload(s, 6, "INSERT INTO txs.t (id) VALUES (2);"),
                             "only ROLLBACK is accepted");
    CHECK(session->transaction_state() == transaction_state_t::failed);

    auto commit = run_session_sql_payload(s, 7, "COMMIT;");
    REQUIRE(commit.has_error());
    CHECK(commit.error().type == core::error_code_t::transaction_finalized);
    CHECK(session->transaction_state() == transaction_state_t::idle);
    CHECK(count_rows(s, 8, "txs.t") == 0);

    auto rolled = run_session_sql_payload(s, 9, "ROLLBACK;");
    REQUIRE_FALSE(rolled.has_error());
    CHECK(rolled.value().transaction == transaction_command_t::rollback);
    CHECK(session->transaction_state() == transaction_state_t::idle);
}

// A prepare whose text does not parse fails an open block, as PostgreSQL's own
// Parse error does (the MySQL frontend writes `?` as `$n` before its first
// prepare, so no prepare is retried under another spelling).
TEST_CASE("transaction state: a prepare that does not parse fails an open block") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_prepare_parse"));
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());
    run_session_or_fail(s, 1, "CREATE DATABASE txp;");
    run_session_or_fail(s, 2, "CREATE TABLE txp.t (id bigint);");
    run_session_or_fail(s, 3, "BEGIN;");
    run_session_or_fail(s, 4, "INSERT INTO txp.t (id) VALUES (1);");
    REQUIRE(otterstax::test::prepare_session_sql(s, 5, "SELEKT id FROM txp.t;").has_error());
    CHECK(session->transaction_state() == transaction_state_t::failed);
    auto refused = run_session_sql_payload(s, 6, "INSERT INTO txp.t (id) VALUES (2);");
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::transaction_finalized);
    auto committed = run_session_sql_payload(s, 7, "COMMIT;");
    CHECK(session->transaction_state() == transaction_state_t::idle);
    static_cast<void>(committed);
    CHECK(count_rows(s, 8, "txp.t") == 0);
}

TEST_CASE("transaction state: SAVEPOINT is refused as not supported and fails an open block") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_savepoint"));
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());
    SECTION("outside a block nothing fails") {
        auto refused = run_session_sql_payload(s, 1, "SAVEPOINT sp;");
        REQUIRE(refused.has_error());
        CHECK(refused.error().type == core::error_code_t::unimplemented_yet);
        CHECK(session->transaction_state() == transaction_state_t::idle);
    }
    SECTION("inside one the block fails, as any error does") {
        run_session_or_fail(s, 1, "BEGIN;");
        for (const char* sql : {"SAVEPOINT sp;", "RELEASE SAVEPOINT sp;", "ROLLBACK TO SAVEPOINT sp;"}) {
            INFO(sql);
            auto refused = run_session_sql_payload(s, 2, sql);
            REQUIRE(refused.has_error());
            if (session->transaction_state() == transaction_state_t::in_block) {
                FAIL("the block is still open after " << sql);
            }
            // The first refusal fails the block; the next ones are refused in it.
            CHECK((refused.error().type == core::error_code_t::unimplemented_yet ||
                   refused.error().type == core::error_code_t::transaction_finalized));
        }
        CHECK(session->transaction_state() == transaction_state_t::failed);
        run_session_or_fail(s, 3, "ROLLBACK;");
    }
}

// A SET never reaches the engine, which would apply it to every session and
// persist it: one session's SET autocommit / timezone, in any form, is answered
// OK without effect, and another session stays in autocommit.
TEST_CASE("settings: a SET of one session changes no other session") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_set_isolated"));
    auto a = owner.open_session();
    auto b = owner.open_session();
    const auto sa = owner.stack().on(a->address());
    const auto sb = owner.stack().on(b->address());
    const auto sc = owner.stack();
    run_session_or_fail(sa, 1, "CREATE DATABASE sets;");
    run_session_or_fail(sa, 2, "CREATE TABLE sets.t (id bigint);");

    session_hash_t id = 10;
    for (const char* sql : {"SET autocommit TO off;",
                            "SET autocommit = 0;",
                            "SET SESSION autocommit = false;",
                            "SET LOCAL autocommit TO off;",
                            "SET timezone TO 'Asia/Tokyo';",
                            "SET TIME ZONE 'UTC';",
                            "SET decimal_width TO '10';",
                            "RESET autocommit;",
                            "RESET ALL;",
                            "SET autocommit TO DEFAULT;",
                            "SET no_such_parameter TO 1;"}) {
        INFO(sql);
        auto answered = run_session_sql_payload(sa, ++id, sql);
        INFO((answered.has_error() ? answered.error().what.c_str() : "ok"));
        REQUIRE_FALSE(answered.has_error());
        CHECK(answered.value().column_count() == 0);
        CHECK(answered.value().tag == T_VariableSetStmt);
        CHECK(a->transaction_state() == transaction_state_t::idle);
    }
    // Prepared, then executed: the same.
    REQUIRE_FALSE(otterstax::test::prepare_session_sql(sa, 40, "SET autocommit TO off;").has_error());
    auto [ns, fut] = actor_zeta::send(sa.session, &query_session_t::execute_statement, stmt(40));
    auto executed = await_session(std::move(fut), 10000ms, sa.resource);
    REQUIRE_FALSE(executed.has_error());

    // b is still in autocommit: its INSERT commits on its own, seen at once.
    run_session_or_fail(sb, 1, "INSERT INTO sets.t (id) VALUES (1);");
    CHECK(b->transaction_state() == transaction_state_t::idle);
    CHECK(count_rows(sc, 2, "sets.t") == 1);
    // And a itself.
    run_session_or_fail(sa, 50, "INSERT INTO sets.t (id) VALUES (2);");
    CHECK(count_rows(sc, 3, "sets.t") == 2);
}

TEST_CASE("settings: a SET inside a block keeps it open and is refused in a failed one") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_set_block"));
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());
    const auto other = owner.stack();
    run_session_or_fail(s, 1, "CREATE DATABASE setb;");
    run_session_or_fail(s, 2, "CREATE TABLE setb.t (id bigint);");

    run_session_or_fail(s, 3, "BEGIN;");
    run_session_or_fail(s, 4, "INSERT INTO setb.t (id) VALUES (1);");
    run_session_or_fail(s, 5, "SET autocommit = 0;");
    run_session_or_fail(s, 6, "SET timezone TO 'UTC';");
    // A SET in a frontend's own grammar (MySQL `SET @@session.autocommit = 0`)
    // is the same answer.
    {
        auto [ns, fut] =
            actor_zeta::send(session->address(), &query_session_t::execute_setting);
        auto answered = otterstax::test::await_session(std::move(fut), std::chrono::milliseconds(10000), s.resource);
        REQUIRE_FALSE(answered.has_error());
        CHECK(answered.value().column_count() == 0);
    }
    CHECK(session->transaction_state() == transaction_state_t::in_block);
    run_session_or_fail(s, 7, "COMMIT;");
    CHECK(count_rows(other, 8, "setb.t") == 1);

    // In a failed block a SET is refused as the engine refuses everything
    // there, and the block stays failed.
    run_session_or_fail(s, 9, "BEGIN;");
    REQUIRE(run_session_sql_payload(s, 10, "SELECT * FROM setb.nowhere;").has_error());
    auto refused = run_session_sql_payload(s, 11, "SET autocommit TO on;");
    require_error_containing(refused, "only ROLLBACK is accepted");
    {
        auto [ns, fut] =
            actor_zeta::send(session->address(), &query_session_t::execute_setting);
        auto answered = otterstax::test::await_session(std::move(fut), std::chrono::milliseconds(10000), s.resource);
        require_error_containing(answered, "only ROLLBACK is accepted");
    }
    CHECK(session->transaction_state() == transaction_state_t::failed);
    run_session_or_fail(s, 12, "ROLLBACK;");
}

// otterbrix has no close-session call: a connection closed inside a block would
// leave its transaction with the engine for good. The session rolls it back as
// it goes.
TEST_CASE("transaction state: a session closed inside a block rolls it back") {
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_close"));
    auto& sessions = owner.sessions();
    const size_t baseline = sessions.live_sessions();
    const auto other = owner.stack();
    run_session_or_fail(other, 1, "CREATE DATABASE txc;");
    run_session_or_fail(other, 2, "CREATE TABLE txc.t (id bigint);");

    auto session = owner.open_session();
    const auto engine_session = session->engine_session();
    const auto s = owner.stack().on(session->address());
    run_session_or_fail(s, 1, "BEGIN;");
    run_session_or_fail(s, 2, "INSERT INTO txc.t (id) VALUES (1), (2);");
    REQUIRE(session->transaction_state() == transaction_state_t::in_block);
    session.reset();
    // The ROLLBACK is queued on the session's own pool; the session frees
    // itself once it ran.
    REQUIRE(wait_for_live(sessions, baseline, 10000ms));
    // Not visible from another session — which MVCC would answer for a block
    // left open too, so it proves nothing about the ROLLBACK by itself.
    CHECK(count_rows(other, 3, "txc.t") == 0);
    // The proof: the closed connection's engine session is no longer inside
    // the block. A COMMIT sent on it now commits nothing; had the block been
    // left with the engine, this COMMIT would publish the two rows.
    auto commit = otterstax::engine::sql_plan(owner.engine().resource(), "COMMIT;");
    REQUIRE_FALSE(commit.has_error());
    auto committed = owner.engine().engine().execute_plan_blocking(engine_session, std::move(commit.value()));
    REQUIRE(committed);
    CHECK(count_rows(other, 4, "txc.t") == 0);
    run_session_or_fail(other, 5, "INSERT INTO txc.t (id) VALUES (3);");
    CHECK(count_rows(other, 6, "txc.t") == 1);
}

// The statements that do not run on the connection's engine session — remote
// DDL, a four-part DML — are refused in a failed block exactly as the engine
// refuses the rest; once the block ended they run again.
TEST_CASE("transaction state: remote DDL and a four-part DML are refused in a failed block") {
    stack_options_t options;
    options.connections.mysql = otterstax::test::mock_mysql({"orders"});
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_txn_remote"), options);
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());

    run_session_or_fail(s, 1, "BEGIN;");
    REQUIRE(run_session_sql_payload(s, 2, "SELECT * FROM orders.db.nowhere_at_all;").has_error());
    REQUIRE(session->transaction_state() == transaction_state_t::failed);

    for (const char* sql : {"UPDATE orders.db.public.\"table\" SET name = 'x' WHERE id = 1;",
                            "CREATE TABLE orders.db.created_in_failed_block (id bigint);"}) {
        INFO(sql);
        auto refused = run_session_sql_payload(s, 3, sql);
        require_error_containing(refused, "only ROLLBACK is accepted");
        CHECK(refused.error().type == core::error_code_t::transaction_finalized);
        CHECK(session->transaction_state() == transaction_state_t::failed);
    }

    run_session_or_fail(s, 4, "ROLLBACK;");
    auto four = run_session_sql_payload(s, 5, "UPDATE orders.db.public.\"table\" SET name = 'x' WHERE id = 1;");
    INFO((four.has_error() ? four.error().what.c_str() : "ok"));
    REQUIRE_FALSE(four.has_error());
    CHECK(four.value().reported_rows() == 42);
}

// A four-part DML target runs on its backend directly, whose write answers a
// count only: a RETURNING list is refused at execute and at prepare, never
// answered as a bare count.
TEST_CASE("remote DML: RETURNING on a four-part target is refused at execute and at prepare") {
    stack_options_t options;
    options.connections.mysql = otterstax::test::mock_mysql({"orders"});
    session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_remote_returning"), options);
    const auto s = owner.stack();
    for (const char* sql : {"UPDATE orders.db.public.\"table\" SET name = 'x' WHERE id = 1 RETURNING id;",
                            "DELETE FROM orders.db.public.\"table\" WHERE id = 1 RETURNING id, name;",
                            "INSERT INTO orders.db.public.\"table\" (id, name) VALUES (5, 'e') RETURNING *;"}) {
        INFO(sql);
        auto executed = run_session_sql_payload(s, 1, sql);
        REQUIRE(executed.has_error());
        CHECK(executed.error().type == core::error_code_t::unimplemented_yet);
        require_error_containing(executed, "RETURNING is not supported");

        auto prepared = otterstax::test::prepare_session_sql(s, 2, sql);
        REQUIRE(prepared.has_error());
        CHECK(prepared.error().type == core::error_code_t::unimplemented_yet);
        require_error_containing(prepared, "RETURNING is not supported");
    }
}
