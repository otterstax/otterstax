// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The query-session scope of the Flight SQL server (FlightSqlCore over its
// SessionEngine): a statement run by text runs on a session of its own that is
// closed when the call returns; a prepared statement's session is owned by its
// entry in the core from create_prepared() to close_prepared(), and every
// execution of it — however many — runs there, each step reaching it under the
// core's lock, so a close racing an execution never frees the session under it.

#include "frontend/flight_sql/session_engine.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <arrow/api.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

    using namespace std::chrono_literals;
    using flight::core::FlightSqlCore;

    // Every client is one owner on an anonymous service.
    const std::string kClient;

    bool wait_for_live(otterstax::session::session_factory_t& sessions, size_t live) {
        for (auto waited = 0ms; waited < 5000ms; waited += 10ms) {
            if (sessions.live_sessions() == live) {
                return true;
            }
            std::this_thread::sleep_for(10ms);
        }
        return sessions.live_sessions() == live;
    }

    int64_t rows_of(const flight::core::QueryResult& result) {
        int64_t rows = 0;
        for (const auto& batch : result.batches) {
            rows += batch->num_rows();
        }
        return rows;
    }

    FlightSqlCore make_core(otterstax::test::session_stack_owner& owner) {
        return FlightSqlCore(
            flight::core::AuthService::anonymous(),
            std::make_unique<flight::engine::SessionEngine>(owner.sessions(), owner.engine().resource()));
    }

    std::string prepare(FlightSqlCore& core, const std::string& query) {
        std::string handle;
        REQUIRE(core.create_prepared(query, kClient, &handle).ok());
        return handle;
    }

    int64_t rows_of_prepared(FlightSqlCore& core, const std::string& handle) {
        flight::core::QueryResult result;
        REQUIRE(core.execute_prepared(handle, kClient, &result).ok());
        return rows_of(result);
    }

    int64_t update_prepared(FlightSqlCore& core, const std::string& handle) {
        int64_t affected = 0;
        REQUIRE(core.execute_update_prepared(handle, kClient, &affected).ok());
        return affected;
    }

} // namespace

TEST_CASE("flight session scope: a statement by text runs on a session closed when it returns") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_flight_scope_text"));
    auto& sessions = owner.sessions();
    const size_t baseline = sessions.live_sessions();
    auto core = make_core(owner);
    auto& engine = core.engine();

    CHECK(engine.execute_update("CREATE DATABASE fs") == 0);
    CHECK(engine.execute_update("CREATE TABLE fs.t (id bigint)") == 0);
    CHECK(engine.execute_update("INSERT INTO fs.t (id) VALUES (1), (2), (3)") == 3);
    CHECK(rows_of(engine.execute("SELECT id FROM fs.t")) == 3);
    CHECK(wait_for_live(sessions, baseline));
}

TEST_CASE("flight session scope: a prepared statement keeps one session until it is closed") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_flight_scope_prepared"));
    auto& sessions = owner.sessions();
    const size_t baseline = sessions.live_sessions();
    auto core = make_core(owner);
    auto& engine = core.engine();
    REQUIRE(engine.execute_update("CREATE DATABASE fp") == 0);
    REQUIRE(engine.execute_update("CREATE TABLE fp.t (id bigint)") == 0);
    REQUIRE(engine.execute_update("INSERT INTO fp.t (id) VALUES (1), (2)") == 2);
    REQUIRE(wait_for_live(sessions, baseline));

    const auto select = prepare(core, "SELECT id FROM fp.t");
    CHECK(sessions.live_sessions() == baseline + 1);
    const auto insert = prepare(core, "INSERT INTO fp.t (id) VALUES (7)");
    CHECK(sessions.live_sessions() == baseline + 2);

    // Executions run on the statement's session: no session is opened for them.
    CHECK(rows_of_prepared(core, select) == 2);
    CHECK(update_prepared(core, insert) == 1);
    CHECK(update_prepared(core, insert) == 1);
    CHECK(rows_of_prepared(core, select) == 4);
    CHECK(sessions.live_sessions() == baseline + 2);

    REQUIRE(core.close_prepared(insert, kClient).ok());
    CHECK(wait_for_live(sessions, baseline + 1));
    REQUIRE(core.close_prepared(select, kClient).ok());
    CHECK(wait_for_live(sessions, baseline));
    // A closed handle is unknown: closing or executing it again is NOT_FOUND
    // and opens no session.
    CHECK(core.close_prepared(select, kClient).error_code() == grpc::StatusCode::NOT_FOUND);
    flight::core::QueryResult result;
    CHECK(core.execute_prepared(select, kClient, &result).error_code() == grpc::StatusCode::NOT_FOUND);
    CHECK(sessions.live_sessions() == baseline);
}

// The entry is the session's only owner and an execution reaches the session
// under the core's lock step by step: a close racing executions closes the
// session at once; an execution answers its rows (the statement the session
// took runs to its end) or NOT_FOUND (a step after the close), never a session
// freed under it, and the session is freed once its last statement ended.
TEST_CASE("flight session scope: a close racing executions of the statement") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_flight_scope_race"));
    auto& sessions = owner.sessions();
    const size_t baseline = sessions.live_sessions();
    auto core = make_core(owner);
    REQUIRE(core.engine().execute_update("CREATE DATABASE fr") == 0);
    REQUIRE(core.engine().execute_update("CREATE TABLE fr.t (id bigint)") == 0);
    REQUIRE(core.engine().execute_update("INSERT INTO fr.t (id) VALUES (1), (2), (3)") == 3);
    REQUIRE(wait_for_live(sessions, baseline));

    for (int round = 0; round < 10; ++round) {
        const auto handle = prepare(core, "SELECT id FROM fr.t");
        std::atomic<int> answered{0};
        std::atomic<int> unexpected{0};
        std::vector<std::thread> executions;
        for (int t = 0; t < 4; ++t) {
            executions.emplace_back([&] {
                for (;;) {
                    flight::core::QueryResult result;
                    const auto status = core.execute_prepared(handle, kClient, &result);
                    if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
                        return;
                    }
                    if (!status.ok() || rows_of(result) != 3) {
                        ++unexpected;
                        return;
                    }
                    ++answered;
                }
            });
        }
        while (answered.load() < 2 && unexpected.load() == 0) {
            std::this_thread::sleep_for(1ms);
        }
        REQUIRE(core.close_prepared(handle, kClient).ok());
        for (auto& execution : executions) {
            execution.join();
        }
        CHECK(unexpected.load() == 0);
        CHECK(wait_for_live(sessions, baseline));
    }
}

// No Flight SQL scope outlives what it runs, so a transaction opened in one
// could never be ended by the client (its COMMIT runs on another session):
// BEGIN is rolled back and refused, by text and prepared alike, and a later
// statement is not inside anything.
TEST_CASE("flight session scope: BEGIN is refused and leaves no transaction open") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_flight_scope_begin"));
    auto& sessions = owner.sessions();
    const size_t baseline = sessions.live_sessions();
    auto core = make_core(owner);
    auto& engine = core.engine();
    REQUIRE(engine.execute_update("CREATE DATABASE fb") == 0);
    REQUIRE(engine.execute_update("CREATE TABLE fb.t (id bigint)") == 0);

    auto refused = [](auto&& call) {
        try {
            call();
        } catch (const flight::core::EngineError& e) {
            return std::string{e.what()};
        }
        return std::string{};
    };
    CHECK(refused([&] { engine.execute_update("BEGIN"); }).find("transaction cannot be opened") != std::string::npos);
    CHECK(refused([&] { engine.execute("BEGIN"); }).find("transaction cannot be opened") != std::string::npos);
    const auto begin = prepare(core, "BEGIN");
    REQUIRE(sessions.live_sessions() == baseline + 1);
    int64_t affected = 0;
    auto status = core.execute_update_prepared(begin, kClient, &affected);
    CHECK(status.error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    CHECK(status.error_message().find("transaction cannot be opened") != std::string::npos);
    // The prepared statement's session lives on until close_prepared, and the
    // refusal rolled its block back: no live session is inside one. (Rows of
    // such a block would be invisible to every other session either way — MVCC
    // — so only the session's own block tells the ROLLBACK ran.)
    CHECK(sessions.live_sessions() == baseline + 1);
    CHECK(sessions.sessions_in_block() == 0);
    // A second execution opens the block again and is rolled back again.
    status = core.execute_update_prepared(begin, kClient, &affected);
    CHECK(status.error_message().find("transaction cannot be opened") != std::string::npos);
    CHECK(sessions.sessions_in_block() == 0);
    REQUIRE(core.close_prepared(begin, kClient).ok());
    CHECK(wait_for_live(sessions, baseline));

    // COMMIT / ROLLBACK by text end nothing and are accepted (autocommit).
    CHECK(engine.execute_update("COMMIT") == 0);
    CHECK(engine.execute_update("INSERT INTO fb.t (id) VALUES (1)") == 1);
    CHECK(rows_of(engine.execute("SELECT id FROM fb.t")) == 1);
}
