// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// query_session_t driven the way a frontend drives it: a remote SELECT over the
// mock MySQL connector, each failure site's error text, many connections at
// once, a throwing parser, plus what only a session has: the statements of one
// connection run one at a time, in the order they were sent, even when the
// frontend sends them back-to-back.

#include "../system/session_stack.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/parser.hpp"
#include "../mock/sql_db_connector.hpp"
#include "../system/test_helpers.hpp"
#include "session/statement_kind.hpp"

#include <catch2/catch_all.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using otterstax::session::query_session_t;
using otterstax::test::await_session;
using otterstax::test::engine_row_count;
using otterstax::test::run_session_or_fail;
using otterstax::test::run_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::session_stack_owner;
using otterstax::test::stack_options_t;
using otterstax::test::stmt;
using otterstax::test::with_session_stack_parser;

namespace {

    // The whole stack over a real engine, the mock MySQL connector `factory`
    // and the mock parser `parser`, with the two MySQL connections ("1", "2")
    // the mock parser's statement resolves against.
    class mock_stack {
    public:
        mock_stack(const std::string& data_dir, mysql::connector_factory factory, otterstax::session::parser_factory_fn parser)
            : owner_(data_dir, options(factory, parser)) {}

        actor_zeta::address_t open_session() { return owner_.open_connection(); }
        std::pmr::memory_resource* resource() { return owner_.engine().resource(); }

    private:
        static stack_options_t options(mysql::connector_factory factory, otterstax::session::parser_factory_fn parser) {
            stack_options_t options;
            options.parser_factory = parser;
            options.connectors.mysql = factory;
            options.connections.mysql = otterstax::test::mock_mysql({"1", "2"});
            return options;
        }

        session_stack_owner owner_;
    };

    core::result_wrapper_t<session_payload>
    execute_on(mock_stack& stack, const actor_zeta::address_t& session, session_hash_t id, const std::string& sql) {
        auto [ns, fut] = actor_zeta::send(session, &query_session_t::execute, stmt(id), sql);
        return await_session(std::move(fut), std::chrono::milliseconds(5000), stack.resource());
    }

    constexpr const char* kSql = "SELECT 1 AS test";

} // namespace

TEST_CASE("session: a remote SELECT runs through the backend and the engine") {
    mock_stack stack("/tmp/test_session_mock_base", &mysql_mock_connector_factory, &make_mock_parser);
    const auto session = stack.open_session();
    auto r = execute_on(stack, session, 1, kSql);
    INFO((r.has_error() ? r.error().what.c_str() : ""));
    REQUIRE_FALSE(r.has_error());
    REQUIRE(r.value().size() == 2);
}

TEST_CASE("session: a connector failure keeps the connector's message") {
    mock_stack stack("/tmp/test_session_mock_connector_error", &mysql_mock_connector_factory_throw, &make_mock_parser);
    auto r = execute_on(stack, stack.open_session(), 1, kSql);
    REQUIRE(r.has_error());
    // The backend's error reaches the client through the engine, under the
    // engine's execution prefix, its text whole.
    REQUIRE(std::string{r.error().what.c_str()} == "Otterbrix execution failed: MockConnector: exception in runQuery");
}

TEST_CASE("session: an engine failure keeps the engine's code under the execution prefix") {
    with_session_stack_parser("/tmp/test_session_engine_error", &make_parser, [](session_stack s) {
        auto r = run_session_sql_payload(s, 1, "SELECT * FROM nodb.nowhere;");
        REQUIRE(r.has_error());
        INFO(r.error().what.c_str());
        REQUIRE(std::string{r.error().what.c_str()}.rfind("Otterbrix execution failed: ", 0) == 0);
        REQUIRE(r.error().type != core::error_code_t::kernel_error);
    });
}

TEST_CASE("session: a parser that throws answers the exception text") {
    mock_stack stack("/tmp/test_session_mock_parser_error", &mysql_mock_connector_factory, &make_throwing_mock_parser);
    auto r = execute_on(stack, stack.open_session(), 1, kSql);
    REQUIRE(r.has_error());
    REQUIRE(r.error().type == core::error_code_t::sql_parse_error);
    REQUIRE(r.error().what == "SimpleMockParser: exception in parse");
}

TEST_CASE("session: an empty backend answer is an empty payload") {
    mock_stack stack("/tmp/test_session_mock_empty", &mysql_mock_connector_factory_return_empty, &make_mock_parser);
    auto r = execute_on(stack, stack.open_session(), 1, kSql);
    INFO((r.has_error() ? r.error().what.c_str() : ""));
    REQUIRE_FALSE(r.has_error());
    REQUIRE(r.value().empty());
}

// N connections in parallel: every call is sent before any is awaited, so the
// sessions run side by side on the sessions pool.
TEST_CASE("session: N connections in parallel each answer without hanging") {
    using namespace std::chrono_literals;
    constexpr size_t N = 32;

    mock_stack stack("/tmp/test_session_mock_parallel", &mysql_mock_connector_factory, &make_mock_parser);
    std::vector<actor_zeta::address_t> sessions;
    sessions.reserve(N);
    for (size_t i = 0; i < N; ++i) {
        sessions.push_back(stack.open_session());
    }

    std::vector<actor_zeta::unique_future<core::result_wrapper_t<session_payload>>> futures;
    futures.reserve(N);
    for (size_t i = 0; i < N; ++i) {
        futures.push_back(
            actor_zeta::send(sessions[i], &query_session_t::execute, stmt(i + 1), std::string{kSql}).second);
    }
    for (size_t i = 0; i < N; ++i) {
        auto r = await_session(std::move(futures[i]), 60000ms, stack.resource());
        INFO("connection " << i << ": " << (r.has_error() ? r.error().what.c_str() : ""));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().size() == 2);
    }
}

// The statements of one connection run one at a time in arrival order. Every
// INSERT is sent before the first is awaited — a pipelining client — and the
// SELECT sent last sees every row; by the time it has answered, every statement
// sent before it has answered too.
TEST_CASE("session: statements sent back-to-back complete in order") {
    using namespace std::chrono_literals;
    constexpr size_t K = 40;

    session_stack_owner owner("/tmp/test_session_order");
    const auto s = owner.stack();

    using future_t = actor_zeta::unique_future<core::result_wrapper_t<session_payload>>;
    std::vector<future_t> futures;
    session_hash_t id = 100;
    auto send = [&](const std::string& sql) {
        futures.push_back(actor_zeta::send(s.session, &query_session_t::execute, stmt(id++), sql).second);
    };
    send("CREATE DATABASE orderdb;");
    send("CREATE TABLE orderdb.t (id bigint);");
    for (size_t i = 0; i < K; ++i) {
        send("INSERT INTO orderdb.t (id) VALUES (" + std::to_string(i) + ");");
    }
    send("SELECT id FROM orderdb.t;");

    auto last = await_session(std::move(futures.back()), 30000ms, s.resource);
    INFO((last.has_error() ? last.error().what.c_str() : ""));
    REQUIRE_FALSE(last.has_error());
    REQUIRE(last.value().size() == K);
    for (size_t i = 0; i + 1 < futures.size(); ++i) {
        INFO("statement " << i);
        REQUIRE(futures[i].is_ready());
        auto r = await_session(std::move(futures[i]), 1000ms, s.resource);
        REQUIRE_FALSE(r.has_error());
    }
}

// Several connections over the real engine, each pipelining its own
// CREATE TABLE / INSERT / SELECT, all in flight at once.
TEST_CASE("session: parallel connections over the real engine keep their own order") {
    using namespace std::chrono_literals;
    constexpr size_t M = 8;
    constexpr size_t ROWS = 5;

    session_stack_owner owner("/tmp/test_session_parallel");
    const auto first = owner.stack();
    run_session_or_fail(first, 1, "CREATE DATABASE pardb;");

    std::vector<session_stack> connections;
    for (size_t c = 0; c < M; ++c) {
        connections.push_back(first.on(owner.open_connection()));
    }

    using future_t = actor_zeta::unique_future<core::result_wrapper_t<session_payload>>;
    std::vector<std::vector<future_t>> futures(M);
    for (size_t c = 0; c < M; ++c) {
        const std::string table = "pardb.t" + std::to_string(c);
        session_hash_t id = 1000 * (c + 1);
        auto& own = futures[c];
        own.push_back(actor_zeta::send(connections[c].session,
                                       &query_session_t::execute,
                                       stmt(id++),
                                       "CREATE TABLE " + table + " (id bigint);")
                          .second);
        for (size_t r = 0; r < ROWS; ++r) {
            own.push_back(actor_zeta::send(connections[c].session,
                                           &query_session_t::execute,
                                           stmt(id++),
                                           "INSERT INTO " + table + " (id) VALUES (" + std::to_string(r) + ");")
                              .second);
        }
        own.push_back(actor_zeta::send(connections[c].session,
                                       &query_session_t::execute,
                                       stmt(id++),
                                       "SELECT id FROM " + table + ";")
                          .second);
    }

    for (size_t c = 0; c < M; ++c) {
        auto& own = futures[c];
        for (size_t i = 0; i < own.size(); ++i) {
            auto r = await_session(std::move(own[i]), 30000ms, first.resource);
            INFO("connection " << c << " statement " << i << ": " << (r.has_error() ? r.error().what.c_str() : ""));
            REQUIRE_FALSE(r.has_error());
            if (i + 1 == own.size()) {
                REQUIRE(r.value().size() == ROWS);
            }
        }
    }
}

namespace {

    constexpr const char* kThrowText = "second_parse_boom";

    // Delegates to a real parser but throws on its second call: the inner query
    // of `COPY (<select>) TO ...`, re-parsed on the session's own parser.
    class throwing_on_second_parser final : public IParser {
    public:
        explicit throwing_on_second_parser(std::pmr::memory_resource* resource)
            : inner_(make_parser(resource)) {}

        core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
            if (++calls_ == 2) {
                throw std::runtime_error(kThrowText);
            }
            return inner_->parse(sql);
        }

    private:
        parser_ptr inner_;
        int calls_{0};
    };

    parser_ptr make_second_call_throwing_parser(std::pmr::memory_resource* resource) {
        return std::make_unique<throwing_on_second_parser>(resource);
    }

    struct not_an_exception {};

    class throwing_non_std_parser final : public IParser {
    public:
        core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string&) override { throw not_an_exception{}; }
    };

    parser_ptr make_non_std_throwing_parser(std::pmr::memory_resource*) {
        return std::make_unique<throwing_non_std_parser>();
    }

} // namespace

TEST_CASE("session: an exception from the inner COPY parse is an error and the session stays usable") {
    with_session_stack_parser("/tmp/test_session_exc_copy", &make_second_call_throwing_parser, [](session_stack s) {
        const std::string out = "/tmp/test_session_exc_copy_out.csv";
        std::filesystem::remove(out);

        std::string err;
        REQUIRE_FALSE(run_session_sql(s, 8100, "COPY (SELECT 1 AS x) TO '" + out + "' WITH (format = 'csv')", err));
        INFO("error was: " << err);
        REQUIRE(err.find(kThrowText) != std::string::npos);
        REQUIRE_FALSE(std::filesystem::exists(out));

        // The same session (the same parser instance, now past its throwing
        // call) answers the next statement.
        const bool ok = run_session_sql(s, 8101, "CREATE DATABASE after_boom;", err);
        INFO("recovery error: " << err);
        REQUIRE(ok);
    });
}

TEST_CASE("session: a non-std exception at the parser boundary becomes a parse error") {
    with_session_stack_parser("/tmp/test_session_exc_nonstd", &make_non_std_throwing_parser, [](session_stack s) {
        auto r = run_session_sql_payload(s, 8300, "CREATE DATABASE nonstd;");
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::sql_parse_error);
        REQUIRE_FALSE(std::string{r.error().what.c_str()}.empty());

        auto again = run_session_sql_payload(s, 8301, "CREATE DATABASE nonstd;");
        REQUIRE(again.has_error());
        REQUIRE(again.error().type == core::error_code_t::sql_parse_error);
    });
}

TEST_CASE("session: a database a connection owns is refused before the engine") {
    with_session_stack_parser("/tmp/test_session_ddl_guard", &make_parser, [](session_stack s) {
        // The kafka object database belongs to the KafkaManager.
        auto r = run_session_sql_payload(s, 8400, "CREATE DATABASE kafka;");
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::invalid_parameter);
        run_session_or_fail(s, 8401, "CREATE DATABASE free_name;");
    });
}

// The statements told by their raw parse tree (session/statement_kind.hpp) are
// told past blanks, `;` and comments, without regard to case; a text whose first
// keyword cannot begin one is answered without a parse, and a text that is more
// than one statement is none.
TEST_CASE("statement kind: SET RESET and DEALLOCATE by the first keyword and the raw tree") {
    auto* resource = std::pmr::new_delete_resource();
    using otterstax::session::deallocate_statement;
    using otterstax::session::is_setting_statement;

    CHECK(is_setting_statement(resource, "SET timezone TO 'UTC'"));
    CHECK(is_setting_statement(resource, "  /* a /* nested */ comment */ -- line\n set autocommit = 0"));
    CHECK(is_setting_statement(resource, ";Reset ALL"));
    CHECK(is_setting_statement(resource, "SET SESSION search_path TO DEFAULT;"));
    CHECK_FALSE(is_setting_statement(resource, "SELECT 1"));
    CHECK_FALSE(is_setting_statement(resource, "settings"));
    CHECK_FALSE(is_setting_statement(resource, "SET x = 1; SELECT 1"));
    CHECK_FALSE(is_setting_statement(resource, "/* unterminated SET x = 1"));
    CHECK_FALSE(is_setting_statement(resource, "DEALLOCATE ALL"));

    auto named = deallocate_statement(resource, "/**/ deallocate PREPARE s1");
    REQUIRE(named.has_value());
    CHECK(named->name == std::optional<std::string>{"s1"});
    auto all = deallocate_statement(resource, "DEALLOCATE ALL;");
    REQUIRE(all.has_value());
    CHECK_FALSE(all->name.has_value());
    CHECK_FALSE(deallocate_statement(resource, "SELECT 1").has_value());
    CHECK_FALSE(deallocate_statement(resource, "SET x = 1").has_value());
    CHECK_FALSE(deallocate_statement(resource, "DEALLOCATE s1; DEALLOCATE s2").has_value());

    using otterstax::session::is_transaction_exit_statement;
    for (const char* sql : {"COMMIT", "end", "ROLLBACK", " /* c */ abort;", "ROLLBACK TO SAVEPOINT sp"}) {
        INFO(sql);
        CHECK(is_transaction_exit_statement(resource, sql));
    }
    for (const char* sql : {"BEGIN", "SAVEPOINT sp", "SELECT 1", "COMMIT; SELECT 1", "endless"}) {
        INFO(sql);
        CHECK_FALSE(is_transaction_exit_statement(resource, sql));
    }
}
