// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#include "scheduler_stack.hpp"
#include "frontend/common/sql_session.hpp"
#include "tests/temporary_directory.hpp"

namespace {
    std::atomic<bool> delayed_entered{false};
    std::atomic<bool> delayed_release{false};

    class delayed_parser final : public IParser {
    public:
        delayed_parser(std::pmr::memory_resource* resource, const otterstax::names::alias_registry_t& aliases)
            : parser_(make_parser(resource, aliases)) {}
        core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
            if (sql.find("/* delayed cleanup */") != std::string::npos) {
                delayed_entered.store(true);
                while (!delayed_release.load()) std::this_thread::yield();
            }
            return parser_->parse(sql);
        }
    private:
        parser_ptr parser_;
    };

    parser_ptr make_delayed_parser(std::pmr::memory_resource* resource,
                                  const otterstax::names::alias_registry_t& aliases) {
        return std::make_unique<delayed_parser>(resource, aliases);
    }

    class reported_error_parser final : public IParser {
    public:
        reported_error_parser(std::pmr::memory_resource* resource, const otterstax::names::alias_registry_t& aliases)
            : resource_(resource), parser_(make_parser(resource, aliases)) {}
        core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
            if (sql.find("/* reported io */") != std::string::npos)
                return core::error_t{core::error_code_t::io_error, std::pmr::string{"reported storage failure", resource_}};
            if (sql.find("/* reported transaction */") != std::string::npos)
                return core::error_t{core::error_code_t::transaction_inactive, std::pmr::string{"reported inactive transaction", resource_}};
            return parser_->parse(sql);
        }
    private:
        std::pmr::memory_resource* resource_;
        parser_ptr parser_;
    };

    parser_ptr make_reported_error_parser(std::pmr::memory_resource* resource,
                                         const otterstax::names::alias_registry_t& aliases) {
        return std::make_unique<reported_error_parser>(resource, aliases);
    }

    struct fixture {
        otterstax::test::temporary_directory directory{"native-sql-session"};
        otterstax::test::scheduler_stack_owner owner{directory.path.string(), &make_parser};
        otterstax::test::scheduler_stack stack{owner.stack()};
        frontend::sql_session writer{stack.resource, stack.scheduler, true};
        frontend::sql_session observer{stack.resource, stack.scheduler, false};
        fixture() {
            run(writer, "CREATE DATABASE txn;");
            run(writer, "CREATE TABLE txn.items (id BIGINT);");
        }
        ~fixture() { writer.close(); observer.close(); }
        session_payload run(frontend::sql_session& session, const std::string& sql) {
            auto value = session.execute(next_request_id(), sql);
            INFO(sql);
            INFO((value.has_error() ? value.error().what.c_str() : "success"));
            REQUIRE_FALSE(value.has_error());
            return std::move(value.value());
        }
        size_t rows(frontend::sql_session& session) { return run(session, "SELECT * FROM txn.items;").size(); }
    };
}

TEST_CASE("native SQL query errors cannot impersonate an expired or broken actor future", "[transactions]") {
    otterstax::test::temporary_directory directory{"reported-query-errors"};
    otterstax::test::scheduler_stack_owner owner(directory.path.string(), &make_reported_error_parser);
    const auto stack = owner.stack();
    for (bool mysql : {false, true}) {
        frontend::sql_session session(stack.resource, stack.scheduler, mysql);
        for (const auto* query : {"SELECT 1; /* reported io */", "SELECT 1; /* reported transaction */"}) {
            auto failed = session.execute(next_request_id(), query);
            REQUIRE(failed.has_error());
            CHECK_FALSE(session.broken());
            auto usable = session.execute(next_request_id(), "SELECT 42;");
            REQUIRE_FALSE(usable.has_error());
            CHECK(usable.value().size() == 1);

            REQUIRE_FALSE(session.execute(next_request_id(), "BEGIN;").has_error());
            failed = session.execute(next_request_id(), query);
            REQUIRE(failed.has_error());
            session.mark_failed(); // Native frontends call this when sending an execution error.
            CHECK(session.status() == frontend::sql_session::state::failed);
            CHECK_FALSE(session.broken());
            REQUIRE_FALSE(session.execute(next_request_id(), "ROLLBACK;").has_error());
            REQUIRE_FALSE(session.execute(next_request_id(), "SELECT 42;").has_error());
        }
        REQUIRE_FALSE(session.close().has_error());
    }
}

TEST_CASE("native SQL sessions retain engine transactions across independent request IDs", "[transactions]") {
    fixture f;
    CHECK(f.run(f.writer, "BEGIN;").transaction_command == "BEGIN");
    f.run(f.writer, "INSERT INTO txn.items VALUES (1);");
    CHECK(f.rows(f.writer) == 1);
    CHECK(f.rows(f.observer) == 0);
    f.run(f.writer, "ROLLBACK;");
    CHECK(f.rows(f.observer) == 0);
    f.run(f.writer, "BEGIN;");
    f.run(f.writer, "INSERT INTO txn.items VALUES (2);");
    f.run(f.writer, "COMMIT;");
    CHECK(f.rows(f.observer) == 1);
    CHECK_FALSE(f.writer.close().has_error());
}

TEST_CASE("native SQL implicit batches commit only when their boundary ends", "[transactions]") {
    fixture f;
    REQUIRE_FALSE(f.writer.begin_implicit().has_error());
    f.run(f.writer, "INSERT INTO txn.items VALUES (1);");
    CHECK(f.rows(f.writer) == 1);
    CHECK(f.rows(f.observer) == 0);
    REQUIRE_FALSE(f.writer.begin_implicit().has_error());
    f.run(f.writer, "INSERT INTO txn.items VALUES (2);");
    CHECK(f.rows(f.observer) == 0);
    REQUIRE_FALSE(f.writer.finish_implicit().has_error());
    CHECK(f.writer.status() == frontend::sql_session::state::idle);
    CHECK(f.rows(f.observer) == 2);
}

TEST_CASE("native SQL implicit batch failure rolls back before returning idle", "[transactions]") {
    fixture f;
    REQUIRE_FALSE(f.writer.begin_implicit().has_error());
    f.run(f.writer, "INSERT INTO txn.items VALUES (1);");
    auto failed = f.writer.execute(next_request_id(), "SELECT * FROM txn.missing;");
    REQUIRE(failed.has_error());
    f.writer.mark_failed();
    CHECK(f.writer.status() == frontend::sql_session::state::failed);
    REQUIRE_FALSE(f.writer.finish_implicit().has_error());
    CHECK(f.writer.status() == frontend::sql_session::state::idle);
    CHECK(f.rows(f.observer) == 0);
}

TEST_CASE("native SQL BEGIN promotes the whole implicit batch until explicit rollback", "[transactions]") {
    fixture f;
    REQUIRE_FALSE(f.writer.begin_implicit().has_error());
    f.run(f.writer, "INSERT INTO txn.items VALUES (1);");
    CHECK(f.run(f.writer, "BEGIN;").transaction_command == "BEGIN");
    REQUIRE_FALSE(f.writer.finish_implicit().has_error());
    CHECK(f.writer.status() == frontend::sql_session::state::active);
    CHECK(f.rows(f.observer) == 0);
    f.run(f.writer, "ROLLBACK;");
    CHECK(f.rows(f.observer) == 0);
}

TEST_CASE("native SQL prepared statements use the transaction active at execution", "[transactions]") {
    fixture f;
    const auto prepared = next_request_id();
    const std::string sql = "INSERT INTO txn.items (id) VALUES ($1);";
    REQUIRE_FALSE(f.writer.prepare(prepared, sql).has_error());
    f.run(f.writer, "BEGIN;");
    std::pmr::vector<components::types::logical_value_t> parameters(f.stack.resource);
    parameters.emplace_back(f.stack.resource, int64_t{17});
    auto inserted = f.writer.execute_prepared(prepared, sql, std::move(parameters));
    INFO((inserted.has_error() ? inserted.error().what.c_str() : "success"));
    REQUIRE_FALSE(inserted.has_error());
    CHECK(f.rows(f.writer) == 1);
    CHECK(f.rows(f.observer) == 0);
    f.run(f.writer, "ROLLBACK;");
    CHECK(f.rows(f.observer) == 0);
}

TEST_CASE("native SQL session rollback includes tables created and described in its transaction", "[transactions]") {
    fixture f;
    f.run(f.writer, "BEGIN;");
    f.run(f.writer, "CREATE TABLE txn.uncommitted (id BIGINT);");
    f.run(f.writer, "INSERT INTO txn.uncommitted (id) VALUES (9);");
    const std::string sql = "SELECT * FROM txn.uncommitted WHERE id = $1;";
    const auto prepared = next_request_id();
    auto schema = f.writer.prepare(prepared, sql);
    INFO((schema.has_error() ? schema.error().what.c_str() : "success"));
    REQUIRE_FALSE(schema.has_error());
    std::pmr::vector<components::types::logical_value_t> parameters(f.stack.resource);
    parameters.emplace_back(f.stack.resource, int64_t{9});
    auto selected = f.writer.execute_prepared(prepared, sql, std::move(parameters));
    INFO((selected.has_error() ? selected.error().what.c_str() : "success"));
    REQUIRE_FALSE(selected.has_error());
    CHECK(selected.value().size() == 1);
    f.run(f.writer, "ROLLBACK;");
    CHECK(f.observer.execute(next_request_id(), "SELECT * FROM txn.uncommitted;").has_error());
}

TEST_CASE("native SQL autocommit settings execute transactions and expose their state", "[transactions]") {
    fixture f;
    f.run(f.writer, "SET AUTOCOMMIT=0;");
    CHECK_FALSE(f.writer.autocommit());
    f.run(f.writer, "INSERT INTO txn.items VALUES (1);");
    CHECK(f.writer.status() == frontend::sql_session::state::active);
    CHECK(f.rows(f.observer) == 0);
    f.run(f.writer, "ROLLBACK;");
    CHECK(f.rows(f.observer) == 0);
    f.run(f.writer, "INSERT INTO txn.items VALUES (2);");
    f.run(f.writer, "SET AUTOCOMMIT=1;");
    CHECK(f.writer.autocommit());
    CHECK(f.writer.status() == frontend::sql_session::state::idle);
    CHECK(f.rows(f.observer) == 1);
    auto invalid = f.writer.execute(next_request_id(), "SET AUTOCOMMIT=7;");
    CHECK(invalid.has_error());
}

TEST_CASE("native SQL failed blocks cannot commit earlier changes or execute new writes", "[transactions]") {
    fixture f;
    f.run(f.writer, "BEGIN;");
    f.run(f.writer, "INSERT INTO txn.items VALUES (1);");
    auto bad = f.writer.execute(next_request_id(), "INSERT INTO txn.missing VALUES (2);");
    REQUIRE(bad.has_error());
    f.writer.mark_failed();
    REQUIRE_FALSE(f.writer.broken());
    CHECK(f.writer.status() == frontend::sql_session::state::failed);
    CHECK(f.writer.execute(next_request_id(), "INSERT INTO txn.items VALUES (3);").has_error());
    CHECK(f.run(f.writer, "COMMIT;").transaction_command == "ROLLBACK");
    CHECK(f.rows(f.observer) == 0);
}

TEST_CASE("native SQL sessions reject savepoints and external side effects inside transactions", "[transactions]") {
    fixture f;
    const auto sql = GENERATE("SAVEPOINT point;", "ROLLBACK TO point;", "RELEASE SAVEPOINT point;");
    f.run(f.writer, "BEGIN;");
    auto rejected = f.writer.execute(next_request_id(), sql);
    INFO(sql);
    REQUIRE(rejected.has_error());
    CHECK(rejected.error().type == core::error_code_t::unimplemented_yet);
    f.writer.mark_failed();
    CHECK(f.writer.status() == frontend::sql_session::state::failed);
    f.run(f.writer, "ROLLBACK;");
}

TEST_CASE("native SQL transaction rejects file export before creating a file", "[transactions]") {
    fixture f;
    const auto output = f.directory.path / "forbidden.csv";
    f.run(f.writer, "BEGIN;");
    auto rejected = f.writer.execute(next_request_id(),
        "COPY (SELECT * FROM txn.items) TO '" + output.string() + "' WITH (format = 'csv');");
    REQUIRE(rejected.has_error());
    CHECK(rejected.error().type == core::error_code_t::unimplemented_yet);
    CHECK_FALSE(std::filesystem::exists(output));
    f.writer.mark_failed();
    f.run(f.writer, "ROLLBACK;");
}

TEST_CASE("native SQL session close rolls back an uncommitted write", "[transactions]") {
    fixture f;
    f.run(f.writer, "BEGIN;");
    f.run(f.writer, "INSERT INTO txn.items VALUES (5);");
    CHECK_FALSE(f.writer.close().has_error());
    CHECK(f.rows(f.observer) == 0);
    CHECK_FALSE(f.writer.close().has_error());
}

TEST_CASE("native SQL failed blocks can end through a prepared ROLLBACK", "[transactions]") {
    fixture f;
    f.run(f.writer, "BEGIN;");
    f.run(f.writer, "INSERT INTO txn.items VALUES (5);");
    REQUIRE(f.writer.execute(next_request_id(), "SAVEPOINT unsupported;").has_error());
    f.writer.mark_failed();
    const auto statement = next_request_id();
    auto prepared = f.writer.prepare(statement, "ROLLBACK;");
    INFO((prepared.has_error() ? prepared.error().what.c_str() : "success"));
    REQUIRE_FALSE(prepared.has_error());
    auto rolled_back = f.writer.execute_prepared(statement, "ROLLBACK;", {});
    INFO((rolled_back.has_error() ? rolled_back.error().what.c_str() : "success"));
    REQUIRE_FALSE(rolled_back.has_error());
    CHECK(rolled_back.value().transaction_command == "ROLLBACK");
    CHECK(f.writer.status() == frontend::sql_session::state::idle);
    CHECK(f.rows(f.observer) == 0);
}

TEST_CASE("connection cleanup waits for a timed-out statement before rolling back", "[transactions]") {
    otterstax::test::temporary_directory directory{"transaction-close-pending"};
    otterstax::test::scheduler_stack_owner owner(directory.path.string(), &make_delayed_parser);
    const auto stack = owner.stack();
    execution_context context;
    context.persistent = true;
    auto execute = [&](const char* sql) {
        auto [scheduled, future] = actor_zeta::send(stack.scheduler, &Scheduler::execute, next_request_id(),
                                                    std::string{sql}, context);
        return otterstax::test::await_session(std::move(future), std::chrono::seconds(10), stack.resource);
    };
    REQUIRE_FALSE(execute("CREATE DATABASE pending;").has_error());
    REQUIRE_FALSE(execute("CREATE TABLE pending.items (id BIGINT);").has_error());
    REQUIRE_FALSE(execute("BEGIN;").has_error());
    context.in_transaction = true;
    delayed_entered.store(false);
    delayed_release.store(false);
    auto [scheduled, pending] = actor_zeta::send(stack.scheduler, &Scheduler::execute, next_request_id(),
                    std::string{"INSERT INTO pending.items VALUES (1); /* delayed cleanup */"}, context);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!delayed_entered.load() && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
    // Always release the parser before an assertion may unwind the owner.
    const bool entered = delayed_entered.load();
    auto timeout = otterstax::test::await_session(std::move(pending), std::chrono::milliseconds(5), stack.resource);
    auto [close_scheduled, close] = actor_zeta::send(stack.scheduler, &Scheduler::close_session, context);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const bool closed_before_statement = close.is_ready();
    delayed_release.store(true);
    auto cleaned = otterstax::test::await_session(std::move(close), std::chrono::seconds(10), stack.resource);
    REQUIRE(entered);
    REQUIRE(timeout.has_error());
    CHECK(timeout.error().type == otterstax::AWAIT_TIMEOUT_CODE);
    CHECK_FALSE(closed_before_statement);
    INFO((cleaned.has_error() ? cleaned.error().what.c_str() : "success"));
    REQUIRE_FALSE(cleaned.has_error());
    // Read through the SAME engine session: an abandoned active transaction
    // would still see its own row even though other sessions cannot see it.
    auto own = stack.otterbrix->dispatcher()->execute_sql(context.engine_session, "SELECT * FROM pending.items;");
    REQUIRE(own);
    REQUIRE_FALSE(own->is_error());
    CHECK(own->size() == 0);
    auto rows = otterstax::test::run_scheduler_sql_payload(stack, next_request_id(), "SELECT * FROM pending.items;");
    REQUIRE_FALSE(rows.has_error());
    CHECK(rows.value().size() == 0);
}
