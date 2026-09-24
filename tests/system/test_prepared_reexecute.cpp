// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Lifecycle of a prepared statement inside the Worker, driven the way the
// extended-protocol frontends drive it: prepare_schema stores the statement,
// one execute consumes it.
//
// The stored entry is single-use. Executing it hands the plan to the engine and
// finalizes the binder, so a second run over the same entry is never valid: on
// success and on failure alike the Worker drops the entry, and any later
// execute on that session answers invalid_parameter with a fixed message that
// tells the frontend to prepare again. A session that was never prepared is
// reported the same way — the Worker never dereferences a missing entry.
//
// close_statement is the frontend's way to drop an entry it will never
// execute (COM_STMT_CLOSE, Close, a refused FlightInfo): the entry is erased
// and a later execute answers the same re-prepare error. Closing is
// idempotent — a consumed, already closed or never prepared session closes
// with success.

#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>

using otterstax::test::close_scheduler_statement;
using otterstax::test::execute_scheduler_prepared;
using otterstax::test::execute_scheduler_statement;
using otterstax::test::prepare_scheduler_sql;
using otterstax::test::scheduler_stack;
using otterstax::test::seed_people_bulk;
using otterstax::test::with_scheduler_stack;

namespace {

    constexpr const char* kReprepare = "prepared statement must be re-prepared";

    std::pmr::vector<components::types::logical_value_t> no_parameters(const scheduler_stack& s) {
        return std::pmr::vector<components::types::logical_value_t>{s.resource};
    }

    void require_reprepare(const core::result_wrapper_t<session_payload>& r) {
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::invalid_parameter);
        REQUIRE(std::string{r.error().what.c_str()} == kReprepare);
    }

} // namespace

TEST_CASE("prepared statement: a successful execute consumes the entry") {
    with_scheduler_stack("/tmp/test_prepared_reexec_success", [](scheduler_stack s) {
        session_hash_t id = 9100;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_scheduler_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());

        auto first = execute_scheduler_statement(s, stmt);
        INFO("execute error: " << first.error().what.c_str());
        REQUIRE_FALSE(first.has_error());
        REQUIRE(first.value().size() == 3);
        REQUIRE(first.value().column_count() == 2);

        // Both execute forms see the same consumed entry.
        require_reprepare(execute_scheduler_prepared(s, stmt, no_parameters(s)));
        require_reprepare(execute_scheduler_statement(s, stmt));
    });
}

TEST_CASE("prepared statement: a failed execute consumes the entry too") {
    with_scheduler_stack("/tmp/test_prepared_reexec_failure", [](scheduler_stack s) {
        session_hash_t id = 9200;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        // Only a row-producing statement is resolved against the engine at
        // prepare time; a DML has no result schema and is stored as parsed,
        // so its missing table surfaces only when the engine runs it.
        auto prepared = prepare_scheduler_sql(s, stmt, "INSERT INTO dmldb.nowhere (id) VALUES (1)");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());

        auto failed = execute_scheduler_statement(s, stmt);
        REQUIRE(failed.has_error());

        // The entry was consumed by the failed run: a retry must not touch it.
        require_reprepare(execute_scheduler_prepared(s, stmt, no_parameters(s)));

        // The session itself stays usable after a fresh prepare.
        auto again = prepare_scheduler_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        INFO("re-prepare error: " << again.error().what.c_str());
        REQUIRE_FALSE(again.has_error());
        auto rerun = execute_scheduler_statement(s, stmt);
        INFO("re-execute error: " << rerun.error().what.c_str());
        REQUIRE_FALSE(rerun.has_error());
        REQUIRE(rerun.value().size() == 3);
    });
}

TEST_CASE("prepared statement: execute on a session that was never prepared") {
    with_scheduler_stack("/tmp/test_prepared_reexec_unknown", [](scheduler_stack s) {
        require_reprepare(execute_scheduler_prepared(s, 9301, no_parameters(s)));
        require_reprepare(execute_scheduler_statement(s, 9302));
    });
}

TEST_CASE("prepared statement: parameters bind once, then the statement must be re-prepared") {
    with_scheduler_stack("/tmp/test_prepared_reexec_params", [](scheduler_stack s) {
        session_hash_t id = 9400;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_scheduler_sql(s, stmt, "SELECT id, name FROM dmldb.people WHERE id = $1");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == 1);

        std::pmr::vector<components::types::logical_value_t> params{s.resource};
        params.emplace_back(s.resource, std::int64_t{2});
        auto first = execute_scheduler_prepared(s, stmt, std::move(params));
        INFO("execute error: " << first.error().what.c_str());
        REQUIRE_FALSE(first.has_error());
        REQUIRE(first.value().size() == 1);

        std::pmr::vector<components::types::logical_value_t> params_again{s.resource};
        params_again.emplace_back(s.resource, std::int64_t{3});
        require_reprepare(execute_scheduler_prepared(s, stmt, std::move(params_again)));
    });
}

TEST_CASE("prepared statement: close drops the entry, a later execute must re-prepare") {
    with_scheduler_stack("/tmp/test_prepared_close_execute", [](scheduler_stack s) {
        session_hash_t id = 9500;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_scheduler_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());

        auto closed = close_scheduler_statement(s, stmt);
        INFO("close error: " << closed.error().what.c_str());
        REQUIRE_FALSE(closed.has_error());
        REQUIRE(closed.value().column_count() == 0);
        REQUIRE(closed.value().empty());

        // The closed entry is gone for both execute forms.
        require_reprepare(execute_scheduler_statement(s, stmt));
        require_reprepare(execute_scheduler_prepared(s, stmt, no_parameters(s)));

        // The session is reusable: a fresh prepare under the same id executes.
        auto again = prepare_scheduler_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        INFO("re-prepare error: " << again.error().what.c_str());
        REQUIRE_FALSE(again.has_error());
        auto rerun = execute_scheduler_statement(s, stmt);
        INFO("re-execute error: " << rerun.error().what.c_str());
        REQUIRE_FALSE(rerun.has_error());
        REQUIRE(rerun.value().size() == 3);
    });
}

TEST_CASE("prepared statement: close is idempotent") {
    with_scheduler_stack("/tmp/test_prepared_close_idempotent", [](scheduler_stack s) {
        session_hash_t id = 9600;
        seed_people_bulk(s, id, 3);

        // Never prepared.
        auto unknown = close_scheduler_statement(s, 9699);
        INFO("close unknown error: " << unknown.error().what.c_str());
        REQUIRE_FALSE(unknown.has_error());

        // Prepared, then closed twice.
        const session_hash_t twice = id++;
        auto prepared = prepare_scheduler_sql(s, twice, "SELECT id, name FROM dmldb.people");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE_FALSE(close_scheduler_statement(s, twice).has_error());
        REQUIRE_FALSE(close_scheduler_statement(s, twice).has_error());
        require_reprepare(execute_scheduler_statement(s, twice));

        // Prepared and consumed by an execute, then closed.
        const session_hash_t consumed = id++;
        auto prepared_consumed = prepare_scheduler_sql(s, consumed, "SELECT id, name FROM dmldb.people");
        INFO("prepare error: " << prepared_consumed.error().what.c_str());
        REQUIRE_FALSE(prepared_consumed.has_error());
        auto executed = execute_scheduler_statement(s, consumed);
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        REQUIRE_FALSE(close_scheduler_statement(s, consumed).has_error());
        require_reprepare(execute_scheduler_statement(s, consumed));
    });
}

TEST_CASE("prepared statement: a parameterized statement that is closed unbound leaves no entry") {
    with_scheduler_stack("/tmp/test_prepared_close_params", [](scheduler_stack s) {
        session_hash_t id = 9800;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        // The shape GetFlightInfoStatement refuses after prepare_schema (an
        // unbound parameter): the frontend closes it instead of leaving the
        // entry behind, and the binder is never touched.
        auto prepared = prepare_scheduler_sql(s, stmt, "SELECT id, name FROM dmldb.people WHERE id = $1");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == 1);

        REQUIRE_FALSE(close_scheduler_statement(s, stmt).has_error());

        std::pmr::vector<components::types::logical_value_t> params{s.resource};
        params.emplace_back(s.resource, std::int64_t{2});
        require_reprepare(execute_scheduler_prepared(s, stmt, std::move(params)));
    });
}
