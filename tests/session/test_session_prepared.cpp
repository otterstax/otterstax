// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The prepared-statement lifecycle of test_prepared_reexecute.cpp, through
// query_session_t: prepare_schema stores the statement under its statement id,
// one execute consumes it (success or failure), close_statement drops an entry
// that will never run and is idempotent, a direct execute of a statement with
// placeholders is refused. The map is the connection's own: a statement
// prepared on one connection is unknown on another.

#include "../system/session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>

using otterstax::test::close_session_statement;
using otterstax::test::engine_row_count;
using otterstax::test::execute_session_prepared;
using otterstax::test::execute_session_statement;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::seed_people_bulk;
using otterstax::test::session_stack;
using otterstax::test::session_stack_owner;
using otterstax::test::with_session_stack;

namespace {

    constexpr const char* kReprepare = "prepared statement must be re-prepared";

    std::pmr::vector<components::types::logical_value_t> no_parameters(const session_stack& s) {
        return std::pmr::vector<components::types::logical_value_t>{s.resource};
    }

    void require_reprepare(const core::result_wrapper_t<session_payload>& r) {
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::invalid_parameter);
        REQUIRE(std::string{r.error().what.c_str()} == kReprepare);
    }

} // namespace

TEST_CASE("session prepared: a successful execute consumes the entry") {
    with_session_stack("/tmp/test_session_prepared_success", [](session_stack s) {
        session_hash_t id = 9100;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());

        auto first = execute_session_statement(s, stmt);
        INFO("execute error: " << first.error().what.c_str());
        REQUIRE_FALSE(first.has_error());
        REQUIRE(first.value().size() == 3);
        REQUIRE(first.value().column_count() == 2);

        require_reprepare(execute_session_prepared(s, stmt, no_parameters(s)));
        require_reprepare(execute_session_statement(s, stmt));
    });
}

TEST_CASE("session prepared: a failed execute consumes the entry too") {
    with_session_stack("/tmp/test_session_prepared_failure", [](session_stack s) {
        session_hash_t id = 9200;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "INSERT INTO dmldb.nowhere (id) VALUES (1)");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());

        auto failed = execute_session_statement(s, stmt);
        REQUIRE(failed.has_error());
        require_reprepare(execute_session_prepared(s, stmt, no_parameters(s)));

        auto again = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        INFO("re-prepare error: " << again.error().what.c_str());
        REQUIRE_FALSE(again.has_error());
        auto rerun = execute_session_statement(s, stmt);
        INFO("re-execute error: " << rerun.error().what.c_str());
        REQUIRE_FALSE(rerun.has_error());
        REQUIRE(rerun.value().size() == 3);
    });
}

TEST_CASE("session prepared: execute of a statement that was never prepared") {
    with_session_stack("/tmp/test_session_prepared_unknown", [](session_stack s) {
        require_reprepare(execute_session_prepared(s, 9301, no_parameters(s)));
        require_reprepare(execute_session_statement(s, 9302));
    });
}

TEST_CASE("session prepared: parameters bind once then the statement must be re-prepared") {
    with_session_stack("/tmp/test_session_prepared_params", [](session_stack s) {
        session_hash_t id = 9400;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people WHERE id = $1");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == 1);

        std::pmr::vector<components::types::logical_value_t> params{s.resource};
        params.emplace_back(s.resource, std::int64_t{2});
        auto first = execute_session_prepared(s, stmt, std::move(params));
        INFO("execute error: " << first.error().what.c_str());
        REQUIRE_FALSE(first.has_error());
        REQUIRE(first.value().size() == 1);

        std::pmr::vector<components::types::logical_value_t> params_again{s.resource};
        params_again.emplace_back(s.resource, std::int64_t{3});
        require_reprepare(execute_session_prepared(s, stmt, std::move(params_again)));
    });
}

TEST_CASE("session prepared: too few parameters are refused with the binder's text") {
    with_session_stack("/tmp/test_session_prepared_missing", [](session_stack s) {
        session_hash_t id = 9450;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT id FROM dmldb.people WHERE id = $1 OR id = $2");
        REQUIRE_FALSE(prepared.has_error());
        std::pmr::vector<components::types::logical_value_t> params{s.resource};
        params.emplace_back(s.resource, std::int64_t{2});
        auto r = execute_session_prepared(s, stmt, std::move(params));
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::sql_parse_error);
        REQUIRE(std::string{r.error().what.c_str()} == "Argument binding failed: Not all parameters were bound: $2");
    });
}

TEST_CASE("session prepared: close drops the entry and a later execute must re-prepare") {
    with_session_stack("/tmp/test_session_prepared_close", [](session_stack s) {
        session_hash_t id = 9500;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());

        auto closed = close_session_statement(s, stmt);
        INFO("close error: " << closed.error().what.c_str());
        REQUIRE_FALSE(closed.has_error());
        REQUIRE(closed.value().column_count() == 0);
        REQUIRE(closed.value().empty());

        require_reprepare(execute_session_statement(s, stmt));
        require_reprepare(execute_session_prepared(s, stmt, no_parameters(s)));

        auto again = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        REQUIRE_FALSE(again.has_error());
        auto rerun = execute_session_statement(s, stmt);
        REQUIRE_FALSE(rerun.has_error());
        REQUIRE(rerun.value().size() == 3);
    });
}

TEST_CASE("session prepared: close is idempotent") {
    with_session_stack("/tmp/test_session_prepared_close_twice", [](session_stack s) {
        session_hash_t id = 9600;
        seed_people_bulk(s, id, 3);

        REQUIRE_FALSE(close_session_statement(s, 9699).has_error());

        const session_hash_t twice = id++;
        REQUIRE_FALSE(prepare_session_sql(s, twice, "SELECT id, name FROM dmldb.people").has_error());
        REQUIRE_FALSE(close_session_statement(s, twice).has_error());
        REQUIRE_FALSE(close_session_statement(s, twice).has_error());
        require_reprepare(execute_session_statement(s, twice));

        const session_hash_t consumed = id++;
        REQUIRE_FALSE(prepare_session_sql(s, consumed, "SELECT id, name FROM dmldb.people").has_error());
        REQUIRE_FALSE(execute_session_statement(s, consumed).has_error());
        REQUIRE_FALSE(close_session_statement(s, consumed).has_error());
        require_reprepare(execute_session_statement(s, consumed));
    });
}

TEST_CASE("session prepared: a parameterized statement closed unbound leaves no entry") {
    with_session_stack("/tmp/test_session_prepared_close_params", [](session_stack s) {
        session_hash_t id = 9800;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people WHERE id = $1");
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == 1);
        REQUIRE_FALSE(close_session_statement(s, stmt).has_error());

        std::pmr::vector<components::types::logical_value_t> params{s.resource};
        params.emplace_back(s.resource, std::int64_t{2});
        require_reprepare(execute_session_prepared(s, stmt, std::move(params)));
    });
}

TEST_CASE("session direct execute: a statement with placeholders and no Bind is refused") {
    with_session_stack("/tmp/test_session_direct_unbound", [](session_stack s) {
        session_hash_t id = 9900;
        seed_people_bulk(s, id, 3);

        for (const char* sql :
             {"INSERT INTO dmldb.people (id, name) VALUES ($1, $2);", "SELECT id, name FROM dmldb.people LIMIT $1;"}) {
            INFO(sql);
            auto r = run_session_sql_payload(s, id++, sql);
            REQUIRE(r.has_error());
            REQUIRE(r.error().type == core::error_code_t::invalid_parameter);
            REQUIRE(std::string{r.error().what.c_str()}.find("no values bound") != std::string::npos);
        }
        REQUIRE(engine_row_count(s, "dmldb", "people") == 3);
    });
}

TEST_CASE("session prepared: a statement prepared on one connection is unknown on another") {
    session_stack_owner owner("/tmp/test_session_prepared_isolation");
    const auto a = owner.stack();
    const auto b = a.on(owner.open_connection());
    session_hash_t id = 9950;
    seed_people_bulk(a, id, 3);

    // The same statement id on both connections: each has its own entry.
    const session_hash_t stmt = id++;
    REQUIRE_FALSE(prepare_session_sql(a, stmt, "SELECT id, name FROM dmldb.people").has_error());
    require_reprepare(execute_session_statement(b, stmt));

    auto on_a = execute_session_statement(a, stmt);
    INFO((on_a.has_error() ? on_a.error().what.c_str() : ""));
    REQUIRE_FALSE(on_a.has_error());
    REQUIRE(on_a.value().size() == 3);
}

TEST_CASE("session prepared: RETURNING is described from the target relation") {
    with_session_stack("/tmp/test_session_prepared_returning", [](session_stack s) {
        session_hash_t id = 9970;
        seed_people_bulk(s, id, 3);
        auto prepared =
            prepare_session_sql(s, id++, "INSERT INTO dmldb.people (id, name) VALUES (10, 'x') RETURNING id, name");
        INFO((prepared.has_error() ? prepared.error().what.c_str() : ""));
        REQUIRE_FALSE(prepared.has_error());
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 2);
        CHECK(schema.child_types()[0].alias() == "id");
        CHECK(schema.child_types()[0].type() == components::types::logical_type::BIGINT);
        CHECK(schema.child_types()[1].alias() == "name");
    });
}
