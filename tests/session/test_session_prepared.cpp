// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The prepared-statement lifecycle through query_session_t: prepare_schema
// keeps the statement's text and the schema the engine described under its
// statement id; every execute parses the text again, binds its values and runs
// it, any number of times, until close_statement drops it (idempotent); a
// direct execute of a statement with placeholders is refused. The map is the
// connection's own: a statement prepared on one connection is unknown on
// another. A window (execute_window) answers rows a page at a time, read on
// with fetch_window and closed with close_window.

#include "../system/session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <components/logical_plan/node_cursor.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>

using otterstax::session::query_session_t;
using otterstax::test::close_session_statement;
using otterstax::test::engine_row_count;
using otterstax::test::execute_session_prepared;
using otterstax::test::execute_session_statement;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::seed_people_bulk;
using otterstax::test::session_stack;
using otterstax::test::session_stack_owner;
using otterstax::test::stmt;
using otterstax::test::with_session_stack;

namespace {

    constexpr const char* kUnknown = "prepared statement does not exist";

    std::pmr::vector<components::vector::vector_t> no_parameters(const session_stack& s) {
        return std::pmr::vector<components::vector::vector_t>{s.resource};
    }

    std::pmr::vector<components::vector::vector_t> one_parameter(const session_stack& s, std::int64_t value) {
        std::pmr::vector<components::vector::vector_t> params{s.resource};
        params.push_back(components::vector::make_scalar(s.resource, value));
        return params;
    }

    void require_unknown(const core::result_wrapper_t<session_payload>& r) {
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::invalid_parameter);
        REQUIRE(std::string{r.error().what.c_str()} == kUnknown);
    }

    core::result_wrapper_t<session_payload> send_and_await(const session_stack& s,
                                                           actor_zeta::unique_future<query_session_t::session_result> fut) {
        return otterstax::test::await_session(std::move(fut), std::chrono::milliseconds(10000), s.resource);
    }

} // namespace

TEST_CASE("session prepared: a statement runs any number of times") {
    with_session_stack("/tmp/test_session_prepared_success", [](session_stack s) {
        session_hash_t id = 9100;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());

        for (int run = 0; run < 3; ++run) {
            auto executed = execute_session_statement(s, stmt);
            INFO("execute error: " << executed.error().what.c_str());
            REQUIRE_FALSE(executed.has_error());
            REQUIRE(executed.value().size() == 3);
            REQUIRE(executed.value().column_count() == 2);
        }
        auto again = execute_session_prepared(s, stmt, no_parameters(s));
        REQUIRE_FALSE(again.has_error());
        REQUIRE(again.value().size() == 3);
    });
}

TEST_CASE("session prepared: a failed execute keeps the entry") {
    with_session_stack("/tmp/test_session_prepared_failure", [](session_stack s) {
        session_hash_t id = 9200;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        // Every execution resolves the names again: the table dropped fails
        // it, the table made again under the name is read.
        REQUIRE_FALSE(run_session_sql_payload(s, id++, "DROP TABLE dmldb.people;").has_error());
        auto failed = execute_session_statement(s, stmt);
        REQUIRE(failed.has_error());
        CHECK(failed.error().type == core::error_code_t::table_not_exists);
        REQUIRE_FALSE(
            run_session_sql_payload(s, id++, "CREATE TABLE dmldb.people (id bigint, name string);").has_error());
        REQUIRE_FALSE(run_session_sql_payload(s, id++, "INSERT INTO dmldb.people (id, name) VALUES (1, 'a');")
                          .has_error());
        auto rerun = execute_session_statement(s, stmt);
        INFO("re-execute error: " << rerun.error().what.c_str());
        REQUIRE_FALSE(rerun.has_error());
        REQUIRE(rerun.value().size() == 1);

        const session_hash_t missing = id++;
        REQUIRE(prepare_session_sql(s, missing, "INSERT INTO dmldb.nowhere (id) VALUES (1)").has_error());
        require_unknown(execute_session_statement(s, missing));
    });
}

TEST_CASE("session prepared: execute of a statement that was never prepared") {
    with_session_stack("/tmp/test_session_prepared_unknown", [](session_stack s) {
        require_unknown(execute_session_prepared(s, 9301, no_parameters(s)));
        require_unknown(execute_session_statement(s, 9302));
    });
}

TEST_CASE("session prepared: every execute binds its own values") {
    with_session_stack("/tmp/test_session_prepared_params", [](session_stack s) {
        session_hash_t id = 9400;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people WHERE id = $1");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == 1);
        // The engine types the parameter by its context.
        REQUIRE(prepared.value().parameter_types.size() == 1);
        CHECK(prepared.value().parameter_types[0].type() == components::types::logical_type::BIGINT);

        for (std::int64_t key : {2, 3, 1}) {
            auto executed = execute_session_prepared(s, stmt, one_parameter(s, key));
            INFO("execute error: " << executed.error().what.c_str());
            REQUIRE_FALSE(executed.has_error());
            REQUIRE(executed.value().size() == 1);
        }
    });
}

// A parameter in the select list (PostgreSQL's `SELECT $1 FROM t`, MySQL's
// `SELECT ? FROM t` once the frontend numbered it): the engine types it by its
// bound value and answers it once per row (fixed in
// otterbrix; its validator used to throw std::bad_variant_access over it).
TEST_CASE("session prepared: a select-list parameter answers its bound value once per row") {
    with_session_stack("/tmp/test_session_prepared_select_param", [](session_stack s) {
        session_hash_t id = 9420;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT $1 FROM dmldb.people");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == 1);

        for (std::int64_t value : {5, 100}) {
            auto executed = execute_session_prepared(s, stmt, one_parameter(s, value));
            INFO("execute error: " << executed.error().what.c_str());
            REQUIRE_FALSE(executed.has_error());
            REQUIRE(executed.value().size() == 3);
            REQUIRE(executed.value().column_count() == 1);
            for (const auto& chunk : executed.value().chunks) {
                if (chunk.size() == 0) {
                    continue;
                }
                // Typed by the bound value: a BIGINT.
                REQUIRE(chunk.data[0].type().type() == components::types::logical_type::BIGINT);
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    CHECK(chunk.data[0].get_value<std::int64_t>(row) == value);
                }
            }
        }
    });
}

TEST_CASE("session prepared: too few parameters are refused with the binder's text") {
    with_session_stack("/tmp/test_session_prepared_missing", [](session_stack s) {
        session_hash_t id = 9450;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        auto prepared = prepare_session_sql(s, stmt, "SELECT id FROM dmldb.people WHERE id = $1 OR id = $2");
        REQUIRE_FALSE(prepared.has_error());
        auto r = execute_session_prepared(s, stmt, one_parameter(s, 2));
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::sql_parse_error);
        REQUIRE(std::string{r.error().what.c_str()} == "Argument binding failed: Not all parameters were bound: $2");
        // The entry stays: the next execute with both values runs.
        std::pmr::vector<components::vector::vector_t> both{s.resource};
        both.push_back(components::vector::make_scalar(s.resource, std::int64_t{2}));
        both.push_back(components::vector::make_scalar(s.resource, std::int64_t{3}));
        auto rerun = execute_session_prepared(s, stmt, std::move(both));
        REQUIRE_FALSE(rerun.has_error());
        REQUIRE(rerun.value().size() == 2);
    });
}

TEST_CASE("session prepared: close drops the entry, idempotently") {
    with_session_stack("/tmp/test_session_prepared_close", [](session_stack s) {
        session_hash_t id = 9500;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;

        REQUIRE_FALSE(close_session_statement(s, 9599).has_error());
        auto prepared = prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people");
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE_FALSE(execute_session_statement(s, stmt).has_error());

        auto closed = close_session_statement(s, stmt);
        INFO("close error: " << closed.error().what.c_str());
        REQUIRE_FALSE(closed.has_error());
        REQUIRE(closed.value().column_count() == 0);
        REQUIRE(closed.value().empty());
        REQUIRE_FALSE(close_session_statement(s, stmt).has_error());

        require_unknown(execute_session_statement(s, stmt));
        require_unknown(execute_session_prepared(s, stmt, no_parameters(s)));
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
    require_unknown(execute_session_statement(b, stmt));

    auto on_a = execute_session_statement(a, stmt);
    INFO((on_a.has_error() ? on_a.error().what.c_str() : ""));
    REQUIRE_FALSE(on_a.has_error());
    REQUIRE(on_a.value().size() == 3);
}

TEST_CASE("session prepared: RETURNING is described by the engine") {
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
        // The describe wrote nothing.
        REQUIRE(engine_row_count(s, "dmldb", "people") == 3);
    });
}

TEST_CASE("session prepared: a describe with values bound types the parameters by them") {
    with_session_stack("/tmp/test_session_prepared_describe_bound", [](session_stack s) {
        session_hash_t id = 9980;
        seed_people_bulk(s, id, 3);
        const session_hash_t stmt = id++;
        REQUIRE_FALSE(prepare_session_sql(s, stmt, "SELECT id, name FROM dmldb.people WHERE id = $1").has_error());
        auto [ns, fut] = actor_zeta::send(s.session,
                                          &query_session_t::describe_bound,
                                          otterstax::test::stmt(stmt),
                                          otterstax::session::parameter_row(s.resource, one_parameter(s, 2)),
                                          uint64_t{0});
        auto described = send_and_await(s, std::move(fut));
        INFO((described.has_error() ? described.error().what.c_str() : ""));
        REQUIRE_FALSE(described.has_error());
        REQUIRE(described.value().schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(described.value().schema.child_types().size() == 2);
        // Nothing ran: no rows in the answer.
        CHECK(described.value().size() == 0);
    });
}

TEST_CASE("session prepared: a window answers a page at a time") {
    with_session_stack("/tmp/test_session_prepared_window", [](session_stack s) {
        session_hash_t id = 9990;
        seed_people_bulk(s, id, 5);
        const session_hash_t stmt = id++;
        REQUIRE_FALSE(prepare_session_sql(s, stmt, "SELECT id FROM dmldb.people ORDER BY id").has_error());

        auto [ns, fut] = actor_zeta::send(s.session,
                                          &query_session_t::execute_window,
                                          otterstax::test::stmt(stmt),
                                          otterstax::session::parameter_row(s.resource, no_parameters(s)),
                                          uint64_t{0},
                                          uint64_t{2});
        auto first = send_and_await(s, std::move(fut));
        INFO((first.has_error() ? first.error().what.c_str() : ""));
        REQUIRE_FALSE(first.has_error());
        REQUIRE(first.value().size() == 2);
        REQUIRE(first.value().has_more);
        const auto portal = first.value().portal;
        REQUIRE(portal != 0);

        auto [ns2, fut2] = actor_zeta::send(s.session, &query_session_t::fetch_window, portal, uint64_t{2});
        auto second = send_and_await(s, std::move(fut2));
        REQUIRE_FALSE(second.has_error());
        REQUIRE(second.value().size() == 2);

        auto [ns3, fut3] =
            actor_zeta::send(s.session, &query_session_t::fetch_window, portal, components::logical_plan::fetch_all);
        auto rest = send_and_await(s, std::move(fut3));
        REQUIRE_FALSE(rest.has_error());
        REQUIRE(rest.value().size() == 1);
        CHECK_FALSE(rest.value().has_more);
        CHECK(rest.value().portal == 0);

        // A window the client abandons is closed.
        auto [ns4, fut4] = actor_zeta::send(s.session,
                                            &query_session_t::execute_window,
                                            otterstax::test::stmt(stmt),
                                            otterstax::session::parameter_row(s.resource, no_parameters(s)),
                                            uint64_t{0},
                                            uint64_t{1});
        auto opened = send_and_await(s, std::move(fut4));
        REQUIRE_FALSE(opened.has_error());
        REQUIRE(opened.value().has_more);
        auto [ns5, fut5] = actor_zeta::send(s.session, &query_session_t::close_window, opened.value().portal);
        REQUIRE_FALSE(send_and_await(s, std::move(fut5)).has_error());
        auto [ns6, fut6] = actor_zeta::send(s.session, &query_session_t::fetch_window, opened.value().portal, uint64_t{1});
        CHECK(send_and_await(s, std::move(fut6)).has_error());
    });
}
