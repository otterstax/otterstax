// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The registration of a table a statement missed (created on the backend after
// startup) on every route a statement takes to the engine: a direct execute, a
// prepare (prepare_schema, the describe every extended-protocol client and
// Flight SQL go through), a pre-built plan (execute_plan / prepare_plan, the
// Spark DataFrame) and the inner query of COPY ... TO. The table is registered
// whatever the transaction block (on the connection's registration session:
// the client's block is not touched); the statement is run once more only when
// its failure left the session outside a block. Also the inner query COPY ...
// TO takes: a query only.
//
// The PostgreSQL mock connection answers every probe with (id int4, name
// varchar) and every read with two rows, so any table name of its schema is a
// table "created on the backend" that the startup did not see.

#include "../system/session_stack.hpp"

#include "frontend/spark_connect_server/plan_translator/relation_to_plan.hpp"
#include "frontend/spark_connect_server/service.hpp"

#include <spark/connect/base.pb.h>
#include <spark/connect/relations.pb.h>

#include <catch2/catch_all.hpp>

#include <filesystem>
#include <fstream>
#include <string>

using otterstax::session::query_session_t;
using otterstax::test::await_session;
using otterstax::test::execute_session_prepared;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_or_fail;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::session_stack_owner;
using otterstax::test::stmt;

namespace {

    namespace sc = ::spark::connect;
    namespace ct = components::types;
    using status_t = query_session_t::transaction_status_t;

    otterstax::test::stack_options_t pg_options() {
        otterstax::test::stack_options_t options;
        options.connections.postgresql.push_back(config::PgConnectionDesc{.alias = "pg",
                                                                          .host = "localhost",
                                                                          .port = "5432",
                                                                          .username = "user",
                                                                          .password = "pass",
                                                                          .database = "pgdb",
                                                                          .schema = "public",
                                                                          .table = ""});
        return options;
    }

    // A table of the connection the startup did not register.
    std::string fresh(const std::string& table) { return "pg.pgdb.public." + table; }

    void require_rows(const core::result_wrapper_t<session_payload>& result, std::size_t rows) {
        INFO((result.has_error() ? result.error().what.c_str() : "ok"));
        REQUIRE_FALSE(result.has_error());
        CHECK(result.value().column_count() == 2);
        CHECK(result.value().reported_rows() == rows);
    }

    void require_id_name(const ct::complex_logical_type& schema) {
        REQUIRE(schema.type() == ct::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 2);
        CHECK(schema.child_types()[0].alias() == "id");
        CHECK(schema.child_types()[1].alias() == "name");
    }

    sc::Plan read_plan(const std::string& identifier) {
        sc::Plan plan;
        plan.mutable_root()->mutable_read()->mutable_named_table()->set_unparsed_identifier(identifier);
        return plan;
    }

    core::result_wrapper_t<session_payload> run_plan(const session_stack& s,
                                                      session_hash_t id,
                                                      const sc::Plan& plan,
                                                      bool prepare) {
        // What the ExecutePlan / AnalyzePlan RPC sends: the translation and its rebuild.
        auto translated = frontend::spark::translate_for_session(plan, s.resource);
        INFO("translate: " << (translated.has_error() ? translated.error().what.c_str() : "ok"));
        REQUIRE_FALSE(translated.has_error());
        auto [ns, fut] = prepare ? actor_zeta::send(s.session,
                                                    &query_session_t::prepare_plan,
                                                    stmt(id),
                                                    std::move(translated.value().data),
                                                    std::move(translated.value().rebuild))
                                 : actor_zeta::send(s.session,
                                                    &query_session_t::execute_plan,
                                                    stmt(id),
                                                    std::move(translated.value().data),
                                                    std::move(translated.value().rebuild));
        return await_session(std::move(fut), std::chrono::milliseconds(10000), s.resource);
    }

} // namespace

TEST_CASE("session registration: a direct execute registers the table and runs once more") {
    session_stack_owner owner("/tmp/test_session_registration_execute", pg_options());
    auto s = owner.stack();
    require_rows(run_session_sql_payload(s, 20000, "SELECT * FROM " + fresh("by_execute") + ";"), 2);
}

TEST_CASE("session registration: a prepare registers the table it missed") {
    session_stack_owner owner("/tmp/test_session_registration_prepare", pg_options());
    auto s = owner.stack();

    SECTION("prepare_schema, then execute") {
        auto prepared = prepare_session_sql(s, 20010, "SELECT id, name FROM " + fresh("by_prepare") + ";");
        INFO((prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        require_id_name(prepared.value().schema);
        require_rows(otterstax::test::execute_session_statement(s, 20010), 2);
    }
    SECTION("a statement with $1: prepared, then bound and run") {
        auto prepared = prepare_session_sql(s, 20011, "SELECT id, name FROM " + fresh("by_bind") + " WHERE id > $1;");
        INFO((prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        CHECK(prepared.value().parameter_count == 1);
        std::pmr::vector<components::vector::vector_t> values(s.resource);
        values.push_back(components::vector::make_scalar(s.resource, int32_t{0}));
        auto ran = execute_session_prepared(s, 20011, std::move(values));
        INFO((ran.has_error() ? ran.error().what.c_str() : "ok"));
        REQUIRE_FALSE(ran.has_error());
    }
}

TEST_CASE("session registration: a DataFrame plan registers the table and is translated again") {
    session_stack_owner owner("/tmp/test_session_registration_plan", pg_options());
    auto s = owner.stack();

    SECTION("execute_plan (spark.table)") { require_rows(run_plan(s, 20020, read_plan(fresh("by_plan")), false), 2); }
    SECTION("prepare_plan (df.schema)") {
        auto described = run_plan(s, 20021, read_plan(fresh("by_schema")), true);
        INFO((described.has_error() ? described.error().what.c_str() : "ok"));
        REQUIRE_FALSE(described.has_error());
        require_id_name(described.value().schema);
    }
    SECTION("a plan without a rebuild registers the table; the next plan finds it") {
        auto translated = frontend::spark::relation_to_plan(read_plan(fresh("no_rebuild")), s.resource);
        REQUIRE_FALSE(translated.has_error());
        auto [ns, fut] = actor_zeta::send(s.session,
                                          &query_session_t::execute_plan,
                                          stmt(20022),
                                          std::move(translated.value().parsed_data),
                                          query_session_t::rebuild_fn{});
        auto first = await_session(std::move(fut), std::chrono::milliseconds(10000), s.resource);
        REQUIRE(first.has_error());
        require_rows(run_plan(s, 20023, read_plan(fresh("no_rebuild")), false), 2);
    }
}

TEST_CASE("session registration: inside a block the table is registered and the statement is not run again") {
    session_stack_owner owner("/tmp/test_session_registration_block", pg_options());
    auto s = owner.stack();
    auto& session = owner.default_session();

    SECTION("a SELECT fails the block; after ROLLBACK the table answers") {
        run_session_or_fail(s, 20030, "BEGIN;");
        auto missed = run_session_sql_payload(s, 20031, "SELECT * FROM " + fresh("in_block") + ";");
        REQUIRE(missed.has_error());
        CHECK(session.transaction_status() == status_t::failed);
        run_session_or_fail(s, 20032, "ROLLBACK;");
        require_rows(run_session_sql_payload(s, 20033, "SELECT * FROM " + fresh("in_block") + ";"), 2);
    }
    SECTION("the next block sees the table") {
        run_session_or_fail(s, 20040, "BEGIN;");
        REQUIRE(run_session_sql_payload(s, 20041, "SELECT * FROM " + fresh("next_block") + ";").has_error());
        run_session_or_fail(s, 20042, "ROLLBACK;");
        run_session_or_fail(s, 20043, "BEGIN;");
        require_rows(run_session_sql_payload(s, 20044, "SELECT * FROM " + fresh("next_block") + ";"), 2);
        CHECK(session.transaction_status() == status_t::in_transaction);
        run_session_or_fail(s, 20045, "COMMIT;");
    }
    SECTION("a prepare inside a block") {
        run_session_or_fail(s, 20050, "BEGIN;");
        REQUIRE(prepare_session_sql(s, 20051, "SELECT * FROM " + fresh("prepare_in_block") + ";").has_error());
        CHECK(session.transaction_status() == status_t::failed);
        run_session_or_fail(s, 20052, "ROLLBACK;");
        auto prepared = prepare_session_sql(s, 20053, "SELECT * FROM " + fresh("prepare_in_block") + ";");
        INFO((prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        require_id_name(prepared.value().schema);
    }
}

TEST_CASE("session registration: the inner query of COPY ... TO registers the table it missed") {
    session_stack_owner owner("/tmp/test_session_registration_copy", pg_options());
    auto s = owner.stack();
    const std::string out = "/tmp/test_session_registration_copy.csv";
    std::filesystem::remove(out);
    run_session_or_fail(s,
                        20060,
                        "COPY (SELECT * FROM " + fresh("by_copy") + ") TO '" + out + "' WITH (format = 'csv')");
    std::ifstream file(out);
    std::string line;
    std::size_t lines = 0;
    while (std::getline(file, line)) {
        ++lines;
    }
    CHECK(lines == 3); // the header and the two rows
    std::filesystem::remove(out);
}

TEST_CASE("session COPY ... TO: the inner statement must be a query") {
    session_stack_owner owner("/tmp/test_session_copy_inner_query", pg_options());
    auto s = owner.stack();
    auto& session = owner.default_session();
    const std::string out = "/tmp/test_session_copy_inner_query.csv";
    std::filesystem::remove(out);
    run_session_or_fail(s, 20070, "CREATE SCHEMA keep;");
    run_session_or_fail(s, 20071, "CREATE TABLE keep.t (id bigint);");
    run_session_or_fail(s, 20072, "INSERT INTO keep.t (id) VALUES (1), (2);");

    for (const std::string inner : {"DROP DATABASE federation", "BEGIN", "DELETE FROM keep.t",
                                    "INSERT INTO keep.t (id) VALUES (3)", "CREATE TABLE keep.u (id bigint)"}) {
        auto refused =
            run_session_sql_payload(s, 20073, "COPY (" + inner + ") TO '" + out + "' WITH (format = 'csv')");
        INFO(inner);
        REQUIRE(refused.has_error());
        CHECK(refused.error().type == core::error_code_t::invalid_parameter);
        CHECK(session.transaction_status() == status_t::idle);
        CHECK_FALSE(std::filesystem::exists(out));
    }
    auto count = run_session_sql_payload(s, 20074, "SELECT count(*) FROM keep.t;");
    REQUIRE_FALSE(count.has_error());
    CHECK(count.value().chunks.front().data[0].get_value<uint64_t>(0) == 2);
    REQUIRE_FALSE(run_session_sql_payload(s, 20075, "SELECT * FROM federation.public.remote_names;").has_error());
    REQUIRE(run_session_sql_payload(s, 20076, "SELECT * FROM keep.u;").has_error());

    // A query still copies: SELECT and VALUES.
    run_session_or_fail(s, 20077, "COPY (SELECT id FROM keep.t) TO '" + out + "' WITH (format = 'csv')");
    std::filesystem::remove(out);
    run_session_or_fail(s, 20078, "COPY (VALUES (1), (2)) TO '" + out + "' WITH (format = 'csv')");
    std::filesystem::remove(out);
}

// `t` and `s.t` take the session's place: in a place that is a connection's
// database (SET search_path = 'c.d.s'), the table a short name missed is the
// connection's, registered and read by the run once more.
TEST_CASE("session registration: a short name in a connection's place registers the table") {
    session_stack_owner owner("/tmp/test_session_registration_short", pg_options());
    auto s = owner.stack();
    run_session_or_fail(s, 20090, "SET search_path = 'pg.pgdb.public';");
    require_rows(run_session_sql_payload(s, 20091, "SELECT * FROM short_name;"), 2);
    require_rows(run_session_sql_payload(s, 20092, "SELECT * FROM public.two_parts;"), 2);
    // A local place: the name is a local table's, nothing is registered for it.
    run_session_or_fail(s, 20093, "SET search_path = 'main.public';");
    auto local = run_session_sql_payload(s, 20094, "SELECT * FROM not_here;");
    REQUIRE(local.has_error());
}
