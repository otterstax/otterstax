// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Shape of the payload a ROW-PRODUCING statement returns — the counterpart of
// test_dml_result_shape.cpp.
//
// Every frontend tells a resultset from an affected-count by the payload's
// column count alone: zero columns is "N rows affected" (MySQL OK packet, PG
// CommandComplete without RowDescription). A SELECT / RETURNING whose result
// has no columns is therefore not a valid answer — it would be reported to the
// client as a DML statement — and the pipeline must return an error instead.
//
// That refusal lives in OtterbrixManager::execute; it is pinned directly, over a
// data manager that can be made to answer a column-less result, in
// test_otterbrix_manager.cpp.
//
// A `hugeint` column used to be the SQL shape that reached it: on rc-2 the engine
// catalog had no pg_type row for a 128-bit integer, the column was typed UNKNOWN,
// and `SELECT big FROM t` came back as a chunk without columns. rc-3 gives 128-bit
// integers their own pg_type rows (components/catalog/catalog_oids.hpp,
// int128_type / uint128_type), so that column resolves as HUGEINT and answers a
// normal one-column resultset on every route — which is what the cases below now
// record. A `uhugeint` column resolves just as well but has no Arrow carrier
// (decimal128 is signed), so the export path is where a column is still refused by
// name. `SELECT id` over the empty table keeps its one column, which is what lets
// the check tell "no columns" from "no rows".

#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>

#include <filesystem>
#include <string>
#include <string_view>

using otterstax::test::execute_scheduler_statement;
using otterstax::test::prepare_scheduler_sql;
using otterstax::test::run_scheduler_sql;
using otterstax::test::run_scheduler_sql_payload;
using otterstax::test::scheduler_stack;
using otterstax::test::with_scheduler_stack;

namespace {

    void run_or_fail(const scheduler_stack& s, session_hash_t id, const std::string& sql) {
        std::string err;
        const bool ok = run_scheduler_sql(s, id, sql, err);
        INFO(sql << ": " << err);
        REQUIRE(ok);
    }

    // hdb.huge (id INT, big hugeint), empty; hdb.plain (id bigint, name string), one row.
    void seed(const scheduler_stack& s, session_hash_t& id) {
        run_or_fail(s, id++, "CREATE DATABASE hdb;");
        run_or_fail(s, id++, "CREATE TABLE hdb.huge (id INT, big hugeint);");
        run_or_fail(s, id++, "CREATE TABLE hdb.plain (id bigint, name string);");
        run_or_fail(s, id++, "INSERT INTO hdb.plain (id, name) VALUES (1, 'one');");
    }

    bool mentions(const core::error_t& error, std::string_view text) {
        return std::string_view{error.what.c_str()}.find(text) != std::string_view::npos;
    }

} // namespace

TEST_CASE("a hugeint column answers a one-column resultset on every route", "[row-shape]") {
    with_scheduler_stack("/tmp/test_row_shape_select_hugeint", [](scheduler_stack s) {
        session_hash_t id = 9800;
        seed(s, id);

        SECTION("simple query route") {
            auto r = run_scheduler_sql_payload(s, id++, "SELECT big FROM hdb.huge;");
            INFO("error: " << r.error().what.c_str());
            REQUIRE_FALSE(r.has_error());
            // The table is empty: one column, no rows — which is a resultset, not
            // the column-less payload a frontend would report as a DML.
            REQUIRE(r.value().column_count() == 1);
            REQUIRE(r.value().size() == 0);
        }

        SECTION("prepared route: the prepare describes the column and the execute answers it") {
            const session_hash_t stmt = id++;
            auto prepared = prepare_scheduler_sql(s, stmt, "SELECT big FROM hdb.huge;");
            INFO("prepare error: " << prepared.error().what.c_str());
            REQUIRE_FALSE(prepared.has_error());
            REQUIRE(prepared.value().schema.type() == components::types::logical_type::STRUCT);
            REQUIRE(prepared.value().schema.child_types().size() == 1);
            REQUIRE(prepared.value().schema.child_types()[0].type() == components::types::logical_type::HUGEINT);

            auto executed = execute_scheduler_statement(s, stmt);
            INFO("error: " << executed.error().what.c_str());
            REQUIRE_FALSE(executed.has_error());
            REQUIRE(executed.value().column_count() == 1);
        }

        SECTION("COPY of such a SELECT writes the file") {
            const std::string path = "/tmp/test_row_shape_select_hugeint.csv";
            std::filesystem::remove(path);
            auto r = run_scheduler_sql_payload(s,
                                               id++,
                                               "COPY (SELECT big FROM hdb.huge) TO '" + path + "' WITH (format = 'csv');");
            INFO("error: " << r.error().what.c_str());
            REQUIRE_FALSE(r.has_error());
            REQUIRE(std::filesystem::exists(path));
        }
    });
}

TEST_CASE("a uhugeint column is refused by the export path, naming the column", "[row-shape]") {
    // The counterpart of the hugeint cases: rc-3 resolves this column too, so the
    // SELECT is a proper one-column resultset, but Arrow's only 128-bit carrier is
    // signed, so the writers refuse it rather than emitting a wrong value.
    with_scheduler_stack("/tmp/test_row_shape_select_uhugeint", [](scheduler_stack s) {
        session_hash_t id = 9850;
        run_or_fail(s, id++, "CREATE DATABASE udb;");
        run_or_fail(s, id++, "CREATE TABLE udb.uhuge (id INT, big uhugeint);");

        auto selected = run_scheduler_sql_payload(s, id++, "SELECT big FROM udb.uhuge;");
        INFO("SELECT error: " << selected.error().what.c_str());
        REQUIRE_FALSE(selected.has_error());
        REQUIRE(selected.value().column_count() == 1);

        const std::string path = "/tmp/test_row_shape_select_uhugeint.csv";
        std::filesystem::remove(path);
        auto r = run_scheduler_sql_payload(s,
                                           id++,
                                           "COPY (SELECT big FROM udb.uhuge) TO '" + path + "' WITH (format = 'csv');");
        REQUIRE(r.has_error());
        INFO("error: " << r.error().what.c_str());
        REQUIRE(mentions(r.error(), "big"));
        REQUIRE_FALSE(std::filesystem::exists(path));
    });
}

TEST_CASE("an empty SELECT result keeps its columns: no rows is not no columns", "[row-shape]") {
    // The premise of the check: the engine answers a SELECT over an empty table
    // with a column-typed, zero-row chunk, so the invariant separates the two.
    with_scheduler_stack("/tmp/test_row_shape_select_empty", [](scheduler_stack s) {
        session_hash_t id = 9900;
        seed(s, id);

        auto r = run_scheduler_sql_payload(s, id++, "SELECT id FROM hdb.huge;");
        INFO("SELECT error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().size() == 0);
        REQUIRE(r.value().column_count() == 1);

        auto whole = run_scheduler_sql_payload(s, id++, "SELECT * FROM hdb.plain WHERE id = 42;");
        INFO("SELECT error: " << whole.error().what.c_str());
        REQUIRE_FALSE(whole.has_error());
        REQUIRE(whole.value().size() == 0);
        REQUIRE(whole.value().column_count() == 2);
    });
}

TEST_CASE("a DML without RETURNING still answers its column-less affected count", "[row-shape]") {
    // Column-less by design: the count travels as the carrier's cardinality on
    // both routes, and the check must leave it alone.
    with_scheduler_stack("/tmp/test_row_shape_dml_count", [](scheduler_stack s) {
        session_hash_t id = 10000;
        seed(s, id);

        auto ins = run_scheduler_sql_payload(s, id++, "INSERT INTO hdb.plain (id, name) VALUES (2, 'two'), (3, 'three');");
        INFO("INSERT error: " << ins.error().what.c_str());
        REQUIRE_FALSE(ins.has_error());
        REQUIRE(ins.value().column_count() == 0);
        REQUIRE(ins.value().size() == 2);

        const session_hash_t stmt = id++;
        auto prepared = prepare_scheduler_sql(s, stmt, "DELETE FROM hdb.plain WHERE id = 2;");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        auto del = execute_scheduler_statement(s, stmt);
        INFO("DELETE error: " << del.error().what.c_str());
        REQUIRE_FALSE(del.has_error());
        REQUIRE(del.value().column_count() == 0);
        REQUIRE(del.value().size() == 1);

        auto none = run_scheduler_sql_payload(s, id++, "DELETE FROM hdb.plain WHERE id = 999;");
        INFO("DELETE error: " << none.error().what.c_str());
        REQUIRE_FALSE(none.has_error());
        REQUIRE(none.value().column_count() == 0);
        REQUIRE(none.value().size() == 0);
    });
}
