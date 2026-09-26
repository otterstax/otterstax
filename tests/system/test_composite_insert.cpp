// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Engine defect E1: a composite (CREATE TYPE ... AS (...)) value written with
// a NULL member — ROW(..., NULL) — through INSERT/UPDATE. The demo's step_3b
// writes exactly this shape into the nine-field spec_t.
//
// Every statement goes through the Scheduler→Worker stack, the way a frontend
// drives it; the read-back is the payload the frontend would encode. The NULL
// member is checked on the value itself: a struct column reads back as a
// logical_value_t whose children() are the members.

#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>

#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <initializer_list>
#include <string>
#include <string_view>

using otterstax::test::run_scheduler_sql;
using otterstax::test::run_scheduler_sql_payload;
using otterstax::test::scheduler_stack;
using otterstax::test::with_scheduler_stack;

namespace {

    void run_all(const scheduler_stack& s, session_hash_t& id, std::initializer_list<const char*> statements) {
        std::string err;
        for (const char* sql : statements) {
            const bool ok = run_scheduler_sql(s, id++, sql, err);
            INFO("statement: " << sql << " error: " << err);
            REQUIRE(ok);
        }
    }

    // Result of a SELECT as one payload; a failed statement is a test failure.
    session_payload select_payload(const scheduler_stack& s, session_hash_t& id, const std::string& sql) {
        auto r = run_scheduler_sql_payload(s, id++, sql);
        INFO("statement: " << sql << " error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        return std::move(r.value());
    }

    // The single row a one-row SELECT returns.
    const components::vector::data_chunk_t& single_row(const session_payload& payload) {
        REQUIRE(payload.size() == 1);
        REQUIRE_FALSE(payload.chunks.empty());
        return payload.chunks.front();
    }

    // Three-member composite over a table with a trailing plain column.
    constexpr std::initializer_list<const char*> k_trio_schema = {
        "CREATE DATABASE cdb;",
        "CREATE TYPE trio_t AS (a INT, b STRING, c STRING);",
        "CREATE TABLE cdb.t (id BIGINT, v trio_t, note STRING);",
    };

} // namespace

TEST_CASE("E1: INSERT VALUES a ROW with a NULL last member into a three-member composite reads it back",
          "[engine-defect-e1]") {
    with_scheduler_stack("/tmp/test_composite_insert_row_null", [](scheduler_stack s) {
        session_hash_t id = 9900;
        run_all(s, id, k_trio_schema);
        run_all(s, id, {"INSERT INTO cdb.t (id, v, note) VALUES (1, ROW(1, 'a', NULL), 'n1');"});

        const auto payload = select_payload(s, id, "SELECT (v).a, (v).b, (v).c, note FROM cdb.t;");
        const auto& row = single_row(payload);
        REQUIRE(row.column_count() == 4);
        REQUIRE(row.value(0, 0).value<int32_t>() == 1);
        REQUIRE(row.value(1, 0).value<std::string_view>() == "a");
        REQUIRE(row.value(2, 0).is_null());
        REQUIRE(row.value(3, 0).value<std::string_view>() == "n1");
    });
}

TEST_CASE("E1: INSERT with a partial column list writes the composite and leaves the omitted column NULL",
          "[engine-defect-e1]") {
    with_scheduler_stack("/tmp/test_composite_insert_partial_columns", [](scheduler_stack s) {
        session_hash_t id = 9920;
        run_all(s, id, k_trio_schema);
        run_all(s, id, {"INSERT INTO cdb.t (id, v) VALUES (2, ROW(2, 'b', NULL));"});

        const auto payload = select_payload(s, id, "SELECT id, v, note FROM cdb.t;");
        const auto& row = single_row(payload);
        REQUIRE(row.column_count() == 3);
        REQUIRE(row.value(0, 0).value<int64_t>() == 2);
        const auto v = row.value(1, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 2);
        REQUIRE(v.children()[1].value<std::string_view>() == "b");
        REQUIRE(v.children()[2].is_null());
        REQUIRE(row.value(2, 0).is_null());
    });
}

TEST_CASE("E1: INSERT ... SELECT copies a composite whose member is NULL", "[engine-defect-e1]") {
    with_scheduler_stack("/tmp/test_composite_insert_insert_select", [](scheduler_stack s) {
        session_hash_t id = 9940;
        run_all(s, id, k_trio_schema);
        run_all(s,
                id,
                {"CREATE TABLE cdb.dst (id BIGINT, v trio_t);",
                 "INSERT INTO cdb.t (id, v, note) VALUES (3, ROW(3, 'c', NULL), 'src');",
                 "INSERT INTO cdb.dst (id, v) SELECT id, v FROM cdb.t;"});

        const auto payload = select_payload(s, id, "SELECT id, v FROM cdb.dst;");
        const auto& row = single_row(payload);
        REQUIRE(row.value(0, 0).value<int64_t>() == 3);
        const auto v = row.value(1, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 3);
        REQUIRE(v.children()[1].value<std::string_view>() == "c");
        REQUIRE(v.children()[2].is_null());
    });
}

TEST_CASE("E1: a nested ROW with a NULL member in the inner ROW keeps that NULL member", "[engine-defect-e1]") {
    with_scheduler_stack("/tmp/test_composite_insert_nested", [](scheduler_stack s) {
        session_hash_t id = 9960;
        run_all(s,
                id,
                {"CREATE DATABASE cdb;",
                 "CREATE TYPE inner_t AS (p INT, q STRING);",
                 "CREATE TYPE outer_t AS (i inner_t, label STRING);",
                 "CREATE TABLE cdb.n (id BIGINT, v outer_t);",
                 "INSERT INTO cdb.n (id, v) VALUES (4, ROW(ROW(1, NULL), 'x'));"});

        const auto payload = select_payload(s, id, "SELECT v FROM cdb.n;");
        const auto& row = single_row(payload);
        const auto v = row.value(0, 0);
        REQUIRE(v.children().size() == 2);
        const auto& inner = v.children()[0];
        REQUIRE(inner.children().size() == 2);
        REQUIRE(inner.children()[0].value<int32_t>() == 1);
        REQUIRE(inner.children()[1].is_null());
        REQUIRE(v.children()[1].value<std::string_view>() == "x");
    });
}

TEST_CASE("E1: a composite column with a ROW(...) DEFAULT is filled in when omitted from the INSERT",
          "[engine-defect-e1]") {
    with_scheduler_stack("/tmp/test_composite_insert_default", [](scheduler_stack s) {
        session_hash_t id = 9980;
        run_all(s,
                id,
                {"CREATE DATABASE cdb;",
                 "CREATE TYPE trio_t AS (a INT, b STRING, c STRING);",
                 "CREATE TABLE cdb.d (id BIGINT, v trio_t DEFAULT ROW(0, 'd', NULL));",
                 "INSERT INTO cdb.d (id) VALUES (5);"});

        const auto payload = select_payload(s, id, "SELECT id, v FROM cdb.d;");
        const auto& row = single_row(payload);
        REQUIRE(row.value(0, 0).value<int64_t>() == 5);
        const auto v = row.value(1, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 0);
        REQUIRE(v.children()[1].value<std::string_view>() == "d");
        REQUIRE(v.children()[2].is_null());
    });
}

TEST_CASE("E1: UPDATE SET c = a two-member ROW with a NULL member replaces the composite", "[engine-defect-e1]") {
    with_scheduler_stack("/tmp/test_composite_insert_update", [](scheduler_stack s) {
        session_hash_t id = 10000;
        run_all(s,
                id,
                {"CREATE DATABASE cdb;",
                 "CREATE TYPE pair_t AS (a INT, b STRING);",
                 "CREATE TABLE cdb.u (id BIGINT, c pair_t);",
                 "INSERT INTO cdb.u (id, c) VALUES (6, ROW(1, 'x'));"});

        auto upd = run_scheduler_sql_payload(s, id++, "UPDATE cdb.u SET c = ROW(2, NULL) WHERE id = 6;");
        INFO("UPDATE error: " << upd.error().what.c_str());
        REQUIRE_FALSE(upd.has_error());
        REQUIRE(upd.value().size() == 1);

        const auto payload = select_payload(s, id, "SELECT c FROM cdb.u WHERE id = 6;");
        const auto& row = single_row(payload);
        const auto c = row.value(0, 0);
        REQUIRE(c.children().size() == 2);
        REQUIRE(c.children()[0].value<int32_t>() == 2);
        REQUIRE(c.children()[1].is_null());
    });
}

// examples/demo/sql/step_3a_ddl.sql + step_3b_insert.sql + step_3c_select.sql.
TEST_CASE("E1: demo step_3b — nine-member spec_t with a NULL photo in three rows — step_3c reads TLV-1 back",
          "[engine-defect-e1]") {
    with_scheduler_stack("/tmp/test_composite_insert_demo_spec", [](scheduler_stack s) {
        session_hash_t id = 10020;
        run_all(s,
                id,
                {"CREATE DATABASE otter;",
                 "CREATE TYPE tier_t AS ENUM('bronze','silver','gold');",
                 "CREATE TYPE address_t AS (city STRING, country STRING, zip STRING);",
                 "CREATE TYPE spec_t AS (weight_1 INT, weight_2 INT, weight_3 INT, weight_4 INT, "
                 "weight_5 INT, weight_6 INT, primary_barcode STRING, secondary_barcode STRING, photo STRING);",
                 "CREATE TABLE otter.warehouses (warehouse_id STRING, code STRING, tier tier_t, "
                 "location address_t, spec spec_t, priority_high BIGINT, priority_med BIGINT, priority_low BIGINT);",
                 "INSERT INTO otter.warehouses "
                 "(warehouse_id, code, tier, location, spec, priority_high, priority_med, priority_low) VALUES "
                 "('11111111-1111-1111-1111-111111111111', 'TLV-1', 'gold', ROW('Tel Aviv', 'IL', '6701101'), "
                 "ROW(10, 20, 30, 15, 25, 35, 'BC-001', 'BC-002', NULL), 1, 2, 3), "
                 "('22222222-2222-2222-2222-222222222222', 'BER-1', 'silver', ROW('Berlin', 'DE', '10115'), "
                 "ROW(5, 10, 15, 20, 25, 30, 'BC-003', 'BC-004', NULL), 2, 3, 4), "
                 "('33333333-3333-3333-3333-333333333333', 'NYC-1', 'gold', ROW('New York', 'US', '10001'), "
                 "ROW(1, 2, 3, 4, 5, 6, 'BC-005', 'BC-006', NULL), 1, 1, 1);"});

        const auto all = select_payload(s, id, "SELECT warehouse_id FROM otter.warehouses;");
        REQUIRE(all.size() == 3);

        const auto payload = select_payload(s,
                                            id,
                                            "SELECT warehouse_id, (location).city, (spec).weight_2, "
                                            "(spec).primary_barcode, priority_med FROM otter.warehouses "
                                            "WHERE (location).country = 'IL' AND (spec).photo IS NULL;");
        const auto& row = single_row(payload);
        REQUIRE(row.column_count() == 5);
        REQUIRE(row.value(0, 0).value<std::string_view>() == "11111111-1111-1111-1111-111111111111");
        REQUIRE(row.value(1, 0).value<std::string_view>() == "Tel Aviv");
        REQUIRE(row.value(2, 0).value<int32_t>() == 20);
        REQUIRE(row.value(3, 0).value<std::string_view>() == "BC-001");
        REQUIRE(row.value(4, 0).value<int64_t>() == 2);
    });
}
