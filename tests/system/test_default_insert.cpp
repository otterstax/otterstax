// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// A column DEFAULT must be stored when the INSERT omits the column. The
// engine fills the omitted column at storage_append from the column's
// default_value(); OtterStax hands the CREATE TABLE and the INSERT to the
// engine unchanged, so the same statements are run twice here — straight
// against the engine (make_otterbrix_manager()->execute_sql) and through the
// Scheduler→Worker stack — and the two must agree.
//
// The visible cases pin the shapes the engine gets right: a DEFAULT whose
// literal type equals the column type (BIGINT/STRING/DOUBLE). The
// [engine-defect-default] cases pin the shapes that are easy to drop: a
// DEFAULT whose literal type differs from the declared column type (INT
// DEFAULT 7 — the literal is BIGINT; a ROW(...) DEFAULT — its members are
// literal-typed). A store that refuses a mistyped value without writing it
// leaves the row's freshly built, zeroed slot in place, so the column reads
// 0 / "" instead of the DEFAULT with no error anywhere; the engine-alone case
// is what tells an engine defect from an OtterStax one.

#include "scheduler_stack.hpp"

#include "otterbrix/config.hpp"
#include "otterbrix/operators/execute_plan.hpp"

#include <catch2/catch_all.hpp>

#include <components/cursor/cursor.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <filesystem>
#include <initializer_list>
#include <string>
#include <string_view>

using otterstax::test::run_scheduler_sql;
using otterstax::test::run_scheduler_sql_payload;
using otterstax::test::scheduler_stack;
using otterstax::test::with_scheduler_stack;

namespace {

    // The engine on its own: no parser, no Worker, no OtterbrixManager actor.
    struct engine_direct {
        db::otterbrix_engine_ptr engine;
        data_manager_ptr manager;

        explicit engine_direct(const char* data_dir) {
            std::filesystem::remove_all(data_dir);
            engine = db::make_otterbrix_engine(make_create_config(data_dir));
            manager = make_otterbrix_manager(engine);
        }

        void run_all(std::initializer_list<const char*> statements) {
            for (const char* sql : statements) {
                auto cursor = manager->execute_sql(sql);
                INFO("statement: " << sql << " error: "
                                   << (cursor && cursor->is_error() ? cursor->get_error().what.c_str() : ""));
                REQUIRE(cursor);
                REQUIRE_FALSE(cursor->is_error());
            }
        }

        // The single row a one-row SELECT returns, as the engine's own cursor.
        components::cursor::cursor_t_ptr select_one(const char* sql) {
            auto cursor = manager->execute_sql(sql);
            INFO("statement: " << sql << " error: "
                               << (cursor && cursor->is_error() ? cursor->get_error().what.c_str() : ""));
            REQUIRE(cursor);
            REQUIRE_FALSE(cursor->is_error());
            REQUIRE(cursor->size() == 1);
            return cursor;
        }
    };

    void run_all(const scheduler_stack& s, session_hash_t& id, std::initializer_list<const char*> statements) {
        std::string err;
        for (const char* sql : statements) {
            const bool ok = run_scheduler_sql(s, id++, sql, err);
            INFO("statement: " << sql << " error: " << err);
            REQUIRE(ok);
        }
    }

    // The payload of a one-row SELECT through the scheduler stack; the row is
    // chunks.front().
    session_payload select_one(const scheduler_stack& s, session_hash_t& id, const std::string& sql) {
        auto r = run_scheduler_sql_payload(s, id++, sql);
        INFO("statement: " << sql << " error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().size() == 1);
        REQUIRE_FALSE(r.value().chunks.empty());
        return std::move(r.value());
    }

    // DEFAULT literals whose type equals the declared column type.
    constexpr std::initializer_list<const char*> k_same_type_defaults = {
        "CREATE DATABASE ddb;",
        "CREATE TABLE ddb.same (id BIGINT, n BIGINT DEFAULT 7, s STRING DEFAULT 'x', d DOUBLE DEFAULT 1.5);",
        "INSERT INTO ddb.same (id) VALUES (1);",
    };

    // INT column, integer literal: the literal is BIGINT.
    constexpr std::initializer_list<const char*> k_int_default = {
        "CREATE DATABASE ddb;",
        "CREATE TABLE ddb.narrow (id BIGINT, n INT DEFAULT 7);",
        "INSERT INTO ddb.narrow (id) VALUES (1);",
    };

    // Composite column, ROW(...) literal: the members are literal-typed.
    constexpr std::initializer_list<const char*> k_row_default = {
        "CREATE DATABASE ddb;",
        "CREATE TYPE trio_t AS (a INT, b STRING, c STRING);",
        "CREATE TABLE ddb.comp (id BIGINT, v trio_t DEFAULT ROW(5, 'd', NULL));",
        "INSERT INTO ddb.comp (id) VALUES (1);",
    };

    void require_trio_default(const components::types::logical_value_t& v) {
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 5);
        REQUIRE(v.children()[1].value<std::string_view>() == "d");
        REQUIRE(v.children()[2].is_null());
    }

} // namespace

TEST_CASE("DEFAULT: engine alone stores same-typed DEFAULTs for omitted BIGINT / STRING / DOUBLE columns",
          "[default]") {
    engine_direct e("/tmp/test_default_insert_engine_same");
    e.run_all(k_same_type_defaults);

    auto cursor = e.select_one("SELECT n, s, d FROM ddb.same;");
    REQUIRE(cursor->value(0, 0).value<int64_t>() == 7);
    REQUIRE(cursor->value(1, 0).value<std::string_view>() == "x");
    REQUIRE(cursor->value(2, 0).value<double>() == 1.5);
}

TEST_CASE("DEFAULT: the scheduler stack stores same-typed DEFAULTs for omitted BIGINT / STRING / DOUBLE columns",
          "[default]") {
    with_scheduler_stack("/tmp/test_default_insert_stack_same", [](scheduler_stack s) {
        session_hash_t id = 10100;
        run_all(s, id, k_same_type_defaults);

        const auto payload = select_one(s, id, "SELECT n, s, d FROM ddb.same;");
        const auto& row = payload.chunks.front();
        REQUIRE(row.column_count() == 3);
        REQUIRE(row.value(0, 0).value<int64_t>() == 7);
        REQUIRE(row.value(1, 0).value<std::string_view>() == "x");
        REQUIRE(row.value(2, 0).value<double>() == 1.5);
    });
}

TEST_CASE("DEFAULT: engine alone — INT DEFAULT 7 omitted from the INSERT reads back 7",
          "[engine-defect-default]") {
    engine_direct e("/tmp/test_default_insert_engine_int");
    e.run_all(k_int_default);

    auto cursor = e.select_one("SELECT n FROM ddb.narrow;");
    const auto n = cursor->value(0, 0);
    REQUIRE_FALSE(n.is_null());
    REQUIRE(n.value<int32_t>() == 7);
}

TEST_CASE("DEFAULT: the scheduler stack — INT DEFAULT 7 omitted from the INSERT reads back 7",
          "[engine-defect-default]") {
    with_scheduler_stack("/tmp/test_default_insert_stack_int", [](scheduler_stack s) {
        session_hash_t id = 10120;
        run_all(s, id, k_int_default);

        const auto payload = select_one(s, id, "SELECT n FROM ddb.narrow;");
        const auto n = payload.chunks.front().value(0, 0);
        REQUIRE_FALSE(n.is_null());
        REQUIRE(n.value<int32_t>() == 7);
    });
}

TEST_CASE("DEFAULT: engine alone — a composite column with a ROW(...) DEFAULT is filled in when omitted",
          "[engine-defect-default]") {
    engine_direct e("/tmp/test_default_insert_engine_row");
    e.run_all(k_row_default);

    auto cursor = e.select_one("SELECT v FROM ddb.comp;");
    require_trio_default(cursor->value(0, 0));
}

TEST_CASE("DEFAULT: the scheduler stack — a composite column with a ROW(...) DEFAULT is filled in when omitted",
          "[engine-defect-default]") {
    with_scheduler_stack("/tmp/test_default_insert_stack_row", [](scheduler_stack s) {
        session_hash_t id = 10140;
        run_all(s, id, k_row_default);

        const auto payload = select_one(s, id, "SELECT v FROM ddb.comp;");
        require_trio_default(payload.chunks.front().value(0, 0));
    });
}
