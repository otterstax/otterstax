// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "integration/otterbrix/otterbrix_engine.hpp"
#include "otterbrix/config.hpp"
#include "tests/temporary_directory.hpp"
#include <catch2/catch_all.hpp>

TEST_CASE("Engine session transactions isolate uncommitted rows and support commit and rollback") {
    otterstax::test::temporary_directory directory{"engine-session-transactions"};
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    const otterbrix::session_id_t writer;
    const otterbrix::session_id_t observer;
    const auto run = [&](auto session, const std::string& sql) {
        auto result = engine->dispatcher()->execute_sql(session, sql);
        REQUIRE(result);
        INFO(sql);
        INFO((result->is_error() ? result->get_error().what.c_str() : "success"));
        REQUIRE_FALSE(result->is_error());
        return result;
    };
    run(writer, "CREATE DATABASE transactions;");
    run(writer, "CREATE TABLE transactions.items (id BIGINT);");
    run(writer, "BEGIN;");
    run(writer, "INSERT INTO transactions.items (id) VALUES (1);");
    CHECK(run(writer, "SELECT * FROM transactions.items;")->size() == 1);
    CHECK(run(observer, "SELECT * FROM transactions.items;")->size() == 0);
    run(writer, "ROLLBACK;");
    CHECK(run(observer, "SELECT * FROM transactions.items;")->size() == 0);
    run(writer, "BEGIN;");
    run(writer, "INSERT INTO transactions.items (id) VALUES (2);");
    run(writer, "COMMIT;");
    auto committed = run(observer, "SELECT * FROM transactions.items;");
    REQUIRE(committed->size() == 1);
    CHECK(committed->value(0, 0).value<int64_t>() == 2);
}

TEST_CASE("Engine rollback preserves a concurrent session's later committed append") {
    otterstax::test::temporary_directory directory{"engine-interleaved-transactions"};
    {
        auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
        const otterbrix::session_id_t first;
        const otterbrix::session_id_t second;
        const auto run = [&](auto session, const std::string& sql) {
            auto result = engine->dispatcher()->execute_sql(session, sql);
            REQUIRE(result);
            INFO(sql);
            REQUIRE_FALSE(result->is_error());
            return result;
        };
        run(first, "CREATE DATABASE transactions;");
        run(first, "CREATE TABLE transactions.items (id BIGINT);");
        run(first, "BEGIN;");
        run(first, "INSERT INTO transactions.items (id) VALUES (1);");
        run(second, "INSERT INTO transactions.items (id) VALUES (2);");
        run(first, "ROLLBACK;");
        auto visible = run(second, "SELECT * FROM transactions.items;");
        REQUIRE(visible->size() == 1);
        CHECK(visible->value(0, 0).value<int64_t>() == 2);
    }
    auto reopened = db::make_otterbrix_engine(make_create_config(directory.path));
    auto visible = reopened->dispatcher()->execute_sql(otterbrix::session_id_t{}, "SELECT * FROM transactions.items;");
    REQUIRE(visible);
    REQUIRE_FALSE(visible->is_error());
    REQUIRE(visible->size() == 1);
    CHECK(visible->value(0, 0).value<int64_t>() == 2);
}
