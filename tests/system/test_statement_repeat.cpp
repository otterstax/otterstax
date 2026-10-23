// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Which slot statements a driver may repeat after a stale connection lost the answer
// (conn::statement_t::repeatable). A read may be sent twice; a DML or DDL must not be, since
// its lost answer may hide an applied change. MySQL and PostgreSQL send a DML as a `rows`
// statement — their translators build the count carrier — so the result kind cannot decide
// it: db::backend_t marks every slot that is not a read `once()`. Each row is a statement
// through the real parser and Scheduler and the flag the typed driver of its alias received.

#include "scheduler_stack.hpp"
#include "sent_queries.hpp"
#include "typed_stacks.hpp"

#include <catch2/catch_all.hpp>

#include <string>
#include <vector>

using namespace otterstax::test;

namespace {
    struct row_t {
        const char* sql;
        const char* alias;
        bool repeatable;
    };

    const std::vector<row_t>& rows() {
        static const std::vector<row_t> table{
            {"SELECT id FROM pg.shop.orders;", "pg", true},
            {"INSERT INTO pg.shop.orders (id) VALUES (1);", "pg", false},
            {"UPDATE pg.shop.orders SET id = 2 WHERE id = 1;", "pg", false},
            {"DELETE FROM pg.shop.orders WHERE id = 1;", "pg", false},
            {"CREATE TABLE pg.shop.fresh (id INT);", "pg", false},
            {"DROP TABLE pg.shop.orders;", "pg", false},

            {"SELECT id FROM my.shop.orders;", "my", true},
            {"INSERT INTO my.shop.orders (id) VALUES (1);", "my", false},
            {"UPDATE my.shop.orders SET id = 2 WHERE id = 1;", "my", false},
            {"DELETE FROM my.shop.orders WHERE id = 1;", "my", false},

            {"SELECT id FROM ch.shop.orders;", "ch", true},
            {"INSERT INTO ch.shop.orders (id) VALUES (1);", "ch", false},
        };
        return table;
    }
} // namespace

TEST_CASE("slot statements: a read may be repeated and a DML or DDL is sent once on every backend") {
    stack_owner owner(
        "/tmp/test_statement_repeat",
        {{.alias = "pg", .backend = backend_type_t::PostgreSQL, .database = "shop", .schema = "public", .table = "orders"},
         {.alias = "my", .backend = backend_type_t::MySQL, .database = "shop"},
         {.alias = "ch", .backend = backend_type_t::ClickHouse, .database = "shop", .table = "orders"}});
    const auto s = owner.stack();
    session_hash_t id = 1;

    for (const auto& row : rows()) {
        mock<pg_backend>::reset({{"id", pg_wire::int4}});
        mock<my_backend>::reset({{"id", my_wire::int_}});
        mock<ch_backend>::reset({{"id", ch_wire::Int32}});
        forget_sent();

        auto result = run_scheduler_sql_payload(s, id++, row.sql);
        INFO("sql: " << row.sql);
        INFO("result: " << (result.has_error() ? result.error().what.c_str() : "ok"));
        REQUIRE_FALSE(result.has_error());

        const auto sent = all_sent();
        REQUIRE(sent.size() == 1);
        CHECK(sent.front().alias == row.alias);
        CHECK(sent.front().repeatable == row.repeatable);
    }
}
