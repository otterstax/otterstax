// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "scheduler_stack.hpp"
#include "sent_queries.hpp"
#include "typed_stacks.hpp"

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <optional>
#include <set>
#include <string>
#include <vector>

using namespace otterstax::test;

namespace {
    struct sent_t {
        std::string alias;
        std::string reference;
    };

    struct expectation_t {
        std::optional<core::error_code_t> error;
        std::vector<sent_t> sent;
        std::string message;
    };

    expectation_t ok(std::vector<sent_t> sent) { return {std::nullopt, std::move(sent), {}}; }

    expectation_t local() { return {std::nullopt, {}, {}}; }

    expectation_t err(core::error_code_t code, std::string message = {}) { return {code, {}, std::move(message)}; }

    struct row_t {
        const char* name;
        std::string sql;
        expectation_t expected;
    };

    const char* const kPgOrders = R"("public"."orders")";
    const char* const kPgbOrders = R"("sales"."orders")";
    const char* const kBacktickOrders = "`shop`.`orders`";

    void check(const scheduler_stack& s, session_hash_t id, const row_t& row) {
        mock<pg_backend>::reset({{"id", pg_wire::int4}});
        mock<my_backend>::reset({{"id", my_wire::int_}});
        mock<ch_backend>::reset({{"id", ch_wire::Int32}});
        forget_sent();

        auto result = run_scheduler_sql_payload(s, id, row.sql);
        const auto sent = all_sent();
        std::string trace;
        for (const auto& query : sent) {
            trace += "\n  sent to " + query.alias + ": " + query.query;
        }
        INFO("row: " << row.name);
        INFO("sql: " << row.sql);
        INFO("result: " << (result.has_error() ? result.error().what.c_str() : "ok") << trace);

        if (row.expected.error) {
            CHECK(sent.empty());
            CHECK(result.has_error());
            if (!result.has_error()) {
                return;
            }
            CHECK(result.error().type == *row.expected.error);
            if (!row.expected.message.empty()) {
                CHECK(std::string{result.error().what.c_str()}.find(row.expected.message) != std::string::npos);
            }
            return;
        }

        CHECK_FALSE(result.has_error());
        if (result.has_error()) {
            return;
        }
        std::set<std::string> reached;
        for (const auto& query : sent) {
            reached.insert(query.alias);
        }
        std::set<std::string> expected_aliases;
        for (const auto& want : row.expected.sent) {
            expected_aliases.insert(want.alias);
            const auto queries = sent_to(want.alias);
            INFO("alias " << want.alias << " must be sent " << want.reference);
            CHECK(std::any_of(queries.begin(), queries.end(), [&](const std::string& query) {
                return query.find(want.reference) != std::string::npos;
            }));
        }
        CHECK(reached == expected_aliases);
    }

    const std::vector<row_t>& statements() {
        using code = core::error_code_t;
        static const std::vector<row_t> table{
            {"alias.db.table takes the alias schema", "SELECT id FROM pg.shop.orders;", ok({{"pg", kPgOrders}})},
            {"second alias on the same database takes its own schema",
             "SELECT id FROM pgb.shop.orders;",
             ok({{"pgb", kPgbOrders}})},
            {"four segments naming the alias schema", "SELECT id FROM pg.shop.public.orders;", ok({{"pg", kPgOrders}})},
            {"three segments without an alias are a local name, and a local table has no schema",
             "SELECT id FROM nosuch.shop.orders;",
             err(code::invalid_parameter)},
            {"alias with a table and no database", "SELECT id FROM pg.orders;", err(code::invalid_parameter)},

            {"table.col", "SELECT orders.id FROM pg.shop.orders;", ok({{"pg", kPgOrders}})},
            {"db.table.col", "SELECT shop.orders.id FROM pg.shop.orders;", ok({{"pg", kPgOrders}})},
            {"FROM alias", "SELECT o.id FROM pg.shop.orders o;", ok({{"pg", kPgOrders}})},
            {"five segments in the canonical spelling",
             "SELECT pg.shop.public.orders.id FROM pg.shop.orders;",
             ok({{"pg", kPgOrders}})},
            {"four segments with the alias first name no element",
             "SELECT pg.shop.orders.id FROM pg.shop.orders;",
             err(code::table_not_exists)},

            {"INSERT", "INSERT INTO pg.shop.orders (id) VALUES (1);", ok({{"pg", kPgOrders}})},
            {"UPDATE", "UPDATE pg.shop.orders SET score = 1 WHERE id = 1;", ok({{"pg", kPgOrders}})},
            {"DELETE", "DELETE FROM pg.shop.orders WHERE id = 1;", ok({{"pg", kPgOrders}})},
            {"INSERT on the second schema", "INSERT INTO pgb.shop.orders (id) VALUES (1);", ok({{"pgb", kPgbOrders}})},
            {"INSERT on MySQL", "INSERT INTO my.shop.orders (id) VALUES (1);", ok({{"my", kBacktickOrders}})},

            {"two backends with the same database and table",
             "SELECT p.id FROM pg.shop.orders p JOIN my.shop.orders m ON p.id = m.id;",
             ok({{"pg", kPgOrders}, {"my", kBacktickOrders}})},
            {"two aliases on one PostgreSQL database one per schema",
             "SELECT a.id FROM pg.shop.orders a JOIN pgb.shop.orders b ON a.id = b.id;",
             ok({{"pg", kPgOrders}, {"pgb", kPgbOrders}})},
            {"local table and a remote one of the same name",
             "SELECT l.id FROM shop.orders l JOIN pg.shop.orders r ON l.id = r.id;",
             ok({{"pg", kPgOrders}})},
            {"write target sharing database and table with a source on another alias",
             "INSERT INTO pg.shop.orders (id) SELECT id FROM my.shop.orders;",
             err(code::ambiguous_name)},

            {"MySQL alias.db.table", "SELECT id FROM my.shop.orders;", ok({{"my", kBacktickOrders}})},
            {"MySQL with a schema segment", "SELECT id FROM my.shop.whatever.orders;", err(code::invalid_parameter)},
            {"ClickHouse alias.db.table", "SELECT id FROM ch.shop.orders;", ok({{"ch", kBacktickOrders}})},
            {"ClickHouse with a schema segment",
             "SELECT id FROM ch.shop.whatever.orders;",
             err(code::invalid_parameter)},

            {"derived table over alias.db.table",
             "SELECT s.id FROM (SELECT id FROM pg.shop.orders) s;",
             ok({{"pg", kPgOrders}})},

            {"refusal names the FROM element canonically",
             "SELECT other.orders.id FROM pg.shop.orders;",
             err(code::table_not_exists, "pg.shop.public.orders")},

            {"PostgreSQL schema other than the alias schema",
             "SELECT id FROM pg.shop.sales.orders;",
             err(code::unimplemented_yet)},

            {"PostgreSQL database other than the alias database",
             "SELECT id FROM pg.otherdb.orders;",
             err(code::invalid_parameter)},
            {"same in four segments", "SELECT id FROM pg.otherdb.public.orders;", err(code::invalid_parameter)},

            {"local two-segment name", "SELECT id FROM shop.orders;", local()},
            {"missing local database", "SELECT id FROM nosuch.orders;", err(code::database_not_exists)},
            {"local table with a schema segment", "SELECT id FROM shop.whatever.orders;", err(code::invalid_parameter)},
            {"local write target with a schema segment",
             "INSERT INTO shop.whatever.orders (id) VALUES (1);",
             err(code::invalid_parameter)},
            {"two local databases", "SELECT a.id FROM shop.orders a JOIN shop2.orders b ON a.id = b.id;", local()},
            {"JOIN USING across two backends",
             "SELECT id FROM pg.shop.public.orders JOIN my.shop.items USING (id);",
             ok({{"pg", kPgOrders}, {"my", "`shop`.`items`"}})},
            {"quoted mixed-case ClickHouse table",
             R"(SELECT id FROM ch.shop."Events";)",
             ok({{"ch", "`shop`.`Events`"}})},
        };
        return table;
    }

    const std::vector<row_t>& definitions() {
        static const std::vector<row_t> table{
            {"CREATE TABLE", "CREATE TABLE pg.shop.fresh (id INT);", ok({{"pg", R"("public"."fresh")"}})},
            {"CREATE TABLE on the second schema",
             "CREATE TABLE pgb.shop.fresh (id INT);",
             ok({{"pgb", R"("sales"."fresh")"}})},
            {"DROP TABLE", "DROP TABLE pg.shop.orders;", ok({{"pg", kPgOrders}})},
        };
        return table;
    }
} // namespace

namespace {
    void check_rows(const char* data_dir, const std::vector<row_t>& table) {
        stack_owner owner(
            data_dir,
            {{.alias = "pg",
              .backend = backend_type_t::PostgreSQL,
              .database = "shop",
              .schema = "public",
              .table = "orders"},
             {.alias = "pgb",
              .backend = backend_type_t::PostgreSQL,
              .database = "shop",
              .schema = "sales",
              .table = "orders"},
             {.alias = "my", .backend = backend_type_t::MySQL, .database = "shop"},
             {.alias = "ch", .backend = backend_type_t::ClickHouse, .database = "shop", .table = "orders"}});
        const auto s = owner.stack();
        session_hash_t id = 1;
        for (const auto* ddl : {"CREATE DATABASE shop;",
                                "CREATE TABLE shop.orders (id INT);",
                                "CREATE DATABASE shop2;",
                                "CREATE TABLE shop2.orders (id INT);"}) {
            auto created = run_scheduler_sql_payload(s, id++, ddl);
            INFO(ddl << ": " << (created.has_error() ? created.error().what.c_str() : "ok"));
            REQUIRE_FALSE(created.has_error());
        }

        for (const auto& row : table) {
            check(s, id++, row);
        }
    }
} // namespace

TEST_CASE("federated names: every statement reaches the table its name means", "[names]") {
    check_rows("/tmp/test_name_canonicalization", statements());
}

TEST_CASE("federated names: every DDL statement reaches the table its name means", "[names]") {
    check_rows("/tmp/test_name_canonicalization_ddl", definitions());
}
