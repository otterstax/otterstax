// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "scheduler_stack.hpp"
#include "tests/temporary_directory.hpp"

namespace {
    using namespace otterstax::test;

    void seed_subqueries(const scheduler_stack& stack, session_hash_t& id) {
        for (const auto* sql : {
                 "CREATE DATABASE subq;",
                 "CREATE TABLE subq.items (id BIGINT);",
                 "CREATE TABLE subq.allowed (id BIGINT);",
                 "INSERT INTO subq.items (id) VALUES (1), (2), (3), (4);",
                 "INSERT INTO subq.allowed (id) VALUES (2), (4), (4);"}) {
            std::string error;
            INFO(sql);
            const bool ok = run_scheduler_sql(stack, id++, sql, error);
            INFO(error);
            REQUIRE(ok);
        }
    }

    void require_ids(core::result_wrapper_t<session_payload> result, const std::vector<int64_t>& expected) {
        INFO((result.has_error() ? result.error().what.c_str() : "query succeeded"));
        REQUIRE_FALSE(result.has_error());
        REQUIRE(result.value().column_count() == 1);
        std::vector<int64_t> actual;
        for (const auto& chunk : result.value().chunks) {
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                actual.push_back(chunk.value(0, row).value<int64_t>());
            }
        }
        REQUIRE(actual == expected);
    }
}

TEST_CASE("expression subqueries execute in simple and prepared local statements", "[subquery]") {
    const bool prepared = GENERATE(false, true);
    temporary_directory directory("otterstax-expression-subqueries");
    scheduler_stack_owner owner(directory.path.string(), &make_parser);
    const auto stack = owner.stack();
    session_hash_t id = 200;
    seed_subqueries(stack, id);
    auto check = [&](const std::string& predicate, const std::vector<int64_t>& expected) {
        const std::string sql = "SELECT id FROM subq.items WHERE " + predicate + " ORDER BY id;";
        INFO(sql << ", prepared=" << prepared);
        std::string prepare_error;
        require_ids(prepared ? run_scheduler_prepared(stack, id++, sql, prepare_error)
                             : run_scheduler_sql_payload(stack, id++, sql), expected);
    };
    check("id IN (SELECT id FROM subq.allowed)", {2, 4});
    check("id NOT IN (SELECT id FROM subq.allowed)", {1, 3});
    check("EXISTS (SELECT id FROM subq.allowed WHERE id = 99)", {});
    check("EXISTS (SELECT id FROM subq.allowed WHERE id = 2)", {1, 2, 3, 4});
    check("id = (SELECT MAX(id) FROM subq.allowed)", {4});
    check("id IN (SELECT id FROM subq.allowed WHERE id IN (SELECT id FROM subq.items WHERE id > 3))", {4});
}

TEST_CASE("expression subqueries retain parameters bound after prepare", "[subquery]") {
    temporary_directory directory("otterstax-bound-subquery");
    scheduler_stack_owner owner(directory.path.string(), &make_parser);
    const auto stack = owner.stack();
    session_hash_t id = 300;
    seed_subqueries(stack, id);
    auto prepared = prepare_scheduler_sql(
        stack, id, "SELECT id FROM subq.items WHERE id IN (SELECT id FROM subq.allowed WHERE id >= $1) ORDER BY id;");
    INFO((prepared.has_error() ? prepared.error().what.c_str() : "prepare succeeded"));
    REQUIRE_FALSE(prepared.has_error());
    std::pmr::vector<components::types::logical_value_t> parameters(stack.resource);
    parameters.emplace_back(stack.resource, int64_t{4});
    require_ids(execute_scheduler_prepared(stack, id, std::move(parameters)), {4});
}

TEST_CASE("expression subquery compaction includes every engine chunk", "[subquery]") {
    temporary_directory directory("otterstax-multichunk-subquery");
    scheduler_stack_owner owner(directory.path.string(), &make_parser);
    const auto stack = owner.stack();
    session_hash_t id = 400;
    constexpr std::size_t rows = 1500;
    seed_people_bulk(stack, id, rows);
    std::vector<int64_t> expected;
    for (std::size_t row = 1; row <= rows; ++row) expected.push_back(static_cast<int64_t>(row));
    require_ids(run_scheduler_sql_payload(stack, id,
                    "SELECT id FROM dmldb.people WHERE id IN (SELECT id FROM dmldb.people) ORDER BY id;"), expected);
}

TEST_CASE("INSERT without a column list binds values to the table columns", "[insert]") {
    temporary_directory directory("otterstax-positional-insert");
    scheduler_stack_owner owner(directory.path.string(), &make_parser);
    const auto stack = owner.stack();
    session_hash_t id = 500;
    std::string error;
    REQUIRE(run_scheduler_sql(stack, id++, "CREATE DATABASE positional;", error));
    REQUIRE(run_scheduler_sql(stack, id++, "CREATE TABLE positional.items (id BIGINT);", error));
    const bool inserted = run_scheduler_sql(stack, id++, "INSERT INTO positional.items VALUES (1), (2);", error);
    INFO(error);
    REQUIRE(inserted);
    require_ids(run_scheduler_sql_payload(stack, id, "SELECT id FROM positional.items ORDER BY id;"), {1, 2});
}

TEST_CASE("positional INSERT binds mixed and NULL values by catalog order", "[insert]") {
    const bool prepared = GENERATE(false, true);
    temporary_directory directory("otterstax-positional-mixed");
    scheduler_stack_owner owner(directory.path.string(), &make_parser);
    const auto stack = owner.stack();
    session_hash_t id = 600;
    std::string error;
    REQUIRE(run_scheduler_sql(stack, id++, "CREATE DATABASE positional;", error));
    REQUIRE(run_scheduler_sql(stack, id++,
                             "CREATE TABLE positional.items (column2 BIGINT, column1 VARCHAR, active BOOLEAN);", error));
    const std::string sql = "INSERT INTO positional.items VALUES (1, 'one', true), (2, NULL, false);";
    auto result = prepared ? run_scheduler_prepared(stack, id++, sql, error)
                           : run_scheduler_sql_payload(stack, id++, sql);
    INFO((result.has_error() ? result.error().what.c_str() : "insert succeeded"));
    REQUIRE_FALSE(result.has_error());
    require_ids(run_scheduler_sql_payload(stack, id++,
                    "SELECT column2 FROM positional.items WHERE column1 = 'one' AND active = true;"), {1});
    require_ids(run_scheduler_sql_payload(stack, id++,
                    "SELECT column2 FROM positional.items WHERE column1 IS NULL AND active = false;"), {2});
    for (const auto* invalid : {"INSERT INTO positional.items VALUES (3, 'three', true, 4);",
                               "INSERT INTO positional.items VALUES (3, 'three', true), (4);"}) {
        auto rejected = run_scheduler_sql_payload(stack, id++, invalid);
        REQUIRE(rejected.has_error());
    }
    require_ids(run_scheduler_sql_payload(stack, id++,
                    "SELECT column2 FROM positional.items ORDER BY column2;"), {1, 2});
}

TEST_CASE("positional INSERT retains parameter bindings and spans multiple chunks", "[insert]") {
    temporary_directory directory("otterstax-positional-bound");
    scheduler_stack_owner owner(directory.path.string(), &make_parser);
    const auto stack = owner.stack();
    session_hash_t id = 700;
    std::string error;
    REQUIRE(run_scheduler_sql(stack, id++, "CREATE DATABASE positional;", error));
    REQUIRE(run_scheduler_sql(stack, id++, "CREATE TABLE positional.items (id BIGINT, label VARCHAR);", error));
    auto prepared = prepare_scheduler_sql(stack, id, "INSERT INTO positional.items VALUES ($1, $2);");
    INFO((prepared.has_error() ? prepared.error().what.c_str() : "prepare succeeded"));
    REQUIRE_FALSE(prepared.has_error());
    std::pmr::vector<components::types::logical_value_t> parameters(stack.resource);
    parameters.emplace_back(stack.resource, int64_t{0});
    parameters.emplace_back(stack.resource, std::string{"bound"});
    auto inserted = execute_scheduler_prepared(stack, id++, std::move(parameters));
    INFO((inserted.has_error() ? inserted.error().what.c_str() : "insert succeeded"));
    REQUIRE_FALSE(inserted.has_error());
    require_ids(run_scheduler_sql_payload(stack, id++,
                    "SELECT id FROM positional.items WHERE label = 'bound';"), {0});
    std::string bulk = "INSERT INTO positional.items VALUES ";
    std::vector<int64_t> expected{0};
    for (int64_t row = 1; row <= 1500; ++row) {
        if (row != 1) bulk += ",";
        bulk += "(" + std::to_string(row) + ", 'bulk')";
        expected.push_back(row);
    }
    bulk += ";";
    const bool ok = run_scheduler_sql(stack, id++, bulk, error);
    INFO(error);
    REQUIRE(ok);
    require_ids(run_scheduler_sql_payload(stack, id++, "SELECT id FROM positional.items ORDER BY id;"), expected);
}
