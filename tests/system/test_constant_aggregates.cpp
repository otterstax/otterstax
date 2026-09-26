// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Aggregates over constants on local tables, through the SQL path.
//
// The engine evaluates a node that reads no column once per chunk, not once per
// row (execution_dag_t::run). On rc-3 `count(1)` and `sum(1)` over a local table
// answered one row per chunk — 1 over three rows, 3 over 2500 — and `count(1)`
// over no rows crashed the process in count_update. OtterbrixDataManager::
// execute_plan now rewrites count(<non-NULL literal>) to COUNT(*) and refuses
// every other aggregate over constants as `unimplemented_yet`, before the engine
// runs the plan. The simple route and prepare + execute both end there.

#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

using otterstax::test::run_scheduler_prepared;
using otterstax::test::run_scheduler_sql;
using otterstax::test::run_scheduler_sql_payload;
using otterstax::test::scheduler_stack;
using otterstax::test::seed_people_bulk;
using otterstax::test::with_scheduler_stack;

namespace {

    namespace ct = components::types;

    void run_or_fail(const scheduler_stack& s, session_hash_t id, const std::string& sql) {
        std::string err;
        const bool ok = run_scheduler_sql(s, id, sql, err);
        INFO(sql << ": " << err);
        REQUIRE(ok);
    }

    // constagg.items: three rows over two groups; constagg.empty: no rows.
    void seed(const scheduler_stack& s, session_hash_t& id) {
        run_or_fail(s, id++, "CREATE DATABASE constagg;");
        run_or_fail(s, id++, "CREATE TABLE constagg.items (id bigint, grp string);");
        run_or_fail(s, id++, "INSERT INTO constagg.items (id, grp) VALUES (1, 'a'), (2, 'a'), (3, 'b');");
        run_or_fail(s, id++, "CREATE TABLE constagg.empty (id bigint);");
    }

    // A count or a sum as int64: count answers UBIGINT, sum keeps the column's BIGINT,
    // and a count plus a literal is widened to HUGEINT.
    int64_t as_int64(const ct::logical_value_t& value) {
        REQUIRE_FALSE(value.is_null());
        switch (value.type().type()) {
            case ct::logical_type::UBIGINT:
                return static_cast<int64_t>(value.value<uint64_t>());
            case ct::logical_type::BIGINT:
                return value.value<int64_t>();
            case ct::logical_type::HUGEINT:
                return static_cast<int64_t>(value.value<ct::int128_t>());
            default:
                FAIL("unexpected result type " << static_cast<int>(value.type().type()));
                return 0;
        }
    }

    // Column `column` of every row of the answer.
    std::vector<int64_t> column_values(const session_payload& payload, uint64_t column) {
        std::vector<int64_t> values;
        for (const auto& chunk : payload.chunks) {
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                values.push_back(as_int64(chunk.value(column, row)));
            }
        }
        return values;
    }

    std::vector<int64_t> first_column(const scheduler_stack& s, session_hash_t id, const std::string& sql) {
        auto result = run_scheduler_sql_payload(s, id, sql);
        INFO(sql << ": " << (result.has_error() ? result.error().what.c_str() : ""));
        REQUIRE_FALSE(result.has_error());
        return column_values(result.value(), 0);
    }

    using counts_t = std::vector<int64_t>;

} // namespace

TEST_CASE("constant aggregates: count of a literal counts every row", "[constant-aggregate]") {
    with_scheduler_stack("/tmp/test_constant_aggregate_count", [](scheduler_stack s) {
        session_hash_t id = 13100;
        seed(s, id);

        CHECK(first_column(s, id++, "SELECT count(1) FROM constagg.items;") == counts_t{3});
        CHECK(first_column(s, id++, "SELECT count('x') FROM constagg.items;") == counts_t{3});
        CHECK(first_column(s, id++, "SELECT count(1) FROM constagg.items WHERE id > 1;") == counts_t{2});
        CHECK(first_column(s, id++, "SELECT count(1) + 1 FROM constagg.items;") == counts_t{4});

        auto grouped =
            run_scheduler_sql_payload(s,
                                      id++,
                                      "SELECT grp, count(1) AS n FROM constagg.items GROUP BY grp ORDER BY grp;");
        INFO((grouped.has_error() ? grouped.error().what.c_str() : ""));
        REQUIRE_FALSE(grouped.has_error());
        CHECK(column_values(grouped.value(), 1) == counts_t{2, 1});

        auto having =
            run_scheduler_sql_payload(s, id++, "SELECT grp FROM constagg.items GROUP BY grp HAVING count(1) > 1;");
        INFO((having.has_error() ? having.error().what.c_str() : ""));
        REQUIRE_FALSE(having.has_error());
        REQUIRE(having.value().size() == 1);
        for (const auto& chunk : having.value().chunks) {
            if (chunk.size() != 0) {
                CHECK(chunk.value(0, 0).value<std::string_view>() == "a");
            }
        }
    });
}

TEST_CASE("constant aggregates: count of a literal over more rows than one chunk", "[constant-aggregate]") {
    with_scheduler_stack("/tmp/test_constant_aggregate_chunks", [](scheduler_stack s) {
        session_hash_t id = 13200;
        // 2500 rows are three chunks of the engine: the fold answered 3.
        seed_people_bulk(s, id, 2500);
        CHECK(first_column(s, id++, "SELECT count(1) FROM dmldb.people;") == counts_t{2500});
    });
}

TEST_CASE("constant aggregates: count of a literal over no rows is zero", "[constant-aggregate]") {
    with_scheduler_stack("/tmp/test_constant_aggregate_empty", [](scheduler_stack s) {
        session_hash_t id = 13300;
        seed(s, id);
        // Both crashed the process before.
        CHECK(first_column(s, id++, "SELECT count(1) FROM constagg.empty;") == counts_t{0});
        CHECK(first_column(s, id++, "SELECT count(1) FROM constagg.items WHERE id > 100;") == counts_t{0});
        CHECK(first_column(s, id++, "SELECT count(*) FROM constagg.items;") == counts_t{3});
    });
}

TEST_CASE("constant aggregates: other aggregates over constants are refused", "[constant-aggregate]") {
    with_scheduler_stack("/tmp/test_constant_aggregate_refused", [](scheduler_stack s) {
        session_hash_t id = 13400;
        seed(s, id);

        struct refused_case {
            const char* sql;
            const char* message;
        };
        // Each answered one row per chunk: a wrong result rather than an error.
        const refused_case cases[] = {
            {"SELECT sum(1) FROM constagg.items;", "sum() over a constant argument is not supported"},
            {"SELECT avg(2) FROM constagg.items;", "avg() over a constant argument is not supported"},
            {"SELECT min(2) FROM constagg.items;", "min() over a constant argument is not supported"},
            {"SELECT count(NULL) FROM constagg.items;", "count() over a constant argument is not supported"},
            {"SELECT count(DISTINCT 1) FROM constagg.items;", "count() over a constant argument is not supported"},
        };
        for (const auto& refused : cases) {
            INFO(refused.sql);
            auto result = run_scheduler_sql_payload(s, id++, refused.sql);
            REQUIRE(result.has_error());
            CHECK(std::string_view{result.error().what.c_str()}.find(refused.message) != std::string_view::npos);
        }

        // An aggregate over a column is not touched.
        CHECK(first_column(s, id++, "SELECT sum(id) FROM constagg.items;") == counts_t{6});
        CHECK(first_column(s, id++, "SELECT count(id) FROM constagg.items;") == counts_t{3});
    });
}

TEST_CASE("constant aggregates: prepare then execute counts every row too", "[constant-aggregate]") {
    with_scheduler_stack("/tmp/test_constant_aggregate_prepared", [](scheduler_stack s) {
        session_hash_t id = 13500;
        seed(s, id);

        std::string prepare_err;
        auto counted = run_scheduler_prepared(s, id++, "SELECT count(1) FROM constagg.items;", prepare_err);
        INFO(prepare_err << (counted.has_error() ? counted.error().what.c_str() : ""));
        REQUIRE_FALSE(counted.has_error());
        CHECK(column_values(counted.value(), 0) == counts_t{3});

        auto empty = run_scheduler_prepared(s, id++, "SELECT count(1) FROM constagg.empty;", prepare_err);
        INFO(prepare_err << (empty.has_error() ? empty.error().what.c_str() : ""));
        REQUIRE_FALSE(empty.has_error());
        CHECK(column_values(empty.value(), 0) == counts_t{0});
    });
}
