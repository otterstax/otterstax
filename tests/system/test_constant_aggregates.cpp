// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Aggregates over constants on local tables, through the SQL path.
//
// On rc-3 the engine evaluated a node that reads no column once per chunk
// (execution_dag_t::run): `count(1)` and `sum(1)` answered one row per chunk and
// `count(1)` over no rows crashed the process, so otterstax rewrote and refused
// such aggregates before the engine ran the plan. The embedded engine evaluates
// them over every row, the guard is gone, and these cases pin the engine's own
// answers on the simple route and on prepare + execute.

#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

using otterstax::test::run_session_prepared;
using otterstax::test::run_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::seed_people_bulk;
using otterstax::test::with_session_stack;

namespace {

    namespace ct = components::types;

    void run_or_fail(const session_stack& s, session_hash_t id, const std::string& sql) {
        std::string err;
        const bool ok = run_session_sql(s, id, sql, err);
        INFO(sql << ": " << err);
        REQUIRE(ok);
    }

    // constagg.items: three rows over two groups; constagg.empty: no rows.
    void seed(const session_stack& s, session_hash_t& id) {
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

    std::vector<int64_t> first_column(const session_stack& s, session_hash_t id, const std::string& sql) {
        auto result = run_session_sql_payload(s, id, sql);
        INFO(sql << ": " << (result.has_error() ? result.error().what.c_str() : ""));
        REQUIRE_FALSE(result.has_error());
        return column_values(result.value(), 0);
    }

    using counts_t = std::vector<int64_t>;

    ct::logical_value_t first_value(const session_payload& payload) {
        for (const auto& chunk : payload.chunks) {
            if (chunk.size() != 0) {
                return chunk.value(0, 0);
            }
        }
        REQUIRE_FALSE(payload.chunks.empty());
        FAIL("no row");
        return payload.chunks.front().value(0, 0);
    }

    double as_double(const ct::logical_value_t& value) {
        if (value.is_null()) {
            return -1;
        }
        switch (value.type().type()) {
            case ct::logical_type::DOUBLE:
                return value.value<double>();
            case ct::logical_type::FLOAT:
                return value.value<float>();
            case ct::logical_type::INTEGER:
                return value.value<int32_t>();
            case ct::logical_type::UINTEGER:
                return value.value<uint32_t>();
            default:
                return static_cast<double>(as_int64(value));
        }
    }

} // namespace

TEST_CASE("constant aggregates: count of a literal counts every row", "[constant-aggregate]") {
    with_session_stack("/tmp/test_constant_aggregate_count", [](session_stack s) {
        session_hash_t id = 13100;
        seed(s, id);

        CHECK(first_column(s, id++, "SELECT count(1) FROM constagg.items;") == counts_t{3});
        CHECK(first_column(s, id++, "SELECT count('x') FROM constagg.items;") == counts_t{3});
        CHECK(first_column(s, id++, "SELECT count(1) FROM constagg.items WHERE id > 1;") == counts_t{2});
        CHECK(first_column(s, id++, "SELECT count(1) + 1 FROM constagg.items;") == counts_t{4});

        auto grouped =
            run_session_sql_payload(s,
                                      id++,
                                      "SELECT grp, count(1) AS n FROM constagg.items GROUP BY grp ORDER BY grp;");
        INFO((grouped.has_error() ? grouped.error().what.c_str() : ""));
        REQUIRE_FALSE(grouped.has_error());
        CHECK(column_values(grouped.value(), 1) == counts_t{2, 1});

        auto having =
            run_session_sql_payload(s, id++, "SELECT grp FROM constagg.items GROUP BY grp HAVING count(1) > 1;");
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
    with_session_stack("/tmp/test_constant_aggregate_chunks", [](session_stack s) {
        session_hash_t id = 13200;
        // 2500 rows are three chunks of the engine: the fold answered 3.
        seed_people_bulk(s, id, 2500);
        CHECK(first_column(s, id++, "SELECT count(1) FROM dmldb.people;") == counts_t{2500});
    });
}

TEST_CASE("constant aggregates: count of a literal over no rows is zero", "[constant-aggregate]") {
    with_session_stack("/tmp/test_constant_aggregate_empty", [](session_stack s) {
        session_hash_t id = 13300;
        seed(s, id);
        // Both crashed the process before.
        CHECK(first_column(s, id++, "SELECT count(1) FROM constagg.empty;") == counts_t{0});
        CHECK(first_column(s, id++, "SELECT count(1) FROM constagg.items WHERE id > 100;") == counts_t{0});
        CHECK(first_column(s, id++, "SELECT count(*) FROM constagg.items;") == counts_t{3});
    });
}

TEST_CASE("constant aggregates: other aggregates over constants answer the engine's values", "[constant-aggregate]") {
    with_session_stack("/tmp/test_constant_aggregate_others", [](session_stack s) {
        session_hash_t id = 13400;
        seed(s, id);

        struct engine_case {
            const char* sql;
            double expected;
        };
        // Once refused by the removed constant-aggregate guard; the engine now
        // evaluates them over every row.
        const engine_case cases[] = {
            {"SELECT sum(1) FROM constagg.items;", 3},
            {"SELECT avg(2) FROM constagg.items;", 2},
            {"SELECT min(2) FROM constagg.items;", 2},
            {"SELECT count(NULL) FROM constagg.items;", 0},
            {"SELECT count(DISTINCT 1) FROM constagg.items;", 1},
        };
        for (const auto& c : cases) {
            auto result = run_session_sql_payload(s, id++, c.sql);
            INFO(c.sql << ": " << (result.has_error() ? result.error().what.c_str() : ""));
            REQUIRE_FALSE(result.has_error());
            REQUIRE(result.value().size() == 1);
            const auto value = first_value(result.value());
            INFO("type " << static_cast<int>(value.type().type()));
            CHECK(as_double(value) == c.expected);
        }

        // An aggregate over a column.
        CHECK(first_column(s, id++, "SELECT sum(id) FROM constagg.items;") == counts_t{6});
        CHECK(first_column(s, id++, "SELECT count(id) FROM constagg.items;") == counts_t{3});
    });
}

TEST_CASE("constant aggregates: prepare then execute counts every row too", "[constant-aggregate]") {
    with_session_stack("/tmp/test_constant_aggregate_prepared", [](session_stack s) {
        session_hash_t id = 13500;
        seed(s, id);

        std::string prepare_err;
        auto counted = run_session_prepared(s, id++, "SELECT count(1) FROM constagg.items;", prepare_err);
        INFO(prepare_err << (counted.has_error() ? counted.error().what.c_str() : ""));
        REQUIRE_FALSE(counted.has_error());
        CHECK(column_values(counted.value(), 0) == counts_t{3});

        auto empty = run_session_prepared(s, id++, "SELECT count(1) FROM constagg.empty;", prepare_err);
        INFO(prepare_err << (empty.has_error() ? empty.error().what.c_str() : ""));
        REQUIRE_FALSE(empty.has_error());
        CHECK(column_values(empty.value(), 0) == counts_t{0});
    });
}
