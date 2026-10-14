// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Sub-queries the transformer flattens out of a statement, over local tables.
//
// `IN (SELECT ...)`, `EXISTS (SELECT ...)` and a scalar `(SELECT ...)` lower to
// sub-queries of the engine's execution_plan_t, run ahead of the statement and
// bound into the parameters it reads. The parser used to keep only the last one
// (the statement), so the engine ran it against result parameters nobody filled:
// `id IN (SELECT id FROM sq.u)` answered no rows instead of two, with no error.
// They now travel on OtterbrixStatement::sub_queries and every execution site
// puts them back. Each statement is checked against the rows the engine answers
// for the same SQL on its own, and against the rows it must answer.

#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <string>
#include <vector>

using otterstax::test::engine_row_count;
using otterstax::test::execute_session_prepared;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_prepared;
using otterstax::test::run_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::with_session_stack;

namespace {

    using ids_t = std::vector<int64_t>;

    void run_or_fail(const session_stack& s, session_hash_t id, const std::string& sql) {
        std::string err;
        const bool ok = run_session_sql(s, id, sql, err);
        INFO(sql << ": " << err);
        REQUIRE(ok);
    }

    // sq.t: ids 1..4; sq.u: ids 2 and 3.
    void seed(const session_stack& s, session_hash_t& id) {
        run_or_fail(s, id++, "CREATE DATABASE sq;");
        run_or_fail(s, id++, "CREATE TABLE sq.t (id bigint, name string);");
        run_or_fail(s, id++, "CREATE TABLE sq.u (id bigint);");
        run_or_fail(s, id++, "INSERT INTO sq.t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');");
        run_or_fail(s, id++, "INSERT INTO sq.u (id) VALUES (2), (3);");
    }

    ids_t first_column(const std::pmr::vector<components::vector::data_chunk_t>& chunks) {
        ids_t ids;
        for (const auto& chunk : chunks) {
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                ids.push_back(chunk.value(0, row).value<int64_t>());
            }
        }
        return ids;
    }

    // The ids `sql` answers through a query session, after checking the engine
    // answers the same ids for the same SQL on its own.
    ids_t ids_of(const session_stack& s, session_hash_t id, const std::string& sql) {
        auto direct = s.engine->sql(sql);
        REQUIRE(direct);
        INFO(sql << " (engine): " << (direct->is_error() ? direct->get_error().what.c_str() : ""));
        REQUIRE_FALSE(direct->is_error());

        auto result = run_session_sql_payload(s, id, sql);
        INFO(sql << ": " << (result.has_error() ? result.error().what.c_str() : ""));
        REQUIRE_FALSE(result.has_error());
        const auto ids = first_column(result.value().chunks);
        CHECK(ids == first_column(direct->chunks()));
        return ids;
    }

} // namespace

TEST_CASE("sub-query: IN EXISTS and scalar sub-queries over local tables", "[subquery]") {
    with_session_stack("/tmp/test_subquery_select", [](session_stack s) {
        session_hash_t id = 14100;
        seed(s, id);

        CHECK(ids_of(s, id++, "SELECT id FROM sq.t WHERE id IN (SELECT id FROM sq.u) ORDER BY id;") == ids_t{2, 3});
        CHECK(ids_of(s, id++, "SELECT id FROM sq.t WHERE EXISTS (SELECT id FROM sq.u) ORDER BY id;") ==
              ids_t{1, 2, 3, 4});
        CHECK(ids_of(s,
                     id++,
                     "SELECT id FROM sq.t WHERE NOT EXISTS (SELECT id FROM sq.u WHERE id > 10) ORDER BY id;") ==
              ids_t{1, 2, 3, 4});
        CHECK(ids_of(s, id++, "SELECT id FROM sq.t WHERE id = (SELECT max(id) FROM sq.u);") == ids_t{3});
        // Two sub-queries in one statement, run in order.
        CHECK(ids_of(s,
                     id++,
                     "SELECT id FROM sq.t WHERE id IN (SELECT id FROM sq.u) AND id < (SELECT max(id) FROM sq.u) "
                     "ORDER BY id;") == ids_t{2});
    });
}

TEST_CASE("sub-query: prepared statements carry their sub-queries", "[subquery]") {
    with_session_stack("/tmp/test_subquery_prepared", [](session_stack s) {
        session_hash_t id = 14200;
        seed(s, id);

        // prepare_schema + execute_statement: the plan validated for its schema
        // and the plan executed both carry the sub-query.
        std::string prepare_err;
        auto prepared = run_session_prepared(s,
                                               id++,
                                               "SELECT id FROM sq.t WHERE id IN (SELECT id FROM sq.u) ORDER BY id;",
                                               prepare_err);
        INFO(prepare_err << (prepared.has_error() ? prepared.error().what.c_str() : ""));
        REQUIRE_FALSE(prepared.has_error());
        CHECK(first_column(prepared.value().chunks) == ids_t{2, 3});

        // A `$n` inside the sub-query, bound at execute.
        const session_hash_t bound_id = id++;
        auto parsed = prepare_session_sql(s,
                                            bound_id,
                                            "SELECT id FROM sq.t WHERE id IN (SELECT id FROM sq.u WHERE id > $1) "
                                            "ORDER BY id;");
        INFO((parsed.has_error() ? parsed.error().what.c_str() : ""));
        REQUIRE_FALSE(parsed.has_error());
        std::pmr::vector<components::types::logical_value_t> params(s.resource);
        params.emplace_back(s.resource, int64_t{2});
        auto executed = execute_session_prepared(s, bound_id, std::move(params));
        INFO((executed.has_error() ? executed.error().what.c_str() : ""));
        REQUIRE_FALSE(executed.has_error());
        CHECK(first_column(executed.value().chunks) == ids_t{3});
    });
}

TEST_CASE("sub-query: DML with an IN sub-query touches the selected rows", "[subquery]") {
    with_session_stack("/tmp/test_subquery_dml", [](session_stack s) {
        session_hash_t id = 14300;
        seed(s, id);

        run_or_fail(s, id++, "DELETE FROM sq.t WHERE id IN (SELECT id FROM sq.u);");
        CHECK(engine_row_count(s, "sq", "t") == 2);
        CHECK(ids_of(s, id++, "SELECT id FROM sq.t ORDER BY id;") == ids_t{1, 4});
    });
}
