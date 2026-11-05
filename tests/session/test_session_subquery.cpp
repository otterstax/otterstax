// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Sub-queries over local tables through query_session_t, and the engine's
// describe of statements that carry them: a relation only a sub-query reads is
// analyzed with the statement, and a scalar sub-query's column is typed.

#include "../mock/cell.hpp"
#include "../system/session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <string>
#include <vector>

using otterstax::test::engine_row_count;
using otterstax::test::execute_session_prepared;
using otterstax::test::execute_session_statement;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_or_fail;
using otterstax::test::run_session_prepared;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::with_session_stack;

namespace {
    using ids_t = std::vector<int64_t>;
    namespace ct = components::types;

    // sq.t: ids 1..4 (local.sq.t); sq.u: ids 2 and 3.
    void seed(const session_stack& s, session_hash_t& id) {
        run_session_or_fail(s, id++, "CREATE SCHEMA sq;");
        run_session_or_fail(s, id++, "CREATE TABLE sq.t (id bigint, name string);");
        run_session_or_fail(s, id++, "CREATE TABLE sq.u (id bigint);");
        run_session_or_fail(s, id++, "INSERT INTO sq.t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');");
        run_session_or_fail(s, id++, "INSERT INTO sq.u (id) VALUES (2), (3);");
    }

    ids_t first_column(const std::pmr::vector<components::vector::data_chunk_t>& chunks) {
        ids_t ids;
        for (const auto& chunk : chunks) {
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                ids.push_back(otterstax::test::cell(chunk, 0, row).value<int64_t>());
            }
        }
        return ids;
    }

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

    const ct::complex_logical_type* field_named(const ct::complex_logical_type& schema, const std::string& name) {
        if (schema.type() != ct::logical_type::STRUCT) {
            return nullptr;
        }
        for (const auto& field : schema.child_types()) {
            if (field.has_alias() && field.alias() == name) {
                return &field;
            }
        }
        return nullptr;
    }
} // namespace

TEST_CASE("session sub-query: IN EXISTS and scalar sub-queries over local tables", "[subquery]") {
    with_session_stack("/tmp/test_session_subquery_select", [](session_stack s) {
        session_hash_t id = 14100;
        seed(s, id);

        CHECK(ids_of(s, id++, "SELECT id FROM local.sq.t WHERE id IN (SELECT id FROM local.sq.u) ORDER BY id;") ==
              ids_t{2, 3});
        CHECK(ids_of(s, id++, "SELECT id FROM local.sq.t WHERE EXISTS (SELECT id FROM local.sq.u) ORDER BY id;") ==
              ids_t{1, 2, 3, 4});
        CHECK(ids_of(s, id++, "SELECT id FROM local.sq.t WHERE id = (SELECT max(id) FROM local.sq.u);") == ids_t{3});
        CHECK(ids_of(s,
                     id++,
                     "SELECT id FROM local.sq.t WHERE id IN (SELECT id FROM local.sq.u) AND id < (SELECT max(id) FROM "
                     "local.sq.u) "
                     "ORDER BY id;") == ids_t{2});
    });
}

TEST_CASE("session sub-query: prepared statements carry their sub-queries", "[subquery]") {
    with_session_stack("/tmp/test_session_subquery_prepared", [](session_stack s) {
        session_hash_t id = 14200;
        seed(s, id);

        std::string prepare_err;
        auto prepared =
            run_session_prepared(s,
                                 id++,
                                 "SELECT id FROM local.sq.t WHERE id IN (SELECT id FROM local.sq.u) ORDER BY id;",
                                 prepare_err);
        INFO(prepare_err << (prepared.has_error() ? prepared.error().what.c_str() : ""));
        REQUIRE_FALSE(prepared.has_error());
        CHECK(first_column(prepared.value().chunks) == ids_t{2, 3});

        const session_hash_t bound_id = id++;
        auto parsed =
            prepare_session_sql(s,
                                bound_id,
                                "SELECT id FROM local.sq.t WHERE id IN (SELECT id FROM local.sq.u WHERE id > $1) "
                                "ORDER BY id;");
        INFO((parsed.has_error() ? parsed.error().what.c_str() : ""));
        REQUIRE_FALSE(parsed.has_error());
        std::pmr::vector<components::vector::vector_t> params(s.resource);
        params.push_back(components::vector::make_scalar(s.resource, int64_t{2}));
        auto executed = execute_session_prepared(s, bound_id, std::move(params));
        INFO((executed.has_error() ? executed.error().what.c_str() : ""));
        REQUIRE_FALSE(executed.has_error());
        CHECK(first_column(executed.value().chunks) == ids_t{3});
    });
}

TEST_CASE("session sub-query: DML with an IN sub-query touches the selected rows", "[subquery]") {
    with_session_stack("/tmp/test_session_subquery_dml", [](session_stack s) {
        session_hash_t id = 14300;
        seed(s, id);

        run_session_or_fail(s, id++, "DELETE FROM local.sq.t WHERE id IN (SELECT id FROM local.sq.u);");
        CHECK(engine_row_count(s, "local.sq", "t") == 2);
        CHECK(ids_of(s, id++, "SELECT id FROM local.sq.t ORDER BY id;") == ids_t{1, 4});
    });
}

// The engine analyzes a prepare as a run analyzes it, sub-queries included: a
// relation only a sub-query reads, missing from the engine, fails the prepare
// as PostgreSQL's Parse fails it.
TEST_CASE("session sub-query: a sub-query relation the engine lacks fails the prepare", "[subquery]") {
    with_session_stack("/tmp/test_session_subquery_missing", [](session_stack s) {
        session_hash_t id = 14350;
        seed(s, id);
        auto prepared = prepare_session_sql(
            s,
            id++,
            "SELECT id FROM local.sq.t WHERE id = $1 AND id IN (SELECT id FROM local.sq.not_there);");
        REQUIRE(prepared.has_error());
        CHECK(prepared.error().type == core::error_code_t::table_not_exists);
    });
}

// The engine's describe analyzes a scalar sub-query as a run does and types
// its column (a NULL of the column's type); the statement runs and answers the
// sub-query's value.
TEST_CASE("session sub-query: a scalar sub-query column of a parameterized statement is typed", "[subquery]") {
    with_session_stack("/tmp/test_session_subquery_describe", [](session_stack s) {
        session_hash_t id = 14400;
        seed(s, id);

        const session_hash_t stmt = id++;
        auto prepared =
            prepare_session_sql(s,
                                stmt,
                                "SELECT id, (SELECT max(id) FROM local.sq.u) AS top FROM local.sq.t WHERE id = $1;");
        INFO((prepared.has_error() ? prepared.error().what.c_str() : ""));
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == 1);

        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == ct::logical_type::STRUCT);
        const auto* top = field_named(schema, "top");
        REQUIRE(top != nullptr);
        CHECK(top->type() == ct::logical_type::BIGINT);
        const auto* plain = field_named(schema, "id");
        REQUIRE(plain != nullptr);
        CHECK(plain->type() == ct::logical_type::BIGINT);

        std::pmr::vector<components::vector::vector_t> params(s.resource);
        params.push_back(components::vector::make_scalar(s.resource, int64_t{2}));
        auto executed = execute_session_prepared(s, stmt, std::move(params));
        INFO((executed.has_error() ? executed.error().what.c_str() : ""));
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(executed.value().size() == 1);
        const auto& chunk = executed.value().chunks.front();
        REQUIRE(chunk.column_count() == 2);
        CHECK(otterstax::test::cell(chunk, 0, 0).value<int64_t>() == 2);
        CHECK(otterstax::test::cell(chunk, 1, 0).value<int64_t>() == 3);
    });
}
