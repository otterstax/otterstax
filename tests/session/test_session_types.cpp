// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The type a prepare announces is the type its execute delivers, over the
// local engine: one row per SELECT-list expression, with and without `$1`
// bound in the WHERE. The targets are the engine's own typing — Trino
// aggregates (a decimal sum widens to DECIMAL(38, s), an integer one stays
// BIGINT, min and max keep the input, avg of an integer is DOUBLE) — and
// every bind carries the type the describe announced for `$1`.

#include "../system/session_stack.hpp"

#include "frontend/common/utils.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

using otterstax::test::execute_session_prepared;
using otterstax::test::execute_session_statement;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_or_fail;
using otterstax::test::session_stack;
using otterstax::test::with_session_stack;

namespace {

    namespace ct = components::types;
    using ct::logical_type;

    constexpr std::string_view kScalars = "loc.scalars";

    std::string label_of(const ct::complex_logical_type& type) {
        return std::string{frontend::logical_type_name(type.type())};
    }

    // loc.scalars: a column of each scalar family, the names the rows use.
    void seed_local_scalars(const session_stack& s, session_hash_t& id) {
        run_session_or_fail(s, id++, "CREATE SCHEMA loc;");
        run_session_or_fail(s,
                            id++,
                            "CREATE TABLE local.loc.scalars (id int, score int, big bigint, ratio double, "
                            "price decimal(10, 2), name string);");
        run_session_or_fail(s,
                            id++,
                            "INSERT INTO local.loc.scalars (id, score, big, ratio, price, name) VALUES "
                            "(0, 0, 3000000000, 0.5, 0.25, 'v0'), (1, 1, 3000000001, 1.5, 1.25, 'v1'), "
                            "(2, 2, 3000000002, 2.5, 2.25, 'v2');");
    }

    // What a prepare announced for the one result column and what its execute
    // delivered: the two complex types, named by the wire's spelling.
    struct announced_delivered {
        ct::complex_logical_type announced;
        ct::complex_logical_type delivered;
    };

    // Prepares `sql`, executes it with `parameters` (empty: a plain execute)
    // and answers the announced and delivered type of its one column. The
    // describe's `engine_types` must carry the type of every value bound —
    // the rule every bind of this file lives by.
    announced_delivered prepare_and_execute(const session_stack& s,
                                            session_hash_t id,
                                            const std::string& sql,
                                            std::pmr::vector<components::vector::vector_t> parameters) {
        auto prepared = prepare_session_sql(s, id, sql);
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == parameters.size());
        REQUIRE(prepared.value().engine_types.size() == parameters.size());
        for (size_t i = 0; i < parameters.size(); ++i) {
            INFO("parameter $" << (i + 1));
            REQUIRE(prepared.value().engine_types[i] == parameters[i].type());
        }
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        const ct::complex_logical_type announced = schema.child_types()[0];

        auto executed = parameters.empty() ? execute_session_statement(s, id)
                                           : execute_session_prepared(s, id, std::move(parameters));
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        REQUIRE_FALSE(executed.value().chunks.empty());
        const auto& chunk = executed.value().chunks.front();
        REQUIRE(chunk.column_count() == 1);
        const auto delivered = chunk.types();
        return {announced, delivered[0]};
    }

    // One SELECT-list expression: its target is the type the engine announces
    // and delivers, a DECIMAL target's width and scale included.
    struct type_row {
        const char* expression;
        std::optional<logical_type> target;
        uint8_t width = 0;
        uint8_t scale = 0;
    };

    void check_decimal(const ct::complex_logical_type& type, uint8_t width, uint8_t scale) {
        const auto* spec = type.extension_as<ct::decimal_logical_type_extension>();
        REQUIRE(spec != nullptr);
        REQUIRE(spec->width() == width);
        REQUIRE(spec->scale() == scale);
    }

    // The expression prepared and executed without a placeholder and with one
    // bound in the WHERE (the INTEGER the describe types `id = $1` by); both
    // runs announce what they deliver.
    void check_expression(const session_stack& s, session_hash_t& id, const type_row& row, std::string_view from) {
        for (const bool bound : {false, true}) {
            std::string sql = std::string{"SELECT "} + row.expression + " AS v FROM " + std::string{from};
            sql += bound ? " WHERE id = $1" : " WHERE id = 1";
            CAPTURE(sql);
            std::pmr::vector<components::vector::vector_t> parameters{s.resource};
            if (bound) {
                parameters.push_back(components::vector::make_scalar(s.resource, int32_t{1}));
            }
            const auto types = prepare_and_execute(s, id++, sql, std::move(parameters));
            CHECK(types.announced.type() == types.delivered.type());
            if (row.target) {
                CHECK(label_of(types.delivered) == label_of(ct::complex_logical_type{*row.target}));
                if (row.width != 0) {
                    check_decimal(types.announced, row.width, row.scale);
                    check_decimal(types.delivered, row.width, row.scale);
                }
            }
        }
    }

} // namespace

TEST_CASE("prepare announces the type its execute delivers over a local table", "[types-29]") {
    with_session_stack("/tmp/test_session_types_local", [](session_stack s) {
        session_hash_t id = 1;
        seed_local_scalars(s, id);

        const std::vector<type_row> rows{
            {"score", logical_type::INTEGER},
            {"big", logical_type::BIGINT},
            {"ratio", logical_type::DOUBLE},
            {"name", logical_type::STRING_LITERAL},
            {"price", logical_type::DECIMAL, 10, 2},
            {"COUNT(*)", logical_type::UBIGINT},
            {"SUM(score)", logical_type::BIGINT},
            {"SUM(big)", logical_type::BIGINT},
            {"SUM(ratio)", logical_type::DOUBLE},
            {"SUM(price)", logical_type::DECIMAL, 38, 2},
            {"AVG(score)", logical_type::DOUBLE},
            {"AVG(ratio)", logical_type::DOUBLE},
            {"MIN(score)", logical_type::INTEGER},
            {"MIN(price)", logical_type::DECIMAL, 10, 2},
            {"MAX(price)", logical_type::DECIMAL, 10, 2},
            {"score + 1"},
            {"1"},
            {"CAST(score AS BIGINT)", logical_type::BIGINT},
        };
        for (const auto& row : rows) {
            check_expression(s, id, row, kScalars);
        }
    });
}

TEST_CASE("prepare announces the type its execute delivers over a view", "[types-29]") {
    with_session_stack("/tmp/test_session_types_view", [](session_stack s) {
        session_hash_t id = 1;
        seed_local_scalars(s, id);
        run_session_or_fail(s, id++, "CREATE VIEW loc.scalars_v AS SELECT id, score, price FROM loc.scalars;");
        // The view is described by the engine, which splices its body in.
        const std::vector<type_row> rows{
            {"score", logical_type::INTEGER},
            {"SUM(price)", logical_type::DECIMAL, 38, 2},
        };
        for (const auto& row : rows) {
            check_expression(s, id, row, "loc.scalars_v");
        }
    });
}

TEST_CASE("a placeholder typed by its context is announced as delivered", "[types-29]") {
    with_session_stack("/tmp/test_session_types_context_param", [](session_stack s) {
        session_hash_t id = 1;
        seed_local_scalars(s, id);
        // `score + $1` types $1 by score: an INTEGER, and the column with it.
        const std::string sql = "SELECT score + $1 AS v FROM " + std::string{kScalars} + " WHERE id = 1";
        CAPTURE(sql);
        std::pmr::vector<components::vector::vector_t> parameters{s.resource};
        parameters.push_back(components::vector::make_scalar(s.resource, int32_t{1}));
        const auto types = prepare_and_execute(s, id++, sql, std::move(parameters));
        CHECK(label_of(types.announced) == "INTEGER");
        CHECK(label_of(types.delivered) == "INTEGER");
    });
}

TEST_CASE("a bare placeholder in the select list announces text and delivers it", "[types-29]") {
    with_session_stack("/tmp/test_session_types_bare_param", [](session_stack s) {
        session_hash_t id = 1;
        seed_local_scalars(s, id);
        // The engine's rule for an output column that is `$1` alone is text;
        // the bind of a string keeps the announced column.
        const std::string sql = "SELECT $1 AS v FROM " + std::string{kScalars} + " WHERE id = 1";
        CAPTURE(sql);
        std::pmr::vector<components::vector::vector_t> parameters{s.resource};
        parameters.push_back(components::vector::make_scalar(s.resource, "one"));
        const auto types = prepare_and_execute(s, id++, sql, std::move(parameters));
        CHECK(label_of(types.announced) == "STRING_LITERAL");
        CHECK(label_of(types.delivered) == "STRING_LITERAL");
    });
}

TEST_CASE("a bind of another type answers its own type", "[types-29]") {
    with_session_stack("/tmp/test_session_types_rebound", [](session_stack s) {
        session_hash_t id = 1;
        seed_local_scalars(s, id);
        // The engine types $1 by the BOUND value: the INTEGER the describe
        // announced for `score + $1` is not imposed on a BIGINT bind, the
        // column widens with it.
        const std::string sql = "SELECT score + $1 AS v FROM " + std::string{kScalars} + " WHERE id = 1";
        CAPTURE(sql);
        auto prepared = prepare_session_sql(s, id, sql);
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == 1);
        REQUIRE(prepared.value().engine_types.size() == 1);
        CHECK(prepared.value().engine_types[0].type() == logical_type::INTEGER);
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        CHECK(label_of(schema.child_types()[0]) == "INTEGER");

        std::pmr::vector<components::vector::vector_t> parameters{s.resource};
        parameters.push_back(components::vector::make_scalar(s.resource, int64_t{1}));
        auto executed = execute_session_prepared(s, id++, std::move(parameters));
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(executed.value().size() == 1);
        const auto& chunk = executed.value().chunks.front();
        REQUIRE(chunk.column_count() == 1);
        const auto delivered = chunk.types();
        CHECK(label_of(delivered[0]) == "BIGINT");
    });
}
