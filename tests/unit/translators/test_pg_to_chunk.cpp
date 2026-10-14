// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// PGresult objects are manufactured without a connection: tuples through libpq's
// public PQsetResultAttrs / PQsetvalue (see pg_result_fixture.cpp). Both
// pg_to_chunk overloads are exercised on every shape, since the enum overload is
// the one the integration layer calls.

#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "pg_result_fixture.hpp"

#include <catch2/catch_all.hpp>

#include <libpq-fe.h>

#include <cstdint>
#include <memory_resource>
#include <string_view>

using namespace components::types;
using otterstax::test::make_tuples_result;
using otterstax::test::pg_column;
using otterstax::test::pg_result_guard;

namespace {

    constexpr Oid INT4OID = 23;
    constexpr Oid INT8OID = 20;
    constexpr Oid TEXTOID = 25;

    // Runs `check` against both pg_to_chunk overloads on the same result.
    template<typename Check>
    void for_both_overloads(std::pmr::memory_resource* res, PGresult* result, Check&& check) {
        {
            INFO("overload without enum map");
            auto converted = tsl::pg_to_chunk(res, result);
            check(converted);
        }
        {
            INFO("overload with enum map");
            tsl::pg_enum_oid_map empty_map;
            auto converted = tsl::pg_to_chunk(res, result, empty_map);
            check(converted);
        }
    }

} // namespace

TEST_CASE("pg_to_struct: empty PGresult produces empty STRUCT") {
    pg_result_guard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    REQUIRE(g.get() != nullptr);

    auto s = tsl::pg_to_struct(std::pmr::new_delete_resource(), g.get());

    REQUIRE(s.type() == logical_type::STRUCT);
    REQUIRE(s.child_types().empty());
}

TEST_CASE("pg_to_struct (enum overload): empty PGresult produces empty STRUCT") {
    pg_result_guard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    tsl::pg_enum_oid_map empty_map;

    auto s = tsl::pg_to_struct(std::pmr::new_delete_resource(), g.get(), empty_map);

    REQUIRE(s.type() == logical_type::STRUCT);
    REQUIRE(s.child_types().empty());
}

TEST_CASE("pg_to_struct: column names and types come from the result attributes") {
    pg_result_guard g(make_tuples_result({{"id", INT4OID}, {"label", TEXTOID}}, {}));

    auto s = tsl::pg_to_struct(std::pmr::new_delete_resource(), g.get());

    REQUIRE(s.child_types().size() == 2);
    REQUIRE(s.child_types()[0].alias() == "id");
    REQUIRE(s.child_types()[0].type() == logical_type::INTEGER);
    REQUIRE(s.child_types()[1].alias() == "label");
    REQUIRE(s.child_types()[1].type() == logical_type::STRING_LITERAL);
}

TEST_CASE("pg_to_chunk: empty PGresult produces zero-row chunk") {
    pg_result_guard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    auto* res = std::pmr::new_delete_resource();

    auto converted = tsl::pg_to_chunk(res, g.get());
    REQUIRE_FALSE(converted.has_error());

    REQUIRE(converted.value().size() == 0);
    REQUIRE(converted.value().column_count() == 0);
}

TEST_CASE("pg_to_chunk (enum overload): empty PGresult produces zero-row chunk") {
    pg_result_guard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    auto* res = std::pmr::new_delete_resource();
    tsl::pg_enum_oid_map empty_map;

    auto converted = tsl::pg_to_chunk(res, g.get(), empty_map);
    REQUIRE_FALSE(converted.has_error());

    REQUIRE(converted.value().size() == 0);
    REQUIRE(converted.value().column_count() == 0);
}

// ── tuples ────────────────────────────────────────────────────────────────────

TEST_CASE("pg_to_chunk: tuples are converted by column type, NULL cells stay NULL") {
    pg_result_guard g(make_tuples_result({{"id", INT4OID}, {"big", INT8OID}, {"label", TEXTOID}},
                                         {{"1", "10000000000", "one"},
                                          {"2", nullptr, "two"},
                                          {nullptr, "3", nullptr}}));
    auto* res = std::pmr::new_delete_resource();

    for_both_overloads(res, g.get(), [](auto& converted) {
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        REQUIRE(chunk.column_count() == 3);
        REQUIRE(chunk.size() == 3);
        REQUIRE(chunk.value(0, 0).template value<int32_t>() == 1);
        REQUIRE(chunk.value(1, 0).template value<int64_t>() == 10000000000LL);
        REQUIRE(chunk.value(2, 0).template value<std::string_view>() == "one");
        REQUIRE(chunk.value(0, 1).template value<int32_t>() == 2);
        REQUIRE(chunk.value(1, 1).is_null());
        REQUIRE(chunk.value(2, 1).template value<std::string_view>() == "two");
        REQUIRE(chunk.value(0, 2).is_null());
        REQUIRE(chunk.value(1, 2).template value<int64_t>() == 3);
        REQUIRE(chunk.value(2, 2).is_null());
    });
}

// ── NUMERIC ─────────────────────────────────────────────────────────────────

namespace {

    constexpr Oid NUMERICOID = 1700;

    // NUMERIC(p, s)'s type modifier, as PostgreSQL sends it in the row description.
    constexpr int numeric_typmod(int precision, int scale) { return ((precision << 16) | (scale & 0x7ff)) + 4; }

    components::types::int128_t unscaled_of(const logical_value_t& value) {
        switch (value.type().to_physical_type()) {
            case physical_type::INT16:
                return value.value<int16_t>();
            case physical_type::INT32:
                return value.value<int32_t>();
            case physical_type::INT64:
                return value.value<int64_t>();
            default:
                return value.value<components::types::int128_t>();
        }
    }

} // namespace

// NUMERIC(p, s) of at most 38 digits is the engine's DECIMAL(p, s) holding every digit PostgreSQL
// printed — the discovery's schema and the executed chunk alike; NaN, which a constrained NUMERIC
// takes, is the engine's DECIMAL NaN; a NULL is NULL.
TEST_CASE("pg_to_chunk: NUMERIC of at most 38 digits is the engine's DECIMAL of its precision and scale") {
    auto* res = std::pmr::new_delete_resource();
    pg_result_guard g(make_tuples_result({{"price", NUMERICOID, numeric_typmod(12, 2)},
                                          {"wide", NUMERICOID, numeric_typmod(38, 10)},
                                          {"whole", NUMERICOID, numeric_typmod(5, 0)}},
                                         {{"1234.50", "1234567890123456789012345678.0123456789", "-42"},
                                          {"-0.05", "-0.0000000001", "0"},
                                          {"NaN", nullptr, "99999"}}));
    const auto schema = tsl::pg_to_struct(res, g.get());
    for_both_overloads(res, g.get(), [&](core::result_wrapper_t<components::vector::data_chunk_t>& converted) {
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        const auto types = chunk.types();
        REQUIRE(types.size() == 3);
        const std::vector<std::pair<int, int>> specs{{12, 2}, {38, 10}, {5, 0}};
        for (size_t col = 0; col < 3; ++col) {
            REQUIRE(types[col].type() == logical_type::DECIMAL);
            const auto* spec = types[col].extension_as<decimal_logical_type_extension>();
            REQUIRE(spec->width() == specs[col].first);
            REQUIRE(spec->scale() == specs[col].second);
            REQUIRE(schema.child_types()[col] == types[col]);
        }
        REQUIRE(unscaled_of(chunk.value(0, 0)) == 123450);
        REQUIRE(unscaled_of(chunk.value(0, 1)) == -5);
        const auto* price = types[0].extension_as<decimal_logical_type_extension>();
        REQUIRE(unscaled_of(chunk.value(0, 2)) == decimal_special::not_a_number(price->stored_as()));
        components::types::int128_t wide = 0;
        for (char digit : std::string_view{"12345678901234567890123456780123456789"}) {
            wide = wide * 10 + (digit - '0');
        }
        REQUIRE(unscaled_of(chunk.value(1, 0)) == wide);
        REQUIRE(unscaled_of(chunk.value(1, 1)) == -1);
        REQUIRE(chunk.value(1, 2).is_null());
        REQUIRE(unscaled_of(chunk.value(2, 0)) == -42);
        REQUIRE(unscaled_of(chunk.value(2, 2)) == 99999);
    });
}

// A NUMERIC no DECIMAL(p, s) holds without rounding or overflowing is its exact text: the
// unconstrained NUMERIC (each value keeps its own scale — also what PostgreSQL answers for
// SUM(bigint) / AVG(...)), one wider than 38 digits, one whose scale lies outside 0..p
// (PostgreSQL 15). Never DOUBLE, which rounds every value past 15 digits.
TEST_CASE("pg_to_chunk: a NUMERIC wider than the engine's DECIMAL is carried as its exact text") {
    auto* res = std::pmr::new_delete_resource();
    pg_result_guard g(make_tuples_result({{"free", NUMERICOID},
                                          {"wide", NUMERICOID, numeric_typmod(50, 2)},
                                          {"negative_scale", NUMERICOID, numeric_typmod(5, -2)}},
                                         {{"0.333333333333333333333333", "123456789012345678901234567890123456789012345678.90", "12300"},
                                          {"7", "-1.00", "-100"}}));
    const auto schema = tsl::pg_to_struct(res, g.get());
    for_both_overloads(res, g.get(), [&](core::result_wrapper_t<components::vector::data_chunk_t>& converted) {
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        for (size_t col = 0; col < 3; ++col) {
            REQUIRE(chunk.types()[col].type() == logical_type::STRING_LITERAL);
            REQUIRE(schema.child_types()[col].type() == logical_type::STRING_LITERAL);
        }
        REQUIRE(chunk.value(0, 0).value<std::string_view>() == "0.333333333333333333333333");
        REQUIRE(chunk.value(1, 0).value<std::string_view>() == "123456789012345678901234567890123456789012345678.90");
        REQUIRE(chunk.value(2, 1).value<std::string_view>() == "-100");
    });
}

// A NUMERIC(p, s) value the declared DECIMAL cannot hold (a server that broke its own typmod) is a
// conversion_failure naming the column, never a value cut down to fit.
TEST_CASE("pg_to_chunk: a NUMERIC value past its declared precision is conversion_failure") {
    auto* res = std::pmr::new_delete_resource();
    pg_result_guard g(make_tuples_result({{"price", NUMERICOID, numeric_typmod(4, 2)}}, {{"123.45"}}));
    for_both_overloads(res, g.get(), [&](core::result_wrapper_t<components::vector::data_chunk_t>& converted) {
        REQUIRE(converted.has_error());
        REQUIRE(converted.error().type == core::error_code_t::conversion_failure);
        REQUIRE(std::string{converted.error().what.c_str()}.find("price") != std::string::npos);
    });
}
