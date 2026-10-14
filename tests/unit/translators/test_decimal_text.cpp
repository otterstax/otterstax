// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// components::casts::kernels::detail::parse_decimal, the engine's own string -> DECIMAL parse the
// translators read a backend's decimal text with (MySQL DECIMAL, PostgreSQL NUMERIC, ClickHouse
// Decimal): the unscaled integer of the engine's DECIMAL(width, scale), a longer fraction rounded
// half away from zero, as the engine's DECIMAL cast rounds.

#include <components/casts/kernels/string_cast.hpp>

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <optional>
#include <string_view>

namespace {

    using components::types::int128_t;

    std::optional<int128_t> parse(std::string_view text, std::uint8_t scale, std::uint8_t width = 38) {
        int128_t out = 0;
        if (!components::casts::kernels::detail::parse_decimal(text, width, scale, &out)) {
            return std::nullopt;
        }
        return out;
    }

    int128_t pow10(unsigned exponent) {
        int128_t out = 1;
        for (unsigned i = 0; i < exponent; ++i) {
            out *= 10;
        }
        return out;
    }

} // namespace

TEST_CASE("parse_decimal: a tie rounds away from zero on either sign") {
    CHECK(parse("0.125", 2) == int128_t{13});
    CHECK(parse("-0.125", 2) == int128_t{-13});
    CHECK(parse("0.1249999", 2) == int128_t{12});
    CHECK(parse("-0.1249999", 2) == int128_t{-12});
    CHECK(parse("0.135", 2) == int128_t{14});
    // Only the first dropped digit decides: the rest can only add to it.
    CHECK(parse("0.12500000000000000000000000000000000000001", 2) == int128_t{13});
}

TEST_CASE("parse_decimal: a carry past the width is refused, never cut down") {
    // 9.995 at (3, 2) is 10.00, four digits.
    CHECK_FALSE(parse("9.995", 2, 3).has_value());
    CHECK(parse("9.995", 2, 4) == int128_t{1000});
    CHECK(parse("-9.995", 2, 4) == int128_t{-1000});
    CHECK(parse("9.994", 2, 3) == int128_t{999});
    // The integer digits are bounded by width - scale.
    CHECK_FALSE(parse("123.4", 1, 3).has_value());
    CHECK(parse("12.3", 1, 3) == int128_t{123});
    // 38 digits fit, 39 do not.
    CHECK(parse("99999999999999999999999999999999999999", 0) == pow10(38) - 1);
    CHECK_FALSE(parse("100000000000000000000000000000000000000", 0).has_value());
}

TEST_CASE("parse_decimal: the forms a backend prints") {
    CHECK(parse(".5", 1) == int128_t{5});
    CHECK(parse("-.5", 1) == int128_t{-5});
    CHECK(parse("1.", 2) == int128_t{100});
    CHECK(parse("-0", 2) == int128_t{0});
    CHECK(parse("+1", 2) == int128_t{100});
    CHECK(parse("007.10", 2) == int128_t{710});
    CHECK(parse("0.001", 3) == int128_t{1});
    CHECK(parse("12", 0) == int128_t{12});
}

TEST_CASE("parse_decimal: a text that is no plain decimal is refused") {
    CHECK_FALSE(parse("1e5", 0).has_value());
    CHECK_FALSE(parse("1.5E2", 1).has_value());
    CHECK_FALSE(parse("", 0).has_value());
    CHECK_FALSE(parse(" 1", 0).has_value());
    CHECK_FALSE(parse("1 ", 0).has_value());
    CHECK_FALSE(parse("1. 5", 1).has_value());
    CHECK_FALSE(parse("-", 0).has_value());
    CHECK_FALSE(parse(".", 0).has_value());
    CHECK_FALSE(parse("+-1", 0).has_value());
    CHECK_FALSE(parse("NaN", 2).has_value());
    CHECK_FALSE(parse("1,5", 1).has_value());
}
