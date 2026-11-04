// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The text of a FLOAT / DOUBLE cell on each wire. PostgreSQL 18 (float.c
// float8out_internal / float4out): extra_float_digits > 0 is the shortest
// exact form (ryu d2s.c / f2s.c to_chars: fixed for a decimal exponent in
// -4 .. 14 (double) / -4 .. 5 (float4), else d.ddde±XX), <= 0 is printf
// "%.*g" with DBL_DIG + n / FLT_DIG + n digits (pg_strfromd: at least 1);
// NaN, Infinity, -Infinity, -0. MySQL 8.4 (Protocol_text::store_double /
// store_float → my_gcvt(x, type, FLOATING_POINT_BUFFER)): the shortest exact
// digits (FLOAT: at most FLT_DIG = 6 significant), fixed while the decimal
// point position (decpt) is in -14 .. 15 (MAX_DECPT_FOR_F_FORMAT = DBL_DIG) or the
// digits reach past the point, else d.ddde[-]X with no '+' and no padding.
// The expected texts are what PostgreSQL 18 / MySQL 8.4 answer.

#include "frontend/common/float_text.hpp"

#include <catch2/catch_all.hpp>

#include <limits>
#include <string>
#include <vector>

using frontend::mysql_double_text;
using frontend::mysql_float_text;
using frontend::pg_float4_text;
using frontend::pg_float8_text;

namespace {
    constexpr double inf = std::numeric_limits<double>::infinity();
    constexpr double not_a_number = std::numeric_limits<double>::quiet_NaN();
} // namespace

TEST_CASE("float text: postgres float8 shortest form at extra_float_digits 1") {
    CHECK(pg_float8_text(0.1 + 0.2, 1) == "0.30000000000000004");
    CHECK(pg_float8_text(1e-9, 1) == "1e-09");
    CHECK(pg_float8_text(1e300, 1) == "1e+300");
    CHECK(pg_float8_text(123456789.123, 1) == "123456789.123");
    CHECK(pg_float8_text(1.5, 1) == "1.5");
    CHECK(pg_float8_text(-2.5, 1) == "-2.5");
    CHECK(pg_float8_text(100, 1) == "100");
    CHECK(pg_float8_text(0.0001, 1) == "0.0001");
    CHECK(pg_float8_text(0.00001, 1) == "1e-05");
    CHECK(pg_float8_text(1e14, 1) == "100000000000000");
    CHECK(pg_float8_text(1e15, 1) == "1e+15");
    CHECK(pg_float8_text(123456789012345.0, 1) == "123456789012345");
    CHECK(pg_float8_text(1234567890123456.0, 1) == "1.234567890123456e+15");
    CHECK(pg_float8_text(-1.5e-300, 1) == "-1.5e-300");
    CHECK(pg_float8_text(5e-324, 1) == "5e-324");
    CHECK(pg_float8_text(std::numeric_limits<double>::max(), 1) == "1.7976931348623157e+308");
    CHECK(pg_float8_text(0.0, 1) == "0");
    CHECK(pg_float8_text(-0.0, 1) == "-0");
    CHECK(pg_float8_text(not_a_number, 1) == "NaN");
    CHECK(pg_float8_text(-not_a_number, 1) == "NaN");
    CHECK(pg_float8_text(inf, 1) == "Infinity");
    CHECK(pg_float8_text(-inf, 1) == "-Infinity");
}

TEST_CASE("float text: postgres float4 shortest form at extra_float_digits 1") {
    CHECK(pg_float4_text(0.1f, 1) == "0.1");
    CHECK(pg_float4_text(3.14159265f, 1) == "3.1415927");
    CHECK(pg_float4_text(1e-9f, 1) == "1e-09");
    CHECK(pg_float4_text(100000.0f, 1) == "100000");
    CHECK(pg_float4_text(1e6f, 1) == "1e+06");
    CHECK(pg_float4_text(16777216.0f, 1) == "1.6777216e+07");
    CHECK(pg_float4_text(-0.0f, 1) == "-0");
    CHECK(pg_float4_text(std::numeric_limits<float>::infinity(), 1) == "Infinity");
    CHECK(pg_float4_text(-std::numeric_limits<float>::infinity(), 1) == "-Infinity");
    CHECK(pg_float4_text(std::numeric_limits<float>::quiet_NaN(), 1) == "NaN");
}

TEST_CASE("float text: postgres printf form at extra_float_digits 0 and below") {
    CHECK(pg_float8_text(0.1 + 0.2, 0) == "0.3");
    CHECK(pg_float8_text(1e-9, 0) == "1e-09");
    CHECK(pg_float8_text(1e300, 0) == "1e+300");
    CHECK(pg_float8_text(1e16, 0) == "1e+16");
    CHECK(pg_float8_text(123456789.123, 0) == "123456789.123");
    CHECK(pg_float8_text(-0.0, 0) == "-0");
    CHECK(pg_float8_text(not_a_number, 0) == "NaN");
    CHECK(pg_float8_text(-inf, 0) == "-Infinity");
    CHECK(pg_float4_text(0.1f, 0) == "0.1");
    CHECK(pg_float4_text(3.14159265f, 0) == "3.14159");
    CHECK(pg_float4_text(16777216.0f, 0) == "1.67772e+07");

    // Every value of the parameter on a pair of values: DBL_DIG + n digits,
    // FLT_DIG + n for a float4, never fewer than one.
    const std::vector<std::string> third_digits{"0.3",
                                                "0.33",
                                                "0.333",
                                                "0.3333",
                                                "0.33333",
                                                "0.333333",
                                                "0.3333333",
                                                "0.33333333",
                                                "0.333333333",
                                                "0.3333333333",
                                                "0.33333333333",
                                                "0.333333333333",
                                                "0.3333333333333",
                                                "0.33333333333333",
                                                "0.333333333333333"};
    const std::vector<std::string> big_digits{"1e+08",
                                              "1.2e+08",
                                              "1.23e+08",
                                              "1.235e+08",
                                              "1.2346e+08",
                                              "1.23457e+08",
                                              "1.234568e+08",
                                              "1.2345679e+08",
                                              "123456789",
                                              "123456789.1",
                                              "123456789.12",
                                              "123456789.123",
                                              "123456789.123",
                                              "123456789.123",
                                              "123456789.123"};
    for (int n = -15; n <= 3; ++n) {
        INFO("extra_float_digits " << n);
        if (n > 0) {
            CHECK(pg_float8_text(1.0 / 3, n) == "0.3333333333333333");
            CHECK(pg_float8_text(123456789.123, n) == "123456789.123");
            CHECK(pg_float4_text(1.0f / 3, n) == "0.33333334");
            CHECK(pg_float4_text(0.1f, n) == "0.1");
            continue;
        }
        const int digits = std::max(1, 15 + n);
        CHECK(pg_float8_text(1.0 / 3, n) == third_digits[digits - 1]);
        CHECK(pg_float8_text(123456789.123, n) == big_digits[digits - 1]);
        const int float_digits = std::max(1, 6 + n);
        CHECK(pg_float4_text(1.0f / 3, n) == third_digits[float_digits - 1]);
        CHECK(pg_float4_text(0.1f, n) == "0.1");
    }
}

TEST_CASE("float text: mysql double is the shortest exact form as my_gcvt writes it") {
    CHECK(mysql_double_text(0.1 + 0.2) == "0.30000000000000004");
    CHECK(mysql_double_text(1e-9) == "0.000000001");
    CHECK(mysql_double_text(1e-15) == "0.000000000000001");
    CHECK(mysql_double_text(1e-16) == "1e-16");
    CHECK(mysql_double_text(1.5e-20) == "1.5e-20");
    CHECK(mysql_double_text(1e300) == "1e300");
    CHECK(mysql_double_text(1e14) == "100000000000000");
    CHECK(mysql_double_text(1e15) == "1e15");
    CHECK(mysql_double_text(1e20) == "1e20");
    CHECK(mysql_double_text(1234567890123456.7) == "1234567890123456.8");
    CHECK(mysql_double_text(12345678901234567.0) == "1.2345678901234568e16");
    CHECK(mysql_double_text(123456789.123) == "123456789.123");
    CHECK(mysql_double_text(-2.5) == "-2.5");
    CHECK(mysql_double_text(100) == "100");
    CHECK(mysql_double_text(0.0) == "0");
    CHECK(mysql_double_text(-0.0) == "-0");
    CHECK(mysql_double_text(std::numeric_limits<double>::max()) == "1.7976931348623157e308");
    CHECK(mysql_double_text(5e-324) == "5e-324");
    // MySQL stores no such value; the spelling every MySQL driver parses.
    CHECK(mysql_double_text(not_a_number) == "NaN");
    CHECK(mysql_double_text(inf) == "Infinity");
    CHECK(mysql_double_text(-inf) == "-Infinity");
}

TEST_CASE("float text: mysql float has at most six significant digits") {
    CHECK(mysql_float_text(0.1f) == "0.1");
    CHECK(mysql_float_text(0.5f) == "0.5");
    CHECK(mysql_float_text(3.14159265f) == "3.14159");
    CHECK(mysql_float_text(1.0f / 3) == "0.333333");
    CHECK(mysql_float_text(1e-9f) == "0.000000001");
    CHECK(mysql_float_text(16777216.0f) == "16777200");
    CHECK(mysql_float_text(1e20f) == "1e20");
    CHECK(mysql_float_text(-1.5f) == "-1.5");
    CHECK(mysql_float_text(-0.0f) == "-0");
}
