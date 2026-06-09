// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/mysql_to_complex.hpp"

#include <catch2/catch.hpp>

using boost::mysql::column_type;
using namespace components::types;

// mysql_to_complex is a pure enum → complex_logical_type mapping with no side effects.
// Every branch is exercised directly here; mysql_to_chunk (which needs boost::mysql::results
// built from live wire data) is covered at system-test level.

TEST_CASE("mysql_to_complex: integer types, signed") {
    REQUIRE(tsl::mysql_to_complex(column_type::tinyint,   false).type() == logical_type::TINYINT);
    REQUIRE(tsl::mysql_to_complex(column_type::smallint,  false).type() == logical_type::SMALLINT);
    REQUIRE(tsl::mysql_to_complex(column_type::mediumint, false).type() == logical_type::INTEGER);
    REQUIRE(tsl::mysql_to_complex(column_type::int_,      false).type() == logical_type::INTEGER);
}

TEST_CASE("mysql_to_complex: integer types, unsigned") {
    REQUIRE(tsl::mysql_to_complex(column_type::tinyint,   true).type() == logical_type::UTINYINT);
    REQUIRE(tsl::mysql_to_complex(column_type::smallint,  true).type() == logical_type::USMALLINT);
    REQUIRE(tsl::mysql_to_complex(column_type::mediumint, true).type() == logical_type::UINTEGER);
    REQUIRE(tsl::mysql_to_complex(column_type::int_,      true).type() == logical_type::UINTEGER);
}

TEST_CASE("mysql_to_complex: floating-point types") {
    REQUIRE(tsl::mysql_to_complex(column_type::float_,  false).type() == logical_type::FLOAT);
    REQUIRE(tsl::mysql_to_complex(column_type::double_, false).type() == logical_type::DOUBLE);
    // is_unsigned flag is ignored for float/double
    REQUIRE(tsl::mysql_to_complex(column_type::float_,  true).type() == logical_type::FLOAT);
    REQUIRE(tsl::mysql_to_complex(column_type::double_, true).type() == logical_type::DOUBLE);
}

TEST_CASE("mysql_to_complex: boolean") {
    REQUIRE(tsl::mysql_to_complex(column_type::bit, false).type() == logical_type::BOOLEAN);
}

TEST_CASE("mysql_to_complex: string-mapped types") {
    REQUIRE(tsl::mysql_to_complex(column_type::decimal, false).type() == logical_type::STRING_LITERAL);
    REQUIRE(tsl::mysql_to_complex(column_type::text,    false).type() == logical_type::STRING_LITERAL);
    REQUIRE(tsl::mysql_to_complex(column_type::char_,   false).type() == logical_type::STRING_LITERAL);
    REQUIRE(tsl::mysql_to_complex(column_type::varchar, false).type() == logical_type::STRING_LITERAL);
    REQUIRE(tsl::mysql_to_complex(column_type::blob,    false).type() == logical_type::STRING_LITERAL);
}

TEST_CASE("mysql_to_complex: unknown / unhandled type falls back to NA") {
    // column_type::geometry is not handled in the switch; the default branch returns NA.
    REQUIRE(tsl::mysql_to_complex(column_type::geometry, false).type() == logical_type::NA);
}
