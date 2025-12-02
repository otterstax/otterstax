// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <cstdint>

namespace mysql_front {
    enum class field_type : uint8_t
    {
        MYSQL_TYPE_DECIMAL,
        MYSQL_TYPE_TINY,
        MYSQL_TYPE_SHORT,
        MYSQL_TYPE_LONG,
        MYSQL_TYPE_FLOAT,
        MYSQL_TYPE_DOUBLE,
        MYSQL_TYPE_NULL,
        MYSQL_TYPE_TIMESTAMP,
        MYSQL_TYPE_LONGLONG,
        MYSQL_TYPE_INT24,
        MYSQL_TYPE_DATE,
        MYSQL_TYPE_TIME,
        MYSQL_TYPE_DATETIME,
        MYSQL_TYPE_YEAR,
        MYSQL_TYPE_NEWDATE, // Internal to MySQL. Not used in protocol
        MYSQL_TYPE_VARCHAR,
        MYSQL_TYPE_BIT,
        MYSQL_TYPE_TIMESTAMP2,
        MYSQL_TYPE_DATETIME2,   // Internal to MySQL. Not used in protocol */
        MYSQL_TYPE_TIME2,       // Internal to MySQL. Not used in protocol */
        MYSQL_TYPE_TYPED_ARRAY, // Used for replication only */
        MYSQL_TYPE_VECTOR = 242,
        MYSQL_TYPE_INVALID = 243,
        MYSQL_TYPE_BOOL = 244, // Currently just a placeholder */
        MYSQL_TYPE_JSON = 245,
        MYSQL_TYPE_NEWDECIMAL = 246,
        MYSQL_TYPE_ENUM = 247,
        MYSQL_TYPE_SET = 248,
        MYSQL_TYPE_TINY_BLOB = 249,
        MYSQL_TYPE_MEDIUM_BLOB = 250,
        MYSQL_TYPE_LONG_BLOB = 251,
        MYSQL_TYPE_BLOB = 252,
        MYSQL_TYPE_VAR_STRING = 253,
        MYSQL_TYPE_STRING = 254,
        MYSQL_TYPE_GEOMETRY = 255
    };
} // namespace mysql_front