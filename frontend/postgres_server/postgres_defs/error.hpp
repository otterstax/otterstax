// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>

namespace frontend::postgres::sql_state {
    inline constexpr const char* SUCCESS = "00000";
    inline constexpr const char* SUCCESSFUL_COMPLETION = "00000";

    inline constexpr const char* CONNECTION_EXCEPTION = "08000";
    inline constexpr const char* CONNECTION_DOES_NOT_EXIST = "08003";
    inline constexpr const char* CONNECTION_FAILURE = "08006";
    inline constexpr const char* PROTOCOL_VIOLATION = "08P01";

    inline constexpr const char* FEATURE_NOT_SUPPORTED = "0A000";

    inline constexpr const char* DATA_EXCEPTION = "22000";
    inline constexpr const char* STRING_DATA_RIGHT_TRUNCATION = "22001";
    inline constexpr const char* NUMERIC_VALUE_OUT_OF_RANGE = "22003";
    inline constexpr const char* DIVISION_BY_ZERO = "22012";
    inline constexpr const char* INVALID_TEXT_REPRESENTATION = "22P02";
    inline constexpr const char* INVALID_DATETIME_FORMAT = "22007";
    inline constexpr const char* DATETIME_FIELD_OVERFLOW = "22008";

    inline constexpr const char* INTEGRITY_CONSTRAINT_VIOLATION = "23000";
    inline constexpr const char* NOT_NULL_VIOLATION = "23502";
    inline constexpr const char* FOREIGN_KEY_VIOLATION = "23503";
    inline constexpr const char* UNIQUE_VIOLATION = "23505";
    inline constexpr const char* CHECK_VIOLATION = "23514";
    inline constexpr const char* EXCLUSION_VIOLATION = "23P01";

    inline constexpr const char* INVALID_TRANSACTION_STATE = "25000";
    inline constexpr const char* ACTIVE_SQL_TRANSACTION = "25001";
    inline constexpr const char* NO_ACTIVE_SQL_TRANSACTION = "25P01";
    inline constexpr const char* IN_FAILED_SQL_TRANSACTION = "25P02";

    inline constexpr const char* INVALID_SQL_STATAMENT_NAME = "26000";

    inline constexpr const char* INVALID_AUTHORIZATION = "28000";
    inline constexpr const char* INVALID_AUTHORIZATION_SPECIFICATION = "28000";
    inline constexpr const char* INVALID_PASSWORD = "28P01";

    inline constexpr const char* INVALID_SAVEPOINT_SPECIFICATION = "3B001";

    inline constexpr const char* SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION = "42000";
    inline constexpr const char* SYNTAX_ERROR = "42601";
    inline constexpr const char* INSUFFICIENT_PRIVILEGE = "42501";
    inline constexpr const char* INVALID_NAME = "42602";
    inline constexpr const char* NAME_TOO_LONG = "42622";
    inline constexpr const char* UNDEFINED_COLUMN = "42703";
    inline constexpr const char* UNDEFINED_TABLE = "42P01";
    inline constexpr const char* UNDEFINED_PARAMETER = "42P02";
    inline constexpr const char* DUPLICATE_TABLE = "42P07";
    inline constexpr const char* DUPLICATE_COLUMN = "42701";
    inline constexpr const char* AMBIGUOUS_COLUMN = "42702";
    inline constexpr const char* AMBIGUOUS_FUNCTION = "42725";
    inline constexpr const char* DATATYPE_MISMATCH = "42804";
    inline constexpr const char* WRONG_OBJECT_TYPE = "42809";
    inline constexpr const char* INVALID_FOREIGN_KEY = "42830";

    inline constexpr const char* INSUFFICIENT_RESOURCES = "53000";
    inline constexpr const char* DISK_FULL = "53100";
    inline constexpr const char* OUT_OF_MEMORY = "53200";
    inline constexpr const char* TOO_MANY_CONNECTIONS = "53300";

    inline constexpr const char* PROGRAM_LIMIT_EXCEEDED = "54000";
    inline constexpr const char* STATEMENT_TOO_COMPLEX = "54001";
    inline constexpr const char* TOO_MANY_COLUMNS = "54011";
    inline constexpr const char* TOO_MANY_ARGUMENTS = "54023";

    inline constexpr const char* OPERATOR_INTERVENTION = "57000";
    inline constexpr const char* QUERY_CANCELED = "57014";
    inline constexpr const char* ADMIN_SHUTDOWN = "57P01";
    inline constexpr const char* CRASH_SHUTDOWN = "57P02";
    inline constexpr const char* CANNOT_CONNECT_NOW = "57P03";

    inline constexpr const char* SYSTEM_ERROR = "58000";
    inline constexpr const char* IO_ERROR = "58030";

    inline constexpr const char* INTERNAL_ERROR = "XX000";
    inline constexpr const char* DATA_CORRUPTED = "XX001";
    inline constexpr const char* INDEX_CORRUPTED = "XX002";
}; // namespace frontend::postgres::sql_state