// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>

namespace frontend::mysql {
    enum class mysql_error : uint16_t
    {
        ER_CON_COUNT_ERROR = 1040,
        ER_ACCESS_DENIED_ERROR = 1045,
        ER_UNKNOWN_COM_ERROR = 1047,
        ER_BAD_DB_ERROR = 1049,
        ER_HANDSHAKE_ERROR = 1043,
        ER_UNKNOWN_ERROR = 1105,
        ER_PACKET_TOO_LARGE = 1153,
        ER_OUT_OF_RESOURCES = 1041,
        ER_MALFORMED_PACKET = 1835,
        ER_SEQUENCE_ERROR = 1836,
        ER_NET_READ_ERROR = 1158,
        ER_PROTOCOL_ERROR = 2027,
        ER_PARSE_ERROR = 1064,   // SQL syntax error
        ER_NO_SUCH_TABLE = 1146, // Table doesn't exist
        ER_NOT_SUPPORTED_AUTH_MODE = 1251,
        ER_DBACCESS_DENIED_ERROR = 1044,    // Access denied for database
        ER_TABLEACCESS_DENIED_ERROR = 1142, // Access denied for table
        ER_WRONG_VALUE_COUNT_ON_ROW = 1136, // Column count doesn't match
        ER_DB_CREATE_EXISTS = 1007,         // Can't create database (exists)
        ER_DB_DROP_EXISTS = 1008,           // Can't drop database (doesn't exist)
        ER_TABLE_EXISTS_ERROR = 1050,       // Table already exists
        ER_UNKNOWN_TABLE = 1109,            // Unknown table
        ER_SYNTAX_ERROR = 1149,             // Syntax error
        ER_EMPTY_QUERY = 1065,              // Query was empty
        ER_UNKNOWN_STMT_HANDLER = 1243,     // Unknown prepared statement handler
        ER_QUERY_TIMEOUT = 3024,
    };

    struct sql_state {
        static constexpr const char* ACCESS_DENIED = "28000";    // Access denied
        static constexpr const char* INVALID_AUTH = "28000";     // Invalid authorization
        static constexpr const char* CONNECTION_ERROR = "08S01"; // Communication link failure
        static constexpr const char* PROTOCOL_ERROR = "HY000";   // General error
        static constexpr const char* PACKET_ERROR = "HY000";     // General error
        static constexpr const char* RESOURCE_ERROR = "HY001";   // Memory allocation error
        static constexpr const char* COMMAND_ERROR = "42000";    // Syntax error or access violation
        static constexpr const char* NOT_SUPPORTED_AUTH_ERROR = "08004";
    };
} // namespace frontend::mysql