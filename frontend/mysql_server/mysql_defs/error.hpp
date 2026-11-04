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
        // 1836 is ER_READ_ONLY_MODE in MySQL 8.4 (HY000); a packet out of
        // order is ER_NET_PACKETS_OUT_OF_ORDER (1156, 08S01) there.
        ER_SEQUENCE_ERROR = 1836,
        ER_NET_READ_ERROR = 1158,
        ER_PROTOCOL_ERROR = 2027, // a client errno (CR_MALFORMED_PACKET), never sent
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
        ER_NOT_SUPPORTED_YET = 1235,        // A result column type the wire cannot encode
        ER_QUERY_TIMEOUT = 3024,
        ER_UNKNOWN_SYSTEM_VARIABLE = 1193, // SELECT @@<a variable the server does not have>
        ER_COLLATION_CHARSET_MISMATCH = 1253, // COLLATION 'x' is not valid for CHARACTER SET 'y'
    };

    // The SQLSTATEs the errnos above carry, MySQL 8.4's own
    // (share/messages_to_clients.txt; an errno listed there without one is
    // HY000). build_error picks one per errno.
    struct sql_state {
        static constexpr const char* GENERAL_ERROR = "HY000";             // no SQLSTATE of its own
        static constexpr const char* ACCESS_DENIED = "28000";             // invalid authorization
        static constexpr const char* CONNECTION_REJECTED = "08004";       // the server rejected the connection
        static constexpr const char* COMMUNICATION_LINK_FAILURE = "08S01";
        static constexpr const char* SYNTAX_ERROR_OR_ACCESS_RULE = "42000";
        static constexpr const char* TABLE_EXISTS = "42S01";
        static constexpr const char* TABLE_NOT_FOUND = "42S02";
        static constexpr const char* VALUE_COUNT_MISMATCH = "21S01";
    };
} // namespace frontend::mysql