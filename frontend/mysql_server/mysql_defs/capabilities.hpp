// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>

namespace frontend::mysql {
    //Values for the capabilities flag bitmask, currently need to fit into 32 bits
    using capabilities_flags_t = uint32_t;

    constexpr uint32_t CLIENT_LONG_PASSWORD = 1; // Use the improved version of Old Password Authentication
    constexpr uint32_t CLIENT_FOUND_ROWS = 2;    // Send found rows instead of affected rows in EOF_Packet
    constexpr uint32_t CLIENT_LONG_FLAG = 4;     // Get all column flags
    constexpr uint32_t CLIENT_CONNECT_WITH_DB =
        8; // Database (schema) name can be specified on connect in Handshake Response Packet
    constexpr uint32_t CLIENT_NO_SCHEMA = 16;        // Don't allow database.table.column
    constexpr uint32_t CLIENT_COMPRESS = 32;         // Compression protocol supported
    constexpr uint32_t CLIENT_ODBC = 64;             // Special handling of ODBC behavior
    constexpr uint32_t CLIENT_LOCAL_FILES = 128;     // Can use LOAD DATA LOCAL
    constexpr uint32_t CLIENT_IGNORE_SPACE = 256;    // Ignore spaces before '('
    constexpr uint32_t CLIENT_PROTOCOL_41 = 512;     // New 4.1 protocol
    constexpr uint32_t CLIENT_INTERACTIVE = 1024;    // This is an interactive client
    constexpr uint32_t CLIENT_SSL = 2048;            // Use SSL encryption for the session
    constexpr uint32_t CLIENT_IGNORE_SIGPIPE = 4096; // Client only flag
    constexpr uint32_t CLIENT_TRANSACTIONS = 8192;   // Client knows about transactions
    constexpr uint32_t CLIENT_RESERVED = 16384;      // DEPRECATED: Old flag for 4.1 protocol
    constexpr uint32_t CLIENT_SECURE_CONNECTION =
        32768; // DEPRECATED: Old flag for 4.1 authentication, required by MariaDB
    constexpr uint32_t CLIENT_MULTI_STATEMENTS = (1UL << 16); // Enable/disable multi-stmt support
    constexpr uint32_t CLIENT_MULTI_RESULTS = (1UL << 17);    // Enable/disable multi-results
    constexpr uint32_t CLIENT_PS_MULTI_RESULTS = (1UL << 18); // Multi-results and OUT parameters in PS-protocol
    constexpr uint32_t CLIENT_PLUGIN_AUTH = (1UL << 19);      // Client supports plugin authentication
    constexpr uint32_t CLIENT_CONNECT_ATTRS = (1UL << 20);    // Client supports connection attributes
    constexpr uint32_t CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA =
        (1UL << 21); // Enable authentication response packet to be larger than 255 bytes
    constexpr uint32_t CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS =
        (1UL << 22); // Don't close the connection for a user account with expired password
    constexpr uint32_t CLIENT_SESSION_TRACK = (1UL << 23); // Capable of handling server state change information
    constexpr uint32_t CLIENT_DEPRECATE_EOF =
        (1UL << 24); // Client no longer needs EOF_Packet and will use OK_Packet instead
    constexpr uint32_t CLIENT_SSL_VERIFY_SERVER_CERT = (1UL << 30); // Verify server certificate
    constexpr uint32_t CLIENT_OPTIONAL_RESULTSET_METADATA =
        (1UL << 25); // The client can handle optional metadata information in the resultset
    constexpr uint32_t CLIENT_REMEMBER_OPTIONS = (1UL << 31); // Don't reset the options after an unsuccessful connect
} // namespace frontend::mysql