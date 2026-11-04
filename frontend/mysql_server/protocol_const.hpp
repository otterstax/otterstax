// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>
#include <string_view>

namespace frontend::mysql {
    inline constexpr uint32_t PACKET_HEADER_SIZE = 4;     // length(int<3>) + seq_id(int<1>)
    inline constexpr uint32_t MAX_PACKET_SIZE = 0xFFFFFF; // 16MB - 1 (max client packet size)
    inline constexpr uint32_t MIN_PACKET_SIZE = 1;
    inline constexpr uint32_t DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024; // 16MB client default limit
    inline constexpr uint8_t PROTOCOL_VERSION = 10;
    inline constexpr std::string_view SERVER_VERSION = "9.5.0";
    // SERVER_VERSION as MySQL numbers it (major * 10000 + minor * 100 + patch):
    // an executable comment `/*!NNNNN ... */` runs when NNNNN is at most this.
    inline constexpr uint32_t SERVER_VERSION_ID = 90500;
    inline constexpr std::string_view AUTH_PLUGIN_NAME = "mysql_native_password";
    // The server's character set and collation: the collation id the handshake
    // announces and every text column definition carries
    // (character_set::UTF8MB4_BIN), and the names @@character_set_* /
    // @@collation_* answer. The frontend takes utf-8 only (SET NAMES) and sends
    // utf-8 text, so utf8mb4 is what it actually speaks; the engine compares
    // strings byte by byte (std::string_view comparison in
    // components/vector/operations/apply_operator.cpp: 'a' <> 'A'), which is
    // MySQL's binary collation, never a case-insensitive one.
    inline constexpr uint8_t SERVER_COLLATION_ID = 46;
    inline constexpr std::string_view SERVER_CHARSET = "utf8mb4";
    inline constexpr std::string_view SERVER_COLLATION = "utf8mb4_bin";

    // @@max_allowed_packet: the largest command the server reads. It does not
    // reassemble a command split over several packets, so one packet's payload
    // bounds it (MAX_PACKET_SIZE), rounded down to the multiple of 1024 MySQL
    // requires of the variable. A driver that splits at it (the Go driver
    // writes at most max_allowed_packet - 1 bytes) never sends a continuation.
    inline constexpr uint32_t MAX_ALLOWED_PACKET = MAX_PACKET_SIZE / 1024 * 1024;
} // namespace frontend::mysql