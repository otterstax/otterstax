// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>
#include <string_view>

namespace frontend::mysql {
    inline constexpr uint32_t PACKET_HEADER_SIZE = 4; // length(int<3>) + seq_id(int<1>)
    // The payload cap of one wire packet (16MB - 1). A longer payload leaves
    // in several packets split at exactly this length: the length is all a
    // client reads to tell a continuation (0xFFFFFF) from the payload's end.
    inline constexpr uint32_t MAX_PACKET_SIZE = 0xFFFFFF;
    inline constexpr uint32_t MIN_PACKET_SIZE = 1;
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

    // @@max_allowed_packet: MariaDB's default 16MB, a multiple of 1024 as the
    // variable requires — the bound of a command's whole payload, however
    // many wire packets it is split into (every one but the last exactly
    // MAX_PACKET_SIZE, the last shorter — or empty, when the payload fills
    // MAX_PACKET_SIZE exactly). A command over the bound tears the
    // connection (ER_PACKET_TOO_LARGE), as MariaDB tears a client whose
    // packet is bigger than max_allowed_packet.
    inline constexpr uint32_t MAX_ALLOWED_PACKET = 16 * 1024 * 1024;
} // namespace frontend::mysql