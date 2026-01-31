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
    inline constexpr std::string_view AUTH_PLUGIN_NAME = "mysql_native_password";
} // namespace frontend::mysql