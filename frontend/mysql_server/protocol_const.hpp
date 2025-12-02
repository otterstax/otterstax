// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <cstdint>
#include <string_view>

namespace mysql_front {
    constexpr uint32_t MAX_PACKET_SIZE = 0xFFFFFF; // 16MB - 1 (max client packet size)
    constexpr uint32_t MIN_PACKET_SIZE = 1;
    constexpr uint32_t MAX_BUFFER_SIZE = 64 * 1024 * 1024;         // 64MB buffer limit
    constexpr uint32_t DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024; // 16MB client default limit
    constexpr uint8_t PROTOCOL_VERSION = 10;
    constexpr uint32_t CONNECTION_TIMEOUT_SEC = 30;
    constexpr std::string_view SERVER_VERSION = "9.5.0";
    constexpr std::string_view AUTH_PLUGIN_NAME = "mysql_native_password";
} // namespace mysql_front