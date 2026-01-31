// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>

namespace frontend ::postgres {
    inline constexpr uint32_t PACKET_HEADER_SIZE = 5;      // type(char) + length(int<4>)
    inline constexpr int32_t MAX_PACKET_SIZE = 2147483647; // 2GB-1 - pgbouncer doc
} // namespace frontend::postgres