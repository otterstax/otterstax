// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>
#include <string_view>

namespace frontend {
    enum class frontend_type : bool
    {
        MYSQL,
        POSTGRES,
    };

    inline constexpr uint32_t CONNECTION_TIMEOUT_SEC = 30;
    inline constexpr uint32_t MAX_BUFFER_SIZE = 64 * 1024 * 1024; // 64MB buffer limit
} // namespace frontend