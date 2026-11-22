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

    // A read the client is in the middle of: the handshake / startup, a message's body.
    inline constexpr uint32_t CONNECTION_TIMEOUT_SEC = 30;
    // The wait for the next message of a connection past its handshake / startup: MySQL's wait_timeout default,
    // so a client's pool keeps an idle connection as it would against MySQL.
    inline constexpr uint32_t IDLE_TIMEOUT_SEC = 28800;
    // Pause of the accept chain after building a connection threw.
    inline constexpr uint32_t ACCEPT_RETRY_DELAY_MS = 100;
    inline constexpr uint32_t MAX_BUFFER_SIZE = 64 * 1024 * 1024; // 64MB buffer limit
} // namespace frontend