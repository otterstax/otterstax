// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <string>
#include <cstdint>

namespace chc {

    enum class Status
    {
        Created,
        Connected,
        Disconnected,
        Working,
        Closed
    };

    struct connect_params {
        std::string host;
        uint16_t port{9000};
        std::string username{"default"};
        std::string password;
        std::string database{"default"};
        std::string table;

        uint32_t reconnect_delay_ms{200};
        uint32_t max_reconnect_attempts{3};
    };

} // namespace chc
