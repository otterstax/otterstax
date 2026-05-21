// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <string>
#include <cstdint>

namespace pg {

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
        uint16_t port{5432};
        std::string username;
        std::string password;
        std::string database;
        std::string schema{"public"};  // PostgreSQL schema, defaults to "public"
        std::string table;             // Table name for this connection

        // Reconnect settings
        uint32_t reconnect_delay_ms{200};
        uint32_t max_reconnect_attempts{3};

        std::string connection_string() const {
            return "host=" + host +
                   " port=" + std::to_string(port) +
                   " dbname=" + database +
                   " user=" + username +
                   " password=" + password;
        }
    };

} // namespace pg
