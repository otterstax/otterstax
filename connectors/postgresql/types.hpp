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

        // Per-attempt connect deadline (seconds); without it PQconnectdb can
        // block the calling (HTTP API) thread until the OS TCP timeout.
        uint32_t connect_timeout_s{10};

        // TCP keepalive on the connection: a backend host that went away (no FIN, no RST) is found
        // after keepalives_idle + keepalives_count * keepalives_interval seconds of silence — the
        // blocking PQexec then fails instead of waiting for the kernel's default of about two hours.
        // A live server running a long statement answers the probes, so the statement is not cut.
        uint32_t keepalives_idle_s{30};
        uint32_t keepalives_interval_s{10};
        uint32_t keepalives_count{3};

        std::string connection_string() const {
            return "host=" + host +
                   " port=" + std::to_string(port) +
                   " dbname=" + database +
                   " user=" + username +
                   " password=" + password +
                   " connect_timeout=" + std::to_string(connect_timeout_s) +
                   " keepalives=1 keepalives_idle=" + std::to_string(keepalives_idle_s) +
                   " keepalives_interval=" + std::to_string(keepalives_interval_s) +
                   " keepalives_count=" + std::to_string(keepalives_count);
        }
    };

} // namespace pg
