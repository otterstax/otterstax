// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

namespace pg {

    struct connect_params {
        std::string host;
        uint16_t port{5432};
        std::string username;
        std::string password;
        std::string database;
        // Bounds the connect (libpq's connect_timeout, whole seconds).
        std::chrono::seconds connect_timeout{10};
        // Client-side deadline per statement: blocking PQexec has none, so the driver waits
        // for the answer with poll() against it (driver_t::send).
        std::chrono::milliseconds statement_timeout{90'000};

        // libpq's conninfo string. Every value is single-quoted with ' and \ escaped, so a
        // password with a space or a quote survives. Besides connect_timeout it sets the
        // transport bounds libpq offers: keepalives notice a peer that vanished while we wait
        // for an answer, and tcp_user_timeout (Linux) bounds data the peer never
        // acknowledges.
        std::string connection_string() const {
            const auto quoted = [](std::string_view value) {
                std::string out = "'";
                for (char c : value) {
                    if (c == '\'' || c == '\\') {
                        out += '\\';
                    }
                    out += c;
                }
                return out + "'";
            };
            return "host=" + quoted(host) + " port=" + std::to_string(port) + " dbname=" + quoted(database) +
                   " user=" + quoted(username) + " password=" + quoted(password) +
                   " connect_timeout=" + std::to_string(connect_timeout.count()) +
                   " keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3" +
                   " tcp_user_timeout=30000";
        }
    };

} // namespace pg
