// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utility/pmr_text.hpp"

#include <chrono>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>

namespace pg {

    // The startup config descriptor of one alias (std::string: built once from config.yaml,
    // never per statement).
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

        // libpq's conninfo string, with every value single-quoted and ' and \ escaped. Besides
        // connect_timeout it sets keepalives (a vanished peer) and tcp_user_timeout (Linux, unacknowledged
        // data). Built on every connect, on the driver's resource.
        std::pmr::string connection_string(std::pmr::memory_resource* resource) const {
            std::pmr::string out(resource);
            const auto quoted = [&out](std::string_view value) {
                out += '\'';
                for (char c : value) {
                    if (c == '\'' || c == '\\') {
                        out += '\\';
                    }
                    out += c;
                }
                out += '\'';
            };
            out += "host=";
            quoted(host);
            out += " port=";
            otterstax::append_number(out, port);
            out += " dbname=";
            quoted(database);
            out += " user=";
            quoted(username);
            out += " password=";
            quoted(password);
            out += " connect_timeout=";
            otterstax::append_number(out, connect_timeout.count());
            out += " keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3"
                   " tcp_user_timeout=30000";
            return out;
        }
    };

} // namespace pg
