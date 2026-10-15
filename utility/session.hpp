// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include <components/session/session.hpp>
#include <atomic>
#include <limits>
#include <stdexcept>

using session_id = components::session::session_id_t;
using session_hash_t = std::size_t;

// A hash is a bucket selector, not a unique request ID. Prepared-statement
// handles are process-local and must never be reused while the process lives.
inline session_hash_t next_request_id() {
    static std::atomic<session_hash_t> next{1};
    auto value = next.load(std::memory_order_relaxed);
    for (;;) {
        if (value == std::numeric_limits<session_hash_t>::max()) {
            throw std::overflow_error("request ID space exhausted");
        }
        if (next.compare_exchange_weak(value, value + 1, std::memory_order_relaxed)) return value;
    }
}

enum class flightsql_session_type : uint8_t
{
    GET_FLIGHT_INFO,
    DO_GET,
};
