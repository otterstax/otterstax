// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Helpers shared by the system tests that build part of an actor graph by
// hand rather than through session_stack_owner: polling a cross-actor future
// from a plain thread.

#pragma once

#include "utility/logger.hpp"

#include <catch2/catch_all.hpp>

#include <chrono>
#include <thread>
#include <utility>

namespace otterstax::test {

    // Polls `future` every 10 ms until it is ready or `timeout` has passed and
    // reports whether it settled. It asserts nothing, so a thread other than the
    // test thread may call it (Catch2 assertions are not thread-safe).
    template<typename Future>
    bool poll_until_ready(Future& future, std::chrono::milliseconds timeout) {
        using namespace std::chrono_literals;
        for (auto waited = 0ms; waited < timeout && !future.is_ready(); waited += 10ms) {
            std::this_thread::sleep_for(10ms);
        }
        return future.is_ready();
    }

    // Test-thread form: the future must settle within 10 s.
    template<typename Future>
    void wait_until_ready(Future& future) {
        using namespace std::chrono_literals;
        REQUIRE(poll_until_ready(future, 10s));
    }

} // namespace otterstax::test
