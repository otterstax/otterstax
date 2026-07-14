// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta/detail/future.hpp>

#include <thread>
#include <utility>

// Drive an actor_zeta::unique_future<T> to completion from a plain (non-actor) test thread and return its value
template<typename T>
T drive_until_ready(actor_zeta::unique_future<T> future) {
    auto handle = future.coroutine_handle();
    while (!future.is_ready()) {
        if (handle && !handle.done()) {
            auto* flags = handle.promise().awaited_flags_;
            if (flags && (flags->load(std::memory_order_acquire) & actor_zeta::detail::state_flags::promise_released)) {
                auto* continuation = handle.promise().awaited_continuation_;
                if (continuation) {
                    if (auto cont = continuation->exchange(nullptr, std::memory_order_acq_rel)) {
                        cont.resume();
                        continue;
                    }
                }
            }
        }
        std::this_thread::yield();
    }
    return std::move(future).take_ready();
}
