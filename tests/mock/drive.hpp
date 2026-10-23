// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>

#include <cassert>
#include <chrono>
#include <memory_resource>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

namespace otterstax::test {

    // What the driving behavior's coroutine frame is allocated from: actor-zeta takes it
    // from the first argument's resource().
    struct drive_frame_t {
        std::pmr::memory_resource* resource_;
        std::pmr::memory_resource* resource() const noexcept { return resource_; }
    };

    namespace detail {
        template<typename T>
        actor_zeta::behavior_t
        await_into(drive_frame_t* frame, actor_zeta::unique_future<T> future, std::optional<T>* out) {
            out->emplace(co_await std::move(future));
        }
    } // namespace detail

    // Runs `future` to completion on the calling thread, the way an actor_mixin's
    // enqueue_impl does (CatalogManager): a behavior awaits it, and while the deepest
    // awaited future is not ready the thread yields; once it is, its continuation is
    // resumed, which unwinds the coroutine chain. This is how a test calls a backend's
    // coroutines (db::backend_t::execute / describe / discover) synchronously, with the
    // statements answered by pool threads. Throws std::runtime_error after `timeout`.
    template<typename T>
    T drive(std::pmr::memory_resource* resource,
            actor_zeta::unique_future<T> future,
            std::chrono::milliseconds timeout = std::chrono::seconds(60)) {
        drive_frame_t frame{resource};
        std::optional<T> out;
        auto behavior = detail::await_into(&frame, std::move(future), &out);
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (behavior.is_busy()) {
            if (behavior.is_awaited_ready()) {
                if (auto cont = behavior.take_awaited_continuation()) {
                    cont.resume();
                }
            } else {
                if (std::chrono::steady_clock::now() > deadline) {
                    throw std::runtime_error("drive: the awaited future did not settle in time");
                }
                std::this_thread::yield();
            }
        }
        assert(out.has_value());
        return std::move(*out);
    }

} // namespace otterstax::test
