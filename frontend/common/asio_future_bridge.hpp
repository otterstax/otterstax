// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utility/pipeline_error.hpp"

#include <actor-zeta.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <system_error>
#include <type_traits>
#include <utility>

namespace otterstax {

    namespace asio = boost::asio;

    inline constexpr std::chrono::milliseconds AWAIT_AZ_FUTURE_POLL_STEP{5};
    inline constexpr std::chrono::milliseconds DEFAULT_TIMEOUT{90000};

    // Exception-free polling bridge between actor-zeta futures and asio coroutines.
    // Uses public actor-zeta 1.1.1 API: wait() fast-path (future.hpp:249-251) returns
    // immediately when promise_released, so get() && called AFTER is_ready()==true is a
    // non-blocking move-extract. take_value() and release_future() are noexcept.
    template<typename T>
        requires std::is_nothrow_move_constructible_v<T>
    asio::awaitable<result<T>>
    await_az_future(actor_zeta::unique_future<result<T>> fut,
                    std::chrono::milliseconds timeout) noexcept {
        auto executor = co_await asio::this_coro::executor;
        asio::steady_timer poll(executor);
        const auto deadline = std::chrono::steady_clock::now() + timeout;

        while (!fut.is_ready() && !fut.failed()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                fut.cancel();
                continue;
            }
            poll.expires_after(AWAIT_AZ_FUTURE_POLL_STEP);
            boost::system::error_code ec_wait;
            co_await poll.async_wait(asio::redirect_error(asio::use_awaitable, ec_wait));
        }

        if (fut.failed()) {
            const auto ec = fut.error();
            if (ec == std::errc::operation_canceled) {
                co_return pipeline_error{error_code_t::timeout, error_tag_t::scheduler, "deadline"};
            }
            co_return pipeline_error{error_code_t::internal_error, error_tag_t::scheduler, ec.message()};
        }

        co_return std::move(fut).get();
    }

} // namespace otterstax
