// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Bridges an actor-zeta unique_future<result<T>> into a boost::asio coroutine.
//
// actor-zeta 1.2.0 futures expose no blocking wait and no completion callback —
// only the non-blocking is_ready()/failed()/take_ready() trio — so we poll with a
// short steady_timer between checks. A frontend handler drives this from a local
// io_context (io.run()) so the handler thread parks on the timer between polls
// instead of busy-spinning.
//
// The future is held BY MOVE (no std::shared_ptr); the bridge never throws —
// timeouts and infra-level future failure (e.g. a closed mailbox /
// operation_canceled) map to a core::error_t whose code tells the two apart.

#pragma once

#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <cassert>
#include <chrono>
#include <memory_resource>
#include <optional>
#include <utility>

namespace otterstax {

    namespace asio = boost::asio;

    inline constexpr std::chrono::milliseconds AWAIT_POLL_STEP{1};
    inline constexpr std::chrono::milliseconds DEFAULT_TIMEOUT{90000};

    // The bridge's own two failure codes. Frontends branch on them to tell "the
    // worker did not answer in time" from "the worker answered with an error",
    // so each must be a code that no Worker result can carry:
    //  - AWAIT_TIMEOUT_CODE: the engine enum has no deadline code, and
    //    `transaction_inactive` is produced neither by the engine nor by any
    //    otterstax connector/manager, which makes it exclusive to the bridge;
    //  - AWAIT_FAILED_CODE: the actor runtime closed the channel before a value
    //    arrived (closed mailbox / cancelled operation) — the same shape as a
    //    broken promise, which the connector barrier also reports as io_error.
    inline constexpr core::error_code_t AWAIT_TIMEOUT_CODE = core::error_code_t::transaction_inactive;
    inline constexpr core::error_code_t AWAIT_FAILED_CODE = core::error_code_t::io_error;

    // Await an actor-zeta future from inside a boost::asio coroutine. Returns the
    // worker's result_wrapper_t<T> on success, or a core::error_t on timeout / infra
    // failure whose message lives on `resource`. `fut` is consumed (moved in).
    template<typename T>
    asio::awaitable<core::result_wrapper_t<T>>
    async_await_future(actor_zeta::unique_future<core::result_wrapper_t<T>> fut,
                       std::pmr::memory_resource* resource,
                       std::chrono::milliseconds timeout = DEFAULT_TIMEOUT) {
        assert(resource != nullptr && "async_await_future: memory resource must not be null");
        auto executor = co_await asio::this_coro::executor;
        asio::steady_timer timer(executor);
        const auto deadline = std::chrono::steady_clock::now() + timeout;

        while (!fut.is_ready() && !fut.failed()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                co_return core::result_wrapper_t<T>{
                    core::error_t{AWAIT_TIMEOUT_CODE, std::pmr::string{"await deadline reached", resource}}};
            }
            timer.expires_after(AWAIT_POLL_STEP);
            boost::system::error_code ec;
            co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        }

        if (fut.failed()) {
            co_return core::result_wrapper_t<T>{
                core::error_t{AWAIT_FAILED_CODE, std::pmr::string{fut.error().message().c_str(), resource}}};
        }
        co_return std::move(fut).take_ready();
    }

    // Drive async_await_future to completion on a private io_context: the
    // calling (handler) thread parks on the poll timer instead of busy-spinning.
    // The result is moved out of the coroutine — never assigned — so an error
    // message keeps the resource it was created on.
    template<typename T>
    core::result_wrapper_t<T> await_future_blocking(actor_zeta::unique_future<core::result_wrapper_t<T>> fut,
                                                    std::pmr::memory_resource* resource,
                                                    std::chrono::milliseconds timeout = DEFAULT_TIMEOUT) {
        std::optional<core::result_wrapper_t<T>> out;
        asio::io_context io;
        asio::co_spawn(
            io,
            [&]() -> asio::awaitable<void> {
                out.emplace(co_await async_await_future<T>(std::move(fut), resource, timeout));
            },
            asio::detached);
        io.run();
        assert(out.has_value() && "await_future_blocking: the bridge always completes");
        return std::move(*out);
    }

} // namespace otterstax
