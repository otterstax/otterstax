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
// Codex rules: the future is held BY MOVE (no std::shared_ptr, rule 14); the
// bridge never throws — timeouts and infra-level future failure (e.g. a closed
// mailbox / operation_canceled) map to a core::error_t (rules 2 & 9).

#pragma once

#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <utility>

namespace otterstax {

    namespace asio = boost::asio;

    inline constexpr std::chrono::milliseconds AWAIT_POLL_STEP{1};
    inline constexpr std::chrono::milliseconds DEFAULT_TIMEOUT{90000};

    // Await an actor-zeta future from inside a boost::asio coroutine. Returns the
    // worker's result_wrapper_t<T> on success, or a core::error_t on timeout / infra
    // failure. `fut` is consumed (moved in).
    template<typename T>
    asio::awaitable<core::result_wrapper_t<T>>
    async_await_future(actor_zeta::unique_future<core::result_wrapper_t<T>> fut,
                       std::chrono::milliseconds timeout = DEFAULT_TIMEOUT) {
        auto executor = co_await asio::this_coro::executor;
        asio::steady_timer timer(executor);
        const auto deadline = std::chrono::steady_clock::now() + timeout;

        while (!fut.is_ready() && !fut.failed()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                co_return core::result_wrapper_t<T>{
                    core::error_t{core::error_code_t::other_error,
                                  std::pmr::string{"timeout: await deadline reached"}}};
            }
            timer.expires_after(AWAIT_POLL_STEP);
            boost::system::error_code ec;
            co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        }

        if (fut.failed()) {
            co_return core::result_wrapper_t<T>{
                core::error_t{core::error_code_t::other_error,
                              std::pmr::string{fut.error().message().c_str()}}};
        }
        co_return std::move(fut).take_ready();
    }

} // namespace otterstax
