// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Bridges an actor-zeta unique_future<result<T>> into an asio-grpc coroutine.
//
// actor-zeta 1.2.0 futures expose no blocking wait and no completion callback —
// only the non-blocking is_ready()/failed()/take_ready() trio — so we poll with
// an agrpc::Alarm between checks. Exponential backoff (16µs -> 1ms cap) keeps
// latency low for fast paths while yielding the gRPC completion-queue thread for
// longer-running work. The worker's result_wrapper_t<T> is returned on success;
// timeouts and infra-level future failure (e.g. a closed mailbox /
// operation_canceled) map to a core::error_t.
//
// Codex rules: the future is held BY MOVE (no std::shared_ptr, rule 14); the
// bridge never throws (rules 2 & 9); the alarm.wait() bool result is implicitly
// discarded (no (void) cast, rule 8).

#pragma once

#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>

#include <agrpc/alarm.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <chrono>
#include <utility>

namespace frontend::spark {

    namespace asio = boost::asio;

    inline constexpr auto AWAIT_INITIAL_DELAY = std::chrono::microseconds(16);
    inline constexpr auto AWAIT_MAX_DELAY = std::chrono::microseconds(1000);  // 1ms cap
    inline constexpr auto AWAIT_DEFAULT_TIMEOUT = std::chrono::milliseconds(90000);

    // Await an actor-zeta future from inside an asio-grpc coroutine running on a
    // agrpc::GrpcExecutor. Returns the worker's result_wrapper_t<T> on success, or
    // a core::error_t on timeout / infra failure. `fut` is consumed (moved in).
    template<typename T>
    asio::awaitable<core::result_wrapper_t<T>, agrpc::GrpcExecutor>
    await_future(actor_zeta::unique_future<core::result_wrapper_t<T>> fut,
                 std::chrono::milliseconds timeout = AWAIT_DEFAULT_TIMEOUT) {
        agrpc::Alarm alarm{co_await asio::this_coro::executor};
        auto delay = AWAIT_INITIAL_DELAY;
        const auto deadline = std::chrono::steady_clock::now() + timeout;

        while (!fut.is_ready() && !fut.failed()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                co_return core::result_wrapper_t<T>{
                    core::error_t{core::error_code_t::other_error,
                                  std::pmr::string{"timeout: await deadline reached"}}};
            }
            co_await alarm.wait(
                std::chrono::system_clock::now() +
                    std::chrono::duration_cast<std::chrono::system_clock::duration>(delay));
            delay = std::min(delay * 2, AWAIT_MAX_DELAY);
        }

        if (fut.failed()) {
            co_return core::result_wrapper_t<T>{
                core::error_t{core::error_code_t::other_error,
                              std::pmr::string{fut.error().message().c_str()}}};
        }
        co_return std::move(fut).take_ready();
    }

}  // namespace frontend::spark
