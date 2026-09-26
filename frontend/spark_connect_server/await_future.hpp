// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Bridges an actor-zeta unique_future<result<T>> into an asio-grpc coroutine —
// the gRPC form of frontend/common/asio_future_bridge.hpp. The Spark handlers
// run on an agrpc::GrpcExecutor: their awaitables cannot co_await the bridge's
// any_io_executor coroutine, and a GrpcContext drives only its completion
// queue, so the wait between polls is an agrpc::Alarm rather than a
// steady_timer. Exponential backoff (16µs -> 1ms cap) keeps latency low for
// fast paths while yielding the completion-queue thread for longer work.
//
// The contract is the bridge's: the worker's result_wrapper_t<T> on success,
// AWAIT_TIMEOUT_CODE at the deadline, AWAIT_FAILED_CODE when the actor runtime
// closed the channel, the error text on the caller's resource. The future is
// held BY MOVE; nothing here throws.

#pragma once

#include "frontend/common/asio_future_bridge.hpp"

#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>

#include <agrpc/alarm.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <memory_resource>
#include <utility>

namespace frontend::spark {

    namespace asio = boost::asio;

    inline constexpr auto AWAIT_INITIAL_DELAY = std::chrono::microseconds(16);
    inline constexpr auto AWAIT_MAX_DELAY = std::chrono::microseconds(1000); // 1ms cap

    // Await an actor-zeta future from inside an asio-grpc coroutine running on
    // an agrpc::GrpcExecutor. `fut` is consumed (moved in).
    template<typename T>
    asio::awaitable<core::result_wrapper_t<T>, agrpc::GrpcExecutor>
    await_future(actor_zeta::unique_future<core::result_wrapper_t<T>> fut,
                 std::pmr::memory_resource* resource,
                 std::chrono::milliseconds timeout = otterstax::DEFAULT_TIMEOUT) {
        assert(resource != nullptr && "await_future: memory resource must not be null");
        agrpc::Alarm alarm{co_await asio::this_coro::executor};
        auto delay = AWAIT_INITIAL_DELAY;
        const auto deadline = std::chrono::steady_clock::now() + timeout;

        while (!fut.is_ready() && !fut.failed()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                co_return core::result_wrapper_t<T>{
                    core::error_t{otterstax::AWAIT_TIMEOUT_CODE, std::pmr::string{"await deadline reached", resource}}};
            }
            co_await alarm.wait(std::chrono::system_clock::now() +
                                std::chrono::duration_cast<std::chrono::system_clock::duration>(delay));
            delay = std::min(delay * 2, AWAIT_MAX_DELAY);
        }

        if (fut.failed()) {
            co_return core::result_wrapper_t<T>{
                core::error_t{otterstax::AWAIT_FAILED_CODE, std::pmr::string{fut.error().message().c_str(), resource}}};
        }
        co_return std::move(fut).take_ready();
    }

} // namespace frontend::spark
