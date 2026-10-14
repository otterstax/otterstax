// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Bridges an actor-zeta unique_future<result<T>> into a boost::asio coroutine.
//
// actor-zeta 2.0 futures expose no blocking wait and no completion callback —
// only the non-blocking is_ready()/failed()/take_ready() trio — so we poll with a
// short steady_timer between checks. A frontend handler drives this from a local
// io_context (io.run()) so the handler thread parks on the timer between polls
// instead of busy-spinning.
//
// The future is held BY MOVE (no std::shared_ptr); the bridge never throws.
// What it answers is an awaited_t: the worker's own result, or the bridge's two
// failures — the deadline reached, the channel closed by the actor runtime —
// told apart by awaited_t::status(), never by an error code (every code may be
// one the engine or a backend answers with).

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
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace otterstax {

    namespace asio = boost::asio;

    inline constexpr std::chrono::milliseconds AWAIT_POLL_STEP{1};
    inline constexpr std::chrono::milliseconds DEFAULT_TIMEOUT{90000};

    // How an await ended.
    enum class await_status_t : std::uint8_t
    {
        // The worker answered: the result is its own, success or error.
        answered,
        // The deadline passed first. The worker goes on (the engine has no
        // cancel) and its answer is never read.
        timed_out,
        // No value arrived: the actor runtime closed the channel (a closed
        // mailbox, a cancelled operation), or an exception escaped a coroutine
        // of the chain that runs the statement (the engine's executor among
        // them) — the shape of a broken promise. The statement got no answer.
        broken,
    };

    // A bridge's answer: the worker's result_wrapper_t<T> — or, when the bridge
    // gave up, an error_t whose message says why — plus how the await ended.
    // The status is the only way to tell the bridge's own failures from the
    // worker's: the error code of a timed-out or broken await is incidental.
    template<typename T>
    class awaited_t : public core::result_wrapper_t<T> {
    public:
        awaited_t(core::result_wrapper_t<T> result, await_status_t status)
            : core::result_wrapper_t<T>(std::move(result))
            , status_(status) {}

        [[nodiscard]] await_status_t status() const noexcept { return status_; }
        // The bridge stopped waiting: the statement may still be running.
        [[nodiscard]] bool timed_out() const noexcept { return status_ == await_status_t::timed_out; }

    private:
        await_status_t status_;
    };

    // Why an open transaction block must be failed behind a statement the
    // bridge gave up on — timed out (it runs on: the engine has no cancel) or
    // broken (the session never settled it) — so a COMMIT cannot publish what
    // the client was told failed; std::nullopt for an answered statement (the
    // session's own error, if any, has failed the block already). Every frontend
    // reports it through query_session_t::fail_transaction.
    [[nodiscard]] inline std::optional<std::string> unanswered_block_failure(await_status_t status) {
        switch (status) {
            case await_status_t::answered:
                return std::nullopt;
            case await_status_t::timed_out:
                return std::string{"the statement exceeded the execution limit"};
            case await_status_t::broken:
                return std::string{"the statement failed without an answer"};
        }
        return std::nullopt;
    }

    // The bridge's own failures, the message on `resource`.
    template<typename T>
    awaited_t<T> await_timed_out(std::pmr::memory_resource* resource) {
        return awaited_t<T>{core::error_t{core::error_code_t::other_error,
                                          std::pmr::string{"await deadline reached", resource}},
                            await_status_t::timed_out};
    }

    // `ec` is the failed future's code. actor-zeta stamps errc::interrupted on
    // a future whose producer died by an exception (promise::exception /
    // unhandled_exception), and its message ("Interrupted system call") says
    // nothing a client could read: the message names what happened instead.
    template<typename T>
    awaited_t<T> await_broken(std::pmr::memory_resource* resource, std::error_code ec) {
        const std::string why =
            ec == std::errc::interrupted
                ? std::string{"the statement failed without an answer: an exception escaped the actor running it"}
                : "the statement failed without an answer: " + ec.message();
        return awaited_t<T>{core::error_t{core::error_code_t::other_error, std::pmr::string{why.c_str(), resource}},
                            await_status_t::broken};
    }

    // Await an actor-zeta future from inside a boost::asio coroutine. Answers
    // the worker's result_wrapper_t<T> (status answered), or the bridge's
    // timeout / broken channel, whose message lives on `resource`. `fut` is
    // consumed (moved in).
    template<typename T>
    asio::awaitable<awaited_t<T>> async_await_future(actor_zeta::unique_future<core::result_wrapper_t<T>> fut,
                                                     std::pmr::memory_resource* resource,
                                                     std::chrono::milliseconds timeout = DEFAULT_TIMEOUT) {
        assert(resource != nullptr && "async_await_future: memory resource must not be null");
        auto executor = co_await asio::this_coro::executor;
        asio::steady_timer timer(executor);
        const auto deadline = std::chrono::steady_clock::now() + timeout;

        while (!fut.is_ready() && !fut.failed()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                co_return await_timed_out<T>(resource);
            }
            timer.expires_after(AWAIT_POLL_STEP);
            boost::system::error_code ec;
            co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        }

        if (fut.failed()) {
            co_return await_broken<T>(resource, fut.error());
        }
        co_return awaited_t<T>{std::move(fut).take_ready(), await_status_t::answered};
    }

    // Drive async_await_future to completion on a private io_context: the
    // calling (handler) thread parks on the poll timer instead of busy-spinning.
    // The result is moved out of the coroutine — never assigned — so an error
    // message keeps the resource it was created on.
    template<typename T>
    awaited_t<T> await_future_blocking(actor_zeta::unique_future<core::result_wrapper_t<T>> fut,
                                       std::pmr::memory_resource* resource,
                                       std::chrono::milliseconds timeout = DEFAULT_TIMEOUT) {
        std::optional<awaited_t<T>> out;
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
