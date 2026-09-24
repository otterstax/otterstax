// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Contract of frontend/common/asio_future_bridge.hpp, the only way a frontend
// picks up an actor result:
//
//   ok      -> the waiter really blocks until the producer answers
//   timeout -> it gives up at the deadline, reports AWAIT_TIMEOUT_CODE
//   error   -> a producer-side failure wakes it early and passes through as-is
//   failed  -> actor-zeta's own failure channel (`promise::error`, also the
//              cancellation channel) wakes it early as AWAIT_FAILED_CODE
//
// Every message the bridge creates lives on the resource the caller hands in.
// Catch2 assertions are not thread-safe: the producer threads only record what
// they observed, and the test thread asserts after joining them.

#include "frontend/common/asio_future_bridge.hpp"
#include "utility/tsan_helper.hpp"

#include <catch2/catch_all.hpp>

#include <actor-zeta.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory_resource>
#include <system_error>
#include <thread>

using namespace std::chrono_literals;

namespace {

    using payload_t = std::pmr::string;
    using result_t = core::result_wrapper_t<payload_t>;
    using future_t = actor_zeta::unique_future<result_t>;

    // Drive the bridge exactly the way a frontend handler does.
    result_t await_on_io(future_t fut, std::chrono::milliseconds timeout, std::pmr::memory_resource* resource) {
        return otterstax::await_future_blocking<payload_t>(std::move(fut), resource, timeout);
    }

    int64_t elapsed_ms_since(std::chrono::steady_clock::time_point start) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    }

} // namespace

TEST_CASE("asio_future_bridge: ok") {
    // Producer and consumer sit on different threads, so the resource must be
    // the synchronized flavour.
    std::pmr::synchronized_pool_resource resource;
    actor_zeta::promise<result_t> producer{&resource};
    auto fut = producer.get_future();
    REQUIRE_FALSE(fut.is_ready());
    REQUIRE_FALSE(fut.failed());

    const auto start = std::chrono::steady_clock::now();
    auto worker = std::jthread([&producer, &resource]() {
        std::this_thread::sleep_for(500ms);
        producer.set_value(result_t{payload_t{"Hello, World!", &resource}});
    });

    auto result = await_on_io(std::move(fut), 5000ms, &resource);
    const auto duration = elapsed_ms_since(start);

    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value() == "Hello, World!");
    // Really waited for the producer, and woke on it rather than on the deadline.
    REQUIRE(duration >= 500);
    REQUIRE(duration < 600);
}

TEST_CASE("asio_future_bridge: timeout") {
    if constexpr (TSAN_ENABLED) {
        return; // skip test, TSAN considers synchronization via sleep as data race, however, this is a valid test case
    }

    std::pmr::synchronized_pool_resource resource;
    // Never satisfied: the promise outlives the wait, so the future stays
    // pending rather than breaking.
    actor_zeta::promise<result_t> producer{&resource};
    auto fut = producer.get_future();
    REQUIRE_FALSE(fut.is_ready());

    std::atomic<bool> gave_up{false};
    std::atomic<bool> consumer_was_gone{false};
    const auto start = std::chrono::steady_clock::now();
    auto worker = std::jthread([&gave_up, &consumer_was_gone, &producer, &resource]() {
        // The late producer must find the consumer already gone.
        std::this_thread::sleep_for(1000ms);
        consumer_was_gone.store(gave_up.load());
        if (!gave_up.load()) {
            producer.set_value(result_t{payload_t{"Hello, World!", &resource}});
        }
    });

    auto result = await_on_io(std::move(fut), 200ms, &resource);
    const auto duration = elapsed_ms_since(start);
    gave_up.store(true);
    worker.join();
    REQUIRE(consumer_was_gone.load());

    REQUIRE(result.has_error());
    // The frontends branch on the code to report a timeout rather than a query
    // error, so it is part of the contract — the message is not.
    REQUIRE(result.error().type == otterstax::AWAIT_TIMEOUT_CODE);
    REQUIRE(result.error().what.get_allocator().resource() == &resource);
    REQUIRE(duration >= 200);
    REQUIRE(duration < 260);
}

TEST_CASE("asio_future_bridge: error") {
    if constexpr (TSAN_ENABLED) {
        return; // skip test, TSAN considers synchronization via sleep as data race, however, this is a valid test case
    }

    std::pmr::synchronized_pool_resource resource;
    actor_zeta::promise<result_t> producer{&resource};
    auto fut = producer.get_future();
    REQUIRE_FALSE(fut.is_ready());

    const auto start = std::chrono::steady_clock::now();
    auto worker = std::jthread([&producer, &resource]() {
        std::this_thread::sleep_for(100ms);
        // A failure travels as a VALUE — no exception crosses the thread boundary.
        producer.set_value(result_t{
            core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{"Some error occurred", &resource}}});
    });

    auto result = await_on_io(std::move(fut), 2000ms, &resource);
    const auto duration = elapsed_ms_since(start);

    REQUIRE(result.has_error());
    // The worker's error passes through untouched: same code, same message, and
    // the message still lives on the producer's resource (moved, never copied).
    REQUIRE(result.error().type == core::error_code_t::sql_parse_error);
    REQUIRE(result.error().what == "Some error occurred");
    REQUIRE(result.error().what.get_allocator().resource() == &resource);
    // Woke on the error at ~100 ms, well before the 2000 ms deadline.
    REQUIRE(duration >= 100);
    REQUIRE(duration < 180);
}

TEST_CASE("asio_future_bridge: infra failure") {
    if constexpr (TSAN_ENABLED) {
        return; // skip test, TSAN considers synchronization via sleep as data race, however, this is a valid test case
    }

    std::pmr::synchronized_pool_resource resource;
    actor_zeta::promise<result_t> producer{&resource};
    auto fut = producer.get_future();
    REQUIRE_FALSE(fut.is_ready());
    REQUIRE_FALSE(fut.failed());

    const auto start = std::chrono::steady_clock::now();
    auto worker = std::jthread([&producer]() {
        std::this_thread::sleep_for(100ms);
        // actor-zeta's own channel: a closed mailbox / cancelled operation never
        // produces a result_wrapper_t at all. The bridge must not sit on it
        // until the deadline.
        producer.error(std::make_error_code(std::errc::operation_canceled));
    });

    auto result = await_on_io(std::move(fut), 2000ms, &resource);
    const auto duration = elapsed_ms_since(start);

    REQUIRE(result.has_error());
    REQUIRE(result.error().type == otterstax::AWAIT_FAILED_CODE);
    REQUIRE_FALSE(result.error().what.empty());
    REQUIRE(result.error().what.get_allocator().resource() == &resource);
    REQUIRE(duration >= 100);
    REQUIRE(duration < 180);
}

TEST_CASE("asio_future_bridge: timeout and infra failure are distinct codes") {
    // A frontend maps the timeout to its protocol's "query cancelled" error and
    // everything else to a generic failure, which only works while the two
    // bridge codes differ from each other.
    STATIC_REQUIRE(otterstax::AWAIT_TIMEOUT_CODE != otterstax::AWAIT_FAILED_CODE);
    STATIC_REQUIRE(otterstax::AWAIT_TIMEOUT_CODE != core::error_code_t::other_error);
    STATIC_REQUIRE(otterstax::AWAIT_TIMEOUT_CODE != core::error_code_t::none);
}
