// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 OtterStax
#include "utility/actor_mailbox.hpp"
#include <catch2/catch_all.hpp>
#include <atomic>
#include <chrono>
#include <thread>

namespace {
    template<otterstax::actor_execution Execution>
    class probe final : public actor_zeta::actor::actor_mixin<probe<Execution>> {
    public:
        using is_cooperative_actor_type = void;
        template<class T> using unique_future = actor_zeta::unique_future<T>;
        explicit probe(std::pmr::memory_resource* resource) : resource_(resource) {}
        auto resource() const { return resource_; }
        actor_zeta::unique_future<int> chain(actor_zeta::unique_future<int> first,
                                             actor_zeta::unique_future<int> second,
                                             actor_zeta::promise<bool> resumed) {
            const auto thread = std::this_thread::get_id();
            const auto a = co_await std::move(first);
            resumed.set_value(true);
            const auto b = co_await std::move(second);
            co_return thread == std::this_thread::get_id() ? a + b : -1;
        }
        actor_zeta::unique_future<int> ping() { co_return 7; }
        using dispatch_traits = actor_zeta::dispatch_traits<&probe::chain, &probe::ping>;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* message) {
            if (message->command() == actor_zeta::msg_id<probe, &probe::chain>) {
                co_await actor_zeta::dispatch(this, &probe::chain, message);
            } else {
                co_await actor_zeta::dispatch(this, &probe::ping, message);
            }
        }
        auto enqueue_impl(actor_zeta::mailbox::message_ptr message) { return mailbox_.enqueue(std::move(message)); }
    private:
        std::pmr::memory_resource* resource_;
        otterstax::actor_mailbox<probe, Execution> mailbox_{*this};
    };

    template<class T>
    bool ready(actor_zeta::unique_future<T>& future) {
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!future.is_ready() && std::chrono::steady_clock::now() < end) std::this_thread::yield();
        return future.is_ready();
    }
}

TEST_CASE("actor mailbox resumes successive awaits without a new message", "[actor-mailbox]") {
    auto* resource = std::pmr::new_delete_resource();
    using actor_t = probe<otterstax::actor_execution::interleaved>;
    auto actor = actor_zeta::spawn<actor_t>(resource);
    actor_zeta::promise<int> first(resource), second(resource);
    actor_zeta::promise<bool> resumed(resource);
    auto signal = resumed.get_future();
    auto [scheduled, result] = actor_zeta::send(actor->address(), &actor_t::chain,
        first.get_future(), second.get_future(), std::move(resumed));
    first.set_value(10);
    CHECK(ready(signal));
    CHECK_FALSE(result.is_ready());
    second.set_value(20);
    REQUIRE(ready(result));
    REQUIRE(std::move(result).take_ready() == 30);
}

TEST_CASE("interleaved actor services another request while awaiting a result", "[actor-mailbox]") {
    auto* resource = std::pmr::new_delete_resource();
    using actor_t = probe<otterstax::actor_execution::interleaved>;
    auto actor = actor_zeta::spawn<actor_t>(resource);
    actor_zeta::promise<int> first(resource), second(resource);
    actor_zeta::promise<bool> resumed(resource);
    auto signal = resumed.get_future();
    auto [scheduled, pending] = actor_zeta::send(actor->address(), &actor_t::chain,
        first.get_future(), second.get_future(), std::move(resumed));
    first.set_value(10);
    CHECK(ready(signal));
    auto [ping_scheduled, ping] = actor_zeta::send(actor->address(), &actor_t::ping);
    const bool progressed = ready(ping);
    CHECK_FALSE(pending.is_ready());
    second.set_value(20);
    REQUIRE(ready(pending));
    CHECK(progressed);
    REQUIRE(ready(ping));
    CHECK(std::move(ping).take_ready() == 7);
    CHECK(std::move(pending).take_ready() == 30);
}

TEST_CASE("serial actor preserves order and drains accepted messages on destruction", "[actor-mailbox]") {
    auto* resource = std::pmr::new_delete_resource();
    using actor_t = probe<otterstax::actor_execution::serial>;
    auto actor = actor_zeta::spawn<actor_t>(resource);
    actor_zeta::promise<int> first(resource), second(resource);
    actor_zeta::promise<bool> resumed(resource);
    auto signal = resumed.get_future();
    auto [scheduled, pending] = actor_zeta::send(actor->address(), &actor_t::chain,
        first.get_future(), second.get_future(), std::move(resumed));
    first.set_value(10);
    CHECK(ready(signal));
    auto [ping_scheduled, ping] = actor_zeta::send(actor->address(), &actor_t::ping);
    CHECK_FALSE(ping.is_ready());
    second.set_value(20);
    actor.reset();
    REQUIRE(pending.is_ready());
    REQUIRE(ping.is_ready());
    CHECK(std::move(pending).take_ready() == 30);
    CHECK(std::move(ping).take_ready() == 7);
}
