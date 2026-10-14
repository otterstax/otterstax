// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// conn::connection_pool_t over a fake connection: checkout / return, the
// one-owner-at-a-time guarantee under concurrency, waiting with a timeout,
// and closing with connections still checked out.

#include "connectors/common/connection_pool.hpp"

#include <catch2/catch_all.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <set>
#include <thread>
#include <vector>

namespace {

    using namespace std::chrono_literals;

    struct fake_connection {
        explicit fake_connection(int id, std::atomic<int>* destroyed = nullptr)
            : id(id)
            , destroyed(destroyed) {}
        ~fake_connection() {
            if (destroyed) {
                ++*destroyed;
            }
        }
        int id;
        std::atomic<int>* destroyed;
        std::atomic<int> owners{0};
    };

    using pool_t = conn::connection_pool_t<fake_connection>;

    std::vector<std::unique_ptr<fake_connection>> make_connections(int n, std::atomic<int>* destroyed = nullptr) {
        std::vector<std::unique_ptr<fake_connection>> connections;
        for (int i = 0; i < n; ++i) {
            connections.push_back(std::make_unique<fake_connection>(i, destroyed));
        }
        return connections;
    }

} // namespace

TEST_CASE("connection_pool: hands out every connection once, then nothing until one is returned") {
    pool_t pool{make_connections(3)};
    REQUIRE(pool.size() == 3);
    REQUIRE(pool.idle() == 3);

    auto a = pool.checkout_for(0ms);
    auto b = pool.checkout_for(0ms);
    auto c = pool.checkout_for(0ms);
    REQUIRE(a);
    REQUIRE(b);
    REQUIRE(c);
    REQUIRE(a->get()->id == 0); // the first checkout gets the first connection
    REQUIRE(std::set<int>{(*a)->id, (*b)->id, (*c)->id}.size() == 3);
    REQUIRE(pool.idle() == 0);
    REQUIRE(pool.in_use() == 3);
    REQUIRE_FALSE(pool.checkout_for(0ms));

    const int returned = (*b)->id;
    b->release();
    REQUIRE_FALSE(*b);
    REQUIRE(pool.idle() == 1);
    auto again = pool.checkout_for(0ms);
    REQUIRE(again);
    REQUIRE((*again)->id == returned);
}

TEST_CASE("connection_pool: a handle returns its connection when destroyed, and a moved-from handle does not") {
    pool_t pool{make_connections(1)};
    {
        auto handle = pool.checkout_for(0ms);
        REQUIRE(handle);
        pool_t::handle_t moved = std::move(*handle);
        REQUIRE_FALSE(*handle);
        REQUIRE(moved);
        handle.reset(); // destroying the moved-from handle returns nothing
        REQUIRE(pool.idle() == 0);
    }
    REQUIRE(pool.idle() == 1);
}

TEST_CASE("connection_pool: checkout_for waits for a returned connection") {
    pool_t pool{make_connections(1)};
    auto held = pool.checkout_for(0ms);
    REQUIRE(held);

    auto waiter = std::async(std::launch::async, [&] { return pool.checkout_for(5s).has_value(); });
    std::this_thread::sleep_for(50ms);
    REQUIRE(waiter.wait_for(0ms) == std::future_status::timeout);
    held->release();
    REQUIRE(waiter.get());
}

TEST_CASE("connection_pool: checkout_for gives up after its timeout") {
    pool_t pool{make_connections(1)};
    auto held = pool.checkout_for(0ms);
    REQUIRE(held);

    const auto started = std::chrono::steady_clock::now();
    auto none = pool.checkout_for(30ms);
    REQUIRE_FALSE(none);
    REQUIRE(std::chrono::steady_clock::now() - started >= 30ms);
}

TEST_CASE("connection_pool: concurrent users never share a connection and never exceed the pool size") {
    constexpr int pool_size = 3;
    constexpr int threads = 8;
    constexpr int rounds = 200;
    pool_t pool{make_connections(pool_size)};
    std::atomic<int> in_use{0};
    std::atomic<int> max_in_use{0};
    std::atomic<int> shared{0};
    std::atomic<int> timeouts{0};

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&] {
            for (int r = 0; r < rounds; ++r) {
                auto handle = pool.checkout_for(10s);
                if (!handle) {
                    ++timeouts;
                    continue;
                }
                if ((*handle)->owners.fetch_add(1) != 0) {
                    ++shared;
                }
                const int now = in_use.fetch_add(1) + 1;
                int seen = max_in_use.load();
                while (now > seen && !max_in_use.compare_exchange_weak(seen, now)) {
                }
                std::this_thread::yield();
                in_use.fetch_sub(1);
                (*handle)->owners.fetch_sub(1);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    REQUIRE(shared == 0);
    REQUIRE(timeouts == 0);
    REQUIRE(max_in_use <= pool_size);
    REQUIRE(pool.idle() == pool_size);
    REQUIRE(pool.in_use() == 0);
}

TEST_CASE("connection_pool: close destroys idle connections now and checked-out ones when returned") {
    std::atomic<int> destroyed{0};
    auto pool = std::make_unique<pool_t>(make_connections(3, &destroyed));
    auto held = pool->checkout_for(0ms);
    auto second = pool->checkout_for(0ms);
    auto third = pool->checkout_for(0ms);
    REQUIRE(held);
    REQUIRE(second);
    REQUIRE(third);
    REQUIRE(pool->idle() == 0);

    // Every connection is out: the waiter cannot be served until one returns.
    auto waiter = std::async(std::launch::async, [&] { return pool->checkout_for(5s).has_value(); });
    REQUIRE(waiter.wait_for(50ms) == std::future_status::timeout);
    second->release();
    REQUIRE(waiter.get()); // served by the returned connection, which it gave back
    third->release();
    REQUIRE(pool->idle() == 2);

    pool->close();
    REQUIRE_FALSE(pool->checkout_for(0ms));
    REQUIRE_FALSE(pool->checkout_for(10ms));
    REQUIRE(destroyed == 2); // the two idle ones
    REQUIRE((*held)->id == 0);

    pool.reset(); // the pool object goes away while a handle is still out
    REQUIRE(destroyed == 2);
    held->release(); // returning into a closed pool destroys the connection
    REQUIRE(destroyed == 3);
}

TEST_CASE("connection_pool: close wakes a waiter, which gets nothing") {
    pool_t pool{make_connections(1)};
    auto held = pool.checkout_for(0ms);
    auto waiter = std::async(std::launch::async, [&] { return pool.checkout_for(10s).has_value(); });
    std::this_thread::sleep_for(20ms);
    pool.close();
    REQUIRE(waiter.wait_for(2s) == std::future_status::ready);
    REQUIRE_FALSE(waiter.get());
}
