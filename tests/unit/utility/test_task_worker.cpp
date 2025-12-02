// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "../../../utility/worker.hpp"

#include <catch2/catch.hpp>
#include <chrono>
#include <functional>
#include <iostream>

TEST_CASE("worker: base test case") {
    std::atomic<int> c{0};
    TaskManager<std::function<void()>> tm;

    auto f1 = [&c]() {
        c++;
        std::cout << "1 Thread: " << std::this_thread::get_id() << std::endl;
    };
    auto f2 = [&c]() {
        c++;
        std::cout << "2 Thread: " << std::this_thread::get_id() << std::endl;
    };
    tm.start();
    tm.addTask(std::move(f1));
    tm.addTask(std::move(f2));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    REQUIRE(c.load() == 2);

    std::cout << "0 Thread: " << std::this_thread::get_id() << std::endl;
}