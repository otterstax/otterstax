// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <future>
#include <iostream>
#include <vector>

template<typename T>
struct QueryHandleWaiter {
    std::vector<std::future<T>> futures;
    std::vector<T> results;
    void wait() {
        for (auto& future : futures) {
            auto result = future.get();
            // Received from DB
            results.emplace_back(std::move(result));
        }
        // Finished
    }

    // unlike std::async, asio futures do NOT block on destruction
    ~QueryHandleWaiter() {
        for (auto& future : futures) {
            if (future.valid()) {
                try {
                    future.wait();
                } catch (...) {
                    // never propagate out of a destructor
                }
            }
        }
    }

    QueryHandleWaiter() = default;
    QueryHandleWaiter(const QueryHandleWaiter&) = delete;
    QueryHandleWaiter& operator=(const QueryHandleWaiter&) = delete;
    QueryHandleWaiter(QueryHandleWaiter&&) = delete;
    QueryHandleWaiter& operator=(QueryHandleWaiter&&) = delete;
};