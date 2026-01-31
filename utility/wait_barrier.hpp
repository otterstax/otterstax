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
            results.push_back(std::move(result));
        }
        // Finished
    }
};