// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

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
            std::cout << "Received from DB rows: " << result->size() << std::endl;
            results.push_back(std::move(result));
        }
        std::cout << "Finished!\n";
    }
};