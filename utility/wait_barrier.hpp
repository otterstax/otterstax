// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <future>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// Connector queries hand their result across the io_context worker -> consumer
// thread boundary. A live std::exception must NEVER cross that boundary: boost.asio's
// use_future captures a thrown exception into an executor_op whose exception_ptr copy
// is destroyed on the io thread, racing the consumer's future.get() read of the same
// object (ThreadSanitizer data race in operator delete, surfaced by boost 1.88). Each
// connector instead marshals any error into this plain value on the io thread; the
// consumer rethrows from the copied string on its own thread.
template<typename T>
struct query_outcome {
    T value{};
    std::string error;  // empty => success
};

// Unwrap a marshaled connector result on the consumer thread: rethrow the error
// string (preserving the call sites that rely on a throwing executeQuery — e.g.
// CatalogManager's try/catch around schema discovery), otherwise hand back the
// value. Mirrors QueryHandleWaiter::wait() for the single-future case.
template<typename T>
T get_or_throw(std::future<query_outcome<T>>& future) {
    auto outcome = future.get();
    if (!outcome.error.empty()) {
        throw std::runtime_error(outcome.error);
    }
    return std::move(outcome.value);
}

template<typename T>
struct QueryHandleWaiter {
    std::vector<std::future<query_outcome<T>>> futures;
    std::vector<T> results;
    void wait() {
        for (auto& future : futures) {
            auto outcome = future.get();
            if (!outcome.error.empty()) {
                throw std::runtime_error(outcome.error);
            }
            results.emplace_back(std::move(outcome.value));
        }
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
