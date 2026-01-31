// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <boost/asio.hpp>
#include <thread>

enum class thread_pool_status
{
    CREATED,
    RUNNING,
    STOPPED
};

class thread_pool_manager {
public:
    thread_pool_manager(size_t pool_size = std::thread::hardware_concurrency())
        : pool_size_{pool_size}
        , pool_status_{thread_pool_status::CREATED}
        , work_guard_(boost::asio::make_work_guard(ctx_)) {}

    boost::asio::io_context& ctx() { return ctx_; }

    thread_pool_status status() const noexcept { return pool_status_; }

    void start() {
        for (int i = 0; i < pool_size_; ++i) {
            thread_pool_.emplace_back([&]() { ctx_.run(); });
        }
        pool_status_ = thread_pool_status::RUNNING;
    }

    void stop() {
        if (pool_status_ != thread_pool_status::RUNNING) {
            return;
        }
        work_guard_.reset();
        for (auto& t : thread_pool_) {
            t.join();
        }
        ctx_.stop();
        pool_status_ = thread_pool_status::STOPPED;
    }

    ~thread_pool_manager() {
        if (pool_status_ == thread_pool_status::RUNNING) {
            stop();
        }
    }

private:
    boost::asio::io_context ctx_;
    size_t pool_size_;
    std::vector<std::jthread> thread_pool_;
    thread_pool_status pool_status_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
};
