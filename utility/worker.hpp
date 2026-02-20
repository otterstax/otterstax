// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

// TODO add concepts
template<typename Task>
class Tasks {
public:
    explicit Tasks(std::size_t max_size = 100)
        : max_size_{max_size} {}

    Tasks(const Tasks&) = delete;
    Tasks& operator=(const Tasks&) = delete;
    Tasks(Tasks&&) = delete;
    Tasks& operator=(Tasks&&) = delete;

    bool addTask(Task task) {
        std::lock_guard<std::mutex> lk(m_);
        if (tasks_.size() >= max_size_) {
            return false;
        }

        tasks_.push(std::move(task));
        cv_.notify_one();
        return true;
    }

    std::optional<Task> waitAndPop(std::atomic_bool& stop) {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [&]() { return stop.load() || !tasks_.empty(); });

        if (stop.load()) {
            return std::nullopt;
        }

        auto task = std::move(tasks_.front());
        tasks_.pop();
        return task;
    }

    void reset() {
        std::lock_guard<std::mutex> lk(m_);
        while (!tasks_.empty()) {
            tasks_.pop();
        }
    }

    size_t size() const noexcept {
        std::lock_guard<std::mutex> lk_(m_);
        return tasks_.size();
    }

    void notify() { cv_.notify_all(); }

private:
    std::size_t max_size_;
    mutable std::mutex m_;
    std::condition_variable cv_;
    std::queue<Task> tasks_;
};

template<typename Task>
class TaskManager {
public:
    void start() {
        worker_ = std::jthread([this]() { process(); });
    }

    void stop() {
        stop_.store(true);
        tasks_.notify();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    void process() {
        while (!stop_.load()) {
            if (auto task_opt = tasks_.waitAndPop(stop_); task_opt) {
                (*task_opt)();
            }
        }
    }

    bool addTask(Task task) { return tasks_.addTask(std::move(task)); }

    ~TaskManager() { stop(); }

private:
    Tasks<Task> tasks_;
    std::jthread worker_;
    std::atomic_bool stop_{false};
};