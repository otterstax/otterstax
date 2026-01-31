// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <shared_mutex>
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
        std::unique_lock<std::shared_mutex> lock;
        if (tasks_.size() >= max_size_) {
            return false;
        }
        tasks_.push(std::move(task));
        return true;
    }
    Task enqueueTask() {
        std::unique_lock<std::shared_mutex> lock;
        auto task = tasks_.front();
        tasks_.pop();
        return task;
    }
    void reset() {
        std::unique_lock<std::shared_mutex> lock;
        while (!tasks_.empty()) {
            tasks_.pop();
        }
    }
    size_t size() const noexcept {
        std::shared_lock<std::shared_mutex> lock_;
        return tasks_.size();
    }

    bool empty() const noexcept {
        std::shared_lock<std::shared_mutex> lock_;
        return tasks_.empty();
    }

private:
    std::size_t max_size_{0};
    std::shared_mutex m_;
    std::queue<Task> tasks_;
};

template<typename Task>
class TaskManager {
public:
    void start() {
        worker_ = std::move(std::jthread([this]() { this->process(); }));
    }
    void stop() {
        tasks_.reset();
        stop_.store(true);
        cv_.notify_one();
        worker_.join();
    }
    void process() {
        while (!stop_.load()) {
            std::unique_lock<std::mutex> lk(m_);
            this->cv_.wait(lk, [this]() { return !this->tasks_.empty() || this->stop_.load(); });
            while (!this->tasks_.empty()) {
                this->tasks_.enqueueTask()();
            }
        }
    }
    bool addTask(Task task) {
        auto res = tasks_.addTask(std::move(task));
        cv_.notify_one();
        return res;
    }
    ~TaskManager() { stop(); }

private:
    Tasks<Task> tasks_;
    std::jthread worker_;
    std::mutex m_;
    std::condition_variable cv_;
    std::atomic_bool stop_{false};
};