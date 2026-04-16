// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace cv_wrapper {
    constexpr std::chrono::milliseconds DEFAULT_TIMEOUT(90000);

    enum class Status : uint8_t
    {
        Ok,
        Empty,
        Timeout,
        Error,
        Unknown
    };

    template<typename T>
    class cv_wrapper_t {
    public:
        explicit cv_wrapper_t(T data)
            : result_(std::move(data)) {}

    public:
        void set_result(T data) {
            {
                std::unique_lock<std::mutex> lock(m_);
                result_ = std::move(data);
                status_.store(Status::Ok);
                ready_.store(true);
            }
            cv_.notify_one();
        }

        T get_result() {
            std::unique_lock<std::mutex> lock(m_);
            return std::move(result_);
        }

        void wait() {
            std::unique_lock<std::mutex> lock(m_);
            cv_.wait(lock, [this]() { return ready_.load(); });
        }

        void wait_for(std::chrono::milliseconds timeout) {
            std::unique_lock<std::mutex> lock(m_);
            auto is_timeout_ = !cv_.wait_for(lock, timeout, [this]() { return ready_.load(); });
            if (is_timeout_) {
                status_.store(Status::Timeout);
                ready_.store(true);
            }
        }

        void release_on_error(std::string error_msg) {
            {
                std::unique_lock<std::mutex> lock(m_);
                error_ = std::move(error_msg);
                status_.store(Status::Error);
                ready_.store(true);
            }
            cv_.notify_one();
        }

        void release_empty() {
            {
                std::unique_lock<std::mutex> lock(m_);
                status_.store(Status::Empty);
                ready_.store(true);
            }
            cv_.notify_one();
        }

        Status status() const noexcept { return status_.load(); }

        std::string error_message() const noexcept {
            std::unique_lock<std::mutex> lock(m_);
            return error_.value_or("");
        }

    private:
        T result_;
        std::atomic<Status> status_{Status::Unknown};
        std::optional<std::string> error_{std::nullopt};
        std::atomic<bool> ready_{false};
        mutable std::mutex m_;
        std::condition_variable cv_;
    };
} // namespace cv_wrapper

template<typename T>
inline std::shared_ptr<cv_wrapper::cv_wrapper_t<T>> create_cv_wrapper(T data) {
    return std::make_shared<cv_wrapper::cv_wrapper_t<T>>(std::move(data));
}
template<typename T>
using shared_data = std::shared_ptr<cv_wrapper::cv_wrapper_t<T>>;
