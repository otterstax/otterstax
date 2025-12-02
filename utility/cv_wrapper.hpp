// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

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
    public:
        explicit cv_wrapper_t(T data)
            : result(std::move(data)) {}
        T result;

    public:
        void wait() {
            std::unique_lock<std::mutex> lock(m_);
            cv_.wait(lock, [this]() { return ready_; });
        }
        void wait_for(std::chrono::milliseconds timeout) {
            std::unique_lock<std::mutex> lock(m_);
            auto is_timeout_ = !cv_.wait_for(lock, timeout, [this]() { return ready_; });
            if (is_timeout_) {
                status_ = Status::Timeout;
            }
        }
        void release() {
            {
                std::unique_lock<std::mutex> lock(m_);
                ready_ = true;
                status_ = Status::Ok;
            }
            cv_.notify_one();
        }

        void release_on_error(std::string error_msg) {
            {
                std::unique_lock<std::mutex> lock(m_);
                error = std::move(error_msg);
                ready_ = true;
                status_ = Status::Error;
            }
            cv_.notify_one();
        }

        void release_empty() {
            {
                std::unique_lock<std::mutex> lock(m_);
                ready_ = true;
                status_ = Status::Empty;
            }
            cv_.notify_one();
        }

        Status status() const noexcept {
            std::unique_lock<std::mutex> lock(m_);
            return status_;
        }

        std::string error_message() const noexcept {
            std::unique_lock<std::mutex> lock(m_);
            return error.value_or("");
        }

    private:
        Status status_{Status::Unknown};
        std::optional<std::string> error{std::nullopt};
        bool ready_{false};
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