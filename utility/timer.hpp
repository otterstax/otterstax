// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include <components/log/log.hpp>

#include <chrono>
#include <iostream>

class Timer {
public:
    Timer(std::string name, log_t log)
        : name_{std::move(name)}, log_{log} {
        start();
    }
    ~Timer() {
        log_->trace("Timer [{}]: Total Time elapsed = {:.2f} ms", name_, elapsed());
    }
    void timePoint(const std::string& sub_name = "") {
        log_->trace("Timer [{}]: Time point [{}] = {:.2f} ms", name_, sub_name, elapsed());
    }

private:
    void start() { start_point_ = std::chrono::system_clock::now(); }
    double elapsed() const {
        auto end_point = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_point - start_point_);
        return duration.count();
    }

private:
    std::string name_;
    log_t log_;
    std::chrono::time_point<std::chrono::system_clock> start_point_;
};