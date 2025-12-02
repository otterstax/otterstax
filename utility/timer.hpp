// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <chrono>
#include <iostream>

class Timer {
public:
    Timer(std::string name = "")
        : name_{std::move(name)} {
        start();
    }
    ~Timer() {
        std::cout << "[" << name_ << "] "
                  << "Total Time elapsed: " << elapsed() << "ms\n";
    }
    void timePoint(const std::string& sub_name = "") {
        std::cout << "[" << name_ << "] "
                  << "[" << sub_name << "] "
                  << "Time elapsed: " << elapsed() << "ms\n";
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
    std::chrono::time_point<std::chrono::system_clock> start_point_;
};