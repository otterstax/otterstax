// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "utility/cv_wrapper.hpp"

#include <catch2/catch.hpp>
#include <chrono>
#include <iostream>

using namespace cv_wrapper;

TEST_CASE("cv_wrapper: ok") {
    auto cv_w = create_cv_wrapper(std::unique_ptr<std::string>());
    REQUIRE(cv_w != nullptr);
    REQUIRE(cv_w->result == nullptr);

    auto start_point_ = std::chrono::system_clock::now();
    auto worker = std::jthread([cv_w]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        cv_w->result = std::make_unique<std::string>("Hello, World!");
        cv_w->release();
    });
    cv_w->wait();
    auto end_point = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_point - start_point_);
    bool is_really_waited = (duration.count() >= 500) && (duration.count() < 600);
    REQUIRE(is_really_waited);
    REQUIRE(cv_w->result != nullptr);
    REQUIRE(*cv_w->result == "Hello, World!");
    REQUIRE(cv_w->status() == Status::Ok);
}

TEST_CASE("cv_wrapper: timeout") {
    using namespace std::chrono_literals;

    auto cv_w = create_cv_wrapper(std::unique_ptr<std::string>());
    REQUIRE(cv_w != nullptr);
    REQUIRE(cv_w->result == nullptr);

    auto start_point_ = std::chrono::system_clock::now();
    auto worker = std::jthread([cv_w]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        REQUIRE(cv_w->status() == Status::Timeout);
        if (!(cv_w->status() == Status::Timeout)) {
            cv_w->result = std::make_unique<std::string>("Hello, World!");
            cv_w->release();
        }
    });
    cv_w->wait_for(200ms);
    auto end_point = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_point - start_point_);
    bool is_really_waited = (duration.count() >= 200) && (duration.count() < 220);
    REQUIRE(is_really_waited);
    REQUIRE(cv_w->result == nullptr);
    REQUIRE(cv_w->status() == Status::Timeout);
}

TEST_CASE("cv_wrapper: error") {
    using namespace std::chrono_literals;

    auto cv_w = create_cv_wrapper(std::unique_ptr<std::string>());
    REQUIRE(cv_w != nullptr);
    REQUIRE(cv_w->result == nullptr);

    auto start_point_ = std::chrono::system_clock::now();
    auto worker = std::jthread([cv_w]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        REQUIRE(cv_w->status() == Status::Unknown);
        cv_w->release_on_error("Some error occurred");
    });
    cv_w->wait_for(200ms);
    auto end_point = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_point - start_point_);
    bool is_really_waited = (duration.count() >= 100) && (duration.count() < 120);
    REQUIRE(is_really_waited);
    REQUIRE(cv_w->result == nullptr);
    REQUIRE(cv_w->status() == Status::Error);
    REQUIRE(cv_w->error_message() == "Some error occurred");
}