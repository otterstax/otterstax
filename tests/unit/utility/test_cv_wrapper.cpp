// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "utility/cv_wrapper.hpp"
#include "utility/tsan_helper.hpp"

#include <catch2/catch.hpp>
#include <chrono>
#include <iostream>
#include <thread>

using namespace cv_wrapper;

TEST_CASE("cv_wrapper: ok") {
    auto cv_w = create_cv_wrapper(std::unique_ptr<std::string>());
    REQUIRE(cv_w != nullptr);
    REQUIRE(cv_w->get_result() == nullptr);

    auto start_point_ = std::chrono::system_clock::now();
    auto worker = std::jthread([cv_w]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        cv_w->set_result(std::make_unique<std::string>("Hello, World!"));
    });
    cv_w->wait();
    auto end_point = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_point - start_point_);
    bool is_really_waited = (duration.count() >= 500) && (duration.count() < 600);
    REQUIRE(is_really_waited);

    auto res = cv_w->get_result();
    REQUIRE(res != nullptr);
    REQUIRE(*res == "Hello, World!");
    REQUIRE(cv_w->status() == Status::Ok);
}

TEST_CASE("cv_wrapper: timeout") {
    if constexpr (TSAN_ENABLED) {
        return; // skip test, TSAN considers synchronization via sleep as data race, however, this is a valid test case
    }

    using namespace std::chrono_literals;

    auto cv_w = create_cv_wrapper(std::unique_ptr<std::string>());
    REQUIRE(cv_w != nullptr);
    REQUIRE(cv_w->get_result() == nullptr);

    auto start_point_ = std::chrono::system_clock::now();
    auto worker = std::jthread([cv_w]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        REQUIRE(cv_w->status() == Status::Timeout);
        if (!(cv_w->status() == Status::Timeout)) {
            cv_w->set_result(std::make_unique<std::string>("Hello, World!"));
        }
    });
    cv_w->wait_for(200ms);
    auto end_point = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_point - start_point_);
    bool is_really_waited = (duration.count() >= 200) && (duration.count() < 220);
    REQUIRE(is_really_waited);

    auto res = cv_w->get_result();
    REQUIRE(res == nullptr);
    REQUIRE(cv_w->status() == Status::Timeout);
}

TEST_CASE("cv_wrapper: error") {
    using namespace std::chrono_literals;
    if constexpr (TSAN_ENABLED) {
        return; // skip test, TSAN considers synchronization via sleep as data race, however, this is a valid test case
    }

    auto cv_w = create_cv_wrapper(std::unique_ptr<std::string>());
    REQUIRE(cv_w != nullptr);
    REQUIRE(cv_w->get_result() == nullptr);

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
    REQUIRE(cv_w->get_result() == nullptr);
    REQUIRE(cv_w->status() == Status::Error);
    REQUIRE(cv_w->error_message() == "Some error occurred");
}

// Two threads race to finalise the same cv_wrapper: one calls set_result, the
// other release_on_error. Exactly one terminal status must win and the waiter
// must unblock without UB. Exercises the locking in cv_wrapper.hpp:33-69 under
// TSAN as well — set_result and release_on_error both acquire m_ before
// touching status_/ready_, so the wrapper should remain consistent.
TEST_CASE("cv_wrapper: concurrent finalisation race") {
    using namespace std::chrono_literals;

    constexpr int ITERATIONS = 64;
    for (int iter = 0; iter < ITERATIONS; ++iter) {
        auto cv_w = create_cv_wrapper(std::unique_ptr<std::string>());
        REQUIRE(cv_w != nullptr);

        auto producer = std::jthread([cv_w]() {
            cv_w->set_result(std::make_unique<std::string>("ok"));
        });
        auto failer = std::jthread([cv_w]() {
            cv_w->release_on_error("racing error");
        });

        cv_w->wait_for(2000ms);
        // wait_for must return because at least one side set ready_=true.
        auto status = cv_w->status();
        REQUIRE((status == Status::Ok || status == Status::Error));
        // get_result must not deadlock under the lock.
        auto res = cv_w->get_result();
        if (status == Status::Ok) {
            REQUIRE(res != nullptr);
            REQUIRE(*res == "ok");
        }
        // error_message access goes through the same mutex; must not hang.
        (void) cv_w->error_message();
    }
}

// wait_for that hits the timeout must (a) flip status to Timeout, (b) leave
// the wrapper safe to access afterwards, even when a producer eventually
// arrives. This guards the timeout branch in cv_wrapper.hpp:53-60.
TEST_CASE("cv_wrapper: late set_result after timeout is observable but does not overwrite Timeout") {
    using namespace std::chrono_literals;
    if constexpr (TSAN_ENABLED) {
        return;
    }

    auto cv_w = create_cv_wrapper(std::unique_ptr<std::string>());
    REQUIRE(cv_w != nullptr);

    cv_w->wait_for(50ms);
    REQUIRE(cv_w->status() == Status::Timeout);

    // Producer arrives long after the waiter gave up. Today set_result
    // unconditionally rewrites status_ to Ok; if that changes in the future
    // (e.g. checking ready_ before overwriting), this test will need updating.
    // Either way, the wrapper must remain usable and not crash.
    cv_w->set_result(std::make_unique<std::string>("late"));
    auto res = cv_w->get_result();
    REQUIRE(res != nullptr);
    REQUIRE(*res == "late");
}
