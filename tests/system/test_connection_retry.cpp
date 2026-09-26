// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Startup policy for opening configured backends: a backend that is not ready
// is retried and, when it stays down, skipped; a descriptor the backend can
// never accept aborts startup on the spot. Neither path may throw or retry the
// other's case.

#include "component_manager/connection_retry.hpp"

#include "utility/logger.hpp"

#include <catch2/catch_all.hpp>
#include <core/result_wrapper.hpp>

#include <chrono>
#include <memory_resource>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace {

    struct attempts_t {
        int opened{0};
        std::vector<std::chrono::milliseconds> sleeps;
    };

    core::result_wrapper_t<std::string> failure(std::pmr::memory_resource* resource, core::error_code_t code) {
        return core::error_t(code, std::pmr::string{"backend said no", resource});
    }

} // namespace

TEST_CASE("open_with_retry: an unreachable backend is retried, then skipped without an error") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto log = get_logger(logger_tag::Main);
    attempts_t attempts;

    auto opened = otterstax::startup::open_with_retry(
        log,
        "MySQL",
        "demo",
        3,
        20ms,
        [&] {
            ++attempts.opened;
            return failure(&arena, core::error_code_t::io_error);
        },
        [&](std::chrono::milliseconds delay) { attempts.sleeps.push_back(delay); });

    REQUIRE_FALSE(opened.has_error());
    REQUIRE(opened.value() == false);
    REQUIRE(attempts.opened == 3);
    // No sleep after the last attempt.
    REQUIRE(attempts.sleeps.size() == 2);
    REQUIRE(attempts.sleeps.front() == 20ms);
}

TEST_CASE("open_with_retry: a backend that comes up mid-way is registered") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto log = get_logger(logger_tag::Main);
    attempts_t attempts;

    auto opened = otterstax::startup::open_with_retry(
        log,
        "PostgreSQL",
        "pg",
        5,
        5ms,
        [&]() -> core::result_wrapper_t<std::string> {
            if (++attempts.opened < 3) {
                return failure(&arena, core::error_code_t::io_error);
            }
            return std::string{"pg"};
        },
        [&](std::chrono::milliseconds delay) { attempts.sleeps.push_back(delay); });

    REQUIRE_FALSE(opened.has_error());
    REQUIRE(opened.value() == true);
    REQUIRE(attempts.opened == 3);
    REQUIRE(attempts.sleeps.size() == 2);
}

TEST_CASE("open_with_retry: an invalid descriptor is an error on the first attempt, never retried") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto log = get_logger(logger_tag::Main);
    attempts_t attempts;

    auto opened = otterstax::startup::open_with_retry(
        log,
        "ClickHouse",
        "ch",
        10,
        1ms,
        [&] {
            ++attempts.opened;
            return failure(&arena, core::error_code_t::invalid_parameter);
        },
        [&](std::chrono::milliseconds delay) { attempts.sleeps.push_back(delay); });

    REQUIRE(opened.has_error());
    REQUIRE(opened.error().type == core::error_code_t::invalid_parameter);
    REQUIRE(std::string{opened.error().what.c_str()} == "backend said no");
    REQUIRE(attempts.opened == 1);
    REQUIRE(attempts.sleeps.empty());
}

TEST_CASE("open_with_retry: a non-positive attempt budget still opens once") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto log = get_logger(logger_tag::Main);
    attempts_t attempts;

    auto opened = otterstax::startup::open_with_retry(
        log,
        "MySQL",
        "demo",
        0,
        1ms,
        [&] {
            ++attempts.opened;
            return failure(&arena, core::error_code_t::io_error);
        },
        [&](std::chrono::milliseconds delay) { attempts.sleeps.push_back(delay); });

    REQUIRE_FALSE(opened.has_error());
    REQUIRE(opened.value() == false);
    REQUIRE(attempts.opened == 1);
    REQUIRE(attempts.sleeps.empty());
}
