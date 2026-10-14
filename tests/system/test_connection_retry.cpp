// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Startup policy for opening configured backends: a backend that is not ready
// is retried and, when it stays down, skipped; an answer it cannot be used with
// is skipped at once; a descriptor the connector can never accept aborts startup
// before any attempt (remote_backends_t::open). The verdict is the attempt's own,
// never a reading of the error's code.

#include "backends/connection_retry.hpp"

#include "utility/logger.hpp"

#include <catch2/catch_all.hpp>
#include <core/result_wrapper.hpp>

#include <chrono>
#include <memory_resource>
#include <optional>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace {

    struct attempts_t {
        int opened{0};
        std::vector<std::chrono::milliseconds> sleeps;
    };

    using otterstax::backends::open_failure_kind_t;
    using otterstax::backends::open_failure_t;

    std::optional<open_failure_t>
    failure(std::pmr::memory_resource* resource, open_failure_kind_t kind, core::error_code_t code) {
        return open_failure_t{kind, core::error_t(code, std::pmr::string{"backend said no", resource})};
    }

} // namespace

TEST_CASE("open_with_retry: an unreachable backend is retried then skipped") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto log = get_logger(logger_tag::Main);
    attempts_t attempts;

    const bool opened = otterstax::backends::open_with_retry(
        log,
        "MySQL",
        "demo",
        3,
        20ms,
        [&] {
            ++attempts.opened;
            return failure(&arena, open_failure_kind_t::unreachable, core::error_code_t::io_error);
        },
        [&](std::chrono::milliseconds delay) { attempts.sleeps.push_back(delay); });

    REQUIRE_FALSE(opened);
    REQUIRE(attempts.opened == 3);
    // No sleep after the last attempt.
    REQUIRE(attempts.sleeps.size() == 2);
    REQUIRE(attempts.sleeps.front() == 20ms);
}

TEST_CASE("open_with_retry: a backend that comes up mid-way is registered") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto log = get_logger(logger_tag::Main);
    attempts_t attempts;

    const bool opened = otterstax::backends::open_with_retry(
        log,
        "PostgreSQL",
        "pg",
        5,
        5ms,
        [&]() -> std::optional<open_failure_t> {
            if (++attempts.opened < 3) {
                return failure(&arena, open_failure_kind_t::unreachable, core::error_code_t::io_error);
            }
            return std::nullopt;
        },
        [&](std::chrono::milliseconds delay) { attempts.sleeps.push_back(delay); });

    REQUIRE(opened);
    REQUIRE(attempts.opened == 3);
    REQUIRE(attempts.sleeps.size() == 2);
}

// The verdict is the attempt's, never the error's code: an unreachable backend
// whose error carries invalid_parameter (a code the backend, the connector or
// the engine may produce) is retried like any other, and nothing aborts.
TEST_CASE("open_with_retry: an invalid_parameter code does not make a failure a configuration error") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto log = get_logger(logger_tag::Main);
    attempts_t attempts;

    const bool opened = otterstax::backends::open_with_retry(
        log,
        "ClickHouse",
        "ch",
        3,
        1ms,
        [&] {
            ++attempts.opened;
            return failure(&arena, open_failure_kind_t::unreachable, core::error_code_t::invalid_parameter);
        },
        [&](std::chrono::milliseconds delay) { attempts.sleeps.push_back(delay); });

    REQUIRE_FALSE(opened);
    REQUIRE(attempts.opened == 3);
    REQUIRE(attempts.sleeps.size() == 2);
}

// An answer the federation metadata cannot record comes back the same on every
// attempt: the backend is left out at once, startup goes on.
TEST_CASE("open_with_retry: an unusable answer is skipped on the first attempt and never retried") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto log = get_logger(logger_tag::Main);
    attempts_t attempts;

    const bool opened = otterstax::backends::open_with_retry(
        log,
        "ClickHouse",
        "ch",
        10,
        1ms,
        [&] {
            ++attempts.opened;
            return failure(&arena, open_failure_kind_t::unusable, core::error_code_t::invalid_parameter);
        },
        [&](std::chrono::milliseconds delay) { attempts.sleeps.push_back(delay); });

    REQUIRE_FALSE(opened);
    REQUIRE(attempts.opened == 1);
    REQUIRE(attempts.sleeps.empty());
}

TEST_CASE("open_with_retry: a non-positive attempt budget still opens once") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto log = get_logger(logger_tag::Main);
    attempts_t attempts;

    const bool opened = otterstax::backends::open_with_retry(
        log,
        "MySQL",
        "demo",
        0,
        1ms,
        [&] {
            ++attempts.opened;
            return failure(&arena, open_failure_kind_t::unreachable, core::error_code_t::io_error);
        },
        [&](std::chrono::milliseconds delay) { attempts.sleeps.push_back(delay); });

    REQUIRE_FALSE(opened);
    REQUIRE(attempts.opened == 1);
    REQUIRE(attempts.sleeps.empty());
}
