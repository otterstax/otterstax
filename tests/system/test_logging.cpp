// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "utility/logger.hpp"
#include <catch2/catch.hpp>
#include <iostream>

TEST_CASE("logging is configured", "[.skip]") {
    auto log = get_logger();

    // Emit a couple of log lines at common levels
    log->info("LoggingTest: info message");
    log->error("LoggingTest: error message");

    // Also print a marker to stdout so test output is easy to spot
    std::cout << "LoggingTest: cout marker" << std::endl;

    SUCCEED();
}
