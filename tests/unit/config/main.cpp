// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

#include "utility/logger.hpp"

#include <filesystem>

// ConfigReader logs through get_logger(logger_tag::Config); the named logger
// must exist before the first construction. Under CTest each test case runs in
// its own process, so initialize here in main.
int main(int argc, char* argv[]) {
    initialize_all_loggers((std::filesystem::temp_directory_path() / "otterstax-test-logs").string());
    return Catch::Session().run(argc, argv);
}
