// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

#include "utility/logger.hpp"

#include <filesystem>

// Several system tests construct GreenplumParser (and other components that
// log through get_logger) directly; the named loggers must exist before the
// first such call. Under CTest every test case runs in its own process, so
// no earlier test case can be relied on to have initialized them.
int main(int argc, char* argv[]) {
    initialize_all_loggers((std::filesystem::temp_directory_path() / "otterstax-test-logs").string());
    return Catch::Session().run(argc, argv);
}
