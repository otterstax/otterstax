// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

#include "utility/logger.hpp"

#include <filesystem>

// GreenplumParser logs through get_logger(logger_tag::PARSER); the named
// loggers must exist before the first parse() call (in production
// ComponentManager initializes them before any actor is spawned).
int main(int argc, char* argv[]) {
    initialize_all_loggers((std::filesystem::temp_directory_path() / "otterstax-test-logs").string());
    return Catch::Session().run(argc, argv);
}
