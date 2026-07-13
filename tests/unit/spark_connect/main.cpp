// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <catch2/catch_session.hpp>

#include "utility/logger.hpp"

#include <filesystem>

// Path B's ExpressionString filter routes through GreenplumParser, which logs
// through get_logger(logger_tag::PARSER); the named loggers must exist before
// the first parse_fragment() call (in production ComponentManager initializes
// them before any actor is spawned).
int main(int argc, char* argv[]) {
    initialize_all_loggers((std::filesystem::temp_directory_path() / "otterstax-test-logs").string());
    return Catch::Session().run(argc, argv);
}
