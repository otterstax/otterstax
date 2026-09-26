// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <catch2/catch_session.hpp>

#include "utility/logger.hpp"

#include <filesystem>

// The kafka transform reuses otterbrix's transformer, which logs through the
// named loggers; initialize them before the first parse (ComponentManager does
// this in production before any actor is spawned).
int main(int argc, char* argv[]) {
    initialize_all_loggers((std::filesystem::temp_directory_path() / "otterstax-test-logs").string());
    return Catch::Session().run(argc, argv);
}
