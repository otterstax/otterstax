// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <catch2/catch_all.hpp>

#include "utility/logger.hpp"

#include <filesystem>

// The connector managers and the integration backends log through named spdlog
// loggers that must exist before the first construction. Under CTest each
// test case runs in its own process, so initialize here in main.
int main(int argc, char* argv[]) {
    initialize_all_loggers((std::filesystem::temp_directory_path() / "otterstax-test-logs").string());
    return Catch::Session().run(argc, argv);
}
