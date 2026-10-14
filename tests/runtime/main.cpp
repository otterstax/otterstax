// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <catch2/catch_all.hpp>

#include "utility/logger.hpp"

#include <filesystem>

// The engine log writes through the otterstax Main logger's sinks
// (runtime/engine.cpp); the named loggers are initialized once here, as
// main.cpp does for the server. Under CTest each case runs in its own process.
int main(int argc, char* argv[]) {
    initialize_all_loggers((std::filesystem::temp_directory_path() / "otterstax-test-logs").string());
    return Catch::Session().run(argc, argv);
}
