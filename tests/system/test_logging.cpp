// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// initialize_logger (utility/logger.hpp): a named logger is registered once,
// writes its file under the prefix it was given, and a second initialization
// of the same name hands back the registered one.

#include "utility/logger.hpp"

#include <catch2/catch_all.hpp>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include <unistd.h>

TEST_CASE("logging: a named logger writes its file under its prefix and is registered once") {
    const auto dir = std::filesystem::temp_directory_path() / ("otterstax_logging_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    const std::string name = "LoggingTest" + std::to_string(::getpid());

    auto log = initialize_logger(name, dir.string());
    REQUIRE(log.is_valid());
    CHECK(get_logger(name).is_valid());
    // An ERROR line is written at every SPDLOG_ACTIVE_LEVEL (ERROR is the default).
    log->error("LoggingTest: error message");
    log->flush();
    auto again = initialize_logger(name, (dir / "other").string());
    CHECK(again.is_valid());
    CHECK_FALSE(std::filesystem::exists(dir / "other"));

    std::string written;
    int files = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        CHECK(entry.path().filename().string().rfind(name + "-", 0) == 0);
        std::ifstream in(entry.path());
        std::stringstream text;
        text << in.rdbuf();
        written += text.str();
        ++files;
    }
    CHECK(files == 1);
    CHECK(written.find("[" + name + "] [error]") != std::string::npos);
    CHECK(written.find("LoggingTest: error message") != std::string::npos);

    spdlog::drop(name);
    std::filesystem::remove_all(dir);
}
