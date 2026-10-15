// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#pragma once

#include <boost/filesystem/operations.hpp>
#include <filesystem>
#include <string>

namespace otterstax::test {
    class temporary_directory {
    public:
        explicit temporary_directory(const std::string& prefix) {
            do {
                path = std::filesystem::temp_directory_path() /
                       boost::filesystem::unique_path(prefix + "-%%%%-%%%%-%%%%").string();
            } while (!std::filesystem::create_directory(path));
        }
        ~temporary_directory() {
            std::error_code ignored;
            std::filesystem::remove_all(path, ignored);
        }
        temporary_directory(const temporary_directory&) = delete;
        temporary_directory& operator=(const temporary_directory&) = delete;
        std::filesystem::path path;
    };
}
