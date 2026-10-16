// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// tsl::write_local_file: a temporary sibling replaces the target only on success, so a failed
// COPY ... TO leaves the existing file as it was and no staging file behind.

#include "otterbrix/translators/output/file_sink.hpp"

#include <catch2/catch_all.hpp>

#include <arrow/io/interfaces.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory_resource>
#include <string>

namespace {

    std::string read_file(const std::string& path) {
        std::ifstream in(path);
        return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }

    // Files in `dir` other than `keep`: a staging file left behind would be one.
    int strays(const std::filesystem::path& dir, const std::string& keep) {
        int count = 0;
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            if (entry.path().filename() != keep) {
                ++count;
            }
        }
        return count;
    }

    struct scratch_dir {
        std::filesystem::path path;
        explicit scratch_dir(const char* name)
            : path(std::filesystem::temp_directory_path() / name) {
            std::filesystem::remove_all(path);
            std::filesystem::create_directories(path);
        }
        ~scratch_dir() { std::filesystem::remove_all(path); }
    };

} // namespace

TEST_CASE("write_local_file: a write that fails half-way keeps the existing file and leaves no staging file") {
    scratch_dir dir{"otterstax_test_file_sink_fail"};
    const std::string target = (dir.path / "out.csv").string();
    {
        std::ofstream existing(target);
        existing << "previous export";
    }

    auto written = tsl::write_local_file(
        std::pmr::new_delete_resource(),
        "test",
        target,
        [](const std::shared_ptr<arrow::io::OutputStream>& sink) -> core::result_wrapper_t<bool> {
            const std::string partial = "half of a new export";
            REQUIRE(sink->Write(partial.data(), static_cast<int64_t>(partial.size())).ok());
            return tsl::make_error(std::pmr::new_delete_resource(), core::error_code_t::conversion_failure,
                                   "a value the writer refuses");
        });

    REQUIRE(written.has_error());
    CHECK(written.error().type == core::error_code_t::conversion_failure);
    CHECK(read_file(target) == "previous export");
    CHECK(strays(dir.path, "out.csv") == 0);
}

TEST_CASE("write_local_file: a write that succeeds replaces the file whole") {
    scratch_dir dir{"otterstax_test_file_sink_ok"};
    const std::string target = (dir.path / "out.csv").string();
    {
        std::ofstream existing(target);
        existing << "previous export";
    }

    auto written = tsl::write_local_file(
        std::pmr::new_delete_resource(),
        "test",
        target,
        [](const std::shared_ptr<arrow::io::OutputStream>& sink) -> core::result_wrapper_t<bool> {
            const std::string body = "new export";
            REQUIRE(sink->Write(body.data(), static_cast<int64_t>(body.size())).ok());
            return true;
        });

    REQUIRE_FALSE(written.has_error());
    CHECK(read_file(target) == "new export");
    CHECK(strays(dir.path, "out.csv") == 0);
}

TEST_CASE("write_local_file: a directory that does not exist is io_error and creates nothing") {
    auto written = tsl::write_local_file(
        std::pmr::new_delete_resource(),
        "test",
        "/nonexistent_dir_otterstax/out.csv",
        [](const std::shared_ptr<arrow::io::OutputStream>&) -> core::result_wrapper_t<bool> { return true; });

    REQUIRE(written.has_error());
    CHECK(written.error().type == core::error_code_t::io_error);
    CHECK_FALSE(std::filesystem::exists("/nonexistent_dir_otterstax"));
}
