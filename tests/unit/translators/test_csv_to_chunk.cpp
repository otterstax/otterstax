// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/csv_to_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory_resource>
#include <string>
#include <string_view>

using components::vector::data_chunk_t;

namespace {

const char* kCsv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Dave\n5,Eve\n";

data_chunk_t parse(const std::string& s, char delim = ',', bool header = true) {
    auto loaded = tsl::csv_to_chunk(std::pmr::new_delete_resource(),
                                    reinterpret_cast<const uint8_t*>(s.data()), s.size(),
                                    delim, header);
    REQUIRE_FALSE(loaded.has_error());
    return std::move(loaded.value());
}

} // namespace

TEST_CASE("csv_to_chunk: buffer overload parses columns, rows and values") {
    auto chunk = parse(kCsv);

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 5);

    REQUIRE(chunk.value(0, 0).value<int64_t>() == 1);
    REQUIRE(chunk.value(0, 4).value<int64_t>() == 5);
    REQUIRE(chunk.value(1, 0).value<std::string_view>() == "Alice");
    REQUIRE(chunk.value(1, 4).value<std::string_view>() == "Eve");
}

TEST_CASE("csv_to_chunk: file overload matches buffer overload") {
    const std::string path = "/tmp/otterstax_test_csv_to_chunk.csv";
    { std::ofstream(path) << kCsv; }

    auto loaded = tsl::csv_to_chunk(std::pmr::new_delete_resource(), path);
    REQUIRE_FALSE(loaded.has_error());
    const auto& chunk = loaded.value();

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 5);
    REQUIRE(chunk.value(0, 2).value<int64_t>() == 3);
    REQUIRE(chunk.value(1, 2).value<std::string_view>() == "Charlie");

    std::filesystem::remove(path);
}

TEST_CASE("csv_to_chunk: custom delimiter") {
    auto chunk = parse("id;name\n10;ten\n20;twenty\n", ';');

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 2);
    REQUIRE(chunk.value(0, 1).value<int64_t>() == 20);
    REQUIRE(chunk.value(1, 1).value<std::string_view>() == "twenty");
}

TEST_CASE("csv_to_chunk: header-only input yields an empty chunk") {
    auto chunk = parse("id,name\n");

    REQUIRE(chunk.size() == 0);
    REQUIRE(chunk.column_count() == 0);
}

TEST_CASE("csv_to_chunk: no-header input auto-generates column names") {
    auto chunk = parse("1,Alice\n2,Bob\n", ',', /*has_header=*/false);

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 2);
    REQUIRE(chunk.value(0, 0).value<int64_t>() == 1);
    REQUIRE(chunk.value(1, 1).value<std::string_view>() == "Bob");
}

TEST_CASE("csv_to_chunk: a missing file is io_error") {
    auto loaded = tsl::csv_to_chunk(std::pmr::new_delete_resource(), "/nonexistent_dir_otterstax/missing.csv");

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().type == core::error_code_t::io_error);
}

TEST_CASE("csv_to_chunk: a malformed body is conversion_failure") {
    // A row with more fields than the header cannot be parsed.
    const std::string bad = "id,name\n1,Alice,extra\n";
    auto loaded = tsl::csv_to_chunk(std::pmr::new_delete_resource(),
                                    reinterpret_cast<const uint8_t*>(bad.data()), bad.size());

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().type == core::error_code_t::conversion_failure);
}

TEST_CASE("csv_to_chunk: the error message lives on the caller's resource") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto loaded = tsl::csv_to_chunk(&arena, "/nonexistent_dir_otterstax/missing.csv");

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().what.get_allocator().resource() == &arena);
}
