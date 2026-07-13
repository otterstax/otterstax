// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/output/chunk_to_csv.hpp"
#include "otterbrix/translators/input/csv_to_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <string>
#include <string_view>

using components::vector::data_chunk_t;

namespace {

// Build a 5-row chunk (BIGINT id, STRING name) via the CSV input translator.
data_chunk_t make_people_chunk(std::pmr::memory_resource* res) {
    const std::string csv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Dave\n5,Eve\n";
    return tsl::csv_to_chunk(res, reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
}

} // namespace

TEST_CASE("chunk_to_csv: round-trips through csv_to_chunk") {
    auto* res = std::pmr::get_default_resource();
    const std::string path = "/tmp/otterstax_test_chunk_to_csv.csv";

    auto chunk = make_people_chunk(res);
    tsl::chunk_to_csv(chunk, path);
    REQUIRE(std::filesystem::exists(path));

    auto back = tsl::csv_to_chunk(res, path);
    REQUIRE(back.column_count() == 2);
    REQUIRE(back.size() == 5);
    REQUIRE(back.value(0, 0).value<int64_t>() == 1);
    REQUIRE(back.value(0, 4).value<int64_t>() == 5);
    REQUIRE(back.value(1, 0).value<std::string_view>() == "Alice");
    REQUIRE(back.value(1, 4).value<std::string_view>() == "Eve");

    std::filesystem::remove(path);
}
