// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/output/chunk_to_ndjson.hpp"
#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/ndjson_to_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <string>
#include <string_view>

using components::vector::data_chunk_t;

namespace {

data_chunk_t make_people_chunk(std::pmr::memory_resource* res) {
    const std::string csv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Dave\n5,Eve\n";
    return tsl::csv_to_chunk(res, reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
}

int col(const data_chunk_t& chunk, std::string_view name) {
    const auto types = chunk.types();
    for (size_t i = 0; i < types.size(); ++i)
        if (std::string_view{types[i].alias()} == name) return static_cast<int>(i);
    return -1;
}

} // namespace

TEST_CASE("chunk_to_ndjson: round-trips through ndjson_to_chunk") {
    auto* res = std::pmr::get_default_resource();
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson.ndjson";

    auto chunk = make_people_chunk(res);
    tsl::chunk_to_ndjson(chunk, path);
    REQUIRE(std::filesystem::exists(path));

    auto back = tsl::ndjson_to_chunk(res, path);
    REQUIRE(back.column_count() == 2);
    REQUIRE(back.size() == 5);
    REQUIRE(back.value(col(back, "id"), 0).value<int64_t>() == 1);
    REQUIRE(back.value(col(back, "id"), 4).value<int64_t>() == 5);
    REQUIRE(back.value(col(back, "name"), 0).value<std::string_view>() == "Alice");
    REQUIRE(back.value(col(back, "name"), 4).value<std::string_view>() == "Eve");

    std::filesystem::remove(path);
}
