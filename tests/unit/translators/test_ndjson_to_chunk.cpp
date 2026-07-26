// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/ndjson_to_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory_resource>
#include <string>
#include <string_view>

using components::vector::data_chunk_t;

namespace {

const char* kNdjson =
    "{\"id\":1,\"name\":\"Alice\"}\n"
    "{\"id\":2,\"name\":\"Bob\"}\n"
    "{\"id\":3,\"name\":\"Charlie\"}\n";

// Resolve a column by name — Arrow's JSON reader does not guarantee field order.
int col(const data_chunk_t& chunk, std::string_view name) {
    const auto types = chunk.types();
    for (size_t i = 0; i < types.size(); ++i)
        if (std::string_view{types[i].alias()} == name) return static_cast<int>(i);
    return -1;
}

data_chunk_t parse(const std::string& s) {
    return tsl::ndjson_to_chunk(std::pmr::get_default_resource(),
                                reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

} // namespace

TEST_CASE("ndjson_to_chunk: buffer overload parses NDJSON columns, rows and values") {
    auto chunk = parse(kNdjson);

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 3);

    const int id = col(chunk, "id");
    const int nm = col(chunk, "name");
    REQUIRE(id >= 0);
    REQUIRE(nm >= 0);

    REQUIRE(chunk.value(id, 0).value<int64_t>() == 1);
    REQUIRE(chunk.value(id, 2).value<int64_t>() == 3);
    REQUIRE(chunk.value(nm, 0).value<std::string_view>() == "Alice");
    REQUIRE(chunk.value(nm, 2).value<std::string_view>() == "Charlie");
}

TEST_CASE("ndjson_to_chunk: buffer overload unwraps a JSON array") {
    auto chunk = parse(R"([{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}])");

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 2);
    REQUIRE(chunk.value(col(chunk, "id"), 1).value<int64_t>() == 2);
    REQUIRE(chunk.value(col(chunk, "name"), 1).value<std::string_view>() == "Bob");
}

TEST_CASE("ndjson_to_chunk: file overload reads NDJSON") {
    const std::string path = "/tmp/otterstax_test_ndjson_to_chunk.ndjson";
    { std::ofstream(path) << kNdjson; }

    auto chunk = tsl::ndjson_to_chunk(std::pmr::get_default_resource(), path);

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 3);
    REQUIRE(chunk.value(col(chunk, "id"), 1).value<int64_t>() == 2);

    std::filesystem::remove(path);
}
