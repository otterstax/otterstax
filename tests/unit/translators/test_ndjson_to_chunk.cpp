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
    auto loaded = tsl::ndjson_to_chunk(std::pmr::new_delete_resource(),
                                       reinterpret_cast<const uint8_t*>(s.data()), s.size());
    REQUIRE_FALSE(loaded.has_error());
    return std::move(loaded.value());
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

TEST_CASE("ndjson_to_chunk: buffer overload unwraps a JSON array with surrounding whitespace") {
    auto chunk = parse("  [ {\"id\":1,\"name\":\"Alice\"} , {\"id\":2,\"name\":\"Bob\"} ]\n");

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 2);
    REQUIRE(chunk.value(col(chunk, "id"), 0).value<int64_t>() == 1);
}

TEST_CASE("ndjson_to_chunk: file overload reads NDJSON") {
    const std::string path = "/tmp/otterstax_test_ndjson_to_chunk.ndjson";
    { std::ofstream(path) << kNdjson; }

    auto loaded = tsl::ndjson_to_chunk(std::pmr::new_delete_resource(), path);
    REQUIRE_FALSE(loaded.has_error());
    const auto& chunk = loaded.value();

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 3);
    REQUIRE(chunk.value(col(chunk, "id"), 1).value<int64_t>() == 2);

    std::filesystem::remove(path);
}

TEST_CASE("ndjson_to_chunk: a missing file is io_error") {
    auto loaded = tsl::ndjson_to_chunk(std::pmr::new_delete_resource(), "/nonexistent_dir_otterstax/missing.ndjson");

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().type == core::error_code_t::io_error);
}

TEST_CASE("ndjson_to_chunk: a malformed body is conversion_failure") {
    const std::string bad = "{\"id\":1,\"name\":\"Alice\"\n{\"id\":2}\n";
    auto loaded = tsl::ndjson_to_chunk(std::pmr::new_delete_resource(),
                                       reinterpret_cast<const uint8_t*>(bad.data()), bad.size());

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().type == core::error_code_t::conversion_failure);
}

TEST_CASE("ndjson_to_chunk: a bare integer past int64 is read as a double, losing digits") {
    // Characterization of Arrow's JSON type inference, and the reason chunk_to_ndjson
    // quotes a 128-bit column: JSON has one number type, and a bare 2^64 makes the whole
    // column `double`. The value read back is 1.8446744073709552e+19 — every digit past
    // the 53rd bit is gone, with no error raised anywhere along the way.
    auto chunk = parse("{\"wide\":18446744073709551616}\n{\"wide\":1}\n");

    const int wide = col(chunk, "wide");
    REQUIRE(wide >= 0);
    REQUIRE(chunk.types()[wide].type() == components::types::logical_type::DOUBLE);
    REQUIRE(chunk.value(wide, 0).value<double>() == 18446744073709551616.0);
}

TEST_CASE("ndjson_to_chunk: a quoted 128-bit integer keeps every digit") {
    // The shape chunk_to_ndjson writes for a HUGEINT column. ndjson carries no types, so
    // it comes back as text, but the text is exact — this is what makes the export
    // round-trip without rounding.
    auto chunk = parse("{\"wide\":\"18446744073709551616\"}\n{\"wide\":\"-170141183460469231731687303715884105728\"}\n");

    const int wide = col(chunk, "wide");
    REQUIRE(wide >= 0);
    REQUIRE(chunk.types()[wide].type() == components::types::logical_type::STRING_LITERAL);
    REQUIRE(chunk.value(wide, 0).value<std::string_view>() == "18446744073709551616");
    REQUIRE(chunk.value(wide, 1).value<std::string_view>() == "-170141183460469231731687303715884105728");
}

TEST_CASE("ndjson_to_chunk: a lone '[' does not crash and is reported") {
    const std::string bad = "[";
    auto loaded = tsl::ndjson_to_chunk(std::pmr::new_delete_resource(),
                                       reinterpret_cast<const uint8_t*>(bad.data()), bad.size());

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().type == core::error_code_t::conversion_failure);
}
