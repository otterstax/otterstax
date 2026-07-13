// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/parquet_to_chunk.hpp"

// otterbrix's parser headers (pulled in above) #define DAY / SECOND, which clash
// with Arrow's TimeUnit/DateUnit enum values.
#undef DAY
#undef SECOND

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory_resource>
#include <string>
#include <string_view>

using components::vector::data_chunk_t;

namespace {

// Write a 5-row parquet file (int32 id, utf8 name) at `path`.
void write_people_parquet(const std::string& path) {
    arrow::Int32Builder  id_b;
    arrow::StringBuilder name_b;
    REQUIRE(id_b.AppendValues({1, 2, 3, 4, 5}).ok());
    REQUIRE(name_b.AppendValues({"Alice", "Bob", "Charlie", "Dave", "Eve"}).ok());

    std::shared_ptr<arrow::Array> id_arr, name_arr;
    REQUIRE(id_b.Finish(&id_arr).ok());
    REQUIRE(name_b.Finish(&name_arr).ok());

    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8())});
    auto table  = arrow::Table::Make(schema, {id_arr, name_arr});

    auto sink = arrow::io::FileOutputStream::Open(path).ValueOrDie();
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1024).ok());
    REQUIRE(sink->Close().ok());
}

std::string read_bytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::string{std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

} // namespace

TEST_CASE("parquet_to_chunk: file overload parses columns, rows and values") {
    const std::string path = "/tmp/otterstax_test_parquet_to_chunk.parquet";
    write_people_parquet(path);

    auto chunk = tsl::parquet_to_chunk(std::pmr::get_default_resource(), path);

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 5);
    // int32 id column round-trips as INTEGER.
    REQUIRE(chunk.value(0, 0).value<int32_t>() == 1);
    REQUIRE(chunk.value(0, 4).value<int32_t>() == 5);
    REQUIRE(chunk.value(1, 0).value<std::string_view>() == "Alice");
    REQUIRE(chunk.value(1, 4).value<std::string_view>() == "Eve");

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_chunk: buffer overload matches file overload") {
    const std::string path = "/tmp/otterstax_test_parquet_to_chunk_buf.parquet";
    write_people_parquet(path);
    const std::string bytes = read_bytes(path);

    auto chunk = tsl::parquet_to_chunk(std::pmr::get_default_resource(),
                                       reinterpret_cast<const uint8_t*>(bytes.data()),
                                       bytes.size());

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 5);
    REQUIRE(chunk.value(0, 2).value<int32_t>() == 3);
    REQUIRE(chunk.value(1, 2).value<std::string_view>() == "Charlie");

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_struct: extracts the schema only") {
    const std::string path = "/tmp/otterstax_test_parquet_to_struct.parquet";
    write_people_parquet(path);

    auto type = tsl::parquet_to_struct(std::pmr::get_default_resource(), path);
    REQUIRE(type.child_types().size() == 2);

    std::filesystem::remove(path);
}
