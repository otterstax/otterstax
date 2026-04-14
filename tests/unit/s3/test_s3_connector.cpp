// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/json_to_chunk.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"

#include <catch2/catch.hpp>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>

TEST_CASE("S3Data: parquet bytes -> data_chunk_t via handler") {
    auto* resource = std::pmr::get_default_resource();

    auto schema = arrow::schema({arrow::field("val", arrow::int64())});
    arrow::Int64Builder b;
    REQUIRE(b.Append(42).ok());
    auto table = arrow::Table::Make(schema, {*b.Finish()});
    auto sink = *arrow::io::BufferOutputStream::Create();
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1024).ok());
    auto buffer = *sink->Finish();

    auto chunk = tsl::parquet_to_chunk(resource, buffer->data(), buffer->size());
    REQUIRE(chunk.column_count() == 1);
    REQUIRE(chunk.size() == 1);
    REQUIRE(chunk.value(0, 0).value<int64_t>() == 42);
}

TEST_CASE("S3Data: csv bytes -> data_chunk_t") {
    auto* resource = std::pmr::get_default_resource();
    std::string csv = "id,name\n1,alice\n2,bob\n";

    auto chunk = tsl::csv_to_chunk(resource,
                                    reinterpret_cast<const uint8_t*>(csv.data()), csv.size(),
                                    ',', true);
    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 2);
}

TEST_CASE("S3Data: json bytes -> data_chunk_t") {
    auto* resource = std::pmr::get_default_resource();
    std::string ndjson = "{\"x\":1}\n{\"x\":2}\n";

    auto chunk = tsl::json_to_chunk(resource,
                                     reinterpret_cast<const uint8_t*>(ndjson.data()), ndjson.size());
    REQUIRE(chunk.column_count() == 1);
    REQUIRE(chunk.size() == 2);
}

TEST_CASE("S3Data: empty bytes") {
    auto* resource = std::pmr::get_default_resource();
    REQUIRE_THROWS(tsl::parquet_to_chunk(resource, nullptr, 0));
}

TEST_CASE("S3Data: corrupted parquet") {
    auto* resource = std::pmr::get_default_resource();
    std::vector<uint8_t> garbage = {0x00, 0x01, 0x02, 0x03};
    REQUIRE_THROWS(tsl::parquet_to_chunk(resource, garbage.data(), garbage.size()));
}
