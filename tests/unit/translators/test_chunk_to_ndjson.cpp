// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/ndjson_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_ndjson.hpp"
#include "rows_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;
using components::vector::data_chunk_t;
using otterstax::test::make_rows_chunk;

namespace {

    data_chunk_t make_people_chunk(std::pmr::memory_resource* res) {
        const std::string csv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Dave\n5,Eve\n";
        auto loaded = tsl::csv_to_chunk(res, reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    int col(const data_chunk_t& chunk, std::string_view name) {
        const auto types = chunk.types();
        for (size_t i = 0; i < types.size(); ++i)
            if (std::string_view{types[i].alias()} == name)
                return static_cast<int>(i);
        return -1;
    }

    std::vector<std::string> read_lines(const std::string& path) {
        std::ifstream in(path);
        std::vector<std::string> lines;
        for (std::string line; std::getline(in, line);) {
            lines.push_back(line);
        }
        return lines;
    }

} // namespace

TEST_CASE("chunk_to_ndjson: a DECIMAL is written with its point not as the unscaled integer") {
    // ndjson does not go through chunk_to_record_batch — it writes the JSON itself — so it has
    // its own dispatch to get right: on the physical type it would write the stored unscaled
    // integer (1.2345 as 12345). The exact digits go out as a JSON *string*, the way the
    // 128-bit integer does: JSON has one number type and readers carry it in a double, so the
    // digits a scale exists to keep would be rounded away in the reader.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson_decimal.ndjson";
    std::filesystem::remove(path);

    auto amount = complex_logical_type::create_decimal(res, 18, 4, "amount");
    REQUIRE_FALSE(amount.has_error());
    std::pmr::vector<complex_logical_type> types{res};
    types.push_back(amount.value());
    data_chunk_t chunk(res, types, 3);
    chunk.set_cardinality(3);
    chunk.set_value(0, 0, logical_value_t::create_decimal(res, types[0], int64_t{12345}));
    chunk.set_value(0, 1, logical_value_t::create_decimal(res, types[0], int64_t{-12345}));
    chunk.set_value(0, 2, logical_value_t{res, nullptr});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_ndjson(res, chunks, path);
    INFO("error: " << written.error().what.c_str());
    REQUIRE_FALSE(written.has_error());

    auto lines = read_lines(path);
    REQUIRE(lines.size() == 3);
    REQUIRE(lines[0] == R"({"amount":"1.2345"})");
    REQUIRE(lines[1] == R"({"amount":"-1.2345"})");
    REQUIRE(lines[2] == R"({"amount":null})");

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_ndjson: round-trips through ndjson_to_chunk") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson.ndjson";

    // data_chunk_t is move-only, so the batch is built explicitly rather than
    // with a braced init-list (which would copy).
    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_people_chunk(res));

    auto written = tsl::chunk_to_ndjson(res, chunks, path);
    REQUIRE_FALSE(written.has_error());
    REQUIRE(written.value());
    REQUIRE(std::filesystem::exists(path));

    auto back = tsl::ndjson_to_chunk(res, path);
    REQUIRE_FALSE(back.has_error());
    const auto& chunk = back.value();
    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 5);
    REQUIRE(chunk.value(col(chunk, "id"), 0).value<int64_t>() == 1);
    REQUIRE(chunk.value(col(chunk, "id"), 4).value<int64_t>() == 5);
    REQUIRE(chunk.value(col(chunk, "name"), 0).value<std::string_view>() == "Alice");
    REQUIRE(chunk.value(col(chunk, "name"), 4).value<std::string_view>() == "Eve");

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_ndjson: two chunks are written in order into one file") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson_two.ndjson";

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_rows_chunk(res, 0, 3));
    chunks.push_back(make_rows_chunk(res, 3, 2));

    auto written = tsl::chunk_to_ndjson(res, chunks, path);
    REQUIRE_FALSE(written.has_error());

    const auto lines = read_lines(path);
    REQUIRE(lines.size() == 5);
    for (size_t i = 0; i < 5; ++i) {
        const auto id = std::to_string(i);
        REQUIRE(lines[i] == "{\"id\":" + id + ",\"name\":\"row_" + id + "\"}");
    }

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_ndjson: a result longer than one chunk (>1024 rows) is complete and ordered") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson_long.ndjson";
    constexpr size_t first = 1024, second = 500;

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_rows_chunk(res, 0, first));
    chunks.push_back(make_rows_chunk(res, first, second));

    auto written = tsl::chunk_to_ndjson(res, chunks, path);
    REQUIRE_FALSE(written.has_error());

    const auto lines = read_lines(path);
    REQUIRE(lines.size() == first + second);
    for (size_t i = 0; i < first + second; ++i) {
        const auto id = std::to_string(i);
        if (lines[i] != "{\"id\":" + id + ",\"name\":\"row_" + id + "\"}") {
            FAIL("row " << i << " out of order or missing: " << lines[i]);
        }
    }

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_ndjson: a column without a name is refused, nothing is written") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson_unnamed.ndjson";
    std::filesystem::remove(path);

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::BIGINT);
    data_chunk_t chunk(res, types, 1);
    chunk.set_cardinality(1);
    chunk.set_value(0, 0, logical_value_t{res, int64_t{42}});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_ndjson(res, chunks, path);
    REQUIRE(written.has_error());
    REQUIRE(written.error().type == core::error_code_t::schema_error);
    REQUIRE_FALSE(std::filesystem::exists(path));
}

TEST_CASE("chunk_to_ndjson: a nested column is refused instead of being written as null") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson_nested.ndjson";
    std::filesystem::remove(path);

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER}, "items"));
    data_chunk_t chunk(res, types, 1);

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_ndjson(res, chunks, path);
    REQUIRE(written.has_error());
    REQUIRE(written.error().type == core::error_code_t::conversion_failure);
    REQUIRE_FALSE(std::filesystem::exists(path));
}

TEST_CASE("chunk_to_ndjson: a hugeint column is written as a JSON string and reads back digit for digit") {
    // The writer gate admits INT128 only as HUGEINT, and the value goes out through
    // the stream operator because std::to_chars has no 128-bit overload. Both values
    // are outside the int64 range, so a narrowing write would be visible here.
    //
    // It is quoted, unlike every other integer column: JSON has a single number type and
    // readers carry it in a double. Written bare, 2^64 comes back from Arrow's own JSON
    // reader as the double 1.8446744073709552e+19 — the column is inferred `double` and
    // every digit past the 53rd is lost, with no error anywhere. Quoted, the digits are
    // exact in the file and the round-trip below reads them back unchanged.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson_hugeint.ndjson";
    std::filesystem::remove(path);

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::HUGEINT, "wide");
    data_chunk_t chunk(res, types, 3);
    chunk.set_cardinality(3);
    const auto big = absl::MakeInt128(1, 0); // 2^64
    chunk.set_value(0, 0, logical_value_t{res, big});
    chunk.set_value(0, 1, logical_value_t{res, -big});
    chunk.set_value(0, 2, logical_value_t{res, nullptr});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_ndjson(res, chunks, path);
    INFO("error: " << written.error().what.c_str());
    REQUIRE_FALSE(written.has_error());

    auto lines = read_lines(path);
    REQUIRE(lines.size() == 3);
    REQUIRE(lines[0] == "{\"wide\":\"18446744073709551616\"}");
    REQUIRE(lines[1] == "{\"wide\":\"-18446744073709551616\"}");
    REQUIRE(lines[2] == "{\"wide\":null}");

    // ndjson carries no types, so the column comes back as text — but as the exact text:
    // nothing about the value was rounded away on the way out.
    auto back = tsl::ndjson_to_chunk(res, path);
    REQUIRE_FALSE(back.has_error());
    const auto& reloaded = back.value();
    REQUIRE(reloaded.size() == 3);
    const int wide = col(reloaded, "wide");
    REQUIRE(wide >= 0);
    REQUIRE(reloaded.types()[wide].type() == logical_type::STRING_LITERAL);
    REQUIRE(reloaded.value(wide, 0).value<std::string_view>() == "18446744073709551616");
    REQUIRE(reloaded.value(wide, 1).value<std::string_view>() == "-18446744073709551616");
    REQUIRE(reloaded.value(wide, 2).is_null());

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_ndjson: a uhugeint column is refused instead of being written as null") {
    // The other 128-bit width has no writer reading, and the gate refuses it by name
    // rather than letting write_value's default emit a silent null.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson_uhugeint.ndjson";
    std::filesystem::remove(path);

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::UHUGEINT, "wide");
    data_chunk_t chunk(res, types, 1);

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_ndjson(res, chunks, path);
    REQUIRE(written.has_error());
    REQUIRE(written.error().type == core::error_code_t::conversion_failure);
    REQUIRE(std::string_view{written.error().what.c_str()}.find("wide") != std::string_view::npos);
    REQUIRE_FALSE(std::filesystem::exists(path));
}

TEST_CASE("chunk_to_ndjson: strings are JSON-escaped, NULL cells are null") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_ndjson_escape.ndjson";

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::STRING_LITERAL, "text");
    types.emplace_back(logical_type::BIGINT, "n");
    data_chunk_t chunk(res, types, 2);
    chunk.set_cardinality(2);
    chunk.set_value(0, 0, logical_value_t{res, std::string{"a\"b\\c\nd\x01"}});
    chunk.set_value(1, 0, logical_value_t{res, int64_t{7}});
    chunk.set_value(0, 1, logical_value_t{res, nullptr});
    chunk.set_value(1, 1, logical_value_t{res, nullptr});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_ndjson(res, chunks, path);
    REQUIRE_FALSE(written.has_error());

    const auto lines = read_lines(path);
    REQUIRE(lines.size() == 2);
    REQUIRE(lines[0] == "{\"text\":\"a\\\"b\\\\c\\nd\\u0001\",\"n\":7}");
    REQUIRE(lines[1] == "{\"text\":null,\"n\":null}");

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_ndjson: an output path that cannot be opened is io_error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_people_chunk(res));

    auto written = tsl::chunk_to_ndjson(res, chunks, "/nonexistent_dir_otterstax/out.ndjson");
    REQUIRE(written.has_error());
    REQUIRE(written.error().type == core::error_code_t::io_error);
}
