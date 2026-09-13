// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_csv.hpp"
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

    // Build a 5-row chunk (BIGINT id, STRING name) via the CSV input translator.
    data_chunk_t make_people_chunk(std::pmr::memory_resource* res) {
        const std::string csv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Dave\n5,Eve\n";
        auto loaded = tsl::csv_to_chunk(res, reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
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

TEST_CASE("chunk_to_csv: round-trips through csv_to_chunk") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_csv.csv";

    // data_chunk_t is move-only, so the batch is built explicitly rather than
    // with a braced init-list (which would copy).
    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_people_chunk(res));

    auto written = tsl::chunk_to_csv(res, chunks, path);
    REQUIRE_FALSE(written.has_error());
    REQUIRE(written.value());
    REQUIRE(std::filesystem::exists(path));

    auto back = tsl::csv_to_chunk(res, path);
    REQUIRE_FALSE(back.has_error());
    REQUIRE(back.value().column_count() == 2);
    REQUIRE(back.value().size() == 5);
    REQUIRE(back.value().value(0, 0).value<int64_t>() == 1);
    REQUIRE(back.value().value(0, 4).value<int64_t>() == 5);
    REQUIRE(back.value().value(1, 0).value<std::string_view>() == "Alice");
    REQUIRE(back.value().value(1, 4).value<std::string_view>() == "Eve");

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_csv: two chunks are written in order into one file") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_csv_two.csv";

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_rows_chunk(res, 0, 3));
    chunks.push_back(make_rows_chunk(res, 3, 2));

    auto written = tsl::chunk_to_csv(res, chunks, path);
    REQUIRE_FALSE(written.has_error());

    const auto lines = read_lines(path);
    REQUIRE(lines.size() == 1 + 5);
    for (size_t i = 0; i < 5; ++i) {
        INFO("line " << i << ": " << lines[i + 1]);
        REQUIRE(lines[i + 1].starts_with(std::to_string(i) + ","));
        REQUIRE(lines[i + 1].find("row_" + std::to_string(i)) != std::string::npos);
    }

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_csv: a result longer than one chunk (>1024 rows) is complete and ordered") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_csv_long.csv";
    constexpr size_t first = 1024, second = 500;

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_rows_chunk(res, 0, first));
    chunks.push_back(make_rows_chunk(res, first, second));

    auto written = tsl::chunk_to_csv(res, chunks, path);
    REQUIRE_FALSE(written.has_error());

    const auto lines = read_lines(path);
    REQUIRE(lines.size() == 1 + first + second);
    for (size_t i = 0; i < first + second; ++i) {
        if (!lines[i + 1].starts_with(std::to_string(i) + ",")) {
            FAIL("row " << i << " out of order or missing: " << lines[i + 1]);
        }
    }

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_csv: a column without a name is refused, nothing is written") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_csv_unnamed.csv";
    std::filesystem::remove(path);

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::BIGINT);
    data_chunk_t chunk(res, types, 1);
    chunk.set_cardinality(1);
    chunk.set_value(0, 0, logical_value_t{res, int64_t{42}});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_csv(res, chunks, path);
    REQUIRE(written.has_error());
    REQUIRE(written.error().type == core::error_code_t::schema_error);
    REQUIRE_FALSE(std::filesystem::exists(path));
}

TEST_CASE("chunk_to_csv: a hugeint column keeps its full width") {
    // csv goes out through chunk_to_record_batch, so this is the decimal128(38, 0)
    // carrier reaching Arrow's CSV writer; both values are outside the int64 range.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_csv_hugeint.csv";
    std::filesystem::remove(path);

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::HUGEINT, "wide");
    data_chunk_t chunk(res, types, 2);
    chunk.set_cardinality(2);
    const auto big = absl::MakeInt128(1, 0); // 2^64
    chunk.set_value(0, 0, logical_value_t{res, big});
    chunk.set_value(0, 1, logical_value_t{res, -big});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_csv(res, chunks, path);
    INFO("error: " << written.error().what.c_str());
    REQUIRE_FALSE(written.has_error());

    auto lines = read_lines(path);
    REQUIRE(lines.size() == 3);
    REQUIRE(lines[0].find("wide") != std::string::npos);
    REQUIRE(lines[1].find("18446744073709551616") != std::string::npos);
    REQUIRE(lines[2].find("-18446744073709551616") != std::string::npos);

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_csv: a hugeint written to csv comes back as a double") {
    // Characterization, not an endorsement: csv carries no types, and Arrow's csv reader
    // infers `double` for a 20-digit integer, so a hugeint exported to csv and loaded
    // again is a double that lost every digit past the 53rd bit. The digits are exact in
    // the file (the case above) — it is the reader's inference that narrows them. Parquet
    // keeps the type and the value; ndjson keeps the digits as text.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_csv_hugeint_rt.csv";
    std::filesystem::remove(path);

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::HUGEINT, "wide");
    data_chunk_t chunk(res, types, 1);
    chunk.set_cardinality(1);
    chunk.set_value(0, 0, logical_value_t{res, absl::MakeInt128(1, 0)}); // 2^64

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));
    REQUIRE_FALSE(tsl::chunk_to_csv(res, chunks, path).has_error());

    auto back = tsl::csv_to_chunk(res, path);
    REQUIRE_FALSE(back.has_error());
    REQUIRE(back.value().types()[0].type() == logical_type::DOUBLE);
    REQUIRE(back.value().value(0, 0).value<double>() == 18446744073709551616.0);

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_csv: an output path that cannot be opened is io_error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_people_chunk(res));

    auto written = tsl::chunk_to_csv(res, chunks, "/nonexistent_dir_otterstax/out.csv");
    REQUIRE(written.has_error());
    REQUIRE(written.error().type == core::error_code_t::io_error);
}
