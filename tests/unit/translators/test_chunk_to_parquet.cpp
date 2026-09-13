// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_parquet.hpp"
#include "rows_chunk.hpp"

// otterbrix's parser headers (pulled in above) #define DAY / SECOND, which clash
// with Arrow's TimeUnit/DateUnit enum values.
#undef DAY
#undef SECOND

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <filesystem>
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

    // BIGINT id, STRING name — built via the CSV input translator.
    data_chunk_t make_people_chunk(std::pmr::memory_resource* res) {
        const std::string csv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Dave\n5,Eve\n";
        auto loaded = tsl::csv_to_chunk(res, reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    // The whole file through Arrow's own reader, independent of the input translator,
    // so a result longer than one engine chunk can be checked row by row.
    std::shared_ptr<arrow::Table> read_parquet_table(const std::string& path) {
        auto file = arrow::io::ReadableFile::Open(path);
        REQUIRE(file.ok());
        auto reader = parquet::arrow::OpenFile(*file, arrow::default_memory_pool());
        REQUIRE(reader.ok());
        std::shared_ptr<arrow::Table> table;
        REQUIRE((*reader)->ReadTable(&table).ok());
        return table;
    }

} // namespace

TEST_CASE("chunk_to_parquet: round-trips through parquet_to_chunk") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_parquet.parquet";

    // data_chunk_t is move-only, so the batch is built explicitly rather than
    // with a braced init-list (which would copy).
    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_people_chunk(res));

    auto written = tsl::chunk_to_parquet(res, chunks, path);
    REQUIRE_FALSE(written.has_error());
    REQUIRE(written.value());
    REQUIRE(std::filesystem::exists(path));

    auto back = tsl::parquet_to_chunk(res, path);
    REQUIRE_FALSE(back.has_error());
    const auto& chunk = back.value();
    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 5);
    // The source chunk's id column is BIGINT, so it round-trips as int64.
    REQUIRE(chunk.value(0, 0).value<int64_t>() == 1);
    REQUIRE(chunk.value(0, 4).value<int64_t>() == 5);
    REQUIRE(chunk.value(1, 0).value<std::string_view>() == "Alice");
    REQUIRE(chunk.value(1, 4).value<std::string_view>() == "Eve");

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_parquet: two chunks are written in order into one file") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_parquet_two.parquet";

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_rows_chunk(res, 0, 3));
    chunks.push_back(make_rows_chunk(res, 3, 2));

    auto written = tsl::chunk_to_parquet(res, chunks, path);
    REQUIRE_FALSE(written.has_error());

    auto table = read_parquet_table(path);
    REQUIRE(table->num_rows() == 5);
    auto ids = table->GetColumnByName("id");
    REQUIRE(ids != nullptr);
    auto names = table->GetColumnByName("name");
    REQUIRE(names != nullptr);
    int64_t row = 0;
    for (const auto& piece : ids->chunks()) {
        const auto& arr = static_cast<const arrow::Int64Array&>(*piece);
        for (int64_t i = 0; i < arr.length(); ++i, ++row) {
            REQUIRE(arr.Value(i) == row);
        }
    }
    REQUIRE(row == 5);

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_parquet: a result longer than one chunk (>1024 rows) is complete and ordered") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_parquet_long.parquet";
    constexpr size_t first = 1024, second = 500;

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_rows_chunk(res, 0, first));
    chunks.push_back(make_rows_chunk(res, first, second));

    auto written = tsl::chunk_to_parquet(res, chunks, path);
    REQUIRE_FALSE(written.has_error());

    auto table = read_parquet_table(path);
    REQUIRE(table->num_rows() == static_cast<int64_t>(first + second));
    auto ids = table->GetColumnByName("id");
    REQUIRE(ids != nullptr);
    int64_t row = 0;
    for (const auto& piece : ids->chunks()) {
        const auto& arr = static_cast<const arrow::Int64Array&>(*piece);
        for (int64_t i = 0; i < arr.length(); ++i, ++row) {
            if (arr.Value(i) != row) {
                FAIL("row " << row << " out of order or missing: " << arr.Value(i));
            }
        }
    }
    REQUIRE(row == static_cast<int64_t>(first + second));

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_parquet: a hugeint column keeps its full width") {
    // parquet goes out through chunk_to_record_batch, so this is the
    // decimal128(38, 0) carrier reaching the parquet writer. The file is read back
    // with Arrow's own reader rather than parquet_to_chunk: what is pinned here is
    // the writer, and both values are outside the int64 range a narrowing write
    // would silently clip to.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_parquet_hugeint.parquet";
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

    auto written = tsl::chunk_to_parquet(res, chunks, path);
    INFO("error: " << written.error().what.c_str());
    REQUIRE_FALSE(written.has_error());

    auto table = read_parquet_table(path);
    REQUIRE(table->num_rows() == 3);
    auto wide = table->GetColumnByName("wide");
    REQUIRE(wide != nullptr);
    REQUIRE(wide->type()->id() == arrow::Type::DECIMAL128);
    std::vector<std::string> values;
    for (const auto& piece : wide->chunks()) {
        const auto& arr = static_cast<const arrow::Decimal128Array&>(*piece);
        for (int64_t i = 0; i < arr.length(); ++i) {
            values.push_back(arr.IsNull(i) ? std::string{"null"} : arr.FormatValue(i));
        }
    }
    REQUIRE(values == std::vector<std::string>{"18446744073709551616", "-18446744073709551616", "null"});

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_parquet: a DECIMAL round-trips through parquet_to_chunk with its scale") {
    // The loop this closes: DECIMAL(18, 4) → decimal128(18, 4) → DECIMAL(18, 4), the unscaled
    // integer unchanged on both crossings and the point in the same place. Read back with
    // Arrow's own reader too, so it is the FILE that is asserted to carry the scale and not
    // just this project's loader agreeing with its own writer.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_parquet_decimal.parquet";
    std::filesystem::remove(path);

    auto amount = complex_logical_type::create_decimal(res, 18, 4, "amount");
    REQUIRE_FALSE(amount.has_error());
    std::pmr::vector<complex_logical_type> types{res};
    types.push_back(amount.value());
    data_chunk_t chunk(res, types, 4);
    chunk.set_cardinality(4);
    chunk.set_value(0, 0, logical_value_t::create_decimal(res, types[0], int64_t{12345}));  // 1.2345
    chunk.set_value(0, 1, logical_value_t::create_decimal(res, types[0], int64_t{-12345})); // -1.2345
    // The largest magnitude DECIMAL(18, 4) may declare: 10^18 - 1 unscaled.
    chunk.set_value(0, 2, logical_value_t::create_decimal(res, types[0], int64_t{999999999999999999}));
    chunk.set_value(0, 3, logical_value_t{res, nullptr});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_parquet(res, chunks, path);
    INFO("error: " << written.error().what.c_str());
    REQUIRE_FALSE(written.has_error());

    auto table = read_parquet_table(path);
    auto column = table->GetColumnByName("amount");
    REQUIRE(column != nullptr);
    REQUIRE(column->type()->id() == arrow::Type::DECIMAL128);
    const auto& declared = static_cast<const arrow::Decimal128Type&>(*column->type());
    REQUIRE(declared.precision() == 18);
    REQUIRE(declared.scale() == 4);
    std::vector<std::string> values;
    for (const auto& piece : column->chunks()) {
        const auto& decimals = static_cast<const arrow::Decimal128Array&>(*piece);
        for (int64_t i = 0; i < decimals.length(); ++i) {
            values.push_back(decimals.IsNull(i) ? std::string{"null"} : decimals.FormatValue(i));
        }
    }
    REQUIRE(values == std::vector<std::string>{"1.2345", "-1.2345", "99999999999999.9999", "null"});

    auto loaded = tsl::parquet_to_chunk(res, path);
    INFO("load error: " << loaded.error().what.c_str());
    REQUIRE_FALSE(loaded.has_error());
    // types() answers a vector by value, so it is kept alive for the checks below.
    const auto loaded_types = loaded.value().types();
    const auto& back = loaded_types[0];
    REQUIRE(back.type() == logical_type::DECIMAL);
    const auto* spec = back.extension_as<components::types::decimal_logical_type_extension>();
    REQUIRE(spec->width() == 18);
    REQUIRE(spec->scale() == 4);
    REQUIRE(loaded.value().value(0, 0).value<int64_t>() == 12345);
    REQUIRE(loaded.value().value(0, 1).value<int64_t>() == -12345);
    REQUIRE(loaded.value().value(0, 2).value<int64_t>() == 999999999999999999);
    REQUIRE(loaded.value().value(0, 3).is_null());

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_parquet: a hugeint column round-trips through parquet_to_chunk") {
    // The whole loop this project owns: HUGEINT → decimal128(38, 0) → HUGEINT, values
    // that no 64-bit column could hold surviving unchanged, NULL staying NULL.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_parquet_hugeint_rt.parquet";
    std::filesystem::remove(path);

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::HUGEINT, "wide");
    data_chunk_t chunk(res, types, 4);
    chunk.set_cardinality(4);
    const auto big = absl::MakeInt128(1, 0); // 2^64
    // The largest magnitudes decimal128(38, 0) may declare, either side of zero.
    components::types::int128_t max_declarable = 1;
    for (int i = 0; i < 38; ++i) {
        max_declarable *= 10;
    }
    max_declarable -= 1;
    chunk.set_value(0, 0, logical_value_t{res, big});
    chunk.set_value(0, 1, logical_value_t{res, -big});
    chunk.set_value(0, 2, logical_value_t{res, max_declarable});
    chunk.set_value(0, 3, logical_value_t{res, nullptr});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_parquet(res, chunks, path);
    INFO("write error: " << written.error().what.c_str());
    REQUIRE_FALSE(written.has_error());

    auto back = tsl::parquet_to_chunk(res, path);
    INFO("read error: " << back.error().what.c_str());
    REQUIRE_FALSE(back.has_error());
    const auto& reloaded = back.value();
    REQUIRE(reloaded.column_count() == 1);
    REQUIRE(reloaded.size() == 4);
    REQUIRE(reloaded.types()[0].type() == logical_type::HUGEINT);
    REQUIRE(reloaded.types()[0].alias() == "wide");
    REQUIRE(reloaded.value(0, 0).value<components::types::int128_t>() == big);
    REQUIRE(reloaded.value(0, 1).value<components::types::int128_t>() == -big);
    REQUIRE(reloaded.value(0, 2).value<components::types::int128_t>() == max_declarable);
    REQUIRE(reloaded.value(0, 3).is_null());

    std::filesystem::remove(path);
}

TEST_CASE("chunk_to_parquet: a hugeint past 10^38 is refused, nothing is written") {
    // int128 reaches ±1.7e38; decimal128(38, 0) — Arrow's only 128-bit carrier — may
    // declare only ±(10^38 - 1). Arrow would append and write such a value silently and
    // hand it back unchanged, but its own ValidateFull calls the array Invalid and the
    // parquet spec forbids writing a value larger than the annotation allows, leaving a
    // strict reader free to refuse it or read NULL. The refusal happens before the file
    // is opened, so no half-written file is left behind.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_parquet_tail.parquet";
    std::filesystem::remove(path);

    components::types::int128_t tail = 1;
    for (int i = 0; i < 38; ++i) {
        tail *= 10; // exactly 10^38, one past the precision window
    }

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::HUGEINT, "wide");
    data_chunk_t chunk(res, types, 1);
    chunk.set_cardinality(1);
    chunk.set_value(0, 0, logical_value_t{res, tail});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_parquet(res, chunks, path);

    REQUIRE(written.has_error());
    REQUIRE(written.error().type == core::error_code_t::conversion_failure);
    REQUIRE(std::string_view{written.error().what.c_str()}.find("wide") != std::string_view::npos);
    REQUIRE(std::string_view{written.error().what.c_str()}.find("100000000000000000000000000000000000000") !=
            std::string_view::npos);
    REQUIRE_FALSE(std::filesystem::exists(path));
}

TEST_CASE("chunk_to_parquet: a column without a name is refused, nothing is written") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;
    const std::string path = "/tmp/otterstax_test_chunk_to_parquet_unnamed.parquet";
    std::filesystem::remove(path);

    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::BIGINT);
    data_chunk_t chunk(res, types, 1);
    chunk.set_cardinality(1);
    chunk.set_value(0, 0, logical_value_t{res, int64_t{42}});

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));

    auto written = tsl::chunk_to_parquet(res, chunks, path);
    REQUIRE(written.has_error());
    REQUIRE(written.error().type == core::error_code_t::schema_error);
    REQUIRE_FALSE(std::filesystem::exists(path));
}

TEST_CASE("chunk_to_parquet: an output path that cannot be opened is io_error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;

    std::pmr::vector<data_chunk_t> chunks(res);
    chunks.push_back(make_people_chunk(res));

    auto written = tsl::chunk_to_parquet(res, chunks, "/nonexistent_dir_otterstax/out.parquet");
    REQUIRE(written.has_error());
    REQUIRE(written.error().type == core::error_code_t::io_error);
}
