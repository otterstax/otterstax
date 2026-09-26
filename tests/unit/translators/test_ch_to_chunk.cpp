// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/ch_to_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nothing.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/time.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/uuid.h>

#include <memory_resource>
#include <optional>

using namespace components::types;
using namespace components::vector;

namespace {

clickhouse::Block make_block_3col(int rows) {
    auto col_id = std::make_shared<clickhouse::ColumnInt32>();
    auto col_score = std::make_shared<clickhouse::ColumnFloat64>();
    auto col_name = std::make_shared<clickhouse::ColumnString>();
    for (int i = 0; i < rows; ++i) {
        col_id->Append(i + 1);
        col_score->Append(static_cast<double>(i) * 1.5);
        col_name->Append("row_" + std::to_string(i));
    }
    clickhouse::Block block;
    block.AppendColumn("id", col_id);
    block.AppendColumn("score", col_score);
    block.AppendColumn("name", col_name);
    return block;
}

} // namespace

TEST_CASE("ch_to_struct: schema extraction for primitive types") {
    clickhouse::Block block;
    block.AppendColumn("id",    std::make_shared<clickhouse::ColumnInt32>());
    block.AppendColumn("score", std::make_shared<clickhouse::ColumnFloat64>());
    block.AppendColumn("label", std::make_shared<clickhouse::ColumnString>());

    auto s = tsl::ch_to_struct(std::pmr::new_delete_resource(), block);

    REQUIRE(s.type() == logical_type::STRUCT);
    REQUIRE(s.child_types().size() == 3);

    REQUIRE(s.child_types()[0].type() == logical_type::INTEGER);
    REQUIRE(s.child_types()[0].alias() == "id");

    REQUIRE(s.child_types()[1].type() == logical_type::DOUBLE);
    REQUIRE(s.child_types()[1].alias() == "score");

    REQUIRE(s.child_types()[2].type() == logical_type::STRING_LITERAL);
    REQUIRE(s.child_types()[2].alias() == "label");
}

TEST_CASE("ch_to_struct: empty block produces empty STRUCT") {
    auto s = tsl::ch_to_struct(std::pmr::new_delete_resource(), clickhouse::Block{});
    REQUIRE(s.type() == logical_type::STRUCT);
    REQUIRE(s.child_types().empty());
}

TEST_CASE("ch_to_chunk: single block, correct cardinality and column count") {
    auto* res = std::pmr::new_delete_resource();
    auto block = make_block_3col(5);

    auto converted = tsl::ch_to_chunk(res, block);
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();

    REQUIRE(chunk.size() == 5);
    REQUIRE(chunk.column_count() == 3);
}

TEST_CASE("ch_to_chunk: Int32 column values round-trip") {
    auto* res = std::pmr::new_delete_resource();

    auto col = std::make_shared<clickhouse::ColumnInt32>();
    col->Append(10);
    col->Append(20);
    col->Append(30);
    clickhouse::Block block;
    block.AppendColumn("x", col);

    auto converted = tsl::ch_to_chunk(res, block);
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();

    REQUIRE(chunk.size() == 3);
    REQUIRE(chunk.value(0, 0).value<int32_t>() == 10);
    REQUIRE(chunk.value(0, 1).value<int32_t>() == 20);
    REQUIRE(chunk.value(0, 2).value<int32_t>() == 30);
}

TEST_CASE("ch_to_chunk: Float64 column values round-trip") {
    auto* res = std::pmr::new_delete_resource();

    auto col = std::make_shared<clickhouse::ColumnFloat64>();
    col->Append(1.5);
    col->Append(2.5);
    clickhouse::Block block;
    block.AppendColumn("val", col);

    auto converted = tsl::ch_to_chunk(res, block);
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();

    REQUIRE(chunk.size() == 2);
    REQUIRE(chunk.value(0, 0).value<double>() == Catch::Approx(1.5));
    REQUIRE(chunk.value(0, 1).value<double>() == Catch::Approx(2.5));
}

TEST_CASE("ch_to_chunk: String column values round-trip") {
    auto* res = std::pmr::new_delete_resource();

    auto col = std::make_shared<clickhouse::ColumnString>();
    col->Append("hello");
    col->Append("world");
    clickhouse::Block block;
    block.AppendColumn("msg", col);

    auto converted = tsl::ch_to_chunk(res, block);
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();

    REQUIRE(chunk.size() == 2);
    REQUIRE(chunk.value(0, 0).value<std::string_view>() == "hello");
    REQUIRE(chunk.value(0, 1).value<std::string_view>() == "world");
}

TEST_CASE("ch_to_chunk: empty block produces empty chunk") {
    auto* res = std::pmr::new_delete_resource();
    auto converted = tsl::ch_to_chunk(res, clickhouse::Block{});
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();
    REQUIRE(chunk.size() == 0);
    REQUIRE(chunk.column_count() == 0);
}

TEST_CASE("ch_to_chunk: multi-block vector merges row counts") {
    auto* res = std::pmr::new_delete_resource();

    auto b1 = make_block_3col(3);
    auto b2 = make_block_3col(4);
    auto converted = tsl::ch_to_chunk(res, std::vector<clickhouse::Block>{b1, b2});
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();

    REQUIRE(chunk.size() == 7);
    REQUIRE(chunk.column_count() == 3);
}

TEST_CASE("ch_to_chunk: multi-block with empty blocks ignored") {
    auto* res = std::pmr::new_delete_resource();

    auto b1 = make_block_3col(2);
    clickhouse::Block empty{};
    auto b2 = make_block_3col(3);
    auto converted = tsl::ch_to_chunk(res, std::vector<clickhouse::Block>{b1, empty, b2});
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();

    REQUIRE(chunk.size() == 5);
}

TEST_CASE("ch_to_chunk: a String override over an Int32 wire column keeps the column INTEGER") {
    auto* res = std::pmr::new_delete_resource();

    // Column sent as Int32 under a name system.columns types String (`toInt32(payload) AS payload`):
    // an Int32 is not a representation of String, so the override does not apply.
    auto col = std::make_shared<clickhouse::ColumnInt32>();
    col->Append(42);
    clickhouse::Block block;
    block.AppendColumn("payload", col);

    std::unordered_map<std::string, std::string> overrides;
    overrides["payload"] = "String";

    auto converted = tsl::ch_to_chunk(res, block, overrides);
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();

    REQUIRE(chunk.size() == 1);
    REQUIRE(chunk.types()[0].type() == logical_type::INTEGER);
    REQUIRE(chunk.value(0, 0).value<int32_t>() == 42);
}

TEST_CASE("ch_to_struct: Int64 and UInt32 columns") {
    clickhouse::Block block;
    block.AppendColumn("big",      std::make_shared<clickhouse::ColumnInt64>());
    block.AppendColumn("unsigned", std::make_shared<clickhouse::ColumnUInt32>());

    auto s = tsl::ch_to_struct(std::pmr::new_delete_resource(), block);

    REQUIRE(s.child_types()[0].type() == logical_type::BIGINT);
    REQUIRE(s.child_types()[1].type() == logical_type::UINTEGER);
}

// Characterization cases: they record what the translator answers today for the
// wire types, the Nullable wrapper and the named-type overrides, so a refactor of
// the type mapping or of the value readers shows up as a changed value.
namespace {

template<typename ColumnT, typename ValueT>
clickhouse::ColumnRef make_nullable(const std::vector<std::optional<ValueT>>& values) {
    auto nested = std::make_shared<ColumnT>();
    auto nulls = std::make_shared<clickhouse::ColumnUInt8>();
    for (const auto& v : values) {
        nested->Append(v ? *v : ValueT{});
        nulls->Append(static_cast<uint8_t>(v ? 0 : 1));
    }
    return std::make_shared<clickhouse::ColumnNullable>(nested, nulls);
}

// One row per column, one column per wire type code the type mapping names, plus
// Nullable(Int32) and Array(Int32), which the mapping has no case for.
clickhouse::Block make_scalar_block() {
    auto i8 = std::make_shared<clickhouse::ColumnInt8>();
    i8->Append(-5);
    auto i16 = std::make_shared<clickhouse::ColumnInt16>();
    i16->Append(-300);
    auto i32 = std::make_shared<clickhouse::ColumnInt32>();
    i32->Append(-70000);
    auto i64 = std::make_shared<clickhouse::ColumnInt64>();
    i64->Append(-(int64_t{1} << 40));
    auto u8 = std::make_shared<clickhouse::ColumnUInt8>();
    u8->Append(200);
    auto u16 = std::make_shared<clickhouse::ColumnUInt16>();
    u16->Append(60000);
    auto u32 = std::make_shared<clickhouse::ColumnUInt32>();
    u32->Append(4000000000U);
    auto u64 = std::make_shared<clickhouse::ColumnUInt64>();
    u64->Append(uint64_t{1} << 63);
    auto f32 = std::make_shared<clickhouse::ColumnFloat32>();
    f32->Append(1.25F);
    auto f64 = std::make_shared<clickhouse::ColumnFloat64>();
    f64->Append(-2.5);
    auto str = std::make_shared<clickhouse::ColumnString>();
    str->Append("text");
    auto fixed = std::make_shared<clickhouse::ColumnFixedString>(4);
    fixed->Append("abcd");
    auto date = std::make_shared<clickhouse::ColumnDate>();
    date->Append(0);
    auto datetime = std::make_shared<clickhouse::ColumnDateTime>();
    datetime->Append(1704164645); // 2024-01-02 03:04:05 UTC
    auto datetime64 = std::make_shared<clickhouse::ColumnDateTime64>(3);
    datetime64->Append(int64_t{1704164645123});
    auto uuid = std::make_shared<clickhouse::ColumnUUID>();
    uuid->Append(clickhouse::UUID{0x0123456789abcdefULL, 0xfedcba9876543210ULL});
    auto nullable_i32 = make_nullable<clickhouse::ColumnInt32, int32_t>({5});
    auto array_items = std::make_shared<clickhouse::ColumnInt32>();
    array_items->Append(1);
    array_items->Append(2);
    auto array = std::make_shared<clickhouse::ColumnArray>(std::make_shared<clickhouse::ColumnInt32>());
    array->AppendAsColumn(array_items);

    clickhouse::Block block;
    block.AppendColumn("i8", i8);
    block.AppendColumn("i16", i16);
    block.AppendColumn("i32", i32);
    block.AppendColumn("i64", i64);
    block.AppendColumn("u8", u8);
    block.AppendColumn("u16", u16);
    block.AppendColumn("u32", u32);
    block.AppendColumn("u64", u64);
    block.AppendColumn("f32", f32);
    block.AppendColumn("f64", f64);
    block.AppendColumn("s", str);
    block.AppendColumn("fs", fixed);
    block.AppendColumn("d", date);
    block.AppendColumn("dt", datetime);
    block.AppendColumn("dt64", datetime64);
    block.AppendColumn("uuid", uuid);
    block.AppendColumn("n_i32", nullable_i32);
    block.AppendColumn("arr", array);
    return block;
}

void require_scalar_types(const data_chunk_t& chunk) {
    const std::vector<std::pair<const char*, logical_type>> expected{
        {"i8", logical_type::TINYINT},
        {"i16", logical_type::SMALLINT},
        {"i32", logical_type::INTEGER},
        {"i64", logical_type::BIGINT},
        {"u8", logical_type::UTINYINT},
        {"u16", logical_type::USMALLINT},
        {"u32", logical_type::UINTEGER},
        {"u64", logical_type::UBIGINT},
        {"f32", logical_type::FLOAT},
        {"f64", logical_type::DOUBLE},
        {"s", logical_type::STRING_LITERAL},
        {"fs", logical_type::STRING_LITERAL},
        {"d", logical_type::STRING_LITERAL},
        {"dt", logical_type::STRING_LITERAL},
        {"dt64", logical_type::STRING_LITERAL},
        {"uuid", logical_type::STRING_LITERAL},
        {"n_i32", logical_type::INTEGER},
        {"arr", logical_type::LIST},
    };
    REQUIRE(chunk.column_count() == expected.size());
    for (size_t col = 0; col < expected.size(); ++col) {
        CAPTURE(expected[col].first);
        REQUIRE(chunk.types()[col].alias() == expected[col].first);
        REQUIRE(chunk.types()[col].type() == expected[col].second);
    }
}

void require_scalar_values(const data_chunk_t& chunk, uint64_t row) {
    REQUIRE(chunk.value(0, row).value<int8_t>() == -5);
    REQUIRE(chunk.value(1, row).value<int16_t>() == -300);
    REQUIRE(chunk.value(2, row).value<int32_t>() == -70000);
    REQUIRE(chunk.value(3, row).value<int64_t>() == -(int64_t{1} << 40));
    REQUIRE(chunk.value(4, row).value<uint8_t>() == 200);
    REQUIRE(chunk.value(5, row).value<uint16_t>() == 60000);
    REQUIRE(chunk.value(6, row).value<uint32_t>() == 4000000000U);
    REQUIRE(chunk.value(7, row).value<uint64_t>() == (uint64_t{1} << 63));
    REQUIRE(chunk.value(8, row).value<float>() == 1.25F);
    REQUIRE(chunk.value(9, row).value<double>() == -2.5);
    REQUIRE(chunk.value(10, row).value<std::string_view>() == "text");
    REQUIRE(chunk.value(11, row).value<std::string_view>() == "abcd");
    REQUIRE(chunk.value(12, row).value<std::string_view>() == "1970-01-01");
    REQUIRE(chunk.value(13, row).value<std::string_view>() == "2024-01-02 03:04:05");
    // Without an override DateTime64 is its text with the column's precision, and an Array is
    // a LIST of its elements.
    REQUIRE(chunk.value(14, row).value<std::string_view>() == "2024-01-02 03:04:05.123");
    REQUIRE(chunk.value(15, row).value<std::string_view>() == "01234567-89ab-cdef-fedc-ba9876543210");
    REQUIRE(chunk.value(16, row).value<int32_t>() == 5);
    REQUIRE(chunk.value(17, row).children().size() == 2);
    REQUIRE(chunk.value(17, row).children()[0].value<int32_t>() == 1);
    REQUIRE(chunk.value(17, row).children()[1].value<int32_t>() == 2);
}

clickhouse::Block make_tuple_block(int32_t first_id, const std::vector<std::optional<std::string>>& names) {
    auto ids = std::make_shared<clickhouse::ColumnInt32>();
    for (size_t i = 0; i < names.size(); ++i) {
        ids->Append(first_id + static_cast<int32_t>(i));
    }
    auto tuple = std::make_shared<clickhouse::ColumnTuple>(
        std::vector<clickhouse::ColumnRef>{ids, make_nullable<clickhouse::ColumnString, std::string>(names)});
    clickhouse::Block block;
    block.AppendColumn("rec", tuple);
    return block;
}

} // namespace

TEST_CASE("ch_to_chunk characterization: column type and value per wire type code") {
    auto* res = std::pmr::new_delete_resource();
    auto block = make_scalar_block();

    SECTION("single block") {
        auto converted = tsl::ch_to_chunk(res, block);
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        REQUIRE(chunk.size() == 1);
        require_scalar_types(chunk);
        require_scalar_values(chunk, 0);
    }
    SECTION("multi block") {
        auto converted = tsl::ch_to_chunk(res, std::vector<clickhouse::Block>{block, clickhouse::Block{}, block});
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        REQUIRE(chunk.size() == 2);
        require_scalar_types(chunk);
        require_scalar_values(chunk, 0);
        require_scalar_values(chunk, 1);
    }
}

TEST_CASE("ch_to_chunk characterization: Nullable columns keep the nested type and carry NULL rows") {
    auto* res = std::pmr::new_delete_resource();

    clickhouse::Block b1;
    b1.AppendColumn("n", make_nullable<clickhouse::ColumnInt32, int32_t>({5, std::nullopt}));
    b1.AppendColumn("s", make_nullable<clickhouse::ColumnString, std::string>({std::nullopt, "x"}));
    clickhouse::Block b2;
    b2.AppendColumn("n", make_nullable<clickhouse::ColumnInt32, int32_t>({std::nullopt, 7}));
    b2.AppendColumn("s", make_nullable<clickhouse::ColumnString, std::string>({"y", std::nullopt}));

    SECTION("single block") {
        auto converted = tsl::ch_to_chunk(res, b1);
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        REQUIRE(chunk.size() == 2);
        REQUIRE(chunk.types()[0].type() == logical_type::INTEGER);
        REQUIRE(chunk.types()[1].type() == logical_type::STRING_LITERAL);
        REQUIRE(chunk.value(0, 0).value<int32_t>() == 5);
        REQUIRE(chunk.value(0, 1).is_null());
        REQUIRE(chunk.value(1, 0).is_null());
        REQUIRE(chunk.value(1, 1).value<std::string_view>() == "x");
    }
    SECTION("multi block") {
        auto converted = tsl::ch_to_chunk(res, std::vector<clickhouse::Block>{b1, b2});
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        REQUIRE(chunk.size() == 4);
        REQUIRE(chunk.types()[0].type() == logical_type::INTEGER);
        REQUIRE(chunk.types()[1].type() == logical_type::STRING_LITERAL);
        REQUIRE(chunk.value(0, 0).value<int32_t>() == 5);
        REQUIRE(chunk.value(0, 1).is_null());
        REQUIRE(chunk.value(0, 2).is_null());
        REQUIRE(chunk.value(0, 3).value<int32_t>() == 7);
        REQUIRE(chunk.value(1, 0).is_null());
        REQUIRE(chunk.value(1, 1).value<std::string_view>() == "x");
        REQUIRE(chunk.value(1, 2).value<std::string_view>() == "y");
        REQUIRE(chunk.value(1, 3).is_null());
    }
}

TEST_CASE("ch_to_chunk characterization: scalar named-type override keeps the primitive read") {
    auto* res = std::pmr::new_delete_resource();

    auto words = std::make_shared<clickhouse::ColumnString>();
    words->Append("p");
    words->Append("q");
    clickhouse::Block block;
    block.AppendColumn("v", make_nullable<clickhouse::ColumnInt64, int64_t>({42, std::nullopt}));
    block.AppendColumn("w", words);
    const std::unordered_map<std::string, std::string> overrides{{"v", "Nullable(Int64)"},
                                                                 {"w", "LowCardinality(String)"}};

    auto converted = tsl::ch_to_chunk(res, block, overrides);
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();

    REQUIRE(chunk.size() == 2);
    REQUIRE(chunk.types()[0].type() == logical_type::BIGINT);
    REQUIRE(chunk.types()[0].alias() == "v");
    REQUIRE(chunk.types()[1].type() == logical_type::STRING_LITERAL);
    REQUIRE(chunk.types()[1].alias() == "w");
    REQUIRE(chunk.value(0, 0).value<int64_t>() == 42);
    REQUIRE(chunk.value(0, 1).is_null());
    REQUIRE(chunk.value(1, 0).value<std::string_view>() == "p");
    REQUIRE(chunk.value(1, 1).value<std::string_view>() == "q");
}

TEST_CASE("ch_to_chunk characterization: Tuple override reads a STRUCT column") {
    auto* res = std::pmr::new_delete_resource();
    const std::unordered_map<std::string, std::string> overrides{{"rec", "Tuple(id Int32, name Nullable(String))"}};

    auto require_struct_type = [](const complex_logical_type& type) {
        REQUIRE(type.type() == logical_type::STRUCT);
        REQUIRE(type.alias() == "rec");
        REQUIRE(type.child_types().size() == 2);
        REQUIRE(type.child_types()[0].type() == logical_type::INTEGER);
        REQUIRE(type.child_types()[0].alias() == "id");
        REQUIRE(type.child_types()[1].type() == logical_type::STRING_LITERAL);
        REQUIRE(type.child_types()[1].alias() == "name");
    };

    SECTION("single block") {
        auto converted = tsl::ch_to_chunk(res, make_tuple_block(1, {"a", std::nullopt}), overrides);
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        REQUIRE(chunk.size() == 2);
        require_struct_type(chunk.types()[0]);
        auto row0 = chunk.value(0, 0);
        REQUIRE(row0.children().size() == 2);
        REQUIRE(row0.children()[0].value<int32_t>() == 1);
        REQUIRE(row0.children()[1].value<std::string_view>() == "a");
        auto row1 = chunk.value(0, 1);
        REQUIRE(row1.children().size() == 2);
        REQUIRE(row1.children()[0].value<int32_t>() == 2);
        REQUIRE(row1.children()[1].is_null());
    }
    SECTION("multi block") {
        auto converted = tsl::ch_to_chunk(
            res,
            std::vector<clickhouse::Block>{make_tuple_block(1, {"a"}), make_tuple_block(10, {std::nullopt, "c"})},
            overrides);
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        REQUIRE(chunk.size() == 3);
        require_struct_type(chunk.types()[0]);
        REQUIRE(chunk.value(0, 0).children()[0].value<int32_t>() == 1);
        REQUIRE(chunk.value(0, 0).children()[1].value<std::string_view>() == "a");
        REQUIRE(chunk.value(0, 1).children()[0].value<int32_t>() == 10);
        REQUIRE(chunk.value(0, 1).children()[1].is_null());
        REQUIRE(chunk.value(0, 2).children()[0].value<int32_t>() == 11);
        REQUIRE(chunk.value(0, 2).children()[1].value<std::string_view>() == "c");
    }
    SECTION("unnamed tuple fields are numbered") {
        auto s = tsl::ch_to_struct(res, make_tuple_block(1, {"a"}), {{"rec", "Tuple(Int32, String)"}});
        REQUIRE(s.child_types().size() == 1);
        const auto& rec = s.child_types()[0];
        REQUIRE(rec.type() == logical_type::STRUCT);
        REQUIRE(rec.child_types().size() == 2);
        REQUIRE(rec.child_types()[0].alias() == "_1");
        REQUIRE(rec.child_types()[1].alias() == "_2");
    }
}

// Types only: the rows of an Array-overridden column are read by the cases below.
TEST_CASE("ch_to_struct characterization: Array override is a LIST") {
    clickhouse::Block block;
    block.AppendColumn("tags", std::make_shared<clickhouse::ColumnArray>(std::make_shared<clickhouse::ColumnString>()));

    auto s = tsl::ch_to_struct(std::pmr::new_delete_resource(), block, {{"tags", "Array(Nullable(String))"}});

    REQUIRE(s.child_types().size() == 1);
    const auto& type = s.child_types()[0];
    REQUIRE(type.type() == logical_type::LIST);
    REQUIRE(type.alias() == "tags");
    REQUIRE(type.child_type().type() == logical_type::STRING_LITERAL);
    REQUIRE(type.extension()->type() == logical_type_extension::extension_type::LIST);
}

namespace {

// One Array row per entry of `nums` and of `tags`, holding that entry's elements.
clickhouse::Block make_array_block(const std::vector<std::vector<int32_t>>& nums,
                                   const std::vector<std::vector<std::optional<std::string>>>& tags) {
    auto nums_col = std::make_shared<clickhouse::ColumnArray>(std::make_shared<clickhouse::ColumnInt32>());
    for (const auto& row : nums) {
        auto items = std::make_shared<clickhouse::ColumnInt32>();
        for (const int32_t v : row) {
            items->Append(v);
        }
        nums_col->AppendAsColumn(items);
    }
    auto tags_col =
        std::make_shared<clickhouse::ColumnArray>(make_nullable<clickhouse::ColumnString, std::string>({}));
    for (const auto& row : tags) {
        tags_col->AppendAsColumn(make_nullable<clickhouse::ColumnString, std::string>(row));
    }
    clickhouse::Block block;
    block.AppendColumn("nums", nums_col);
    block.AppendColumn("tags", tags_col);
    return block;
}

} // namespace

TEST_CASE("ch_to_chunk: Array override reads every row's elements across block boundaries") {
    auto* res = std::pmr::new_delete_resource();
    const std::unordered_map<std::string, std::string> overrides{{"nums", "Array(Int32)"},
                                                                 {"tags", "Array(Nullable(String))"}};
    const std::vector<std::vector<int32_t>> nums{{1, 2}, {-3, 40000}, {7, 0}};
    const std::vector<std::vector<std::optional<std::string>>> tags{
        {"a", std::nullopt},
        {std::nullopt, "bc"},
        {"d", "e"}};

    auto require_rows = [&](const data_chunk_t& chunk) {
        REQUIRE(chunk.size() == nums.size());
        REQUIRE(chunk.types()[0].type() == logical_type::LIST);
        REQUIRE(chunk.types()[0].child_type().type() == logical_type::INTEGER);
        REQUIRE(chunk.types()[1].type() == logical_type::LIST);
        REQUIRE(chunk.types()[1].child_type().type() == logical_type::STRING_LITERAL);
        for (size_t row = 0; row < nums.size(); ++row) {
            CAPTURE(row);
            const auto num_value = chunk.value(0, row);
            REQUIRE(num_value.children().size() == nums[row].size());
            for (size_t i = 0; i < nums[row].size(); ++i) {
                CAPTURE(i);
                REQUIRE(num_value.children()[i].value<int32_t>() == nums[row][i]);
            }
            const auto tag_value = chunk.value(1, row);
            REQUIRE(tag_value.children().size() == tags[row].size());
            for (size_t i = 0; i < tags[row].size(); ++i) {
                CAPTURE(i);
                if (tags[row][i]) {
                    REQUIRE(tag_value.children()[i].value<std::string_view>() == *tags[row][i]);
                } else {
                    REQUIRE(tag_value.children()[i].is_null());
                }
            }
        }
    };

    SECTION("single block") {
        auto converted = tsl::ch_to_chunk(res, make_array_block(nums, tags), overrides);
        REQUIRE_FALSE(converted.has_error());
        require_rows(converted.value());
    }
    SECTION("multi block") {
        // Row 0 in the first block, rows 1 and 2 in the last, an empty block between them.
        const std::vector<clickhouse::Block> blocks{make_array_block({nums[0]}, {tags[0]}),
                                                    clickhouse::Block{},
                                                    make_array_block({nums[1], nums[2]}, {tags[1], tags[2]})};
        auto converted = tsl::ch_to_chunk(res, blocks, overrides);
        REQUIRE_FALSE(converted.has_error());
        require_rows(converted.value());
    }
}

// The native protocol carries a Date / Date32 body as the day number since the Unix epoch
// (AppendRaw stores that number as is); the driver's At() answers it multiplied into seconds.
TEST_CASE("ch_to_chunk: Date and Date32 columns read the calendar day the wire carries") {
    auto* res = std::pmr::new_delete_resource();
    // 2024-01-15, the epoch and Date's last day; before the epoch, and Date32's first and last day.
    const std::vector<uint16_t> days{19737, 0, 65535};
    const std::vector<int32_t> days32{-1, -25567, 120529};
    // Nullable(Date) filled through Append(time_t): 2024-01-15 00:00:00 UTC, NULL, the epoch.
    const std::vector<std::optional<std::time_t>> nullable_seconds{std::time_t{1705276800}, std::nullopt, 0};

    auto make_date_block = [&](size_t first, size_t last) {
        auto date = std::make_shared<clickhouse::ColumnDate>();
        auto date32 = std::make_shared<clickhouse::ColumnDate32>();
        std::vector<std::optional<std::time_t>> seconds;
        for (size_t row = first; row < last; ++row) {
            date->AppendRaw(days[row]);
            date32->AppendRaw(days32[row]);
            seconds.push_back(nullable_seconds[row]);
        }
        clickhouse::Block block;
        block.AppendColumn("d", date);
        block.AppendColumn("d32", date32);
        block.AppendColumn("n_d", make_nullable<clickhouse::ColumnDate, std::time_t>(seconds));
        return block;
    };

    auto require_rows = [](const data_chunk_t& chunk) {
        REQUIRE(chunk.size() == 3);
        REQUIRE(chunk.types()[0].type() == logical_type::STRING_LITERAL);
        REQUIRE(chunk.types()[1].type() == logical_type::STRING_LITERAL);
        REQUIRE(chunk.types()[2].type() == logical_type::STRING_LITERAL);
        REQUIRE(chunk.value(0, 0).value<std::string_view>() == "2024-01-15");
        REQUIRE(chunk.value(0, 1).value<std::string_view>() == "1970-01-01");
        REQUIRE(chunk.value(0, 2).value<std::string_view>() == "2149-06-06");
        REQUIRE(chunk.value(1, 0).value<std::string_view>() == "1969-12-31");
        REQUIRE(chunk.value(1, 1).value<std::string_view>() == "1900-01-01");
        REQUIRE(chunk.value(1, 2).value<std::string_view>() == "2299-12-31");
        REQUIRE(chunk.value(2, 0).value<std::string_view>() == "2024-01-15");
        REQUIRE(chunk.value(2, 1).is_null());
        REQUIRE(chunk.value(2, 2).value<std::string_view>() == "1970-01-01");
    };

    SECTION("single block") {
        auto converted = tsl::ch_to_chunk(res, make_date_block(0, 3));
        REQUIRE_FALSE(converted.has_error());
        require_rows(converted.value());
    }
    SECTION("multi block") {
        auto converted = tsl::ch_to_chunk(
            res,
            std::vector<clickhouse::Block>{make_date_block(0, 1), clickhouse::Block{}, make_date_block(1, 3)});
        REQUIRE_FALSE(converted.has_error());
        require_rows(converted.value());
    }
}

// A named-type override is the column's type, and every value written under it must have that
// type: the engine's vector_t::set_value refuses a value of another type (the write is skipped on
// rc-2, the process aborts on rc-3). Each case reads back a non-NULL payload an untouched slot
// would not hold, and checks that the value read back has the column vector's type.
namespace {

    using overrides_t = std::unordered_map<std::string, std::string>;

    clickhouse::Block one_column_block(const char* name, clickhouse::ColumnRef column) {
        clickhouse::Block block;
        block.AppendColumn(name, column);
        return block;
    }

    template<typename T>
    std::vector<T> rows_of(const std::vector<T>& values, size_t first, size_t last) {
        return std::vector<T>(values.begin() + static_cast<std::ptrdiff_t>(first),
                              values.begin() + static_cast<std::ptrdiff_t>(last));
    }

    template<typename ColumnT, typename ValueT>
    clickhouse::ColumnRef make_column(const std::vector<ValueT>& values) {
        auto column = std::make_shared<ColumnT>();
        for (const auto& value : values) {
            column->Append(value);
        }
        return column;
    }

    // The cell is not NULL and carries the type of the vector it was written to.
    void require_written(const data_chunk_t& chunk, size_t col, size_t row) {
        CAPTURE(col, row);
        const auto value = chunk.value(col, row);
        REQUIRE_FALSE(value.is_null());
        REQUIRE(value.type() == chunk.data[col].type());
    }

    // Converts rows [0, rows) once as one block and once as [0, 1), an empty block and [1, rows).
    template<typename MakeBlock, typename RequireRows>
    void convert_single_and_split(const MakeBlock& make_block,
                                  size_t rows,
                                  const overrides_t& overrides,
                                  const RequireRows& require_rows) {
        auto* res = std::pmr::new_delete_resource();
        SECTION("single block") {
            auto converted = tsl::ch_to_chunk(res, make_block(0, rows), overrides);
            INFO((converted.has_error() ? converted.error().what.c_str() : "converted"));
            REQUIRE_FALSE(converted.has_error());
            require_rows(converted.value());
        }
        SECTION("split blocks") {
            const std::vector<clickhouse::Block> blocks{make_block(0, 1), clickhouse::Block{}, make_block(1, rows)};
            auto converted = tsl::ch_to_chunk(res, blocks, overrides);
            INFO((converted.has_error() ? converted.error().what.c_str() : "converted"));
            REQUIRE_FALSE(converted.has_error());
            require_rows(converted.value());
        }
    }

    // The same two shapes, each answered with conversion_failure naming the column.
    template<typename MakeBlock>
    void require_conversion_failure(const MakeBlock& make_block,
                                    size_t rows,
                                    const overrides_t& overrides,
                                    std::string_view column) {
        auto* res = std::pmr::new_delete_resource();
        auto require_failure = [column](const core::result_wrapper_t<data_chunk_t>& converted) {
            REQUIRE(converted.has_error());
            INFO(converted.error().what.c_str());
            REQUIRE(converted.error().type == core::error_code_t::conversion_failure);
            REQUIRE(std::string_view{converted.error().what}.find(column) != std::string_view::npos);
        };
        SECTION("single block") { require_failure(tsl::ch_to_chunk(res, make_block(0, rows), overrides)); }
        SECTION("split blocks") {
            const std::vector<clickhouse::Block> blocks{make_block(0, 1), clickhouse::Block{}, make_block(1, rows)};
            require_failure(tsl::ch_to_chunk(res, blocks, overrides));
        }
    }

    // The schema ch_to_struct registers for `block` under `overrides` is the chunk's, column by column.
    void require_struct_is_chunk_types(const clickhouse::Block& block,
                                       const overrides_t& overrides,
                                       const data_chunk_t& chunk) {
        const auto schema = tsl::ch_to_struct(std::pmr::new_delete_resource(), block, overrides);
        const auto types = chunk.types();
        REQUIRE(schema.child_types().size() == types.size());
        for (size_t col = 0; col < types.size(); ++col) {
            CAPTURE(col);
            REQUIRE(schema.child_types()[col] == types[col]);
        }
    }

    // `make_block`'s one column converted under `overrides` in both shapes: the column is `type`, a
    // non-NULL cell carries the vector's type, `require_row` accepts every row's value, and the schema
    // discovery registers for the block is the chunk's.
    template<typename MakeBlock, typename RequireRow>
    void require_wire_column(const MakeBlock& make_block,
                             size_t rows,
                             const overrides_t& overrides,
                             logical_type type,
                             const RequireRow& require_row) {
        convert_single_and_split(make_block, rows, overrides, [&](const data_chunk_t& chunk) {
            REQUIRE(chunk.size() == rows);
            REQUIRE(chunk.types()[0].type() == type);
            for (size_t row = 0; row < rows; ++row) {
                CAPTURE(row);
                const auto value = chunk.value(0, row);
                REQUIRE((value.is_null() || value.type() == chunk.data[0].type()));
                require_row(value, row);
            }
            require_struct_is_chunk_types(make_block(0, rows), overrides, chunk);
        });
    }

} // namespace

TEST_CASE("ch_to_chunk: a named-type override reads each wire value as the column type") {
    SECTION("Bool over the UInt8 the wire carries") {
        const std::vector<uint8_t> flags{1, 0, 1};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("flag", make_column<clickhouse::ColumnUInt8>(rows_of(flags, first, last)));
        };
        convert_single_and_split(make_block, flags.size(), {{"flag", "Bool"}}, [&](const data_chunk_t& chunk) {
            REQUIRE(chunk.size() == flags.size());
            REQUIRE(chunk.types()[0].type() == logical_type::BOOLEAN);
            for (size_t row = 0; row < flags.size(); ++row) {
                require_written(chunk, 0, row);
                REQUIRE(chunk.value(0, row).value<bool>() == (flags[row] != 0));
            }
        });
    }
    SECTION("Nullable(Bool) over Nullable(UInt8)") {
        const std::vector<std::optional<uint8_t>> flags{1, std::nullopt, 1};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("flag",
                                    make_nullable<clickhouse::ColumnUInt8, uint8_t>(rows_of(flags, first, last)));
        };
        convert_single_and_split(make_block,
                                 flags.size(),
                                 {{"flag", "Nullable(Bool)"}},
                                 [&](const data_chunk_t& chunk) {
                                     REQUIRE(chunk.types()[0].type() == logical_type::BOOLEAN);
                                     require_written(chunk, 0, 0);
                                     REQUIRE(chunk.value(0, 0).value<bool>());
                                     REQUIRE(chunk.value(0, 1).is_null());
                                     require_written(chunk, 0, 2);
                                     REQUIRE(chunk.value(0, 2).value<bool>());
                                 });
    }
    SECTION("LowCardinality(String) is String") {
        const std::vector<std::string_view> words{"x", "yy", "x"};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block(
                "word",
                make_column<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>(rows_of(words, first, last)));
        };
        convert_single_and_split(make_block,
                                 words.size(),
                                 {{"word", "LowCardinality(String)"}},
                                 [&](const data_chunk_t& chunk) {
                                     REQUIRE(chunk.types()[0].type() == logical_type::STRING_LITERAL);
                                     for (size_t row = 0; row < words.size(); ++row) {
                                         require_written(chunk, 0, row);
                                         REQUIRE(chunk.value(0, row).value<std::string_view>() == words[row]);
                                     }
                                 });
    }
    SECTION("LowCardinality(Nullable(String)) is String with its NULLs") {
        const std::vector<std::optional<std::string_view>> words{"z", std::nullopt, "zz"};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block(
                "word",
                make_column<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnNullableT<clickhouse::ColumnString>>>(
                    rows_of(words, first, last)));
        };
        convert_single_and_split(make_block,
                                 words.size(),
                                 {{"word", "LowCardinality(Nullable(String))"}},
                                 [&](const data_chunk_t& chunk) {
                                     REQUIRE(chunk.types()[0].type() == logical_type::STRING_LITERAL);
                                     require_written(chunk, 0, 0);
                                     REQUIRE(chunk.value(0, 0).value<std::string_view>() == "z");
                                     REQUIRE(chunk.value(0, 1).is_null());
                                     require_written(chunk, 0, 2);
                                     REQUIRE(chunk.value(0, 2).value<std::string_view>() == "zz");
                                 });
    }
    SECTION("SimpleAggregateFunction(f, T) is T") {
        const std::vector<uint64_t> totals{7, uint64_t{1} << 40, 9};
        const std::vector<std::optional<int32_t>> lasts{-3, std::nullopt, 11};
        auto make_block = [&](size_t first, size_t last) {
            clickhouse::Block block;
            block.AppendColumn("total", make_column<clickhouse::ColumnUInt64>(rows_of(totals, first, last)));
            block.AppendColumn("last", make_nullable<clickhouse::ColumnInt32, int32_t>(rows_of(lasts, first, last)));
            return block;
        };
        const overrides_t overrides{{"total", "SimpleAggregateFunction(sum, UInt64)"},
                                    {"last", "SimpleAggregateFunction(anyLast, Nullable(Int32))"}};
        convert_single_and_split(make_block, totals.size(), overrides, [&](const data_chunk_t& chunk) {
            REQUIRE(chunk.types()[0].type() == logical_type::UBIGINT);
            REQUIRE(chunk.types()[1].type() == logical_type::INTEGER);
            for (size_t row = 0; row < totals.size(); ++row) {
                require_written(chunk, 0, row);
                REQUIRE(chunk.value(0, row).value<uint64_t>() == totals[row]);
            }
            require_written(chunk, 1, 0);
            REQUIRE(chunk.value(1, 0).value<int32_t>() == -3);
            REQUIRE(chunk.value(1, 1).is_null());
            require_written(chunk, 1, 2);
            REQUIRE(chunk.value(1, 2).value<int32_t>() == 11);
        });
    }
    SECTION("a Tuple field declared Bool reads the UInt8 the wire carries") {
        const std::vector<int32_t> ids{1, 2, 3};
        const std::vector<uint8_t> oks{1, 0, 1};
        auto make_block = [&](size_t first, size_t last) {
            auto tuple = std::make_shared<clickhouse::ColumnTuple>(
                std::vector<clickhouse::ColumnRef>{make_column<clickhouse::ColumnInt32>(rows_of(ids, first, last)),
                                                   make_column<clickhouse::ColumnUInt8>(rows_of(oks, first, last))});
            return one_column_block("rec", tuple);
        };
        convert_single_and_split(make_block,
                                 ids.size(),
                                 {{"rec", "Tuple(id Int32, ok Bool)"}},
                                 [&](const data_chunk_t& chunk) {
                                     const auto types = chunk.types();
                                     const auto& type = types[0];
                                     REQUIRE(type.type() == logical_type::STRUCT);
                                     REQUIRE(type.child_types().size() == 2);
                                     REQUIRE(type.child_types()[0].type() == logical_type::INTEGER);
                                     REQUIRE(type.child_types()[1].type() == logical_type::BOOLEAN);
                                     for (size_t row = 0; row < ids.size(); ++row) {
                                         require_written(chunk, 0, row);
                                         const auto value = chunk.value(0, row);
                                         REQUIRE(value.children().size() == 2);
                                         REQUIRE(value.children()[0].value<int32_t>() == ids[row]);
                                         REQUIRE(value.children()[1].type() == type.child_types()[1]);
                                         REQUIRE(value.children()[1].value<bool>() == (oks[row] != 0));
                                     }
                                 });
    }
}

// The overrides are system.columns' types of the base table, keyed by column NAME, and a result column
// can carry a base column's name without being that column: `AVG(x) AS x`, `toString(id) AS id`,
// `count() AS x`, `x + 1 AS x`. An override names the column's type only when the wire column is a
// representation of it; any other column is the wire's type holding the wire's values, as without an
// override — neither converted to the override's type nor a conversion_failure — and discovery
// registers that same type.
TEST_CASE("ch_to_chunk: a wire column that does not represent its override's type is the wire's type") {
    SECTION("AVG(x) AS x: a Float64 under an Int32 column's name is DOUBLE") {
        const std::vector<double> averages{1.5, -2.25, 4000000000.5};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("x", make_column<clickhouse::ColumnFloat64>(rows_of(averages, first, last)));
        };
        require_wire_column(make_block,
                            averages.size(),
                            {{"x", "Int32"}},
                            logical_type::DOUBLE,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<double>() == averages[row]);
                            });
    }
    SECTION("avg over a Nullable column: a Nullable(Float64) under Nullable(Int32) is DOUBLE with its NULLs") {
        const std::vector<std::optional<double>> averages{0.5, std::nullopt, 7.0};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("x",
                                    make_nullable<clickhouse::ColumnFloat64, double>(rows_of(averages, first, last)));
        };
        require_wire_column(make_block,
                            averages.size(),
                            {{"x", "Nullable(Int32)"}},
                            logical_type::DOUBLE,
                            [&](const logical_value_t& value, size_t row) {
                                if (averages[row]) {
                                    REQUIRE(value.value<double>() == *averages[row]);
                                } else {
                                    REQUIRE(value.is_null());
                                }
                            });
    }
    SECTION("toString(id) AS id: a String under an Int32 column's name is STRING") {
        const std::vector<std::string_view> texts{"1", "twenty-two", "-3"};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("id", make_column<clickhouse::ColumnString>(rows_of(texts, first, last)));
        };
        require_wire_column(make_block,
                            texts.size(),
                            {{"id", "Int32"}},
                            logical_type::STRING_LITERAL,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<std::string_view>() == texts[row]);
                            });
    }
    SECTION("count() AS x: a UInt64 under an Int32 column's name is UBIGINT") {
        const std::vector<uint64_t> counts{0, 5, uint64_t{1} << 40};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("x", make_column<clickhouse::ColumnUInt64>(rows_of(counts, first, last)));
        };
        require_wire_column(make_block,
                            counts.size(),
                            {{"x", "Int32"}},
                            logical_type::UBIGINT,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<uint64_t>() == counts[row]);
                            });
    }
    SECTION("x + 1 AS x and SUM(qty) AS qty: an Int64 under an Int32 column's name is BIGINT whatever its value") {
        // ClickHouse widens Int32 arithmetic and sums to Int64: a value inside Int32 and one past it alike.
        const std::vector<int64_t> sums{5, -7, 2147483647, int64_t{1} << 40};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("qty", make_column<clickhouse::ColumnInt64>(rows_of(sums, first, last)));
        };
        require_wire_column(make_block,
                            sums.size(),
                            {{"qty", "Int32"}},
                            logical_type::BIGINT,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<int64_t>() == sums[row]);
                            });
    }
    SECTION("a negative Int32 under a UInt32 override is INTEGER") {
        const std::vector<int32_t> values{1, -1};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("n", make_column<clickhouse::ColumnInt32>(rows_of(values, first, last)));
        };
        require_wire_column(make_block,
                            values.size(),
                            {{"n", "UInt32"}},
                            logical_type::INTEGER,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<int32_t>() == values[row]);
                            });
    }
    SECTION("a fractional Float64 under an Int64 override is DOUBLE") {
        const std::vector<double> values{1.0, 2.5};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("n", make_column<clickhouse::ColumnFloat64>(rows_of(values, first, last)));
        };
        require_wire_column(make_block,
                            values.size(),
                            {{"n", "Int64"}},
                            logical_type::DOUBLE,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<double>() == values[row]);
                            });
    }
    SECTION("a Float64 under a Float32 override is DOUBLE") {
        const std::vector<double> values{1.5, 2.5};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("f", make_column<clickhouse::ColumnFloat64>(rows_of(values, first, last)));
        };
        require_wire_column(make_block,
                            values.size(),
                            {{"f", "Float32"}},
                            logical_type::DOUBLE,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<double>() == values[row]);
                            });
    }
    SECTION("a Float32 under a Float64 override is FLOAT") {
        const std::vector<float> ratios{1.5F, -0.25F, 8.0F};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("ratio", make_column<clickhouse::ColumnFloat32>(rows_of(ratios, first, last)));
        };
        require_wire_column(make_block,
                            ratios.size(),
                            {{"ratio", "Float64"}},
                            logical_type::FLOAT,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<float>() == ratios[row]);
                            });
    }
    SECTION("numbers under a String override keep their number types") {
        const std::vector<int32_t> codes{42, -1, 7};
        const std::vector<double> ratios{1.5, -0.25, 3};
        auto make_block = [&](size_t first, size_t last) {
            clickhouse::Block block;
            block.AppendColumn("code", make_column<clickhouse::ColumnInt32>(rows_of(codes, first, last)));
            block.AppendColumn("ratio", make_column<clickhouse::ColumnFloat64>(rows_of(ratios, first, last)));
            return block;
        };
        const overrides_t overrides{{"code", "String"}, {"ratio", "String"}};
        convert_single_and_split(make_block, codes.size(), overrides, [&](const data_chunk_t& chunk) {
            REQUIRE(chunk.types()[0].type() == logical_type::INTEGER);
            REQUIRE(chunk.types()[1].type() == logical_type::DOUBLE);
            for (size_t row = 0; row < codes.size(); ++row) {
                require_written(chunk, 0, row);
                REQUIRE(chunk.value(0, row).value<int32_t>() == codes[row]);
                require_written(chunk, 1, row);
                REQUIRE(chunk.value(1, row).value<double>() == ratios[row]);
            }
            require_struct_is_chunk_types(make_block(0, codes.size()), overrides, chunk);
        });
    }
    SECTION("a UInt16 under a Bool override is USMALLINT") {
        const std::vector<uint16_t> values{1, 0};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("flag", make_column<clickhouse::ColumnUInt16>(rows_of(values, first, last)));
        };
        require_wire_column(make_block,
                            values.size(),
                            {{"flag", "Bool"}},
                            logical_type::USMALLINT,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<uint16_t>() == values[row]);
                            });
    }
    SECTION("a scalar under a Tuple override is the scalar") {
        const std::vector<int32_t> values{1, 2};
        auto make_block = [&](size_t first, size_t last) {
            return one_column_block("rec", make_column<clickhouse::ColumnInt32>(rows_of(values, first, last)));
        };
        require_wire_column(make_block,
                            values.size(),
                            {{"rec", "Tuple(a Int32)"}},
                            logical_type::INTEGER,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.value<int32_t>() == values[row]);
                            });
    }
    SECTION("a Tuple of fewer fields than its Tuple override is the wire's STRUCT") {
        const std::vector<int32_t> ids{1, 2};
        auto make_block = [&](size_t first, size_t last) {
            auto tuple = std::make_shared<clickhouse::ColumnTuple>(
                std::vector<clickhouse::ColumnRef>{make_column<clickhouse::ColumnInt32>(rows_of(ids, first, last))});
            return one_column_block("rec", tuple);
        };
        require_wire_column(make_block,
                            ids.size(),
                            {{"rec", "Tuple(id Int32, name String)"}},
                            logical_type::STRUCT,
                            [&](const logical_value_t& value, size_t row) {
                                REQUIRE(value.type().child_types().size() == 1);
                                REQUIRE(value.type().child_types()[0].type() == logical_type::INTEGER);
                                REQUIRE(value.children().size() == 1);
                                REQUIRE(value.children()[0].value<int32_t>() == ids[row]);
                            });
    }
    SECTION("tuple(name, id) AS rec: a Tuple of other field types under a named Tuple is the wire's STRUCT") {
        const std::vector<std::string_view> names{"a", "b", "c"};
        const std::vector<int32_t> ids{1, 2, 3};
        auto make_block = [&](size_t first, size_t last) {
            auto tuple = std::make_shared<clickhouse::ColumnTuple>(
                std::vector<clickhouse::ColumnRef>{make_column<clickhouse::ColumnString>(rows_of(names, first, last)),
                                                   make_column<clickhouse::ColumnInt32>(rows_of(ids, first, last))});
            return one_column_block("rec", tuple);
        };
        require_wire_column(make_block,
                            ids.size(),
                            {{"rec", "Tuple(id Int32, name String)"}},
                            logical_type::STRUCT,
                            [&](const logical_value_t& value, size_t row) {
                                const auto& fields = value.type().child_types();
                                REQUIRE(fields.size() == 2);
                                REQUIRE(fields[0].type() == logical_type::STRING_LITERAL);
                                REQUIRE(fields[0].alias() != "id");
                                REQUIRE(fields[1].type() == logical_type::INTEGER);
                                REQUIRE(value.children()[0].value<std::string_view>() == names[row]);
                                REQUIRE(value.children()[1].value<int32_t>() == ids[row]);
                            });
    }
}

// A LIST has no fixed length: every row holds exactly the elements the wire carries, each of the
// LIST's element type — Array(Bool) reads the UInt8 elements the wire carries as BOOLEAN.
TEST_CASE("ch_to_chunk: Array override is a LIST of each row's own length") {
    auto* res = std::pmr::new_delete_resource();
    const overrides_t overrides{{"nums", "Array(Int32)"},
                                {"tags", "Array(Nullable(String))"},
                                {"flags", "Array(Bool)"}};
    // Rows of zero, one and three elements; NULL elements in the first row and the last.
    const std::vector<std::vector<int32_t>> nums{{}, {7}, {1, -2, 3}};
    const std::vector<std::vector<std::optional<std::string>>> tags{{std::nullopt}, {}, {"a", std::nullopt, "c"}};
    const std::vector<std::vector<uint8_t>> flags{{1, 0, 1}, {}, {1}};

    auto make_block = [&](size_t first, size_t last) {
        auto block = make_array_block(rows_of(nums, first, last), rows_of(tags, first, last));
        auto flags_col = std::make_shared<clickhouse::ColumnArray>(std::make_shared<clickhouse::ColumnUInt8>());
        for (size_t row = first; row < last; ++row) {
            flags_col->AppendAsColumn(make_column<clickhouse::ColumnUInt8>(flags[row]));
        }
        block.AppendColumn("flags", flags_col);
        return block;
    };

    auto require_rows = [&](const data_chunk_t& chunk) {
        REQUIRE(chunk.size() == nums.size());
        const auto types = chunk.types();
        REQUIRE(types[0].type() == logical_type::LIST);
        REQUIRE(types[0].child_type().type() == logical_type::INTEGER);
        REQUIRE(types[1].type() == logical_type::LIST);
        REQUIRE(types[1].child_type().type() == logical_type::STRING_LITERAL);
        REQUIRE(types[2].type() == logical_type::LIST);
        REQUIRE(types[2].child_type().type() == logical_type::BOOLEAN);
        // The schema discovery registers under the same overrides is the chunk's.
        const auto schema = tsl::ch_to_struct(res, make_block(0, nums.size()), overrides);
        REQUIRE(schema.child_types().size() == types.size());
        for (size_t col = 0; col < types.size(); ++col) {
            CAPTURE(col);
            REQUIRE(schema.child_types()[col] == types[col]);
        }
        for (size_t row = 0; row < nums.size(); ++row) {
            CAPTURE(row);
            const auto num_value = chunk.value(0, row);
            REQUIRE(num_value.type() == chunk.data[0].type());
            REQUIRE(num_value.children().size() == nums[row].size());
            for (size_t i = 0; i < nums[row].size(); ++i) {
                CAPTURE(i);
                REQUIRE(num_value.children()[i].value<int32_t>() == nums[row][i]);
            }
            const auto tag_value = chunk.value(1, row);
            REQUIRE(tag_value.type() == chunk.data[1].type());
            REQUIRE(tag_value.children().size() == tags[row].size());
            for (size_t i = 0; i < tags[row].size(); ++i) {
                CAPTURE(i);
                if (tags[row][i]) {
                    REQUIRE(tag_value.children()[i].value<std::string_view>() == *tags[row][i]);
                } else {
                    REQUIRE(tag_value.children()[i].is_null());
                }
            }
            const auto flag_value = chunk.value(2, row);
            REQUIRE(flag_value.type() == chunk.data[2].type());
            REQUIRE(flag_value.children().size() == flags[row].size());
            for (size_t i = 0; i < flags[row].size(); ++i) {
                CAPTURE(i);
                REQUIRE(flag_value.children()[i].type() == types[2].child_type());
                REQUIRE(flag_value.children()[i].value<bool>() == (flags[row][i] != 0));
            }
        }
    };

    convert_single_and_split(make_block, nums.size(), overrides, require_rows);
}

// Wire types the engine has no scalar type for are STRING columns holding their text — with or
// without the system.columns type discovery keeps as the override — and discovery registers those
// columns as STRING. DateTime64 and Time64 print the column's precision, with the wall clock in UTC
// (the column's timezone is not applied, as for DateTime); a Decimal keeps its scale's digits.
TEST_CASE("ch_to_chunk: wire types without a numeric mapping are read as their text") {
    auto* res = std::pmr::new_delete_resource();
    clickhouse::Int128 decimal38_max = 1;
    for (int digit = 0; digit < 38; ++digit) {
        decimal38_max *= 10;
    }
    decimal38_max -= 1;
    const std::string decimal38_max_text = std::string(28, '9') + "." + std::string(10, '9');

    const std::vector<int64_t> dt64_3{1704164645123, -1};
    const std::vector<int64_t> dt64_0{0, 86399};
    const std::vector<int64_t> dt64_6{1704164645000001, -1000000};
    const std::vector<clickhouse::Int128> dec_9_2{12345, -5};
    const std::vector<clickhouse::Int128> dec_38_10{decimal38_max, -decimal38_max};
    const std::vector<clickhouse::Int128> dec_18_0{-42, 0};
    const std::vector<int8_t> enum8{1, 2};
    const std::vector<int16_t> enum16{-300, 300};
    const std::vector<std::string> ip4{"192.168.0.1", "0.0.0.0"};
    const std::vector<std::string> ip6{"2001:db8::1", "::1"};
    const std::vector<clickhouse::Int128> i128{absl::Int128Min(), 42};
    const std::vector<clickhouse::UInt128> u128{absl::Uint128Max(), 0};
    const std::vector<int32_t> times{45296, -3723};
    const std::vector<int64_t> times64_3{45296789, -500};
    const std::vector<std::string_view> words{"a", "bb"};

    struct text_column_t {
        const char* name;
        const char* named_type;
        std::vector<std::string> texts;
    };
    const std::vector<text_column_t> expected{
        {"dt64_3", "DateTime64(3)", {"2024-01-02 03:04:05.123", "1969-12-31 23:59:59.999"}},
        {"dt64_0", "DateTime64(0)", {"1970-01-01 00:00:00", "1970-01-01 23:59:59"}},
        {"dt64_6", "DateTime64(6, 'Europe/Moscow')", {"2024-01-02 03:04:05.000001", "1969-12-31 23:59:59.000000"}},
        {"dec_9_2", "Decimal(9, 2)", {"123.45", "-0.05"}},
        {"dec_38_10", "Decimal(38, 10)", {decimal38_max_text, "-" + decimal38_max_text}},
        {"dec_18_0", "Decimal(18, 0)", {"-42", "0"}},
        {"e8", "Enum8('red' = 1, 'green' = 2)", {"red", "green"}},
        {"e16", "Enum16('neg' = -300, 'pos' = 300)", {"neg", "pos"}},
        {"ip4", "IPv4", {"192.168.0.1", "0.0.0.0"}},
        {"ip6", "IPv6", {"2001:db8::1", "::1"}},
        {"i128", "Int128", {"-170141183460469231731687303715884105728", "42"}},
        {"u128", "UInt128", {"340282366920938463463374607431768211455", "0"}},
        {"t", "Time", {"12:34:56", "-01:02:03"}},
        {"t64_3", "Time64(3)", {"12:34:56.789", "-00:00:00.500"}},
        {"lc", "LowCardinality(String)", {"a", "bb"}},
    };

    auto make_block = [&](size_t first, size_t last) {
        clickhouse::Block block;
        auto datetime64 = [&](size_t precision, const char* timezone, const std::vector<int64_t>& ticks) {
            auto column = std::make_shared<clickhouse::ColumnDateTime64>(precision, timezone);
            for (size_t row = first; row < last; ++row) {
                column->Append(ticks[row]);
            }
            return column;
        };
        auto decimal = [&](size_t precision, size_t scale, const std::vector<clickhouse::Int128>& values) {
            auto column = std::make_shared<clickhouse::ColumnDecimal>(precision, scale);
            for (size_t row = first; row < last; ++row) {
                column->Append(values[row]);
            }
            return column;
        };
        auto e8 = std::make_shared<clickhouse::ColumnEnum8>(clickhouse::Type::CreateEnum8({{"red", 1}, {"green", 2}}));
        auto e16 =
            std::make_shared<clickhouse::ColumnEnum16>(clickhouse::Type::CreateEnum16({{"neg", -300}, {"pos", 300}}));
        auto t64 = std::make_shared<clickhouse::ColumnTime64>(3);
        for (size_t row = first; row < last; ++row) {
            e8->Append(enum8[row]);
            e16->Append(enum16[row]);
            t64->Append(times64_3[row]);
        }
        block.AppendColumn("dt64_3", datetime64(3, "", dt64_3));
        block.AppendColumn("dt64_0", datetime64(0, "", dt64_0));
        block.AppendColumn("dt64_6", datetime64(6, "Europe/Moscow", dt64_6));
        block.AppendColumn("dec_9_2", decimal(9, 2, dec_9_2));
        block.AppendColumn("dec_38_10", decimal(38, 10, dec_38_10));
        block.AppendColumn("dec_18_0", decimal(18, 0, dec_18_0));
        block.AppendColumn("e8", e8);
        block.AppendColumn("e16", e16);
        block.AppendColumn("ip4", make_column<clickhouse::ColumnIPv4>(rows_of(ip4, first, last)));
        block.AppendColumn("ip6", make_column<clickhouse::ColumnIPv6>(rows_of(ip6, first, last)));
        block.AppendColumn("i128", make_column<clickhouse::ColumnInt128>(rows_of(i128, first, last)));
        block.AppendColumn("u128", make_column<clickhouse::ColumnUInt128>(rows_of(u128, first, last)));
        block.AppendColumn("t", make_column<clickhouse::ColumnTime>(rows_of(times, first, last)));
        block.AppendColumn("t64_3", t64);
        block.AppendColumn(
            "lc",
            make_column<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>(rows_of(words, first, last)));
        return block;
    };

    auto require_texts = [&](const data_chunk_t& chunk) {
        REQUIRE(chunk.size() == 2);
        const auto types = chunk.types();
        REQUIRE(types.size() == expected.size());
        for (size_t col = 0; col < expected.size(); ++col) {
            CAPTURE(expected[col].name);
            REQUIRE(types[col].alias() == expected[col].name);
            REQUIRE(types[col].type() == logical_type::STRING_LITERAL);
            for (size_t row = 0; row < 2; ++row) {
                require_written(chunk, col, row);
                REQUIRE(chunk.value(col, row).value<std::string_view>() == expected[col].texts[row]);
            }
        }
    };

    overrides_t named_types;
    for (const auto& column : expected) {
        named_types.emplace(column.name, column.named_type);
    }
    const overrides_t no_overrides;

    SECTION("without overrides") { convert_single_and_split(make_block, 2, no_overrides, require_texts); }
    SECTION("under the system.columns types") { convert_single_and_split(make_block, 2, named_types, require_texts); }
    SECTION("the schema discovery registers is the chunk's") {
        const auto block = make_block(0, 2);
        for (const auto* overrides : std::initializer_list<const overrides_t*>{&no_overrides, &named_types}) {
            const auto schema = tsl::ch_to_struct(res, block, *overrides);
            auto converted = tsl::ch_to_chunk(res, block, *overrides);
            INFO((converted.has_error() ? converted.error().what.c_str() : "converted"));
            REQUIRE_FALSE(converted.has_error());
            const auto types = converted.value().types();
            REQUIRE(schema.child_types().size() == types.size());
            for (size_t col = 0; col < types.size(); ++col) {
                CAPTURE(expected[col].name);
                REQUIRE(schema.child_types()[col].type() == logical_type::STRING_LITERAL);
                REQUIRE(schema.child_types()[col] == types[col]);
            }
        }
    }
}

// A wire type with no reading as the column's type fails the conversion instead of writing a
// placeholder; discovery still registers the column (as STRING), so the failure is the query's.
TEST_CASE("ch_to_chunk: a wire type without a reading is conversion_failure") {
    SECTION("Map") {
        auto make_block = [](size_t first, size_t last) {
            auto entries = std::make_shared<clickhouse::ColumnArray>(std::make_shared<clickhouse::ColumnTuple>(
                std::vector<clickhouse::ColumnRef>{std::make_shared<clickhouse::ColumnString>(),
                                                   std::make_shared<clickhouse::ColumnUInt64>()}));
            for (size_t row = first; row < last; ++row) {
                auto keys = std::make_shared<clickhouse::ColumnString>();
                keys->Append("k");
                auto values = std::make_shared<clickhouse::ColumnUInt64>();
                values->Append(static_cast<uint64_t>(row));
                entries->AppendAsColumn(
                    std::make_shared<clickhouse::ColumnTuple>(std::vector<clickhouse::ColumnRef>{keys, values}));
            }
            return one_column_block("attrs", std::make_shared<clickhouse::ColumnMap>(entries));
        };
        SECTION("without an override") { require_conversion_failure(make_block, 2, {}, "attrs"); }
        SECTION("under its system.columns type") {
            require_conversion_failure(make_block, 2, {{"attrs", "Map(String, UInt64)"}}, "attrs");
        }
        SECTION("discovery registers it as STRING") {
            const auto schema = tsl::ch_to_struct(std::pmr::new_delete_resource(),
                                                  make_block(0, 2),
                                                  {{"attrs", "Map(String, UInt64)"}});
            REQUIRE(schema.child_types().size() == 1);
            REQUIRE(schema.child_types()[0].type() == logical_type::STRING_LITERAL);
        }
    }
    SECTION("Nothing") {
        auto make_block = [](size_t first, size_t last) {
            return one_column_block("nothing", std::make_shared<clickhouse::ColumnNothing>(last - first));
        };
        require_conversion_failure(make_block, 2, {}, "nothing");
    }
}

// SELECT NULL: a NULL is NULL whatever the type the wire gives it.
TEST_CASE("ch_to_chunk: a Nullable(Nothing) column is its NULL rows") {
    auto make_block = [](size_t first, size_t last) {
        auto nulls = std::make_shared<clickhouse::ColumnUInt8>();
        for (size_t row = first; row < last; ++row) {
            nulls->Append(1);
        }
        return one_column_block(
            "x",
            std::make_shared<clickhouse::ColumnNullable>(std::make_shared<clickhouse::ColumnNothing>(last - first),
                                                         nulls));
    };
    convert_single_and_split(make_block, 2, {}, [](const data_chunk_t& chunk) {
        REQUIRE(chunk.size() == 2);
        REQUIRE(chunk.types()[0].type() == logical_type::STRING_LITERAL);
        REQUIRE(chunk.value(0, 0).is_null());
        REQUIRE(chunk.value(0, 1).is_null());
    });
}

// The override a wire column does represent keeps shaping it through nesting: an Array of named Tuples
// whose Bool field travels as UInt8.
TEST_CASE("ch_to_chunk: an Array(Tuple) override the wire column represents names the fields and reads the Bools") {
    const std::vector<std::vector<int32_t>> ids{{1, 2}, {}, {3}};
    const std::vector<std::vector<uint8_t>> oks{{1, 0}, {}, {1}};
    auto make_block = [&](size_t first, size_t last) {
        auto items = std::make_shared<clickhouse::ColumnTuple>(
            std::vector<clickhouse::ColumnRef>{std::make_shared<clickhouse::ColumnInt32>(),
                                               std::make_shared<clickhouse::ColumnUInt8>()});
        auto array = std::make_shared<clickhouse::ColumnArray>(items);
        for (size_t row = first; row < last; ++row) {
            array->AppendAsColumn(std::make_shared<clickhouse::ColumnTuple>(
                std::vector<clickhouse::ColumnRef>{make_column<clickhouse::ColumnInt32>(ids[row]),
                                                   make_column<clickhouse::ColumnUInt8>(oks[row])}));
        }
        return one_column_block("recs", array);
    };
    const overrides_t overrides{{"recs", "Array(Tuple(id Int32, ok Bool))"}};
    convert_single_and_split(make_block, ids.size(), overrides, [&](const data_chunk_t& chunk) {
        const auto types = chunk.types();
        REQUIRE(types[0].type() == logical_type::LIST);
        const auto& item = types[0].child_type();
        REQUIRE(item.type() == logical_type::STRUCT);
        REQUIRE(item.child_types().size() == 2);
        REQUIRE(item.child_types()[0].alias() == "id");
        REQUIRE(item.child_types()[0].type() == logical_type::INTEGER);
        REQUIRE(item.child_types()[1].alias() == "ok");
        REQUIRE(item.child_types()[1].type() == logical_type::BOOLEAN);
        for (size_t row = 0; row < ids.size(); ++row) {
            CAPTURE(row);
            const auto value = chunk.value(0, row);
            REQUIRE(value.children().size() == ids[row].size());
            for (size_t i = 0; i < ids[row].size(); ++i) {
                CAPTURE(i);
                REQUIRE(value.children()[i].children()[0].value<int32_t>() == ids[row][i]);
                REQUIRE(value.children()[i].children()[1].value<bool>() == (oks[row][i] != 0));
            }
        }
        require_struct_is_chunk_types(make_block(0, ids.size()), overrides, chunk);
    });
}

// What the ClickHouse prepare probe relies on: the 0-row header block a statement's
// `SELECT * FROM (...) LIMIT 0` wrap answers, read by ch_to_struct under the base table's
// named types, names the very types ch_to_chunk gives the statement's data blocks under the
// same overrides — for the columns whose prepared type used to be read off the plan:
// COUNT(*) (UInt64, not BIGINT), `score + 1 AS score` (Int64, not the Int32 column's type),
// `length(name) AS name` (UInt64, not the String column's type) — and for a named Tuple
// base column the overrides shape both the same way.
TEST_CASE("ch_to_struct of a statement's header block is the chunk types of its data blocks") {
    auto* res = std::pmr::new_delete_resource();
    const std::unordered_map<std::string, std::string> overrides{
        {"id", "Int32"},
        {"score", "Int32"},
        {"name", "String"},
        {"rec", "Tuple(a Int32, b Nullable(String))"},
    };
    auto make_block = [](size_t rows) {
        auto n = std::make_shared<clickhouse::ColumnUInt64>();
        auto score = std::make_shared<clickhouse::ColumnInt64>();
        auto name = std::make_shared<clickhouse::ColumnUInt64>();
        auto rec_a = std::make_shared<clickhouse::ColumnInt32>();
        std::vector<std::optional<std::string>> rec_b;
        for (size_t row = 0; row < rows; ++row) {
            n->Append(row + 7);
            score->Append(static_cast<int64_t>(row) + 3000000000LL);
            name->Append(row * 2);
            rec_a->Append(static_cast<int32_t>(row));
            rec_b.push_back(row % 2 == 0 ? std::optional<std::string>{"b" + std::to_string(row)} : std::nullopt);
        }
        clickhouse::Block block;
        block.AppendColumn("n", n);
        block.AppendColumn("score", score);
        block.AppendColumn("name", name);
        block.AppendColumn("rec",
                           std::make_shared<clickhouse::ColumnTuple>(std::vector<clickhouse::ColumnRef>{
                               rec_a, make_nullable<clickhouse::ColumnString, std::string>(rec_b)}));
        return block;
    };

    const auto header = tsl::ch_to_struct(res, make_block(0), overrides);
    REQUIRE(header.child_types().size() == 4);
    REQUIRE(header.child_types()[0].type() == logical_type::UBIGINT);
    REQUIRE(header.child_types()[0].alias() == "n");
    REQUIRE(header.child_types()[1].type() == logical_type::BIGINT);
    REQUIRE(header.child_types()[1].alias() == "score");
    REQUIRE(header.child_types()[2].type() == logical_type::UBIGINT);
    REQUIRE(header.child_types()[2].alias() == "name");
    REQUIRE(header.child_types()[3].type() == logical_type::STRUCT);
    REQUIRE(header.child_types()[3].child_types().size() == 2);
    REQUIRE(header.child_types()[3].child_types()[0].alias() == "a");
    REQUIRE(header.child_types()[3].child_types()[1].alias() == "b");

    auto converted = tsl::ch_to_chunk(res, std::vector<clickhouse::Block>{make_block(3), make_block(2)}, overrides);
    INFO(converted.error().what.c_str());
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();
    REQUIRE(chunk.size() == 5);
    const auto chunk_types = chunk.types();
    REQUIRE(chunk_types.size() == header.child_types().size());
    for (size_t col = 0; col < chunk_types.size(); ++col) {
        CAPTURE(col);
        REQUIRE(chunk_types[col] == header.child_types()[col]);
    }
    REQUIRE(chunk.value(0, 4).value<uint64_t>() == 8);
    REQUIRE(chunk.value(1, 0).value<int64_t>() == 3000000000LL);
    REQUIRE(chunk.value(2, 2).value<uint64_t>() == 4);
    REQUIRE(chunk.value(3, 1).children()[1].is_null());
}
