// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/ch_to_chunk.hpp"

#include <catch2/catch.hpp>

#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

#include <memory_resource>

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

    auto s = tsl::ch_to_struct(block);

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
    auto s = tsl::ch_to_struct(clickhouse::Block{});
    REQUIRE(s.type() == logical_type::STRUCT);
    REQUIRE(s.child_types().empty());
}

TEST_CASE("ch_to_chunk: single block, correct cardinality and column count") {
    auto* res = std::pmr::get_default_resource();
    auto block = make_block_3col(5);

    auto chunk = tsl::ch_to_chunk(res, block);

    REQUIRE(chunk.size() == 5);
    REQUIRE(chunk.column_count() == 3);
}

TEST_CASE("ch_to_chunk: Int32 column values round-trip") {
    auto* res = std::pmr::get_default_resource();

    auto col = std::make_shared<clickhouse::ColumnInt32>();
    col->Append(10);
    col->Append(20);
    col->Append(30);
    clickhouse::Block block;
    block.AppendColumn("x", col);

    auto chunk = tsl::ch_to_chunk(res, block);

    REQUIRE(chunk.size() == 3);
    REQUIRE(chunk.value(0, 0).value<int32_t>() == 10);
    REQUIRE(chunk.value(0, 1).value<int32_t>() == 20);
    REQUIRE(chunk.value(0, 2).value<int32_t>() == 30);
}

TEST_CASE("ch_to_chunk: Float64 column values round-trip") {
    auto* res = std::pmr::get_default_resource();

    auto col = std::make_shared<clickhouse::ColumnFloat64>();
    col->Append(1.5);
    col->Append(2.5);
    clickhouse::Block block;
    block.AppendColumn("val", col);

    auto chunk = tsl::ch_to_chunk(res, block);

    REQUIRE(chunk.size() == 2);
    REQUIRE(chunk.value(0, 0).value<double>() == Approx(1.5));
    REQUIRE(chunk.value(0, 1).value<double>() == Approx(2.5));
}

TEST_CASE("ch_to_chunk: String column values round-trip") {
    auto* res = std::pmr::get_default_resource();

    auto col = std::make_shared<clickhouse::ColumnString>();
    col->Append("hello");
    col->Append("world");
    clickhouse::Block block;
    block.AppendColumn("msg", col);

    auto chunk = tsl::ch_to_chunk(res, block);

    REQUIRE(chunk.size() == 2);
    REQUIRE(chunk.value(0, 0).value<std::string_view>() == "hello");
    REQUIRE(chunk.value(0, 1).value<std::string_view>() == "world");
}

TEST_CASE("ch_to_chunk: empty block produces empty chunk") {
    auto* res = std::pmr::get_default_resource();
    auto chunk = tsl::ch_to_chunk(res, clickhouse::Block{});
    REQUIRE(chunk.size() == 0);
    REQUIRE(chunk.column_count() == 0);
}

TEST_CASE("ch_to_chunk: multi-block vector merges row counts") {
    auto* res = std::pmr::get_default_resource();

    auto b1 = make_block_3col(3);
    auto b2 = make_block_3col(4);
    auto chunk = tsl::ch_to_chunk(res, std::vector<clickhouse::Block>{b1, b2});

    REQUIRE(chunk.size() == 7);
    REQUIRE(chunk.column_count() == 3);
}

TEST_CASE("ch_to_chunk: multi-block with empty blocks ignored") {
    auto* res = std::pmr::get_default_resource();

    auto b1 = make_block_3col(2);
    clickhouse::Block empty{};
    auto b2 = make_block_3col(3);
    auto chunk = tsl::ch_to_chunk(res, std::vector<clickhouse::Block>{b1, empty, b2});

    REQUIRE(chunk.size() == 5);
}

TEST_CASE("ch_to_chunk: type override changes column logical type") {
    auto* res = std::pmr::get_default_resource();

    // Column stored as Int32, but we override to String
    auto col = std::make_shared<clickhouse::ColumnInt32>();
    col->Append(42);
    clickhouse::Block block;
    block.AppendColumn("payload", col);

    std::unordered_map<std::string, std::string> overrides;
    overrides["payload"] = "String";

    auto chunk = tsl::ch_to_chunk(res, block, overrides);

    REQUIRE(chunk.size() == 1);
    REQUIRE(chunk.types()[0].type() == logical_type::STRING_LITERAL);
}

TEST_CASE("ch_to_struct: Int64 and UInt32 columns") {
    clickhouse::Block block;
    block.AppendColumn("big",      std::make_shared<clickhouse::ColumnInt64>());
    block.AppendColumn("unsigned", std::make_shared<clickhouse::ColumnUInt32>());

    auto s = tsl::ch_to_struct(block);

    REQUIRE(s.child_types()[0].type() == logical_type::BIGINT);
    REQUIRE(s.child_types()[1].type() == logical_type::UINTEGER);
}
