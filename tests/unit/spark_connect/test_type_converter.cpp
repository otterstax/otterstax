// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/spark_connect_server/plan_translator/type_converter.hpp"

#include <spark/connect/types.pb.h>

#include <components/types/types.hpp>

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <string>

namespace {

namespace sc = ::spark::connect;
namespace ct = components::types;

} // namespace

// ── Scalar types ──────────────────────────────────────────────────────────────

TEST_CASE("type_converter: boolean scalar") {
    ct::complex_logical_type type{ct::logical_type::BOOLEAN};
    auto dt = frontend::spark::to_spark_data_type(type);
    REQUIRE(dt.kind_case() == sc::DataType::kBoolean);
}

TEST_CASE("type_converter: integer (int32) scalar") {
    ct::complex_logical_type type{ct::logical_type::INTEGER};
    auto dt = frontend::spark::to_spark_data_type(type);
    REQUIRE(dt.kind_case() == sc::DataType::kInteger);
}

TEST_CASE("type_converter: double (float64) scalar") {
    ct::complex_logical_type type{ct::logical_type::DOUBLE};
    auto dt = frontend::spark::to_spark_data_type(type);
    REQUIRE(dt.kind_case() == sc::DataType::kDouble);
}

TEST_CASE("type_converter: string scalar") {
    ct::complex_logical_type type{ct::logical_type::STRING_LITERAL};
    auto dt = frontend::spark::to_spark_data_type(type);
    REQUIRE(dt.kind_case() == sc::DataType::kString);
}

// ── Date / Timestamp ─────────────────────────────────────────────────────────

TEST_CASE("type_converter: date") {
    ct::complex_logical_type type{ct::logical_type::DATE};
    auto dt = frontend::spark::to_spark_data_type(type);
    REQUIRE(dt.kind_case() == sc::DataType::kDate);
}

TEST_CASE("type_converter: timestamp") {
    ct::complex_logical_type type{ct::logical_type::TIMESTAMP};
    auto dt = frontend::spark::to_spark_data_type(type);
    REQUIRE(dt.kind_case() == sc::DataType::kTimestamp);
}

// ── Compound types ───────────────────────────────────────────────────────────

TEST_CASE("type_converter: struct with multiple fields") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    std::pmr::vector<ct::complex_logical_type> fields{resource};
    auto id_field = ct::complex_logical_type{ct::logical_type::INTEGER};
    id_field.set_alias("id");
    fields.push_back(id_field);
    auto name_field = ct::complex_logical_type{ct::logical_type::STRING_LITERAL};
    name_field.set_alias("name");
    fields.push_back(name_field);

    auto struct_type = ct::complex_logical_type::create_struct("row", fields);

    auto dt = frontend::spark::to_spark_data_type(struct_type);
    REQUIRE(dt.kind_case() == sc::DataType::kStruct);
    REQUIRE(dt.struct_().fields_size() == 2);
    CHECK(dt.struct_().fields(0).name() == "id");
    CHECK(dt.struct_().fields(0).data_type().kind_case() == sc::DataType::kInteger);
    CHECK(dt.struct_().fields(1).name() == "name");
    CHECK(dt.struct_().fields(1).data_type().kind_case() == sc::DataType::kString);
}

TEST_CASE("type_converter: list/array of integers") {
    auto elem_type = ct::complex_logical_type{ct::logical_type::INTEGER};
    auto list_type = ct::complex_logical_type::create_list(elem_type);

    auto dt = frontend::spark::to_spark_data_type(list_type);
    REQUIRE(dt.kind_case() == sc::DataType::kArray);
    CHECK(dt.array().element_type().kind_case() == sc::DataType::kInteger);
    CHECK(dt.array().contains_null() == true);
}

TEST_CASE("type_converter: schema from flat column vector") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    std::pmr::vector<ct::complex_logical_type> columns{resource};
    auto c0 = ct::complex_logical_type{ct::logical_type::BIGINT};
    c0.set_alias("k");
    columns.push_back(c0);
    auto c1 = ct::complex_logical_type{ct::logical_type::DOUBLE};
    c1.set_alias("v");
    columns.push_back(c1);

    auto schema = frontend::spark::to_spark_schema(columns);
    REQUIRE(schema.kind_case() == sc::DataType::kStruct);
    REQUIRE(schema.struct_().fields_size() == 2);
    CHECK(schema.struct_().fields(0).name() == "k");
    CHECK(schema.struct_().fields(1).name() == "v");
}

TEST_CASE("type_converter: schema from flat column vector falls back to colN for unaliased columns") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    std::pmr::vector<ct::complex_logical_type> columns{resource};
    // An unaliased leaf type: complex_logical_type::alias() dereferences a null
    // extension_ and crashes, so to_spark_schema must fall back to a stable colN.
    // This is the shape execute_plan's result chunk carries for a Path-B .schema.
    columns.push_back(ct::complex_logical_type{ct::logical_type::BIGINT});
    auto named = ct::complex_logical_type{ct::logical_type::DOUBLE};
    named.set_alias("price");
    columns.push_back(named);

    auto schema = frontend::spark::to_spark_schema(columns);
    REQUIRE(schema.kind_case() == sc::DataType::kStruct);
    REQUIRE(schema.struct_().fields_size() == 2);
    CHECK(schema.struct_().fields(0).name() == "col0");
    CHECK(schema.struct_().fields(1).name() == "price");
}
