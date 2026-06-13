// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/output/chunk_to_arrow.hpp"

#include <catch2/catch.hpp>

#include <arrow/api.h>

#include <memory_resource>
#include <vector>

using namespace components::types;

namespace {

// Build a flat struct type with named scalar children for schema tests.
complex_logical_type make_struct(std::vector<std::pair<std::string, logical_type>> cols) {
    std::pmr::vector<complex_logical_type> fields(std::pmr::get_default_resource());
    fields.reserve(cols.size());
    for (auto& [name, lt] : cols) {
        fields.emplace_back(lt);
        fields.back().set_alias(name);
    }
    return complex_logical_type::create_struct("", std::move(fields));
}

} // namespace

TEST_CASE("to_arrow_schema(struct): scalar types map to correct Arrow types") {
    auto struct_t = make_struct({
        {"flag",   logical_type::BOOLEAN},
        {"i8",    logical_type::TINYINT},
        {"i16",   logical_type::SMALLINT},
        {"i32",   logical_type::INTEGER},
        {"i64",   logical_type::BIGINT},
        {"u8",    logical_type::UTINYINT},
        {"u16",   logical_type::USMALLINT},
        {"u32",   logical_type::UINTEGER},
        {"u64",   logical_type::UBIGINT},
        {"f32",   logical_type::FLOAT},
        {"f64",   logical_type::DOUBLE},
        {"txt",   logical_type::STRING_LITERAL},
    });

    auto schema = to_arrow_schema(struct_t);
    REQUIRE(schema->num_fields() == 12);

    REQUIRE(schema->field(0)->type()->id()  == arrow::Type::BOOL);
    REQUIRE(schema->field(1)->type()->id()  == arrow::Type::INT8);
    REQUIRE(schema->field(2)->type()->id()  == arrow::Type::INT16);
    REQUIRE(schema->field(3)->type()->id()  == arrow::Type::INT32);
    REQUIRE(schema->field(4)->type()->id()  == arrow::Type::INT64);
    REQUIRE(schema->field(5)->type()->id()  == arrow::Type::UINT8);
    REQUIRE(schema->field(6)->type()->id()  == arrow::Type::UINT16);
    REQUIRE(schema->field(7)->type()->id()  == arrow::Type::UINT32);
    REQUIRE(schema->field(8)->type()->id()  == arrow::Type::UINT64);
    REQUIRE(schema->field(9)->type()->id()  == arrow::Type::FLOAT);
    REQUIRE(schema->field(10)->type()->id() == arrow::Type::DOUBLE);
    REQUIRE(schema->field(11)->type()->id() == arrow::Type::STRING);
}

TEST_CASE("to_arrow_schema(struct): field names are preserved") {
    auto struct_t = make_struct({{"user_id", logical_type::INTEGER}, {"email", logical_type::STRING_LITERAL}});

    auto schema = to_arrow_schema(struct_t);

    REQUIRE(schema->field(0)->name() == "user_id");
    REQUIRE(schema->field(1)->name() == "email");
}

TEST_CASE("to_arrow_schema(struct): NA type maps to arrow::null()") {
    auto struct_t = make_struct({{"null_col", logical_type::NA}});

    auto schema = to_arrow_schema(struct_t);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::NA);
}

TEST_CASE("to_arrow_schema(struct): non-STRUCT input (NA) produces empty schema") {
    // When the input is not a STRUCT, to_arrow_schema returns an empty schema.
    complex_logical_type na_type{logical_type::NA};
    auto schema = to_arrow_schema(na_type);
    REQUIRE(schema->num_fields() == 0);
}

TEST_CASE("to_arrow_schema(struct): empty struct produces zero-field schema") {
    auto empty = complex_logical_type::create_struct(
        "",
        std::pmr::vector<complex_logical_type>(std::pmr::get_default_resource()));
    auto schema = to_arrow_schema(empty);
    REQUIRE(schema->num_fields() == 0);
}

TEST_CASE("to_arrow_schema(struct): LIST child maps to arrow::list()") {
    auto inner = complex_logical_type{logical_type::INTEGER};
    auto list_t = complex_logical_type::create_list(inner, "items");

    std::pmr::vector<complex_logical_type> fields({list_t}, std::pmr::get_default_resource());
    auto struct_t = complex_logical_type::create_struct("", fields);

    auto schema = to_arrow_schema(struct_t);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::LIST);
}

TEST_CASE("to_arrow_schema(struct): ARRAY child maps to arrow::list()") {
    auto inner = complex_logical_type{logical_type::DOUBLE};
    auto arr_t = complex_logical_type::create_array(inner, 4, "coords");

    std::pmr::vector<complex_logical_type> fields({arr_t}, std::pmr::get_default_resource());
    auto struct_t = complex_logical_type::create_struct("", fields);

    auto schema = to_arrow_schema(struct_t);

    REQUIRE(schema->num_fields() == 1);
    // ARRAY emits variable-length list on the Arrow wire — same id as LIST.
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::LIST);
}

TEST_CASE("to_arrow_schema(struct): nested STRUCT child maps to arrow::struct_()") {
    auto inner = make_struct({{"x", logical_type::INTEGER}, {"y", logical_type::DOUBLE}});
    inner.set_alias("point");
    std::pmr::vector<complex_logical_type> fields({inner}, std::pmr::get_default_resource());
    auto outer = complex_logical_type::create_struct("", fields);

    auto schema = to_arrow_schema(outer);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::STRUCT);

    // The nested struct should carry its own fields.
    auto nested = std::static_pointer_cast<arrow::StructType>(schema->field(0)->type());
    REQUIRE(nested->num_fields() == 2);
    REQUIRE(nested->field(0)->name() == "x");
    REQUIRE(nested->field(1)->name() == "y");
}

TEST_CASE("to_arrow_schema(vec): vector overload works like struct overload") {
    std::pmr::vector<complex_logical_type> types{std::pmr::get_default_resource()};
    complex_logical_type t1{logical_type::INTEGER};
    t1.set_alias("id");
    complex_logical_type t2{logical_type::STRING_LITERAL};
    t2.set_alias("name");
    types.push_back(t1);
    types.push_back(t2);

    auto schema = to_arrow_schema(types);

    REQUIRE(schema->num_fields() == 2);
    REQUIRE(schema->field(0)->name() == "id");
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::INT32);
    REQUIRE(schema->field(1)->name() == "name");
    REQUIRE(schema->field(1)->type()->id() == arrow::Type::STRING);
}
