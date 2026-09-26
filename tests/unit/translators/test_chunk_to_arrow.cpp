// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/output/chunk_to_arrow.hpp"

#include <catch2/catch_all.hpp>

#include <arrow/api.h>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace components::types;

namespace {

// Build a flat struct type with named scalar children for schema tests.
complex_logical_type make_struct(std::vector<std::pair<std::string, logical_type>> cols) {
    std::pmr::vector<complex_logical_type> fields(std::pmr::new_delete_resource());
    fields.reserve(cols.size());
    for (auto& [name, lt] : cols) {
        fields.emplace_back(lt);
        fields.back().set_alias(name);
    }
    return complex_logical_type::create_struct("", std::move(fields));
}

// The existing schema cases assert on the Arrow schema itself; the helper checks
// the result before unwrapping it, so a conversion error fails the case.
template<typename Types>
std::shared_ptr<arrow::Schema> schema_of(const Types& types) {
    auto converted = to_arrow_schema(std::pmr::new_delete_resource(), types);
    REQUIRE_FALSE(converted.has_error());
    return converted.value();
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

    auto schema = schema_of(struct_t);
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

    auto schema = schema_of(struct_t);

    REQUIRE(schema->field(0)->name() == "user_id");
    REQUIRE(schema->field(1)->name() == "email");
}

TEST_CASE("to_arrow_schema(struct): NA type maps to arrow::null()") {
    auto struct_t = make_struct({{"null_col", logical_type::NA}});

    auto schema = schema_of(struct_t);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::NA);
}

TEST_CASE("to_arrow_schema(struct): non-STRUCT input (NA) produces empty schema") {
    // When the input is not a STRUCT, to_arrow_schema returns an empty schema.
    complex_logical_type na_type{logical_type::NA};
    auto schema = schema_of(na_type);
    REQUIRE(schema->num_fields() == 0);
}

TEST_CASE("to_arrow_schema(struct): empty struct produces zero-field schema") {
    auto empty = complex_logical_type::create_struct(
        "",
        std::pmr::vector<complex_logical_type>(std::pmr::new_delete_resource()));
    auto schema = schema_of(empty);
    REQUIRE(schema->num_fields() == 0);
}

TEST_CASE("to_arrow_schema(struct): LIST child maps to arrow::list()") {
    auto inner = complex_logical_type{logical_type::INTEGER};
    auto list_t = complex_logical_type::create_list(inner, "items");

    std::pmr::vector<complex_logical_type> fields({list_t}, std::pmr::new_delete_resource());
    auto struct_t = complex_logical_type::create_struct("", fields);

    auto schema = schema_of(struct_t);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::LIST);
}

TEST_CASE("to_arrow_schema(struct): ARRAY child maps to arrow::list()") {
    auto inner = complex_logical_type{logical_type::DOUBLE};
    auto arr_t = complex_logical_type::create_array(inner, 4, "coords");

    std::pmr::vector<complex_logical_type> fields({arr_t}, std::pmr::new_delete_resource());
    auto struct_t = complex_logical_type::create_struct("", fields);

    auto schema = schema_of(struct_t);

    REQUIRE(schema->num_fields() == 1);
    // ARRAY emits variable-length list on the Arrow wire — same id as LIST.
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::LIST);
}

TEST_CASE("to_arrow_schema(struct): nested STRUCT child maps to arrow::struct_()") {
    auto inner = make_struct({{"x", logical_type::INTEGER}, {"y", logical_type::DOUBLE}});
    inner.set_alias("point");
    std::pmr::vector<complex_logical_type> fields({inner}, std::pmr::new_delete_resource());
    auto outer = complex_logical_type::create_struct("", fields);

    auto schema = schema_of(outer);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::STRUCT);

    // The nested struct should carry its own fields.
    auto nested = std::static_pointer_cast<arrow::StructType>(schema->field(0)->type());
    REQUIRE(nested->num_fields() == 2);
    REQUIRE(nested->field(0)->name() == "x");
    REQUIRE(nested->field(1)->name() == "y");
}

TEST_CASE("to_arrow_schema(vec): vector overload works like struct overload") {
    std::pmr::vector<complex_logical_type> types{std::pmr::new_delete_resource()};
    complex_logical_type t1{logical_type::INTEGER};
    t1.set_alias("id");
    complex_logical_type t2{logical_type::STRING_LITERAL};
    t2.set_alias("name");
    types.push_back(t1);
    types.push_back(t2);

    auto schema = schema_of(types);

    REQUIRE(schema->num_fields() == 2);
    REQUIRE(schema->field(0)->name() == "id");
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::INT32);
    REQUIRE(schema->field(1)->name() == "name");
    REQUIRE(schema->field(1)->type()->id() == arrow::Type::STRING);
}

// ── columns without an alias ──────────────────────────────────────────────────
// A column built from a bare logical_type carries no alias at all, and alias()
// asserts on it; the schema conversion must not read it blindly.

TEST_CASE("to_arrow_schema(vec): a column without an alias becomes a field with an empty name") {
    std::pmr::vector<complex_logical_type> types{std::pmr::new_delete_resource()};
    types.emplace_back(logical_type::INTEGER);
    complex_logical_type named{logical_type::STRING_LITERAL};
    named.set_alias("name");
    types.push_back(named);

    auto schema = schema_of(types);

    REQUIRE(schema->num_fields() == 2);
    REQUIRE(schema->field(0)->name().empty());
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::INT32);
    REQUIRE(schema->field(1)->name() == "name");
}

TEST_CASE("to_arrow_schema(struct): a child without an alias becomes a field with an empty name") {
    std::pmr::vector<complex_logical_type> fields(std::pmr::new_delete_resource());
    fields.emplace_back(logical_type::DOUBLE);
    auto struct_t = complex_logical_type::create_struct("", std::move(fields));

    auto schema = schema_of(struct_t);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->name().empty());
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::DOUBLE);
}

TEST_CASE("to_arrow_schema(struct): a nested STRUCT child without an alias does not crash") {
    std::pmr::vector<complex_logical_type> inner_fields(std::pmr::new_delete_resource());
    inner_fields.emplace_back(logical_type::INTEGER);
    auto inner = complex_logical_type::create_struct("", std::move(inner_fields));
    std::pmr::vector<complex_logical_type> fields({inner}, std::pmr::new_delete_resource());
    auto outer = complex_logical_type::create_struct("", fields);

    auto schema = schema_of(outer);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::STRUCT);
    auto nested = std::static_pointer_cast<arrow::StructType>(schema->field(0)->type());
    REQUIRE(nested->num_fields() == 1);
    REQUIRE(nested->field(0)->name().empty());
}

// ── the 128-bit width ─────────────────────────────────────────────────────────
// UINT128 and BIT have no Arrow counterpart in the stream, and neither do the
// INT128 readings that are not integers (UUID, a DECIMAL wider than 18 digits);
// the conversion reports them as an error on the caller's resource instead of
// unwinding. HUGEINT is the exception: Arrow has no 128-bit integer type, so it
// maps to decimal128(38, 0) — the only 128-bit integral carrier Arrow offers,
// with scale 0 leaving the value an integer.

TEST_CASE("to_arrow_schema(vec): a HUGEINT column maps to decimal128(38, 0)") {
    std::pmr::vector<complex_logical_type> types{std::pmr::new_delete_resource()};
    types.emplace_back(logical_type::BIGINT, "id");
    types.emplace_back(logical_type::HUGEINT, "wide");

    auto schema = schema_of(types);

    REQUIRE(schema->num_fields() == 2);
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::INT64);
    REQUIRE(schema->field(1)->name() == "wide");
    REQUIRE(schema->field(1)->type()->id() == arrow::Type::DECIMAL128);
    const auto& decimal = static_cast<const arrow::Decimal128Type&>(*schema->field(1)->type());
    REQUIRE(decimal.precision() == 38);
    REQUIRE(decimal.scale() == 0);
}

TEST_CASE("to_arrow_schema(vec): a UHUGEINT column is conversion_failure naming the column") {
    // decimal128 is signed, so it cannot carry the upper half of a 128-bit
    // unsigned range; UINT128 keeps its refusal where HUGEINT gained a mapping.
    std::pmr::vector<complex_logical_type> types{std::pmr::new_delete_resource()};
    types.emplace_back(logical_type::BIGINT, "id");
    types.emplace_back(logical_type::UHUGEINT, "wide");

    auto schema = to_arrow_schema(std::pmr::new_delete_resource(), types);

    REQUIRE(schema.has_error());
    REQUIRE(schema.error().type == core::error_code_t::conversion_failure);
    REQUIRE(std::string_view{schema.error().what.c_str()}.find("wide") != std::string_view::npos);
}

TEST_CASE("to_arrow_schema(struct): a DECIMAL needing 128-bit storage maps to decimal128 with its scale") {
    // 38 is both the engine's widest DECIMAL precision (DECIMAL_MAX_WIDTH) and the widest
    // decimal128 declares (Decimal128Type::kMaxPrecision), so the two windows coincide and the
    // widest DECIMAL the engine can build still has a carrier — no decimal256, no refusal.
    std::pmr::vector<complex_logical_type> fields(std::pmr::new_delete_resource());
    auto amount = complex_logical_type::create_decimal(std::pmr::new_delete_resource(), 38, 10, "amount");
    REQUIRE_FALSE(amount.has_error());
    fields.push_back(amount.value());
    auto struct_t = complex_logical_type::create_struct("", std::move(fields));

    auto schema = schema_of(struct_t);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->name() == "amount");
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::DECIMAL128);
    const auto& declared = static_cast<const arrow::Decimal128Type&>(*schema->field(0)->type());
    REQUIRE(declared.precision() == 38);
    REQUIRE(declared.scale() == 10);
}

TEST_CASE("to_arrow_schema(struct): a DECIMAL that fits 64 bits maps to decimal128 with its scale") {
    // Its physical type is INT64 — shared with BIGINT — and mapping by that is what dropped the
    // scale: the column went out as a bare int64 of the unscaled integer, so DECIMAL(18, 4)
    // 1.2345 reached the client as 12345 under a schema that could not say otherwise.
    std::pmr::vector<complex_logical_type> fields(std::pmr::new_delete_resource());
    auto amount = complex_logical_type::create_decimal(std::pmr::new_delete_resource(), 18, 4, "amount");
    REQUIRE_FALSE(amount.has_error());
    REQUIRE(amount.value().to_physical_type() == physical_type::INT64);
    fields.push_back(amount.value());
    auto struct_t = complex_logical_type::create_struct("", std::move(fields));

    auto schema = schema_of(struct_t);

    REQUIRE(schema->num_fields() == 1);
    REQUIRE(schema->field(0)->type()->id() == arrow::Type::DECIMAL128);
    const auto& declared = static_cast<const arrow::Decimal128Type&>(*schema->field(0)->type());
    REQUIRE(declared.precision() == 18);
    REQUIRE(declared.scale() == 4);
}

TEST_CASE("to_arrow_schema(struct): an unmappable LIST element propagates the error") {
    auto list_t = complex_logical_type::create_list(complex_logical_type{logical_type::UHUGEINT}, "items");
    std::pmr::vector<complex_logical_type> fields({list_t}, std::pmr::new_delete_resource());
    auto struct_t = complex_logical_type::create_struct("", fields);

    auto schema = to_arrow_schema(std::pmr::new_delete_resource(), struct_t);

    REQUIRE(schema.has_error());
    REQUIRE(schema.error().type == core::error_code_t::conversion_failure);
}

TEST_CASE("to_arrow_schema(vec): the error message lives on the caller's resource") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<complex_logical_type> types{&arena};
    types.emplace_back(logical_type::UHUGEINT, "wide");

    auto schema = to_arrow_schema(&arena, types);

    REQUIRE(schema.has_error());
    REQUIRE(schema.error().what.get_allocator().resource() == &arena);
}

// ── chunk_to_record_batch ─────────────────────────────────────────────────────

using components::vector::data_chunk_t;
using components::types::logical_value_t;

namespace {

    std::pmr::vector<complex_logical_type> named_types(std::pmr::memory_resource* res,
                                                       std::vector<std::pair<std::string, logical_type>> cols) {
        std::pmr::vector<complex_logical_type> types{res};
        for (auto& [name, lt] : cols) {
            types.emplace_back(lt, name);
        }
        return types;
    }

} // namespace

TEST_CASE("chunk_to_record_batch: a HUGEINT column carries values no 64-bit column could") {
    // The point of the 128-bit carrier: the values below do not fit int64, and the
    // decimal128 words must be assembled from the raw two's-complement integer
    // rather than rescaled, so a negative value stays negative.
    auto* res = std::pmr::new_delete_resource();
    auto types = named_types(res, {{"wide", logical_type::HUGEINT}});
    data_chunk_t chunk(res, types, 3);
    chunk.set_cardinality(3);
    const auto positive = absl::MakeInt128(1, 0);  // 2^64, past the int64 range
    chunk.set_value(0, 0, logical_value_t{res, positive});
    chunk.set_value(0, 1, logical_value_t{res, -positive});
    chunk.set_value(0, 2, logical_value_t{res, nullptr});

    auto batch = chunk_to_record_batch(res, chunk);
    INFO("error: " << batch.error().what.c_str());
    REQUIRE_FALSE(batch.has_error());
    const auto& rb = *batch.value();
    REQUIRE(rb.num_rows() == 3);
    REQUIRE(rb.schema()->field(0)->name() == "wide");
    REQUIRE(rb.schema()->field(0)->type()->id() == arrow::Type::DECIMAL128);
    const auto& wide = static_cast<const arrow::Decimal128Array&>(*rb.column(0));
    REQUIRE(wide.FormatValue(0) == "18446744073709551616");
    REQUIRE(wide.FormatValue(1) == "-18446744073709551616");
    REQUIRE(wide.IsNull(2));
}

TEST_CASE("chunk_to_record_batch: a hugeint decimal128 can declare is appended and the batch validates") {
    // The companion of the refusal below: at the edge of the precision window the value
    // is carried, and Arrow's own full validation accepts the array — which is what the
    // refusal protects.
    auto* res = std::pmr::new_delete_resource();
    auto types = named_types(res, {{"wide", logical_type::HUGEINT}});
    data_chunk_t chunk(res, types, 2);
    chunk.set_cardinality(2);
    components::types::int128_t max_declarable = 1;
    for (int i = 0; i < 38; ++i) {
        max_declarable *= 10;
    }
    max_declarable -= 1; // 10^38 - 1, the largest decimal128(38, 0) may declare
    chunk.set_value(0, 0, logical_value_t{res, max_declarable});
    chunk.set_value(0, 1, logical_value_t{res, -max_declarable});

    auto batch = chunk_to_record_batch(res, chunk);
    INFO("error: " << batch.error().what.c_str());
    REQUIRE_FALSE(batch.has_error());
    INFO("validation: " << batch.value()->ValidateFull().ToString());
    REQUIRE(batch.value()->ValidateFull().ok());
    const auto& wide = static_cast<const arrow::Decimal128Array&>(*batch.value()->column(0));
    REQUIRE(wide.FormatValue(0) == "99999999999999999999999999999999999999");
    REQUIRE(wide.FormatValue(1) == "-99999999999999999999999999999999999999");
}

TEST_CASE("chunk_to_record_batch: a hugeint past 10^38 is refused, naming the column and the value") {
    // int128 runs to ±1.7e38, decimal128(38, 0) only to ±(10^38 - 1). Arrow appends such
    // a value without complaint and reads it back unchanged, but ValidateFull calls the
    // array Invalid ("does not fit in precision of decimal128(38, 0)") and the parquet
    // spec forbids writing it, so a consumer is free to reject it or read NULL. Refusing
    // here is what keeps every batch this builds valid under the schema it declares.
    auto* res = std::pmr::new_delete_resource();
    auto types = named_types(res, {{"wide", logical_type::HUGEINT}});
    data_chunk_t chunk(res, types, 1);
    chunk.set_cardinality(1);
    components::types::int128_t tail = 1;
    for (int i = 0; i < 38; ++i) {
        tail *= 10; // exactly 10^38
    }
    chunk.set_value(0, 0, logical_value_t{res, tail});

    auto batch = chunk_to_record_batch(res, chunk);

    REQUIRE(batch.has_error());
    REQUIRE(batch.error().type == core::error_code_t::conversion_failure);
    REQUIRE(std::string_view{batch.error().what.c_str()}.find("wide") != std::string_view::npos);
    REQUIRE(std::string_view{batch.error().what.c_str()}.find("100000000000000000000000000000000000000") !=
            std::string_view::npos);
}

TEST_CASE("chunk_to_record_batch: a DECIMAL keeps its scale so the value is the number it stored") {
    // The engine stores a DECIMAL as the unscaled integer value * 10^scale at the width
    // its precision needs: DECIMAL(18, 4) 1.2345 is the int64 payload 12345. Mapping the
    // column by its PHYSICAL type makes that payload a bare int64 on the wire, and the
    // client reads 12345 — the scale is nowhere in the schema to put the point back.
    auto* res = std::pmr::new_delete_resource();
    auto amount = complex_logical_type::create_decimal(res, 18, 4, "amount");
    REQUIRE_FALSE(amount.has_error());
    std::pmr::vector<complex_logical_type> types{res};
    types.push_back(amount.value());

    data_chunk_t chunk(res, types, 3);
    chunk.set_cardinality(3);
    chunk.set_value(0, 0, logical_value_t::create_decimal(res, types[0], int64_t{12345}));   // 1.2345
    chunk.set_value(0, 1, logical_value_t::create_decimal(res, types[0], int64_t{-12345}));  // -1.2345
    chunk.set_value(0, 2, logical_value_t{res, nullptr});

    auto batch = chunk_to_record_batch(res, chunk);
    INFO("error: " << batch.error().what.c_str());
    REQUIRE_FALSE(batch.has_error());
    const auto& rb = *batch.value();
    REQUIRE(rb.schema()->field(0)->type()->id() == arrow::Type::DECIMAL128);
    const auto& declared = static_cast<const arrow::Decimal128Type&>(*rb.schema()->field(0)->type());
    REQUIRE(declared.precision() == 18);
    REQUIRE(declared.scale() == 4);
    const auto& values = static_cast<const arrow::Decimal128Array&>(*rb.column(0));
    REQUIRE(values.FormatValue(0) == "1.2345");
    REQUIRE(values.FormatValue(1) == "-1.2345");
    REQUIRE(values.IsNull(2));
    REQUIRE(rb.ValidateFull().ok());
}

TEST_CASE("chunk_to_record_batch: a UHUGEINT column is refused before any array is built") {
    auto* res = std::pmr::new_delete_resource();
    auto types = named_types(res, {{"wide", logical_type::UHUGEINT}});
    data_chunk_t chunk(res, types, 1);

    auto batch = chunk_to_record_batch(res, chunk);

    REQUIRE(batch.has_error());
    REQUIRE(batch.error().type == core::error_code_t::conversion_failure);
    REQUIRE(std::string_view{batch.error().what.c_str()}.find("wide") != std::string_view::npos);
}

TEST_CASE("chunk_to_record_batch: flat named scalar columns become a RecordBatch with the same values") {
    auto* res = std::pmr::new_delete_resource();
    auto types = named_types(res, {{"id", logical_type::BIGINT}, {"name", logical_type::STRING_LITERAL}});
    data_chunk_t chunk(res, types, 3);
    chunk.set_cardinality(3);
    for (int64_t r = 0; r < 3; ++r) {
        chunk.set_value(0, static_cast<uint64_t>(r), logical_value_t{res, r});
        chunk.set_value(1, static_cast<uint64_t>(r), logical_value_t{res, std::string{"row_"} + std::to_string(r)});
    }
    chunk.set_value(1, 2, logical_value_t{res, nullptr});

    auto batch = chunk_to_record_batch(res, chunk);
    REQUIRE_FALSE(batch.has_error());
    const auto& rb = *batch.value();
    REQUIRE(rb.num_rows() == 3);
    REQUIRE(rb.num_columns() == 2);
    REQUIRE(rb.schema()->field(0)->name() == "id");
    REQUIRE(static_cast<const arrow::Int64Array&>(*rb.column(0)).Value(2) == 2);
    const auto& names = static_cast<const arrow::StringArray&>(*rb.column(1));
    REQUIRE(names.GetString(0) == "row_0");
    REQUIRE(names.IsNull(2));
}

TEST_CASE("chunk_to_record_batch: an NA column is a NullArray under a null-typed field") {
    auto* res = std::pmr::new_delete_resource();
    auto types = named_types(res, {{"nothing", logical_type::NA}});
    data_chunk_t chunk(res, types, 2);
    chunk.set_cardinality(2);

    auto batch = chunk_to_record_batch(res, chunk);
    REQUIRE_FALSE(batch.has_error());
    REQUIRE(batch.value()->schema()->field(0)->type()->id() == arrow::Type::NA);
    REQUIRE(batch.value()->column(0)->length() == 2);
    REQUIRE(batch.value()->column(0)->null_count() == 2);
}

TEST_CASE("chunk_to_record_batch: a LIST column is an error, not a NullArray") {
    auto* res = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER}, "items"));
    data_chunk_t chunk(res, types, 1);

    auto batch = chunk_to_record_batch(res, chunk);
    REQUIRE(batch.has_error());
    REQUIRE(batch.error().type == core::error_code_t::conversion_failure);
    REQUIRE(std::string_view{batch.error().what.c_str()}.find("items") != std::string_view::npos);
}

TEST_CASE("chunk_to_record_batch: a STRUCT column is an error, not a NullArray") {
    auto* res = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> inner(res);
    inner.emplace_back(logical_type::INTEGER, "x");
    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(complex_logical_type::create_struct("", inner, "point"));
    data_chunk_t chunk(res, types, 1);

    auto batch = chunk_to_record_batch(res, chunk);
    REQUIRE(batch.has_error());
    REQUIRE(batch.error().type == core::error_code_t::conversion_failure);
}

TEST_CASE("chunk_to_record_batch: an ARRAY column is an error, not a NullArray") {
    auto* res = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(complex_logical_type::create_array(complex_logical_type{logical_type::DOUBLE}, 2, "coords"));
    data_chunk_t chunk(res, types, 1);

    auto batch = chunk_to_record_batch(res, chunk);
    REQUIRE(batch.has_error());
    REQUIRE(batch.error().type == core::error_code_t::conversion_failure);
}

TEST_CASE("chunk_to_record_batch: a column without a name is schema_error") {
    auto* res = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> types{res};
    types.emplace_back(logical_type::INTEGER);
    data_chunk_t chunk(res, types, 1);
    chunk.set_cardinality(1);
    chunk.set_value(0, 0, logical_value_t{res, int32_t{1}});

    auto batch = chunk_to_record_batch(res, chunk);
    REQUIRE(batch.has_error());
    REQUIRE(batch.error().type == core::error_code_t::schema_error);
}

TEST_CASE("chunk_to_record_batch: the error message lives on the caller's resource") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<complex_logical_type> types{&arena};
    types.emplace_back(logical_type::INTEGER);
    data_chunk_t chunk(&arena, types, 1);

    auto batch = chunk_to_record_batch(&arena, chunk);
    REQUIRE(batch.has_error());
    REQUIRE(batch.error().what.get_allocator().resource() == &arena);
}
