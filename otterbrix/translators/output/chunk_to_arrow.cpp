// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_arrow.hpp"

#include "utility/tracy_profiler.hpp"
#include <arrow/builder.h>

using namespace components::vector;
using namespace components::types;
using namespace components;

namespace {
    std::shared_ptr<arrow::DataType> arrow_type_from_logical(const types::complex_logical_type& t) {
        switch (t.to_physical_type()) {
            case types::physical_type::BOOL:
                return arrow::boolean();
            case types::physical_type::UINT8:
                return arrow::uint8();
            case types::physical_type::UINT16:
                return arrow::uint16();
            case types::physical_type::UINT32:
                return arrow::uint32();
            case types::physical_type::UINT64:
                return arrow::uint64();
            case types::physical_type::INT8:
                return arrow::int8();
            case types::physical_type::INT16:
                return arrow::int16();
            case types::physical_type::INT32:
                return arrow::int32();
            case types::physical_type::INT64:
                return arrow::int64();
            case types::physical_type::FLOAT:
                return arrow::float32();
            case types::physical_type::DOUBLE:
                return arrow::float64();
            case types::physical_type::STRING:
                return arrow::utf8();
            case types::physical_type::NA:
                return arrow::null();
            case types::physical_type::STRUCT: {
                arrow::FieldVector fields;
                fields.reserve(t.child_types().size());
                for (const auto& child : t.child_types()) {
                    fields.push_back(arrow::field(child.alias(), arrow_type_from_logical(child)));
                }
                return arrow::struct_(std::move(fields));
            }
            case types::physical_type::LIST:
                return arrow::list(arrow_type_from_logical(t.child_type()));
            case types::physical_type::ARRAY:
                // emit a variable-length arrow::list, the wire format is identical
                return arrow::list(arrow_type_from_logical(t.child_type()));
            default:
                throw std::runtime_error("Chunk to arrow: Unknown type: " +
                                         std::to_string(static_cast<uint8_t>(t.to_physical_type())));
        }
    }
} // namespace

std::shared_ptr<arrow::Schema> to_arrow_schema(const std::pmr::vector<components::types::complex_logical_type>& types) {
    OTX_ZONE_N("tsl::to_arrow_schema(vec)");
    arrow::FieldVector field_vector;
    field_vector.reserve(types.size());
    for (const auto& type : types) {
        field_vector.push_back(arrow::field(type.alias(), arrow_type_from_logical(type)));
    }
    return arrow::schema(std::move(field_vector));
}

std::shared_ptr<arrow::Schema> to_arrow_schema(const components::types::complex_logical_type& struct_t) {
    OTX_ZONE_N("tsl::to_arrow_schema(struct)");
    if (struct_t.type() != types::logical_type::STRUCT) {
        // logical_type::NA case - empty schema
        return arrow::schema({});
    }

    arrow::FieldVector field_vector;
    field_vector.reserve(struct_t.child_types().size());
    for (const auto& type : struct_t.child_types()) {
        field_vector.push_back(arrow::field(type.alias(), arrow_type_from_logical(type)));
    }

    return arrow::schema(std::move(field_vector));
}

std::shared_ptr<arrow::RecordBatch> chunk_to_record_batch(const data_chunk_t& chunk) {
    const auto types = chunk.types();
    const auto schema = to_arrow_schema(types);
    const auto nrows  = static_cast<int64_t>(chunk.size());
    const size_t ncols = static_cast<size_t>(chunk.column_count());

    arrow::ArrayVector arrays;
    arrays.reserve(ncols);

#define BUILD_COL(Builder, cpp_type)                                        \
    {                                                                        \
        arrow::Builder b;                                                    \
        for (int64_t r = 0; r < nrows; r++) {                               \
            auto v = chunk.value(static_cast<uint64_t>(c),                  \
                                 static_cast<uint64_t>(r));                  \
            if (v.is_null()) (void)b.AppendNull();                          \
            else             (void)b.Append(v.value<cpp_type>());           \
        }                                                                    \
        std::shared_ptr<arrow::Array> arr;                                   \
        (void)b.Finish(&arr);                                                \
        arrays.push_back(std::move(arr));                                    \
        break;                                                               \
    }

    for (size_t c = 0; c < ncols; c++) {
        switch (types[c].to_physical_type()) {
            case physical_type::BOOL:   BUILD_COL(BooleanBuilder, bool)
            case physical_type::INT8:   BUILD_COL(Int8Builder,    int8_t)
            case physical_type::INT16:  BUILD_COL(Int16Builder,   int16_t)
            case physical_type::INT32:  BUILD_COL(Int32Builder,   int32_t)
            case physical_type::INT64:  BUILD_COL(Int64Builder,   int64_t)
            case physical_type::UINT8:  BUILD_COL(UInt8Builder,   uint8_t)
            case physical_type::UINT16: BUILD_COL(UInt16Builder,  uint16_t)
            case physical_type::UINT32: BUILD_COL(UInt32Builder,  uint32_t)
            case physical_type::UINT64: BUILD_COL(UInt64Builder,  uint64_t)
            case physical_type::FLOAT:  BUILD_COL(FloatBuilder,   float)
            case physical_type::DOUBLE: BUILD_COL(DoubleBuilder,  double)
            case physical_type::STRING: {
                arrow::StringBuilder b;
                for (int64_t r = 0; r < nrows; r++) {
                    auto v = chunk.value(static_cast<uint64_t>(c),
                                        static_cast<uint64_t>(r));
                    if (v.is_null()) (void)b.AppendNull();
                    else             (void)b.Append(v.value<const std::string&>());
                }
                std::shared_ptr<arrow::Array> arr;
                (void)b.Finish(&arr);
                arrays.push_back(std::move(arr));
                break;
            }
            default: {
                arrow::NullBuilder b;
                (void)b.AppendNulls(nrows);
                std::shared_ptr<arrow::Array> arr;
                (void)b.Finish(&arr);
                arrays.push_back(std::move(arr));
                break;
            }
        }
    }

#undef BUILD_COL

    return arrow::RecordBatch::Make(schema, nrows, std::move(arrays));
}