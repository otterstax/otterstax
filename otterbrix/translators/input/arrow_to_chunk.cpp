// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "arrow_to_chunk.hpp"

#include <arrow/array.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

using namespace components::vector;
using namespace components::types;

namespace tsl {

namespace impl {

    static complex_logical_type arrow_field_to_type(const arrow::Field& field) {
        switch (field.type()->id()) {
            case arrow::Type::BOOL:    return {logical_type::BOOLEAN,       field.name().c_str()};
            case arrow::Type::INT8:    return {logical_type::TINYINT,       field.name().c_str()};
            case arrow::Type::INT16:   return {logical_type::SMALLINT,      field.name().c_str()};
            case arrow::Type::INT32:   return {logical_type::INTEGER,       field.name().c_str()};
            case arrow::Type::INT64:   return {logical_type::BIGINT,        field.name().c_str()};
            case arrow::Type::UINT8:   return {logical_type::UTINYINT,      field.name().c_str()};
            case arrow::Type::UINT16:  return {logical_type::USMALLINT,     field.name().c_str()};
            case arrow::Type::UINT32:  return {logical_type::UINTEGER,      field.name().c_str()};
            case arrow::Type::UINT64:  return {logical_type::UBIGINT,       field.name().c_str()};
            case arrow::Type::FLOAT:   return {logical_type::FLOAT,         field.name().c_str()};
            case arrow::Type::DOUBLE:  return {logical_type::DOUBLE,        field.name().c_str()};
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING:
            case arrow::Type::DATE32:
            case arrow::Type::DATE64:
            case arrow::Type::TIMESTAMP:
            default:                   return {logical_type::STRING_LITERAL, field.name().c_str()};
        }
    }

    static void set_column_value(data_chunk_t& chunk,
                                 size_t col, size_t row,
                                 const arrow::Array& arr,
                                 const complex_logical_type& type) {
        if (arr.IsNull(static_cast<int64_t>(row))) {
            chunk.set_value(col, row, logical_value_t{chunk.resource(), nullptr});
            return;
        }
        switch (type.type()) {
            case logical_type::BOOLEAN:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<bool>(static_cast<const arrow::BooleanArray&>(arr).Value(row))});
                break;
            case logical_type::TINYINT:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<int8_t>(static_cast<const arrow::Int8Array&>(arr).Value(row))});
                break;
            case logical_type::SMALLINT:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<int16_t>(static_cast<const arrow::Int16Array&>(arr).Value(row))});
                break;
            case logical_type::INTEGER:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<int32_t>(static_cast<const arrow::Int32Array&>(arr).Value(row))});
                break;
            case logical_type::BIGINT:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<int64_t>(static_cast<const arrow::Int64Array&>(arr).Value(row))});
                break;
            case logical_type::UTINYINT:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<uint8_t>(static_cast<const arrow::UInt8Array&>(arr).Value(row))});
                break;
            case logical_type::USMALLINT:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<uint16_t>(static_cast<const arrow::UInt16Array&>(arr).Value(row))});
                break;
            case logical_type::UINTEGER:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<uint32_t>(static_cast<const arrow::UInt32Array&>(arr).Value(row))});
                break;
            case logical_type::UBIGINT:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<uint64_t>(static_cast<const arrow::UInt64Array&>(arr).Value(row))});
                break;
            case logical_type::FLOAT:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<float>(static_cast<const arrow::FloatArray&>(arr).Value(row))});
                break;
            case logical_type::DOUBLE:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        static_cast<double>(static_cast<const arrow::DoubleArray&>(arr).Value(row))});
                break;
            case logical_type::STRING_LITERAL:
            default:
                chunk.set_value(col, row,
                    logical_value_t{chunk.resource(),
                        std::string(static_cast<const arrow::StringArray&>(arr).GetString(row))});
                break;
        }
    }

} // namespace impl

data_chunk_t arrow_to_chunk(std::pmr::memory_resource* res,
                             const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) {
        return data_chunk_t(res, {}, 0);
    }
    const int ncols = batch->num_columns();
    const int64_t nrows = batch->num_rows();

    std::pmr::vector<complex_logical_type> types(res);
    types.reserve(ncols);
    for (int c = 0; c < ncols; c++) {
        types.emplace_back(impl::arrow_field_to_type(*batch->schema()->field(c)));
    }

    data_chunk_t chunk(res, types, static_cast<size_t>(nrows));
    chunk.set_cardinality(static_cast<size_t>(nrows));

    for (int c = 0; c < ncols; c++) {
        const auto& arr = *batch->column(c);
        for (int64_t r = 0; r < nrows; r++) {
            impl::set_column_value(chunk, static_cast<size_t>(c),
                                   static_cast<size_t>(r), arr, types[c]);
        }
    }
    return chunk;
}

complex_logical_type arrow_schema_to_struct(std::pmr::memory_resource* res,
                                            const std::shared_ptr<arrow::Schema>& schema) {
    std::pmr::vector<complex_logical_type> fields(res);
    fields.reserve(schema->num_fields());
    for (int i = 0; i < schema->num_fields(); i++) {
        fields.emplace_back(impl::arrow_field_to_type(*schema->field(i)));
    }
    return complex_logical_type::create_struct("", std::move(fields));
}

} // namespace tsl
