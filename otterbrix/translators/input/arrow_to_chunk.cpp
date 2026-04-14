// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "arrow_to_chunk.hpp"

#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/type.h>

namespace tsl {

    namespace impl {

        types::logical_type arrow_type_to_logical(const std::shared_ptr<arrow::DataType>& dt) {
            switch (dt->id()) {
                case arrow::Type::BOOL:
                    return types::logical_type::BOOLEAN;
                case arrow::Type::INT8:
                    return types::logical_type::TINYINT;
                case arrow::Type::INT16:
                    return types::logical_type::SMALLINT;
                case arrow::Type::INT32:
                    return types::logical_type::INTEGER;
                case arrow::Type::INT64:
                    return types::logical_type::BIGINT;
                case arrow::Type::UINT8:
                    return types::logical_type::UTINYINT;
                case arrow::Type::UINT16:
                    return types::logical_type::USMALLINT;
                case arrow::Type::UINT32:
                    return types::logical_type::UINTEGER;
                case arrow::Type::UINT64:
                    return types::logical_type::UBIGINT;
                case arrow::Type::FLOAT:
                case arrow::Type::HALF_FLOAT:
                    return types::logical_type::FLOAT;
                case arrow::Type::DOUBLE:
                    return types::logical_type::DOUBLE;
                case arrow::Type::STRING:
                case arrow::Type::LARGE_STRING:
                case arrow::Type::BINARY:
                case arrow::Type::LARGE_BINARY:
                case arrow::Type::FIXED_SIZE_BINARY:
                case arrow::Type::DATE32:
                case arrow::Type::DATE64:
                case arrow::Type::TIMESTAMP:
                case arrow::Type::TIME32:
                case arrow::Type::TIME64:
                    return types::logical_type::STRING_LITERAL;
                default:
                    return types::logical_type::STRING_LITERAL;
            }
        }

        void set_value_from_array(data_chunk_t& chunk,
                                  const std::shared_ptr<arrow::Array>& array,
                                  size_t col,
                                  size_t row,
                                  types::logical_type logical_t) {
            if (array->IsNull(row)) {
                chunk.set_value(col, row, types::logical_value_t{nullptr});
                return;
            }

            switch (logical_t) {
                case types::logical_type::BOOLEAN: {
                    auto arr = std::static_pointer_cast<arrow::BooleanArray>(array);
                    chunk.set_value(col, row, types::logical_value_t{arr->Value(row)});
                    break;
                }
                case types::logical_type::TINYINT: {
                    auto arr = std::static_pointer_cast<arrow::Int8Array>(array);
                    chunk.set_value(col, row, types::logical_value_t{static_cast<int8_t>(arr->Value(row))});
                    break;
                }
                case types::logical_type::SMALLINT: {
                    auto arr = std::static_pointer_cast<arrow::Int16Array>(array);
                    chunk.set_value(col, row, types::logical_value_t{static_cast<int16_t>(arr->Value(row))});
                    break;
                }
                case types::logical_type::INTEGER: {
                    auto arr = std::static_pointer_cast<arrow::Int32Array>(array);
                    chunk.set_value(col, row, types::logical_value_t{static_cast<int32_t>(arr->Value(row))});
                    break;
                }
                case types::logical_type::BIGINT: {
                    auto arr = std::static_pointer_cast<arrow::Int64Array>(array);
                    chunk.set_value(col, row, types::logical_value_t{static_cast<int64_t>(arr->Value(row))});
                    break;
                }
                case types::logical_type::UTINYINT: {
                    auto arr = std::static_pointer_cast<arrow::UInt8Array>(array);
                    chunk.set_value(col, row, types::logical_value_t{static_cast<uint8_t>(arr->Value(row))});
                    break;
                }
                case types::logical_type::USMALLINT: {
                    auto arr = std::static_pointer_cast<arrow::UInt16Array>(array);
                    chunk.set_value(col, row, types::logical_value_t{static_cast<uint16_t>(arr->Value(row))});
                    break;
                }
                case types::logical_type::UINTEGER: {
                    auto arr = std::static_pointer_cast<arrow::UInt32Array>(array);
                    chunk.set_value(col, row, types::logical_value_t{static_cast<uint32_t>(arr->Value(row))});
                    break;
                }
                case types::logical_type::UBIGINT: {
                    auto arr = std::static_pointer_cast<arrow::UInt64Array>(array);
                    chunk.set_value(col, row, types::logical_value_t{static_cast<uint64_t>(arr->Value(row))});
                    break;
                }
                case types::logical_type::FLOAT: {
                    if (array->type_id() == arrow::Type::FLOAT) {
                        auto arr = std::static_pointer_cast<arrow::FloatArray>(array);
                        chunk.set_value(col, row, types::logical_value_t{static_cast<float>(arr->Value(row))});
                    } else {
                        auto arr = std::static_pointer_cast<arrow::HalfFloatArray>(array);
                        chunk.set_value(col, row, types::logical_value_t{static_cast<float>(arr->Value(row))});
                    }
                    break;
                }
                case types::logical_type::DOUBLE: {
                    auto arr = std::static_pointer_cast<arrow::DoubleArray>(array);
                    chunk.set_value(col, row, types::logical_value_t{static_cast<double>(arr->Value(row))});
                    break;
                }
                case types::logical_type::STRING_LITERAL: {
                    switch (array->type_id()) {
                        case arrow::Type::STRING: {
                            auto arr = std::static_pointer_cast<arrow::StringArray>(array);
                            chunk.set_value(col, row, types::logical_value_t{std::string(arr->GetView(row))});
                            break;
                        }
                        case arrow::Type::LARGE_STRING: {
                            auto arr = std::static_pointer_cast<arrow::LargeStringArray>(array);
                            chunk.set_value(col, row, types::logical_value_t{std::string(arr->GetView(row))});
                            break;
                        }
                        case arrow::Type::BINARY: {
                            auto arr = std::static_pointer_cast<arrow::BinaryArray>(array);
                            chunk.set_value(col, row, types::logical_value_t{std::string(arr->GetView(row))});
                            break;
                        }
                        default: {
                            // Date, Timestamp, etc. — convert via ToString
                            auto scalar = *array->GetScalar(row);
                            chunk.set_value(col, row, types::logical_value_t{scalar->ToString()});
                            break;
                        }
                    }
                    break;
                }
                default:
                    chunk.set_value(col, row, types::logical_value_t{nullptr});
                    break;
            }
        }

    } // namespace impl

    types::complex_logical_type arrow_schema_to_struct(const std::shared_ptr<arrow::Schema>& schema) {
        std::vector<types::complex_logical_type> fields;
        fields.reserve(schema->num_fields());

        for (int i = 0; i < schema->num_fields(); ++i) {
            const auto& field = schema->field(i);
            auto logical_t = impl::arrow_type_to_logical(field->type());
            fields.emplace_back(logical_t, field->name().c_str());
        }

        return types::complex_logical_type::create_struct(std::move(fields));
    }

    data_chunk_t arrow_to_chunk(std::pmr::memory_resource* res,
                                const std::shared_ptr<arrow::RecordBatch>& batch) {
        const int64_t nrows = batch->num_rows();
        const int ncols = batch->num_columns();

        std::pmr::vector<types::complex_logical_type> types(res);
        types.reserve(ncols);

        std::vector<types::logical_type> logical_types;
        logical_types.reserve(ncols);

        for (int col = 0; col < ncols; ++col) {
            auto logical_t = impl::arrow_type_to_logical(batch->column(col)->type());
            types.emplace_back(logical_t, batch->schema()->field(col)->name().c_str());
            logical_types.push_back(logical_t);
        }

        data_chunk_t chunk(res, types, static_cast<size_t>(nrows));
        chunk.set_cardinality(static_cast<size_t>(nrows));

        for (int col = 0; col < ncols; ++col) {
            const auto& array = batch->column(col);
            for (int64_t row = 0; row < nrows; ++row) {
                impl::set_value_from_array(chunk, array, col, row, logical_types[col]);
            }
        }

        return chunk;
    }

    data_chunk_t arrow_to_chunk(std::pmr::memory_resource* res,
                                const std::shared_ptr<arrow::Table>& table) {
        auto result = table->CombineChunks();
        if (!result.ok()) {
            throw std::runtime_error("arrow_to_chunk: CombineChunks failed: " + result.status().ToString());
        }
        auto combined = *result;

        const int64_t nrows = combined->num_rows();
        const int ncols = combined->num_columns();

        std::pmr::vector<types::complex_logical_type> types(res);
        types.reserve(ncols);

        std::vector<types::logical_type> logical_types;
        logical_types.reserve(ncols);

        for (int col = 0; col < ncols; ++col) {
            auto logical_t = impl::arrow_type_to_logical(combined->column(col)->type());
            types.emplace_back(logical_t, combined->schema()->field(col)->name().c_str());
            logical_types.push_back(logical_t);
        }

        data_chunk_t chunk(res, types, static_cast<size_t>(nrows));
        chunk.set_cardinality(static_cast<size_t>(nrows));

        for (int col = 0; col < ncols; ++col) {
            const auto& chunked = combined->column(col);
            // After CombineChunks there's exactly one chunk per column
            const auto& array = chunked->chunk(0);
            for (int64_t row = 0; row < nrows; ++row) {
                impl::set_value_from_array(chunk, array, col, row, logical_types[col]);
            }
        }

        return chunk;
    }

} // namespace tsl
