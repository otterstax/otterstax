// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "batch_reader.hpp"

ChunkBatchReader::ChunkBatchReader(std::shared_ptr<arrow::Schema> schema, components::vector::data_chunk_t chunk)
    : schema_ptr_{std::move(schema)}
    , chunk_{std::move(chunk)} {}

arrow::Result<std::shared_ptr<ChunkBatchReader>> ChunkBatchReader::Make(std::shared_ptr<arrow::Schema> schema,
                                                                        components::vector::data_chunk_t chunk) {
    return std::make_shared<ChunkBatchReader>(std::move(schema), std::move(chunk));
}

std::shared_ptr<arrow::Schema> ChunkBatchReader::schema() const { return schema_ptr_; }

arrow::Status ChunkBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    if (used) {
        *out = nullptr;
        return arrow::Status::OK();
    }
    used = true;

    if (chunk_.empty()) {
        *out = nullptr;
        return arrow::Status::OK();
    }

    const auto num_fields = schema_ptr_->num_fields();
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(num_fields);

    // Initialize builders
    for (int i = 0; i < num_fields; i++) {
        ARROW_RETURN_NOT_OK(MakeBuilder(arrow::default_memory_pool(), schema_ptr_->field(i)->type(), &builders[i]));
    }

    // Helper lambda to populate builder
    auto populateBuilder = [](auto* builder, const auto& value, bool is_null) -> arrow::Status {
        return is_null ? builder->AppendNull() : builder->Append(value);
    };

    // Fill builders with row data
    for (size_t i = 0; i < chunk_.column_count(); i++) {
        // field order could be different
        auto index = schema_ptr_->GetFieldIndex(chunk_.data[i].type().alias());
        const auto& field = schema_ptr_->field(index);
        const auto& field_type = field->type();
        for (size_t j = 0; j < chunk_.size(); j++) {
            auto value = chunk_.value(i, j);
            bool is_null = value.is_null();

            switch (field_type->id()) {
                case arrow::Type::BOOL:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::BooleanBuilder*>(builders[index].get()),
                                                        value.value<bool>(),
                                                        is_null));
                    break;
                case arrow::Type::INT8:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::Int8Builder*>(builders[index].get()),
                                                        value.value<int8_t>(),
                                                        is_null));
                    break;
                case arrow::Type::INT16:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::Int16Builder*>(builders[index].get()),
                                                        value.value<int16_t>(),
                                                        is_null));
                    break;
                case arrow::Type::INT32:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::Int32Builder*>(builders[index].get()),
                                                        value.value<int32_t>(),
                                                        is_null));
                    break;
                case arrow::Type::INT64:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::Int64Builder*>(builders[index].get()),
                                                        value.value<int64_t>(),
                                                        is_null));
                    break;
                case arrow::Type::UINT8:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::UInt8Builder*>(builders[index].get()),
                                                        value.value<uint8_t>(),
                                                        is_null));
                    break;
                case arrow::Type::UINT16:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::UInt16Builder*>(builders[index].get()),
                                                        value.value<uint16_t>(),
                                                        is_null));
                    break;
                case arrow::Type::UINT32:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::UInt32Builder*>(builders[index].get()),
                                                        value.value<uint32_t>(),
                                                        is_null));
                    break;
                case arrow::Type::UINT64:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::UInt64Builder*>(builders[index].get()),
                                                        value.value<uint64_t>(),
                                                        is_null));
                    break;
                case arrow::Type::FLOAT:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::FloatBuilder*>(builders[index].get()),
                                                        value.value<float>(),
                                                        is_null));
                    break;
                case arrow::Type::DOUBLE:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::DoubleBuilder*>(builders[index].get()),
                                                        value.value<double>(),
                                                        is_null));
                    break;
                case arrow::Type::STRING:
                    ARROW_RETURN_NOT_OK(populateBuilder(dynamic_cast<arrow::StringBuilder*>(builders[index].get()),
                                                        *value.value<std::string*>(),
                                                        is_null));
                    break;
                default:
                    return arrow::Status::TypeError("Unknown builder type");
            }
        }
    }

    std::vector<std::shared_ptr<arrow::Array>> columns(num_fields);
    for (int i = 0; i < num_fields; i++) {
        ARROW_RETURN_NOT_OK(builders[i]->Finish(&columns[i]));
    }

    *out = arrow::RecordBatch::Make(schema_ptr_, chunk_.size(), columns);
    return arrow::Status::OK();
}
