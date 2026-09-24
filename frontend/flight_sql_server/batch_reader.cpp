// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "batch_reader.hpp"

#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "utility/tracy_profiler.hpp"

#include <memory>
#include <string>

namespace {

    using components::types::logical_value_t;

    // Every Append goes through here so that the value is never read before the
    // NULL check: for a NULL string cell value<std::string*>() has nothing to
    // dereference.
    template<typename Builder, typename Value>
    arrow::Status append_scalar(arrow::ArrayBuilder* builder, const logical_value_t& value) {
        auto* typed = static_cast<Builder*>(builder);
        if (value.is_null()) {
            return typed->AppendNull();
        }
        return typed->Append(value.template value<Value>());
    }

    arrow::Status append_string(arrow::ArrayBuilder* builder, const logical_value_t& value) {
        auto* typed = static_cast<arrow::StringBuilder*>(builder);
        if (value.is_null()) {
            return typed->AppendNull();
        }
        return typed->Append(*value.value<std::string*>());
    }

    // decimal128 is the field type of both a DECIMAL column (its own precision and scale) and a
    // HUGEINT one (precision 38, scale 0, Arrow having no 128-bit integer type). The value is
    // the unscaled integer the engine stored; the scale rides in the field type, so nothing is
    // rescaled here. Building it — and refusing a payload decimal128 cannot declare — is the
    // same job chunk_to_record_batch does, so it is the same function.
    arrow::Status append_decimal(std::pmr::memory_resource* res,
                                 const std::shared_ptr<arrow::Field>& field,
                                 arrow::ArrayBuilder* builder,
                                 const logical_value_t& value) {
        auto* typed = static_cast<arrow::Decimal128Builder*>(builder);
        if (value.is_null()) {
            return typed->AppendNull();
        }
        auto carried = to_arrow_decimal(res, value, "FlightSQL", field->name());
        if (carried.has_error()) {
            return arrow::Status::Invalid(carried.error().what.c_str());
        }
        return typed->Append(carried.value());
    }

    arrow::Status append_cell(std::pmr::memory_resource* res,
                              const std::shared_ptr<arrow::Field>& field,
                              arrow::ArrayBuilder* builder,
                              const logical_value_t& value) {
        switch (field->type()->id()) {
            case arrow::Type::NA:
                // `SELECT NULL` and friends: the column has no value type, every
                // cell is a null of the null type.
                return static_cast<arrow::NullBuilder*>(builder)->AppendNull();
            case arrow::Type::BOOL:
                return append_scalar<arrow::BooleanBuilder, bool>(builder, value);
            case arrow::Type::INT8:
                return append_scalar<arrow::Int8Builder, int8_t>(builder, value);
            case arrow::Type::INT16:
                return append_scalar<arrow::Int16Builder, int16_t>(builder, value);
            case arrow::Type::INT32:
                return append_scalar<arrow::Int32Builder, int32_t>(builder, value);
            case arrow::Type::INT64:
                return append_scalar<arrow::Int64Builder, int64_t>(builder, value);
            case arrow::Type::UINT8:
                return append_scalar<arrow::UInt8Builder, uint8_t>(builder, value);
            case arrow::Type::UINT16:
                return append_scalar<arrow::UInt16Builder, uint16_t>(builder, value);
            case arrow::Type::UINT32:
                return append_scalar<arrow::UInt32Builder, uint32_t>(builder, value);
            case arrow::Type::UINT64:
                return append_scalar<arrow::UInt64Builder, uint64_t>(builder, value);
            case arrow::Type::FLOAT:
                return append_scalar<arrow::FloatBuilder, float>(builder, value);
            case arrow::Type::DOUBLE:
                return append_scalar<arrow::DoubleBuilder, double>(builder, value);
            case arrow::Type::STRING:
                return append_string(builder, value);
            case arrow::Type::DECIMAL128:
                return append_decimal(res, field, builder, value);
            default:
                return arrow::Status::NotImplemented("FlightSQL: column '", field->name(), "' has type ",
                                                     field->type()->ToString(),
                                                     ", which the record batch stream cannot encode");
        }
    }

} // namespace

ChunkBatchReader::ChunkBatchReader(std::shared_ptr<arrow::Schema> schema,
                                   std::pmr::vector<components::vector::data_chunk_t> chunks)
    : schema_ptr_{std::move(schema)}
    , chunks_{std::move(chunks)} {}

arrow::Result<std::shared_ptr<ChunkBatchReader>>
ChunkBatchReader::Make(std::shared_ptr<arrow::Schema> schema,
                       std::pmr::vector<components::vector::data_chunk_t> chunks) {
    if (!schema) {
        return arrow::Status::Invalid("FlightSQL: record batch stream needs a schema");
    }
    return std::make_shared<ChunkBatchReader>(std::move(schema), std::move(chunks));
}

std::shared_ptr<arrow::Schema> ChunkBatchReader::schema() const { return schema_ptr_; }

arrow::Status ChunkBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    OTX_ZONE_N("flight::ChunkBatchReader::ReadNext");
    // Per-batch scratch lives on the chunks' resource; only the Arrow arrays and
    // builders stay on Arrow's pool, which is the Arrow API's own contract.
    std::pmr::memory_resource* resource = chunks_.get_allocator().resource();

    while (next_ < chunks_.size()) {
        auto& cur = chunks_[next_++];
        if (cur.empty()) {
            // skip empty chunks but keep scanning for trailing data
            continue;
        }

        const auto num_fields = schema_ptr_->num_fields();
        std::pmr::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(static_cast<size_t>(num_fields), resource);
        for (int i = 0; i < num_fields; i++) {
            ARROW_RETURN_NOT_OK(MakeBuilder(arrow::default_memory_pool(), schema_ptr_->field(i)->type(), &builders[i]));
        }

        // The schema handed to the client in FlightInfo is the contract: every
        // schema field must be fed from exactly one chunk column, matched by name
        // because the chunk's column order may differ. Names are not unique — an
        // engine JOIN result keeps both key columns — so the n-th chunk column
        // named X feeds the n-th schema field named X.
        std::pmr::vector<uint8_t> field_filled(static_cast<size_t>(num_fields), 0, resource);
        for (size_t i = 0; i < cur.column_count(); i++) {
            const auto& column_type = cur.data[i].type();
            if (!column_type.has_alias()) {
                return arrow::Status::Invalid("FlightSQL: result column ", i,
                                              " has no name and cannot be matched to the schema");
            }
            const std::string& name = column_type.alias();
            int index = -1;
            for (int j = 0; j < num_fields; j++) {
                if (!field_filled[static_cast<size_t>(j)] && schema_ptr_->field(j)->name() == name) {
                    index = j;
                    break;
                }
            }
            if (index == -1) {
                // the chunk carries more than the schema promised; the extra
                // column is not deliverable through this stream
                continue;
            }
            field_filled[static_cast<size_t>(index)] = 1;

            const auto& field = schema_ptr_->field(index);
            auto* builder = builders[static_cast<size_t>(index)].get();
            for (size_t j = 0; j < cur.size(); j++) {
                ARROW_RETURN_NOT_OK(append_cell(resource, field, builder, cur.value(i, j)));
            }
        }

        for (int i = 0; i < num_fields; i++) {
            if (!field_filled[static_cast<size_t>(i)]) {
                // An unfed builder would finish as an empty array next to arrays
                // of cur.size() rows: an invalid batch, not a batch with NULLs.
                return arrow::Status::Invalid("FlightSQL: schema field '", schema_ptr_->field(i)->name(),
                                              "' is missing from the result chunk");
            }
        }

        std::vector<std::shared_ptr<arrow::Array>> columns(static_cast<size_t>(num_fields));
        for (int i = 0; i < num_fields; i++) {
            ARROW_RETURN_NOT_OK(builders[static_cast<size_t>(i)]->Finish(&columns[static_cast<size_t>(i)]));
        }

        *out = arrow::RecordBatch::Make(schema_ptr_, static_cast<int64_t>(cur.size()), std::move(columns));
        return arrow::Status::OK();
    }

    *out = nullptr;
    return arrow::Status::OK();
}
