// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "result_encoder.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/vector/arrow/arrow_converter.hpp> // to_arrow_schema, to_arrow_array

#include <arrow/buffer.h>       // arrow::Buffer (full type for data()/size())
#include <arrow/c/bridge.h>     // ImportSchema, ImportRecordBatch
#include <arrow/io/memory.h>    // BufferOutputStream
#include <arrow/ipc/writer.h>   // MakeStreamWriter
#include <arrow/record_batch.h> // arrow::RecordBatch (full type for num_rows())
#include <arrow/result.h>       // arrow::Result
#include <arrow/status.h>       // Status

#include <string>
#include <string_view>
#include <utility>

namespace frontend::spark {

    namespace {

        namespace ct = components::types;
        namespace cv = components::vector;
        namespace ca = components::vector::arrow;

        using encode_result = core::result_wrapper_t<EncodedBatch>;

        // Spark (and Arrow-for-Spark) has no unsigned integer types — but the engine
        // produces them (e.g. COUNT() yields UBIGINT). Map each to the SAME-SIZE signed
        // type: the column buffers are bit-identical, so re-tagging the Arrow schema and
        // letting ImportRecordBatch reinterpret the buffers preserves the values (all
        // non-negative aggregate results, well within the signed range).
        ct::logical_type spark_signed_type(ct::logical_type t) {
            switch (t) {
                case ct::logical_type::UTINYINT:
                    return ct::logical_type::TINYINT;
                case ct::logical_type::USMALLINT:
                    return ct::logical_type::SMALLINT;
                case ct::logical_type::UINTEGER:
                    return ct::logical_type::INTEGER;
                case ct::logical_type::UBIGINT:
                    return ct::logical_type::BIGINT;
                default:
                    return t;
            }
        }

        encode_result make_error(core::error_code_t code, std::string_view what, std::pmr::memory_resource* resource) {
            return encode_result{core::error_t{code, std::pmr::string{what.data(), what.size(), resource}}};
        }

        // An Arrow failure while building the stream: the encoder's conversion_failure.
        encode_result arrow_error(const arrow::Status& status, std::pmr::memory_resource* resource) {
            return make_error(core::error_code_t::conversion_failure, status.ToString(), resource);
        }

        // Whether the otterbrix C ABI converters carry a column of this type. Both throw
        // on a type they do not map — to_arrow_schema's format switch and the array
        // appender's per-type dispatch — so a column is checked against the types both
        // accept before either runs. MAP is left out: its appender throws on a NULL key,
        // which only the data can tell. Recursive over the nested types.
        bool arrow_encodable(const ct::complex_logical_type& type) {
            switch (type.type()) {
                case ct::logical_type::BOOLEAN:
                case ct::logical_type::TINYINT:
                case ct::logical_type::SMALLINT:
                case ct::logical_type::INTEGER:
                case ct::logical_type::BIGINT:
                case ct::logical_type::UTINYINT:
                case ct::logical_type::USMALLINT:
                case ct::logical_type::UINTEGER:
                case ct::logical_type::UBIGINT:
                case ct::logical_type::FLOAT:
                case ct::logical_type::DOUBLE:
                case ct::logical_type::STRING_LITERAL:
                case ct::logical_type::DATE:
                case ct::logical_type::TIME:
                case ct::logical_type::TIMESTAMP:
                case ct::logical_type::TIMESTAMP_TZ:
                case ct::logical_type::INTERVAL:
                    return true;
                case ct::logical_type::DECIMAL: {
                    // The appender reads a DECIMAL at the width its precision is stored in.
                    const ct::physical_type stored = type.to_physical_type();
                    return stored == ct::physical_type::INT16 || stored == ct::physical_type::INT32 ||
                           stored == ct::physical_type::INT64 || stored == ct::physical_type::INT128;
                }
                case ct::logical_type::LIST:
                case ct::logical_type::ARRAY:
                    return arrow_encodable(type.child_type());
                case ct::logical_type::STRUCT:
                    for (const auto& child : type.child_types()) {
                        if (!arrow_encodable(child)) {
                            return false;
                        }
                    }
                    return true;
                default:
                    return false;
            }
        }

        // RAII for the Arrow C ABI structs. The otterbrix converters and Arrow's
        // Import* helpers install a `release` callback on these structs. Import* takes
        // ownership on success (invoking release and nulling the callback); on failure
        // or exception the owner is still us. A guard that releases only when the
        // callback is still set therefore frees exactly once in every path.
        struct c_schema_guard {
            ArrowSchema value{};
            ~c_schema_guard() {
                if (value.release != nullptr) {
                    value.release(&value);
                }
            }
        };

        struct c_array_guard {
            ArrowArray value{};
            ~c_array_guard() {
                if (value.release != nullptr) {
                    value.release(&value);
                }
            }
        };

    } // namespace

    core::result_wrapper_t<EncodedBatch> encode_arrow_batch(const ct::complex_logical_type& schema,
                                                            const cv::data_chunk_t& chunk,
                                                            int64_t start_offset,
                                                            std::pmr::memory_resource* resource) {
        OTX_ZONE_N("spark::encode_arrow_batch");
        c_schema_guard schema_guard;
        c_array_guard array_guard;

        // otterbrix's to_arrow_schema derives each Arrow field name via
        // complex_logical_type::alias(), which dereferences the type's extension_
        // with NO null guard — an unaliased (leaf) type therefore crashes it.
        // Ensure every field is named: prefer the authoritative struct field names
        // carried by `schema`, else a stable "colN" fallback.
        std::pmr::vector<ct::complex_logical_type> field_types(chunk.types(), resource);
        const bool named_schema =
            schema.type() == ct::logical_type::STRUCT && schema.child_types().size() == field_types.size();
        for (size_t i = 0; i < field_types.size(); ++i) {
            // Resolve the field name BEFORE any type remap below (rebuilding the
            // type drops the alias): authoritative struct names win, else keep the
            // chunk's own alias, else a stable colN.
            std::string name;
            if (named_schema && schema.child_types()[i].has_alias()) {
                name = schema.child_types()[i].alias();
            } else if (field_types[i].has_alias()) {
                name = field_types[i].alias();
            } else {
                name = "col" + std::to_string(i);
            }

            // A column the converters below cannot carry is refused here, before
            // either of them could throw on it.
            if (!arrow_encodable(field_types[i])) {
                return make_error(core::error_code_t::conversion_failure,
                                  "column '" + name + "' has a type the Spark Arrow batch cannot carry (logical type " +
                                      std::to_string(static_cast<unsigned>(field_types[i].type())) + ")",
                                  resource);
            }

            // Re-tag unsigned integer columns as signed so pyspark accepts the
            // schema (Spark has no unsigned types); the buffers are unchanged.
            const ct::logical_type signed_lt = spark_signed_type(field_types[i].type());
            if (signed_lt != field_types[i].type()) {
                field_types[i] = ct::complex_logical_type{signed_lt};
            }
            field_types[i].set_alias(name);
        }

        // Otterbrix C ABI converters. to_arrow_array's signature takes a
        // non-const reference even though it only reads from the chunk (it
        // forwards into non-const vector_t accessors); casting away const here
        // is therefore safe.
        ca::to_arrow_schema(&schema_guard.value, field_types);
        ca::to_arrow_array(const_cast<cv::data_chunk_t&>(chunk), &array_guard.value);

        // ImportSchema / ImportRecordBatch consume their C structs on success
        // (the guards become no-ops); on failure they leave them for the guards.
        auto imported_schema = arrow::ImportSchema(&schema_guard.value);
        if (!imported_schema.ok()) {
            return arrow_error(imported_schema.status(), resource);
        }
        auto schema_ptr = imported_schema.MoveValueUnsafe();
        auto imported_batch = arrow::ImportRecordBatch(&array_guard.value, schema_ptr);
        if (!imported_batch.ok()) {
            return arrow_error(imported_batch.status(), resource);
        }
        auto batch = imported_batch.MoveValueUnsafe();

        // MakeStreamWriter (not NewStreamWriter) writes the Schema message on
        // the first write and the EOS marker on Close(), yielding a complete
        // IPC *stream* as required by arrow_batch.data.
        auto created_sink = arrow::io::BufferOutputStream::Create();
        if (!created_sink.ok()) {
            return arrow_error(created_sink.status(), resource);
        }
        auto sink = created_sink.MoveValueUnsafe();
        auto created_writer = arrow::ipc::MakeStreamWriter(sink.get(), schema_ptr);
        if (!created_writer.ok()) {
            return arrow_error(created_writer.status(), resource);
        }
        auto writer = created_writer.MoveValueUnsafe();

        arrow::Status st = writer->WriteRecordBatch(*batch);
        if (!st.ok()) {
            return arrow_error(st, resource);
        }
        st = writer->Close();
        if (!st.ok()) {
            return arrow_error(st, resource);
        }

        auto finished = sink->Finish();
        if (!finished.ok()) {
            return arrow_error(finished.status(), resource);
        }
        auto buffer = finished.MoveValueUnsafe();

        return EncodedBatch{
            std::string(reinterpret_cast<const char*>(buffer->data()), static_cast<size_t>(buffer->size())),
            batch->num_rows(),
            start_offset};
    }

} // namespace frontend::spark
