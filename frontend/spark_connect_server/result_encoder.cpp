// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "result_encoder.hpp"

#include <components/vector/arrow/arrow_converter.hpp>  // to_arrow_schema, to_arrow_array

#include <arrow/buffer.h>       // arrow::Buffer (full type for data()/size())
#include <arrow/c/bridge.h>     // ImportSchema, ImportRecordBatch
#include <arrow/io/memory.h>    // BufferOutputStream
#include <arrow/ipc/writer.h>   // MakeStreamWriter
#include <arrow/record_batch.h> // arrow::RecordBatch (full type for num_rows())
#include <arrow/result.h>       // Result::ValueOrDie
#include <arrow/status.h>       // Status

#include <exception>
#include <string>
#include <string_view>
#include <utility>

namespace frontend::spark {

namespace {

namespace ct = components::types;
namespace cv = components::vector;
namespace ca = components::vector::arrow;

using encode_result = core::result_wrapper_t<EncodedBatch>;

encode_result make_error(core::error_code_t code,
                         std::string_view what,
                         std::pmr::memory_resource* resource) {
    return encode_result{core::error_t{code, std::pmr::string{what.data(), what.size(), resource}}};
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

}  // namespace

core::result_wrapper_t<EncodedBatch>
encode_arrow_batch(const ct::complex_logical_type& schema,
                   const cv::data_chunk_t& chunk,
                   int64_t start_offset,
                   std::pmr::memory_resource* resource) {
    try {
        c_schema_guard schema_guard;
        c_array_guard array_guard;

        // otterbrix's to_arrow_schema derives each Arrow field name via
        // complex_logical_type::alias(), which dereferences the type's extension_
        // with NO null guard — an unaliased (leaf) type therefore crashes it.
        // Ensure every field is named: prefer the authoritative struct field names
        // carried by `schema`, else a stable "colN" fallback.
        std::pmr::vector<ct::complex_logical_type> field_types(chunk.types(), resource);
        const bool named_schema = schema.type() == ct::logical_type::STRUCT &&
                                  schema.child_types().size() == field_types.size();
        for (size_t i = 0; i < field_types.size(); ++i) {
            if (named_schema && schema.child_types()[i].has_alias()) {
                field_types[i].set_alias(schema.child_types()[i].alias());
            } else if (!field_types[i].has_alias()) {
                field_types[i].set_alias("col" + std::to_string(i));
            }
        }

        // Otterbrix C ABI converters. to_arrow_array's signature takes a
        // non-const reference even though it only reads from the chunk (it
        // forwards into non-const vector_t accessors); casting away const here
        // is therefore safe.
        ca::to_arrow_schema(&schema_guard.value, field_types);
        ca::to_arrow_array(const_cast<cv::data_chunk_t&>(chunk), &array_guard.value);

        // ImportSchema / ImportRecordBatch consume their C structs on success
        // (the guards become no-ops); on failure they leave them for the guards.
        auto schema_ptr = arrow::ImportSchema(&schema_guard.value).ValueOrDie();
        auto batch = arrow::ImportRecordBatch(&array_guard.value, schema_ptr).ValueOrDie();

        // MakeStreamWriter (not NewStreamWriter) writes the Schema message on
        // the first write and the EOS marker on Close(), yielding a complete
        // IPC *stream* as required by arrow_batch.data.
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(sink.get(), schema_ptr).ValueOrDie();

        arrow::Status st = writer->WriteRecordBatch(*batch);
        if (!st.ok()) {
            return make_error(core::error_code_t::conversion_failure, st.ToString(), resource);
        }
        st = writer->Close();
        if (!st.ok()) {
            return make_error(core::error_code_t::conversion_failure, st.ToString(), resource);
        }

        auto buffer = sink->Finish().ValueOrDie();

        return EncodedBatch{
            std::string(reinterpret_cast<const char*>(buffer->data()),
                        static_cast<size_t>(buffer->size())),
            batch->num_rows(),
            start_offset};
    } catch (const std::exception& e) {
        return make_error(core::error_code_t::conversion_failure, e.what(), resource);
    } catch (...) {
        return make_error(core::error_code_t::conversion_failure,
                          "arrow batch encoding failed: unknown error",
                          resource);
    }
}

}  // namespace frontend::spark
