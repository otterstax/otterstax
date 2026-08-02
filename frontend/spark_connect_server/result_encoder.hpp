// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Encodes a shared_session_payload result (data_chunk + schema) into a single
// complete Arrow IPC *stream* (Schema message + RecordBatch message + EOS
// marker) suitable for ExecutePlanResponse.arrow_batch.data.

#include <components/types/types.hpp>        // components::types::complex_logical_type
#include <components/vector/data_chunk.hpp>  // components::vector::data_chunk_t
#include <core/result_wrapper.hpp>           // core::result_wrapper_t

#include <cstdint>          // int64_t
#include <memory_resource>  // std::pmr::memory_resource
#include <string>           // std::string

namespace frontend::spark {

// One Spark Connect ArrowBatch payload.
struct EncodedBatch {
    std::string data;            // complete IPC stream bytes (Schema + RecordBatch + EOS)
    int64_t row_count = 0;       // rows in `data` (== batch->num_rows())
    int64_t start_offset = 0;    // optional row offset carried from the caller
};

// Converts a session_payload's chunk + schema into ArrowBatch IPC stream bytes.
//
// The function never throws: every otterbrix / Arrow failure is mapped to a
// `core::error_t` (usually `conversion_failure`). `resource` backs the
// `std::pmr::string` of any emitted error.
//
// `schema` is accepted to mirror `session_payload`'s fields, but the Arrow
// schema is derived from `chunk.types()` (the authoritative per-column types of
// the data being serialised).
core::result_wrapper_t<EncodedBatch>
encode_arrow_batch(const components::types::complex_logical_type& schema,
                   const components::vector::data_chunk_t& chunk,
                   int64_t start_offset,
                   std::pmr::memory_resource* resource);

}  // namespace frontend::spark
