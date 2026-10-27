// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/io/type_fwd.h>

#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>

namespace tsl {

// Writes a cursor's run of chunks, in order, as one CSV file at `path`. Columns must be named
// flat scalars (writable_columns.hpp); errors are owned by `res`. A failed write leaves `path`
// as it was (write_local_file).
[[nodiscard]] core::result_wrapper_t<bool>
chunk_to_csv(std::pmr::memory_resource* res,
             const std::pmr::vector<components::vector::data_chunk_t>& chunks,
             std::string_view path);

// The same rows written to `sink`, which stays open: committing it (Close) or
// discarding it (Abort — an S3 upload must never publish a partial object) is the
// caller's decision. A null sink is invalid_parameter.
[[nodiscard]] core::result_wrapper_t<bool>
chunk_to_csv(std::pmr::memory_resource* res,
             const std::pmr::vector<components::vector::data_chunk_t>& chunks,
             const std::shared_ptr<arrow::io::OutputStream>& sink);

} // namespace tsl
