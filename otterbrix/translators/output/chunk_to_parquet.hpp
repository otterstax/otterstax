// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/io/type_fwd.h>

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>

namespace tsl {

// The most rows one parquet row group holds. Sized for readers (fewer, larger
// groups compress better and need less footer metadata), not cut at the engine's
// DEFAULT_VECTOR_CAPACITY-row chunks.
inline constexpr int64_t parquet_row_group_rows = 128 * 1024;

// Writes a cursor's run of chunks, in order, as one parquet file at `path`: snappy, row groups
// of up to parquet_row_group_rows. Columns must be named flat scalars (writable_columns.hpp);
// errors are owned by `res`. A failed write leaves `path` as it was (write_local_file).
[[nodiscard]] core::result_wrapper_t<bool>
chunk_to_parquet(std::pmr::memory_resource* res,
                 const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                 const std::string& path);

// The same rows written to `sink`, which stays open: committing it (Close) or
// discarding it (Abort — an S3 upload must never publish a partial object) is the
// caller's decision. A null sink is invalid_parameter.
[[nodiscard]] core::result_wrapper_t<bool>
chunk_to_parquet(std::pmr::memory_resource* res,
                 const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                 const std::shared_ptr<arrow::io::OutputStream>& sink);

} // namespace tsl
