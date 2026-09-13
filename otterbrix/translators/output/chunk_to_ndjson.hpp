// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>

namespace tsl {

// Writes every chunk of an engine result (a run of <=DEFAULT_VECTOR_CAPACITY-row
// chunks, never combined) into a single NDJSON file at `path`, one object per
// row, in order. Columns must be named flat scalars (see writable_columns.hpp).
// Error messages are owned by `res`; the value is true on success.
[[nodiscard]] core::result_wrapper_t<bool>
chunk_to_ndjson(std::pmr::memory_resource* res,
                const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                const std::string& path);

} // namespace tsl
