// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "arrow_to_chunk.hpp"

namespace tsl {

    // Read NDJSON (newline-delimited JSON) from a file on disk.
    // Also supports JSON array format [{"a":1}, {"a":2}] via preprocessing.
    data_chunk_t json_to_chunk(std::pmr::memory_resource* res,
                               const std::string& file_path);

    // Read NDJSON from an in-memory buffer
    data_chunk_t json_to_chunk(std::pmr::memory_resource* res,
                               const uint8_t* data, size_t size);

} // namespace tsl
