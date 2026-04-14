// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "arrow_to_chunk.hpp"

namespace tsl {

    // Read CSV from a file on disk
    data_chunk_t csv_to_chunk(std::pmr::memory_resource* res,
                              const std::string& file_path,
                              char delimiter = ',',
                              bool has_header = true);

    // Read CSV from an in-memory buffer
    data_chunk_t csv_to_chunk(std::pmr::memory_resource* res,
                              const uint8_t* data, size_t size,
                              char delimiter = ',',
                              bool has_header = true);

} // namespace tsl
