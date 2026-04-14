// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "arrow_to_chunk.hpp"

namespace tsl {

    // Read Parquet from a file on disk
    data_chunk_t parquet_to_chunk(std::pmr::memory_resource* res,
                                  const std::string& file_path);

    // Read Parquet from an in-memory buffer (e.g. downloaded from S3)
    data_chunk_t parquet_to_chunk(std::pmr::memory_resource* res,
                                  const uint8_t* data, size_t size);

    // Extract schema from a Parquet file
    types::complex_logical_type parquet_to_struct(const std::string& file_path);

    // Extract schema from an in-memory Parquet buffer
    types::complex_logical_type parquet_to_struct(const uint8_t* data, size_t size);

} // namespace tsl
