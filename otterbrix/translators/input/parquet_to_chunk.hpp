// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include <otterbrix/otterbrix.hpp>
#include <core/result_wrapper.hpp>
#include <memory_resource>
#include <string>

namespace tsl {

// Error messages are owned by `res`; a file that cannot be opened is io_error, a
// body the parquet reader rejects is conversion_failure, an empty buffer is
// invalid_parameter.
core::result_wrapper_t<components::vector::data_chunk_t>
parquet_to_chunk(std::pmr::memory_resource* res, const std::string& file_path);

core::result_wrapper_t<components::vector::data_chunk_t>
parquet_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size);

// Schema only, without reading the row groups.
core::result_wrapper_t<components::types::complex_logical_type>
parquet_to_struct(std::pmr::memory_resource* res, const std::string& file_path);

} // namespace tsl
