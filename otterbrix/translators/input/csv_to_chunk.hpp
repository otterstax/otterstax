// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include <otterbrix/otterbrix.hpp>
#include <core/result_wrapper.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/io/type_fwd.h>

#include <memory>
#include <memory_resource>
#include <string>

namespace tsl {

// Error messages are owned by `res`; a file that cannot be opened is io_error, a
// body Arrow cannot parse is conversion_failure. Arrow's own buffers are allocated
// on `res` too (arrow_memory_pool.hpp), so it must be thread-safe.
core::result_wrapper_t<components::vector::data_chunk_t>
csv_to_chunk(std::pmr::memory_resource* res, const std::string& file_path,
             char delimiter = ',', bool has_header = true);

core::result_wrapper_t<components::vector::data_chunk_t>
csv_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size,
             char delimiter = ',', bool has_header = true);

// Any sequential source — a local file, an S3 object, a buffer. A null input is
// invalid_parameter.
core::result_wrapper_t<components::vector::data_chunk_t>
csv_to_chunk(std::pmr::memory_resource* res, std::shared_ptr<arrow::io::InputStream> input,
             char delimiter = ',', bool has_header = true);

} // namespace tsl
