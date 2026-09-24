// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include <otterbrix/otterbrix.hpp>
#include <core/result_wrapper.hpp>
#include <memory_resource>
#include <string>

namespace tsl {

// Error messages are owned by `res`; a file that cannot be opened is io_error, a
// body Arrow cannot parse is conversion_failure.
core::result_wrapper_t<components::vector::data_chunk_t>
ndjson_to_chunk(std::pmr::memory_resource* res, const std::string& file_path);

// A buffer holding a JSON array of objects is accepted as well and unwrapped
// into one object per line before parsing.
core::result_wrapper_t<components::vector::data_chunk_t>
ndjson_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size);

} // namespace tsl
