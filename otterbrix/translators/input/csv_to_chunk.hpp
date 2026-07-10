// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include <otterbrix/otterbrix.hpp>
#include <memory_resource>
#include <string>

namespace tsl {

components::vector::data_chunk_t
csv_to_chunk(std::pmr::memory_resource* res, const std::string& file_path,
             char delimiter = ',', bool has_header = true);

components::vector::data_chunk_t
csv_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size,
             char delimiter = ',', bool has_header = true);

} // namespace tsl
