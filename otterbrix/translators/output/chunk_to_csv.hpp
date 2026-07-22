// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/vector/data_chunk.hpp>

#include <memory_resource>
#include <string>

namespace tsl {

void chunk_to_csv(const components::vector::data_chunk_t& chunk, const std::string& path);

// Multi-chunk overload: b1/b2 cursors return a result as a vector of <=1024-row
// chunks (never combined). Writes every chunk into a single output file.
void chunk_to_csv(const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                  const std::string& path);

} // namespace tsl
