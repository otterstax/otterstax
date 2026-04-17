// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/vector/data_chunk.hpp>
#include <string>

namespace tsl {

void chunk_to_parquet(const components::vector::data_chunk_t& chunk, const std::string& path);

} // namespace tsl
