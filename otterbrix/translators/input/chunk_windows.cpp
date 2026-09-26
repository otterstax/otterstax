// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_windows.hpp"

#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <cstdint>

namespace tsl {

    using components::vector::data_chunk_t;
    using components::vector::DEFAULT_VECTOR_CAPACITY;

    std::pmr::vector<data_chunk_t> split_to_capacity(std::pmr::memory_resource* resource, data_chunk_t chunk) {
        OTX_ZONE_N("tsl::split_to_capacity");
        std::pmr::vector<data_chunk_t> windows(resource);
        const uint64_t rows = chunk.size();
        if (chunk.column_count() == 0 || rows <= DEFAULT_VECTOR_CAPACITY) {
            windows.emplace_back(std::move(chunk));
            return windows;
        }
        windows.reserve((rows + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY);
        for (uint64_t offset = 0; offset < rows; offset += DEFAULT_VECTOR_CAPACITY) {
            const uint64_t count = std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, rows - offset);
            windows.emplace_back(chunk.partial_copy(resource, offset, count));
        }
        return windows;
    }

} // namespace tsl
