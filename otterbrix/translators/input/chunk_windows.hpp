// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/vector/data_chunk.hpp>

#include <memory_resource>

namespace tsl {

    // The engine's chunk contract: a data_chunk_t it is handed — the rows of an insert (the file /
    // s3 load) — holds at most DEFAULT_VECTOR_CAPACITY rows; a wider result is a run of such chunks
    // sharing one column shape. The input translators build a whole file as ONE chunk, so it is cut
    // into that run where it enters the engine. (A remote table's rows are handed out in windows by
    // the federation's operators instead, otterbrix/federation.)
    //
    // A chunk within the bound is moved in as the only element. So is a column-less chunk,
    // whatever its cardinality: a zero-column window is the engine pipeline's drain sentinel.
    //
    // A wider chunk becomes consecutive windows of DEFAULT_VECTOR_CAPACITY rows, the last holding
    // the rest. Each window is data_chunk_t::partial_copy of its row range: it references the
    // source column buffers (reference-counted, so they live as long as the last window) rather
    // than copying rows. The vector and each window's own bookkeeping are allocated on `resource`.
    std::pmr::vector<components::vector::data_chunk_t> split_to_capacity(std::pmr::memory_resource* resource,
                                                                         components::vector::data_chunk_t chunk);

} // namespace tsl
