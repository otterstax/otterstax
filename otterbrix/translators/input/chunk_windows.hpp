// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/vector/data_chunk.hpp>

#include <memory_resource>

namespace tsl {

    // The engine's chunk contract: a data_chunk_t it is handed — the raw data substituted for a
    // backend slice (node_raw_data), the rows of an insert — holds at most DEFAULT_VECTOR_CAPACITY
    // rows; a wider result is a run of such chunks sharing one column shape. The input translators
    // build a whole backend result set or file as ONE chunk, so it is cut into that run where it
    // enters the engine.
    //
    // A chunk within the bound is moved in as the only element. So is a column-less chunk,
    // whatever its cardinality: that is the DML affected-row carrier (make_affected_rows_carrier),
    // which capture_remote_dml_count reads whole, and a zero-column window is the engine
    // pipeline's drain sentinel.
    //
    // A wider chunk becomes consecutive windows of DEFAULT_VECTOR_CAPACITY rows, the last holding
    // the rest. Each window is data_chunk_t::partial_copy of its row range: it references the
    // source column buffers (reference-counted, so they live as long as the last window) rather
    // than copying rows. The vector and each window's own bookkeeping are allocated on `resource`.
    std::pmr::vector<components::vector::data_chunk_t> split_to_capacity(std::pmr::memory_resource* resource,
                                                                         components::vector::data_chunk_t chunk);

} // namespace tsl
