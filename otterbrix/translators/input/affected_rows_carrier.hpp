// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <memory_resource>

namespace tsl {

    // Carrier for a backend's affected-row count: a column-less chunk whose cardinality IS the count,
    // the same shape the engine gives a local DML result. capture_remote_dml_count reads it whole, so
    // it is never split into DEFAULT_VECTOR_CAPACITY windows.
    //
    // A chunk without columns owns no per-row buffers, so its capacity is only the bound that
    // set_cardinality() checks, not a buffer size. Constructing with capacity 0 keeps the
    // constructor's DEFAULT_VECTOR_CAPACITY invariant and allocates nothing per row; the bound is
    // then raised to the count the carrier has to report.
    inline components::vector::data_chunk_t make_affected_rows_carrier(std::pmr::memory_resource* resource,
                                                                       uint64_t affected_rows) {
        components::vector::data_chunk_t carrier(
            resource,
            std::pmr::vector<components::types::complex_logical_type>{resource},
            0);
        carrier.set_capacity(affected_rows);
        carrier.set_cardinality(affected_rows);
        return carrier;
    }

} // namespace tsl
