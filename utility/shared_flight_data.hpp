// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "cv_wrapper.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

struct flight_data {
    components::types::complex_logical_type schema;
    components::vector::data_chunk_t chunk;
    size_t parameter_count;

    explicit flight_data(std::pmr::memory_resource* resource)
        : schema()
        , chunk(resource, {}, 0)
        , parameter_count(0) {}

    flight_data(components::types::complex_logical_type schema,
                components::vector::data_chunk_t chunk,
                size_t parameter_count)
        : schema(std::move(schema))
        , chunk(std::move(chunk))
        , parameter_count(parameter_count) {}
};

using shared_flight_data = shared_data<flight_data>;