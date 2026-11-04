// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/casts/cast_registry.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/date/date_types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <span>
#include <string_view>

namespace otterstax::federation {

    // The rows a backend answered held to the columns otterbrix planned them with (a page of a scan,
    // the answer of a taken block, RETURNING rows): a column of another type is converted by
    // otterbrix's own explicit cast (`casts`, the default casts: an integer widened, a DECIMAL
    // rescaled to the declared width and scale — MySQL answers SUM over DECIMAL(10, 2) as
    // DECIMAL(32, 2) —, the decimal text of a wider DECIMAL / an unconstrained NUMERIC parsed, a
    // text BOOLEAN / DOUBLE read the engine's way), then named by the declared column. Another column
    // count is datatype_mismatch naming the statement (the remote table changed since it was
    // registered); a column with no cast, or a value the cast refuses, is the cast's error naming
    // the column and the statement.
    [[nodiscard]] core::error_t conform_to_result(std::pmr::memory_resource* resource,
                                                  const components::casts::cast_registry_t& casts,
                                                  std::string_view statement,
                                                  std::span<const components::types::complex_logical_type> declared,
                                                  core::date::timezone_offset_t timezone,
                                                  components::vector::data_chunk_t& chunk);

} // namespace otterstax::federation
