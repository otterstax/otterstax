// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <otterbrix/otterbrix.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include "otterbrix/types.hpp"

#include <arrow/api.h>

#include <memory>

using namespace components::vector;
using namespace components;

namespace tsl {

    data_chunk_t arrow_to_chunk(std::pmr::memory_resource* res,
                                const std::shared_ptr<arrow::RecordBatch>& batch);

    data_chunk_t arrow_to_chunk(std::pmr::memory_resource* res,
                                const std::shared_ptr<arrow::Table>& table);

    types::complex_logical_type arrow_schema_to_struct(const std::shared_ptr<arrow::Schema>& schema);

} // namespace tsl
