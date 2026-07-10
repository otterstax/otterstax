// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include <otterbrix/otterbrix.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <memory_resource>

namespace tsl {

components::vector::data_chunk_t
arrow_to_chunk(std::pmr::memory_resource* res,
               const std::shared_ptr<arrow::RecordBatch>& batch);

components::types::complex_logical_type
arrow_schema_to_struct(std::pmr::memory_resource* res,
                       const std::shared_ptr<arrow::Schema>& schema);

} // namespace tsl
