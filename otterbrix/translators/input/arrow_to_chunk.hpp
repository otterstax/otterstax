// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include <core/result_wrapper.hpp>
#include <integration/cpp/otterbrix.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <memory_resource>

namespace tsl {

// Every loader's (and FlightSQL DoPut's) Arrow → chunk step: the batch as a run of chunks of at
// most DEFAULT_VECTOR_CAPACITY rows (the chunk contract), sharing one column shape; a batch without
// rows is one chunk of its columns and no row, so the schema survives an empty input (a null
// batch is an empty run). Each type otterbrix's importer reads goes through it zero-copy (every
// piece aliases the batch, kept alive by every column); decimal128 (HUGEINT at scale 0, else
// DECIMAL, out-of-precision refused) and null are read here per cell. Any other type is
// refused naming the column.
core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
arrow_to_chunk(std::pmr::memory_resource* res,
               const std::shared_ptr<arrow::RecordBatch>& batch);

// The schema-only answer discovery uses; the same readings, so a file whose column has no
// reading is refused before a table is created for it rather than at the first row.
core::result_wrapper_t<components::types::complex_logical_type>
arrow_schema_to_struct(std::pmr::memory_resource* res,
                       const std::shared_ptr<arrow::Schema>& schema);

} // namespace tsl
