// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include <core/result_wrapper.hpp>
#include <otterbrix/otterbrix.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <memory_resource>

namespace tsl {

// The shim every file loader (parquet / csv / ndjson) ends in. Each Arrow type either has
// a reading of its own here or is refused as conversion_failure naming the column and its
// Arrow type: a column read under another type's layout is undefined behaviour, not a
// fallback, so no type is declared one thing and read as another.
//
// decimal128(p, 0) — the carrier chunk_to_arrow gives a HUGEINT, Arrow having no 128-bit
// integer type — reads back as HUGEINT, which is exact for every value a decimal128 can
// hold. decimal128(p, s) with a scale is the engine's own fixed-point DECIMAL(p, s), the
// symmetric half of the writer, which emits exactly that pair: the unscaled integer crosses
// unchanged and the scale rides in the type. The engine's window is the narrower one — the
// scale must be positive and at most the precision — so a decimal outside it is refused
// rather than read under a spec the engine would reinterpret, and so is a value the column's
// own precision cannot hold (below precision 19 the engine's storage is narrower than 128
// bits, so such a payload would be cut down to a different number).
core::result_wrapper_t<components::vector::data_chunk_t>
arrow_to_chunk(std::pmr::memory_resource* res,
               const std::shared_ptr<arrow::RecordBatch>& batch);

// The schema-only answer discovery uses; the same mapping, so a file whose column has no
// reading is refused before a table is created for it rather than at the first row.
core::result_wrapper_t<components::types::complex_logical_type>
arrow_schema_to_struct(std::pmr::memory_resource* res,
                       const std::shared_ptr<arrow::Schema>& schema);

} // namespace tsl
