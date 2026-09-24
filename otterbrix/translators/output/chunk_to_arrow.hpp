// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <arrow/api.h>

#include <otterbrix/otterbrix.hpp>

#include <memory_resource>
#include <string_view>

// Arrow schema of a column list. A column without an alias maps to an Arrow field
// with an empty name: the schema carries exactly the information the column has,
// and nothing is dereferenced. A column whose physical type has no Arrow mapping
// (UINT128, BIT, UNKNOWN, and the other INT128 reading — UUID) is a
// conversion_failure naming the column; nothing here throws. HUGEINT is the one
// INT128 reading that maps: Arrow has no 128-bit integer type, so it becomes
// decimal128(38, 0), scale 0 keeping it an integer.
//
// A DECIMAL is decided by its LOGICAL type, ahead of the physical switch, and
// becomes decimal128(width, scale). Its physical type is only the width the
// precision needs (INT16 … INT128) and is shared with the plain integers, so
// dispatching on it would put the stored unscaled integer on the wire as a bare
// int and drop the scale — DECIMAL(18, 4) 1.2345 arriving as 12345. The engine's
// precision window (1 … 38) is exactly the one decimal128 declares, so every
// engine DECIMAL has a carrier and none needs decimal256.
// Error messages are owned by `res`; the shared_ptr in the result is Arrow's contract.
core::result_wrapper_t<std::shared_ptr<arrow::Schema>>
to_arrow_schema(std::pmr::memory_resource* res, const std::pmr::vector<components::types::complex_logical_type>& types);
// A non-STRUCT `struct_t` (the NA a statement without a result schema carries)
// is an empty schema.
core::result_wrapper_t<std::shared_ptr<arrow::Schema>>
to_arrow_schema(std::pmr::memory_resource* res, const components::types::complex_logical_type& struct_t);

// Flat scalar columns only (see writable_columns.hpp); every column must be named.
// Error messages are owned by `res`; the shared_ptr in the result is Arrow's contract.
core::result_wrapper_t<std::shared_ptr<arrow::RecordBatch>>
chunk_to_record_batch(std::pmr::memory_resource* res, const components::vector::data_chunk_t& chunk);

// The Decimal128 one DECIMAL or HUGEINT cell travels as: the unscaled integer the engine
// stored, read at the width that column keeps it in and widened to the 128 bits Arrow's only
// decimal carrier uses. The scale is not applied to the value — it rides in the Arrow type,
// which is what makes the pair exact.
//
// Two payloads are refused as conversion_failure, `scope` and `column_name` opening the
// message (built only when refusing, so a cell costs no string): a DECIMAL holding one of the
// engine's non-finite sentinels (±Infinity / NaN, the extremes of its storage width — see
// components::types::decimal_special), which decimal128 has no representation for at all; and
// a value the declared precision cannot hold, which Arrow's own ValidateFull calls Invalid and
// the parquet spec forbids writing, so a consumer is free to refuse it or read it as NULL.
// Every batch and file this layer builds is therefore valid under the schema it declares.
//
// Shared with the FlightSQL batch stream, which encodes the same two column types cell by cell
// against the prepared schema rather than through chunk_to_record_batch.
core::result_wrapper_t<arrow::Decimal128> to_arrow_decimal(std::pmr::memory_resource* res,
                                                           const components::types::logical_value_t& value,
                                                           std::string_view scope,
                                                           std::string_view column_name);
