// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <arrow/api.h>

#include <otterbrix/otterbrix.hpp>

std::shared_ptr<arrow::Schema> to_arrow_schema(const std::pmr::vector<components::types::complex_logical_type>& types);
std::shared_ptr<arrow::Schema> to_arrow_schema(const components::types::complex_logical_type& struct_t);

std::shared_ptr<arrow::RecordBatch> chunk_to_record_batch(const components::vector::data_chunk_t& chunk);