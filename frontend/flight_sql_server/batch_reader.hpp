// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "arrow/api.h"
#include "arrow/flight/api.h"
#include "arrow/flight/sql/api.h"

#include "arrow/builder.h"
#include "arrow/record_batch.h"

#include <otterbrix/otterbrix.hpp>

#include <memory_resource>

class ChunkBatchReader : public arrow::RecordBatchReader {
public:
    ChunkBatchReader(std::shared_ptr<arrow::Schema> schema, components::vector::data_chunk_t chunk);

    static arrow::Result<std::shared_ptr<ChunkBatchReader>> Make(std::shared_ptr<arrow::Schema> schema,
                                                                 components::vector::data_chunk_t chunk);

    std::shared_ptr<arrow::Schema> schema() const override;

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

private:
    std::shared_ptr<arrow::Schema> schema_ptr_;
    components::vector::data_chunk_t chunk_;
    bool used{false};
};