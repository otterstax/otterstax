// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_parquet.hpp"
#include "chunk_to_arrow.hpp"

#undef DAY
#undef SECOND

#include <arrow/io/file.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>

namespace tsl {

void chunk_to_parquet(const components::vector::data_chunk_t& chunk, const std::string& path) {
    auto batch = chunk_to_record_batch(chunk);

    auto table_result = arrow::Table::FromRecordBatches({batch});
    if (!table_result.ok())
        throw std::runtime_error("chunk_to_parquet: FromRecordBatches failed: " +
                                 table_result.status().ToString());

    auto sink_result = arrow::io::FileOutputStream::Open(path);
    if (!sink_result.ok())
        throw std::runtime_error("chunk_to_parquet: cannot open output: " +
                                 sink_result.status().ToString());

    auto status = parquet::arrow::WriteTable(
        **table_result, arrow::default_memory_pool(), *sink_result, /*chunk_size=*/1024);
    if (!status.ok())
        throw std::runtime_error("chunk_to_parquet: WriteTable failed: " + status.ToString());

    if (!(*sink_result)->Close().ok())
        throw std::runtime_error("chunk_to_parquet: Close failed");
}

} // namespace tsl
