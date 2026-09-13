// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_csv.hpp"
#include "chunk_to_arrow.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#undef DAY
#undef SECOND

#include <arrow/csv/writer.h>
#include <arrow/io/file.h>
#include <arrow/table.h>

#include <memory>
#include <memory_resource>
#include <vector>

namespace tsl {

core::result_wrapper_t<bool> chunk_to_csv(std::pmr::memory_resource* res,
                                          const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                          const std::string& path) {
    OTX_ZONE_N("tsl::chunk_to_csv");
    // Arrow's table API takes a std::vector of batches.
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(chunks.size());
    for (const auto& chunk : chunks) {
        auto batch = chunk_to_record_batch(res, chunk);
        if (batch.has_error()) {
            return batch.convert_error<bool>();
        }
        batches.push_back(std::move(batch.value()));
    }

    auto table_result = arrow::Table::FromRecordBatches(batches);
    if (!table_result.ok()) {
        return arrow_error(res, core::error_code_t::conversion_failure,
                           "chunk_to_csv: FromRecordBatches failed", table_result.status());
    }

    auto sink_result = arrow::io::FileOutputStream::Open(path);
    if (!sink_result.ok()) {
        return arrow_error(res, core::error_code_t::io_error,
                           "chunk_to_csv: cannot open output", sink_result.status());
    }

    auto write_opts = arrow::csv::WriteOptions::Defaults();
    auto status = arrow::csv::WriteCSV(**table_result, write_opts, sink_result->get());
    if (!status.ok()) {
        return arrow_error(res, core::error_code_t::io_error, "chunk_to_csv: WriteCSV failed", status);
    }

    if (auto closed = (*sink_result)->Close(); !closed.ok()) {
        return arrow_error(res, core::error_code_t::io_error, "chunk_to_csv: Close failed", closed);
    }
    return true;
}

} // namespace tsl
