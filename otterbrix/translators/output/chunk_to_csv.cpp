// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_csv.hpp"
#include "chunk_to_arrow.hpp"

#undef DAY
#undef SECOND

#include <arrow/csv/writer.h>
#include <arrow/io/file.h>

namespace tsl {

void chunk_to_csv(const components::vector::data_chunk_t& chunk, const std::string& path) {
    auto batch = chunk_to_record_batch(chunk);

    auto sink_result = arrow::io::FileOutputStream::Open(path);
    if (!sink_result.ok())
        throw std::runtime_error("chunk_to_csv: cannot open output: " +
                                 sink_result.status().ToString());

    auto write_opts = arrow::csv::WriteOptions::Defaults();
    auto status = arrow::csv::WriteCSV(*batch, write_opts, sink_result->get());
    if (!status.ok())
        throw std::runtime_error("chunk_to_csv: WriteCSV failed: " + status.ToString());

    if (!(*sink_result)->Close().ok())
        throw std::runtime_error("chunk_to_csv: Close failed");
}

} // namespace tsl
