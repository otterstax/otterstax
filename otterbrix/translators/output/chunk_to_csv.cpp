// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_csv.hpp"
#include "file_sink.hpp"
#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#undef DAY
#undef SECOND

#include <arrow/csv/writer.h>
#include <arrow/io/interfaces.h>
// csv/writer.h only forward-declares the RecordBatchWriter it returns.
#include <arrow/ipc/writer.h>

#include <memory>
#include <memory_resource>

namespace tsl {

core::result_wrapper_t<bool> chunk_to_csv(std::pmr::memory_resource* res,
                                          const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                          const std::shared_ptr<arrow::io::OutputStream>& sink) {
    OTX_ZONE_N("tsl::chunk_to_csv(stream)");
    if (!sink) {
        return make_error(res, core::error_code_t::invalid_parameter, "chunk_to_csv: null sink");
    }
    auto options = arrow::csv::WriteOptions::Defaults();
    options.io_context = arrow::io::IOContext(arrow_pool(res));
    return write_batches(res, "chunk_to_csv", chunks, [&](const std::shared_ptr<arrow::Schema>& schema) {
        return arrow::csv::MakeCSVWriter(sink, schema, options);
    });
}

core::result_wrapper_t<bool> chunk_to_csv(std::pmr::memory_resource* res,
                                          const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                          const std::string& path) {
    OTX_ZONE_N("tsl::chunk_to_csv(file)");
    return write_local_file(res, "chunk_to_csv", path, [&](const std::shared_ptr<arrow::io::OutputStream>& sink) {
        return chunk_to_csv(res, chunks, sink);
    });
}

} // namespace tsl
