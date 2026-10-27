// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_parquet.hpp"
#include "file_sink.hpp"
#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#undef DAY
#undef SECOND

#include <arrow/io/interfaces.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <memory>
#include <memory_resource>

namespace tsl {

core::result_wrapper_t<bool> chunk_to_parquet(std::pmr::memory_resource* res,
                                              const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                              const std::shared_ptr<arrow::io::OutputStream>& sink) {
    OTX_ZONE_N("tsl::chunk_to_parquet(stream)");
    if (!sink) {
        return make_error(res, core::error_code_t::invalid_parameter, "chunk_to_parquet: null sink");
    }
    // Row groups are sized for readers, not cut at the engine's 1024-row chunks (the buffered
    // writer fills one across batches up to max_row_group_length), and compressed with the codec
    // every parquet reader ships (parquet-cpp's own default is uncompressed).
    const auto properties = parquet::WriterProperties::Builder()
                                .compression(parquet::Compression::SNAPPY)
                                ->max_row_group_length(parquet_row_group_rows)
                                ->build();
    return write_batches(res, "chunk_to_parquet", chunks, [&](const std::shared_ptr<arrow::Schema>& schema) {
        return parquet::arrow::FileWriter::Open(*schema, arrow_pool(res), sink, properties);
    });
}

core::result_wrapper_t<bool> chunk_to_parquet(std::pmr::memory_resource* res,
                                              const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                              std::string_view path) {
    OTX_ZONE_N("tsl::chunk_to_parquet(file)");
    return write_local_file(res, "chunk_to_parquet", path, [&](const std::shared_ptr<arrow::io::OutputStream>& sink) {
        return chunk_to_parquet(res, chunks, sink);
    });
}

} // namespace tsl
