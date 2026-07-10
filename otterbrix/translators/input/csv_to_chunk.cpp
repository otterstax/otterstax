// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "csv_to_chunk.hpp"
#include "arrow_to_chunk.hpp"

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/buffer.h>
#include <arrow/csv/api.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>

namespace tsl {

using components::vector::data_chunk_t;

namespace {
    data_chunk_t read_csv(std::pmr::memory_resource* res,
                          const std::shared_ptr<arrow::io::InputStream>& input,
                          char delimiter, bool has_header) {
        auto read_opts = arrow::csv::ReadOptions::Defaults();
        read_opts.use_threads = false;

        auto parse_opts = arrow::csv::ParseOptions::Defaults();
        parse_opts.delimiter = delimiter;

        auto convert_opts = arrow::csv::ConvertOptions::Defaults();

        if (!has_header) {
            read_opts.autogenerate_column_names = true;
        }

        auto reader_result = arrow::csv::TableReader::Make(
            arrow::io::IOContext(arrow::default_memory_pool()),
            input, read_opts, parse_opts, convert_opts);
        if (!reader_result.ok())
            throw std::runtime_error("CSV reader creation failed: " +
                                     reader_result.status().ToString());

        auto table_result = (*reader_result)->Read();
        if (!table_result.ok())
            throw std::runtime_error("CSV read failed: " + table_result.status().ToString());

        auto batches = (*table_result)->CombineChunksToBatch();
        if (!batches.ok())
            throw std::runtime_error("CSV combine batches failed: " + batches.status().ToString());

        return arrow_to_chunk(res, *batches);
    }
} // namespace

data_chunk_t csv_to_chunk(std::pmr::memory_resource* res, const std::string& file_path,
                           char delimiter, bool has_header) {
    auto file_result = arrow::io::ReadableFile::Open(file_path);
    if (!file_result.ok())
        throw std::runtime_error("Cannot open CSV file: " + file_result.status().ToString());
    return read_csv(res, *file_result, delimiter, has_header);
}

data_chunk_t csv_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size,
                           char delimiter, bool has_header) {
    auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(size));
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    return read_csv(res, input, delimiter, has_header);
}

} // namespace tsl
