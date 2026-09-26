// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "parquet_to_chunk.hpp"
#include "arrow_to_chunk.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>

namespace tsl {

using components::vector::data_chunk_t;

namespace {
    using file_reader_ptr = std::unique_ptr<parquet::arrow::FileReader>;

    core::result_wrapper_t<file_reader_ptr> open_parquet(std::pmr::memory_resource* res,
                                                         const std::shared_ptr<arrow::io::RandomAccessFile>& input) {
        auto reader_result = parquet::arrow::OpenFile(input, arrow::default_memory_pool());
        if (!reader_result.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure,
                               "parquet reader: cannot open parquet reader", reader_result.status());
        }
        return std::move(*reader_result);
    }

    core::result_wrapper_t<file_reader_ptr> open_parquet_file(std::pmr::memory_resource* res,
                                                              const std::string& file_path) {
        auto file_result = arrow::io::ReadableFile::Open(file_path);
        if (!file_result.ok()) {
            return arrow_error(res, core::error_code_t::io_error,
                               "parquet reader: cannot open file", file_result.status());
        }
        return open_parquet(res, *file_result);
    }

    core::result_wrapper_t<data_chunk_t> read_table(std::pmr::memory_resource* res,
                                                    parquet::arrow::FileReader& reader) {
        std::shared_ptr<arrow::Table> table;
        auto status = reader.ReadTable(&table);
        if (!status.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure,
                               "parquet_to_chunk: cannot read parquet table", status);
        }
        auto batch = table->CombineChunksToBatch();
        if (!batch.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure,
                               "parquet_to_chunk: table to batch failed", batch.status());
        }
        return arrow_to_chunk(res, *batch);
    }
} // namespace

core::result_wrapper_t<data_chunk_t> parquet_to_chunk(std::pmr::memory_resource* res, const std::string& file_path) {
    OTX_ZONE_N("tsl::parquet_to_chunk(file)");
    auto reader = open_parquet_file(res, file_path);
    if (reader.has_error()) {
        return reader.convert_error<data_chunk_t>();
    }
    return read_table(res, *reader.value());
}

core::result_wrapper_t<data_chunk_t>
parquet_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size) {
    OTX_ZONE_N("tsl::parquet_to_chunk(buffer)");
    if (!data || size == 0) {
        return make_error(res, core::error_code_t::invalid_parameter, "parquet_to_chunk: empty or null buffer");
    }

    auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(size));
    auto reader_io = std::make_shared<arrow::io::BufferReader>(buffer);

    auto reader = open_parquet(res, reader_io);
    if (reader.has_error()) {
        return reader.convert_error<data_chunk_t>();
    }
    return read_table(res, *reader.value());
}

core::result_wrapper_t<components::types::complex_logical_type> parquet_to_struct(std::pmr::memory_resource* res,
                                                                                  const std::string& file_path) {
    OTX_ZONE_N("tsl::parquet_to_struct");
    auto reader = open_parquet_file(res, file_path);
    if (reader.has_error()) {
        return reader.convert_error<components::types::complex_logical_type>();
    }

    std::shared_ptr<arrow::Schema> schema;
    auto status = reader.value()->GetSchema(&schema);
    if (!status.ok()) {
        return arrow_error(res, core::error_code_t::conversion_failure,
                           "parquet_to_struct: cannot read parquet schema", status);
    }

    return arrow_schema_to_struct(res, schema);
}

} // namespace tsl
