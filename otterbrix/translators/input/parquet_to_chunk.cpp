// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "parquet_to_chunk.hpp"
#include "arrow_to_chunk.hpp"

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
    data_chunk_t table_to_chunk(std::pmr::memory_resource* res,
                                const std::shared_ptr<arrow::Table>& table) {
        auto batches_result = table->CombineChunksToBatch();
        if (!batches_result.ok())
            throw std::runtime_error("Parquet table to batch failed: " +
                                     batches_result.status().ToString());
        return arrow_to_chunk(res, *batches_result);
    }
} // namespace

data_chunk_t parquet_to_chunk(std::pmr::memory_resource* res, const std::string& file_path) {
    auto file_result = arrow::io::ReadableFile::Open(file_path);
    if (!file_result.ok())
        throw std::runtime_error("Cannot open parquet file: " + file_result.status().ToString());

    auto reader_result = parquet::arrow::OpenFile(*file_result, arrow::default_memory_pool());
    if (!reader_result.ok())
        throw std::runtime_error("Cannot open parquet reader: " + reader_result.status().ToString());
    auto reader = std::move(reader_result).ValueOrDie();

    std::shared_ptr<arrow::Table> table;
    auto status = reader->ReadTable(&table);
    if (!status.ok())
        throw std::runtime_error("Cannot read parquet table: " + status.ToString());

    return table_to_chunk(res, table);
}

data_chunk_t parquet_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size) {
    if (!data || size == 0)
        throw std::runtime_error("parquet_to_chunk: empty or null buffer");

    auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(size));
    auto reader_io = std::make_shared<arrow::io::BufferReader>(buffer);

    auto reader_result = parquet::arrow::OpenFile(reader_io, arrow::default_memory_pool());
    if (!reader_result.ok())
        throw std::runtime_error("Cannot open parquet reader from buffer: " + reader_result.status().ToString());
    auto reader = std::move(reader_result).ValueOrDie();

    std::shared_ptr<arrow::Table> table;
    auto status = reader->ReadTable(&table);
    if (!status.ok())
        throw std::runtime_error("Cannot read parquet table from buffer: " + status.ToString());

    return table_to_chunk(res, table);
}

components::types::complex_logical_type parquet_to_struct(std::pmr::memory_resource* res,
                                                         const std::string& file_path) {
    auto file_result = arrow::io::ReadableFile::Open(file_path);
    if (!file_result.ok())
        throw std::runtime_error("Cannot open parquet file: " + file_result.status().ToString());

    auto reader_result = parquet::arrow::OpenFile(*file_result, arrow::default_memory_pool());
    if (!reader_result.ok())
        throw std::runtime_error("Cannot open parquet reader: " + reader_result.status().ToString());
    auto reader = std::move(reader_result).ValueOrDie();

    std::shared_ptr<arrow::Schema> schema;
    auto status = reader->GetSchema(&schema);
    if (!status.ok())
        throw std::runtime_error("Cannot read parquet schema: " + status.ToString());

    return arrow_schema_to_struct(res, schema);
}

} // namespace tsl
