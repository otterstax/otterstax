// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "parquet_to_chunk.hpp"
#include "arrow_to_chunk.hpp"
#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/util/thread_pool.h>
#include <parquet/arrow/reader.h>
#include <parquet/properties.h>

namespace tsl {

using components::vector::data_chunk_t;
using chunk_run_t = std::pmr::vector<data_chunk_t>;

namespace {
    using file_reader_ptr = std::unique_ptr<parquet::arrow::FileReader>;

    core::result_wrapper_t<file_reader_ptr> open_parquet(std::pmr::memory_resource* res,
                                                         const std::shared_ptr<arrow::io::RandomAccessFile>& input,
                                                         const parquet::ArrowReaderProperties& properties) {
        parquet::arrow::FileReaderBuilder builder;
        if (auto opened = builder.Open(input); !opened.ok()) {
            return arrow_error(res, core::error_code_t::invalid_text_representation,
                               "parquet reader: cannot open parquet reader", opened);
        }
        auto reader_result = builder.memory_pool(arrow_pool(res))->properties(properties)->Build();
        if (!reader_result.ok()) {
            return arrow_error(res, core::error_code_t::invalid_text_representation,
                               "parquet reader: cannot open parquet reader", reader_result.status());
        }
        return std::move(*reader_result);
    }

    core::result_wrapper_t<std::shared_ptr<arrow::io::RandomAccessFile>> open_file(std::pmr::memory_resource* res,
                                                                                    const std::string& file_path) {
        auto file_result = arrow::io::ReadableFile::Open(file_path, arrow_pool(res));
        if (!file_result.ok()) {
            return arrow_error(res, core::error_code_t::io_error,
                               "parquet reader: cannot open file", file_result.status());
        }
        return std::shared_ptr<arrow::io::RandomAccessFile>{std::move(*file_result)};
    }

    // The reader is opened, read and destroyed here, before its read pool is stopped.
    core::result_wrapper_t<chunk_run_t> read_table(std::pmr::memory_resource* res,
                                                    const std::shared_ptr<arrow::io::RandomAccessFile>& input,
                                                    arrow::internal::Executor* read_pool) {
        parquet::ArrowReaderProperties properties;
        properties.set_io_context(arrow::io::IOContext(arrow_pool(res), read_pool));
        auto reader = open_parquet(res, input, properties);
        if (reader.has_error()) {
            return reader.convert_error<chunk_run_t>();
        }
        auto table = reader.value()->ReadTable();
        if (!table.ok()) {
            return arrow_error(res, core::error_code_t::invalid_text_representation,
                               "parquet_to_chunk: cannot read parquet table", table.status());
        }
        auto batch = (*table)->CombineChunksToBatch(arrow_pool(res));
        if (!batch.ok()) {
            return arrow_error(res, core::error_code_t::internal_error,
                               "parquet_to_chunk: table to batch failed", batch.status());
        }
        return arrow_to_chunk(res, *batch);
    }

    // The reader pre-buffers the column chunks it reads (ArrowReaderProperties::pre_buffer): each
    // read is a task of the executor its IOContext names, and a worker lets the task go — the
    // read's future, holding the buffer the read allocated on `res` — only after waking the reader.
    // On Arrow's process-wide IO pool that last free could reach `res` after the load returned,
    // when the caller's resource may be gone. The reads run on a pool of this load's own instead,
    // shut down before it returns: every task, with what it held, is released by then.
    core::result_wrapper_t<chunk_run_t> load(std::pmr::memory_resource* res,
                                              const std::shared_ptr<arrow::io::RandomAccessFile>& input) {
        auto read_pool = arrow::internal::ThreadPool::Make(arrow::io::GetIOThreadPoolCapacity());
        if (!read_pool.ok()) {
            return arrow_error(res, core::error_code_t::internal_error,
                               "parquet_to_chunk: cannot start the read pool", read_pool.status());
        }
        auto run = read_table(res, input, read_pool->get());
        if (auto stopped = (*read_pool)->Shutdown(); !stopped.ok()) {
            return arrow_error(res, core::error_code_t::internal_error,
                               "parquet_to_chunk: cannot stop the read pool", stopped);
        }
        return run;
    }
} // namespace

core::result_wrapper_t<chunk_run_t> parquet_to_chunk(std::pmr::memory_resource* res, const std::string& file_path) {
    OTX_ZONE_N("tsl::parquet_to_chunk(file)");
    auto file = open_file(res, file_path);
    if (file.has_error()) {
        return file.convert_error<chunk_run_t>();
    }
    return load(res, file.value());
}

core::result_wrapper_t<chunk_run_t>
parquet_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size) {
    OTX_ZONE_N("tsl::parquet_to_chunk(buffer)");
    if (!data || size == 0) {
        return make_error(res, core::error_code_t::invalid_parameter, "parquet_to_chunk: empty or null buffer");
    }

    auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(size));
    auto reader_io = std::make_shared<arrow::io::BufferReader>(buffer);
    return load(res, reader_io);
}

core::result_wrapper_t<chunk_run_t> parquet_to_chunk(std::pmr::memory_resource* res,
                                                      std::shared_ptr<arrow::io::RandomAccessFile> input) {
    OTX_ZONE_N("tsl::parquet_to_chunk(stream)");
    if (!input) {
        return make_error(res, core::error_code_t::invalid_parameter, "parquet_to_chunk: null input");
    }
    return load(res, input);
}

core::result_wrapper_t<components::types::complex_logical_type> parquet_to_struct(std::pmr::memory_resource* res,
                                                                                  const std::string& file_path) {
    OTX_ZONE_N("tsl::parquet_to_struct");
    auto file = open_file(res, file_path);
    if (file.has_error()) {
        return file.convert_error<components::types::complex_logical_type>();
    }
    // Only the footer is read, at the open, on this thread.
    auto reader = open_parquet(res, file.value(), parquet::default_arrow_reader_properties());
    if (reader.has_error()) {
        return reader.convert_error<components::types::complex_logical_type>();
    }

    std::shared_ptr<arrow::Schema> schema;
    auto status = reader.value()->GetSchema(&schema);
    if (!status.ok()) {
        return arrow_error(res, core::error_code_t::invalid_text_representation,
                           "parquet_to_struct: cannot read parquet schema", status);
    }

    return arrow_schema_to_struct(res, schema);
}

} // namespace tsl
