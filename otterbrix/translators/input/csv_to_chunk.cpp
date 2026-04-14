// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "csv_to_chunk.hpp"

#include <arrow/csv/api.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>

namespace tsl {

    namespace impl {

        arrow::csv::ReadOptions make_read_options(bool has_header) {
            auto opts = arrow::csv::ReadOptions::Defaults();
            opts.autogenerate_column_names = !has_header;
            return opts;
        }

        arrow::csv::ParseOptions make_parse_options(char delimiter) {
            auto opts = arrow::csv::ParseOptions::Defaults();
            opts.delimiter = delimiter;
            return opts;
        }

    } // namespace impl

    data_chunk_t csv_to_chunk(std::pmr::memory_resource* res,
                              const std::string& file_path,
                              char delimiter,
                              bool has_header) {
        auto maybe_infile = arrow::io::ReadableFile::Open(file_path);
        if (!maybe_infile.ok()) {
            throw std::runtime_error("csv_to_chunk: cannot open file: " + file_path +
                                     " (" + maybe_infile.status().ToString() + ")");
        }

        auto reader_result = arrow::csv::TableReader::Make(
            arrow::io::default_io_context(),
            *maybe_infile,
            impl::make_read_options(has_header),
            impl::make_parse_options(delimiter),
            arrow::csv::ConvertOptions::Defaults());

        if (!reader_result.ok()) {
            throw std::runtime_error("csv_to_chunk: TableReader::Make failed: " +
                                     reader_result.status().ToString());
        }

        auto table_result = (*reader_result)->Read();
        if (!table_result.ok()) {
            throw std::runtime_error("csv_to_chunk: Read failed: " + table_result.status().ToString());
        }

        return arrow_to_chunk(res, *table_result);
    }

    data_chunk_t csv_to_chunk(std::pmr::memory_resource* res,
                              const uint8_t* data, size_t size,
                              char delimiter,
                              bool has_header) {
        auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(size));
        auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);

        auto reader_result = arrow::csv::TableReader::Make(
            arrow::io::default_io_context(),
            buf_reader,
            impl::make_read_options(has_header),
            impl::make_parse_options(delimiter),
            arrow::csv::ConvertOptions::Defaults());

        if (!reader_result.ok()) {
            throw std::runtime_error("csv_to_chunk: TableReader::Make failed: " +
                                     reader_result.status().ToString());
        }

        auto table_result = (*reader_result)->Read();
        if (!table_result.ok()) {
            throw std::runtime_error("csv_to_chunk: Read failed: " + table_result.status().ToString());
        }

        return arrow_to_chunk(res, *table_result);
    }

} // namespace tsl
