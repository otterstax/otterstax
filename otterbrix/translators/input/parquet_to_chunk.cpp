// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "parquet_to_chunk.hpp"

#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>

namespace tsl {

    namespace impl {

        std::shared_ptr<arrow::Table> read_parquet_file(const std::string& file_path) {
            auto maybe_infile = arrow::io::ReadableFile::Open(file_path);
            if (!maybe_infile.ok()) {
                throw std::runtime_error("parquet_to_chunk: cannot open file: " + file_path +
                                         " (" + maybe_infile.status().ToString() + ")");
            }
            auto infile = *maybe_infile;

            std::unique_ptr<parquet::arrow::FileReader> reader;
            auto st = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
            if (!st.ok()) {
                throw std::runtime_error("parquet_to_chunk: cannot read parquet: " + file_path +
                                         " (" + st.ToString() + ")");
            }

            std::shared_ptr<arrow::Table> table;
            st = reader->ReadTable(&table);
            if (!st.ok()) {
                throw std::runtime_error("parquet_to_chunk: ReadTable failed: " + st.ToString());
            }
            return table;
        }

        std::shared_ptr<arrow::Table> read_parquet_buffer(const uint8_t* data, size_t size) {
            auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(size));
            auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);

            std::unique_ptr<parquet::arrow::FileReader> reader;
            auto st = parquet::arrow::OpenFile(buf_reader, arrow::default_memory_pool(), &reader);
            if (!st.ok()) {
                throw std::runtime_error("parquet_to_chunk: cannot read parquet from buffer (" +
                                         st.ToString() + ")");
            }

            std::shared_ptr<arrow::Table> table;
            st = reader->ReadTable(&table);
            if (!st.ok()) {
                throw std::runtime_error("parquet_to_chunk: ReadTable from buffer failed: " +
                                         st.ToString());
            }
            return table;
        }

    } // namespace impl

    data_chunk_t parquet_to_chunk(std::pmr::memory_resource* res, const std::string& file_path) {
        auto table = impl::read_parquet_file(file_path);
        return arrow_to_chunk(res, table);
    }

    data_chunk_t parquet_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size) {
        auto table = impl::read_parquet_buffer(data, size);
        return arrow_to_chunk(res, table);
    }

    types::complex_logical_type parquet_to_struct(const std::string& file_path) {
        auto table = impl::read_parquet_file(file_path);
        return arrow_schema_to_struct(table->schema());
    }

    types::complex_logical_type parquet_to_struct(const uint8_t* data, size_t size) {
        auto table = impl::read_parquet_buffer(data, size);
        return arrow_schema_to_struct(table->schema());
    }

} // namespace tsl
