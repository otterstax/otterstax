// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "ndjson_to_chunk.hpp"
#include "arrow_to_chunk.hpp"

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/json/api.h>
#include <arrow/table.h>

namespace tsl {

using components::vector::data_chunk_t;

namespace {
    // If data starts with '[', unwrap JSON array to NDJSON (one object per line)
    std::vector<uint8_t> maybe_unwrap_array(const uint8_t* data, size_t size) {
        const char* p = reinterpret_cast<const char*>(data);
        size_t i = 0;
        while (i < size && std::isspace(static_cast<unsigned char>(p[i]))) i++;
        if (i < size && p[i] == '[') {
            // Simple unwrap: strip outer brackets, replace '},{' with '}\n{'
            std::string s(p + i + 1, size - i - 2);
            std::vector<uint8_t> out;
            out.reserve(s.size());
            bool in_str = false;
            int depth = 0;
            for (char c : s) {
                if (c == '"' && (out.empty() || out.back() != '\\')) in_str = !in_str;
                if (!in_str) {
                    if (c == '{') depth++;
                    if (c == '}') { depth--; if (depth == 0) { out.push_back('}'); out.push_back('\n'); continue; } }
                    if (c == ',' && depth == 0) continue;
                }
                out.push_back(static_cast<uint8_t>(c));
            }
            return out;
        }
        return {data, data + size};
    }

    data_chunk_t read_json(std::pmr::memory_resource* res,
                           const std::shared_ptr<arrow::io::InputStream>& input) {
        auto read_opts = arrow::json::ReadOptions::Defaults();
        read_opts.use_threads = false;

        auto parse_opts = arrow::json::ParseOptions::Defaults();
        parse_opts.newlines_in_values = false;

        auto reader_result = arrow::json::TableReader::Make(
            arrow::default_memory_pool(), input, read_opts, parse_opts);
        if (!reader_result.ok())
            throw std::runtime_error("JSON reader creation failed: " +
                                     reader_result.status().ToString());

        auto table_result = (*reader_result)->Read();
        if (!table_result.ok())
            throw std::runtime_error("JSON read failed: " + table_result.status().ToString());

        auto batches = (*table_result)->CombineChunksToBatch();
        if (!batches.ok())
            throw std::runtime_error("JSON combine batches failed: " + batches.status().ToString());

        return arrow_to_chunk(res, *batches);
    }
} // namespace

data_chunk_t ndjson_to_chunk(std::pmr::memory_resource* res, const std::string& file_path) {
    auto file_result = arrow::io::ReadableFile::Open(file_path);
    if (!file_result.ok())
        throw std::runtime_error("Cannot open JSON file: " + file_result.status().ToString());
    return read_json(res, *file_result);
}

data_chunk_t ndjson_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size) {
    auto unwrapped = maybe_unwrap_array(data, size);
    auto buffer = std::make_shared<arrow::Buffer>(unwrapped.data(),
                                                  static_cast<int64_t>(unwrapped.size()));
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    return read_json(res, input);
}

} // namespace tsl
