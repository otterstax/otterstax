// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "ndjson_to_chunk.hpp"
#include "arrow_to_chunk.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/json/api.h>
#include <arrow/table.h>

#include <cctype>

namespace tsl {

using components::vector::data_chunk_t;

namespace {
    // If data starts with '[', unwrap JSON array to NDJSON (one object per line)
    std::pmr::vector<uint8_t> maybe_unwrap_array(std::pmr::memory_resource* res, const uint8_t* data, size_t size) {
        const char* p = reinterpret_cast<const char*>(data);
        size_t first = 0;
        while (first < size && std::isspace(static_cast<unsigned char>(p[first]))) first++;
        size_t last = size;
        while (last > first && std::isspace(static_cast<unsigned char>(p[last - 1]))) last--;
        if (first < last && p[first] == '[' && p[last - 1] == ']') {
            // Simple unwrap: strip outer brackets, replace '},{' with '}\n{'
            std::pmr::vector<uint8_t> out(res);
            out.reserve(last - first);
            bool in_str = false;
            int depth = 0;
            for (size_t i = first + 1; i + 1 < last; ++i) {
                const char c = p[i];
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
        return std::pmr::vector<uint8_t>(data, data + size, res);
    }

    core::result_wrapper_t<data_chunk_t> read_json(std::pmr::memory_resource* res,
                                                   const std::shared_ptr<arrow::io::InputStream>& input) {
        auto read_opts = arrow::json::ReadOptions::Defaults();
        read_opts.use_threads = false;

        auto parse_opts = arrow::json::ParseOptions::Defaults();
        parse_opts.newlines_in_values = false;

        auto reader_result = arrow::json::TableReader::Make(
            arrow::default_memory_pool(), input, read_opts, parse_opts);
        if (!reader_result.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure,
                               "ndjson_to_chunk: reader creation failed", reader_result.status());
        }

        auto table_result = (*reader_result)->Read();
        if (!table_result.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure,
                               "ndjson_to_chunk: read failed", table_result.status());
        }

        auto batch = (*table_result)->CombineChunksToBatch();
        if (!batch.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure,
                               "ndjson_to_chunk: combine batches failed", batch.status());
        }

        return arrow_to_chunk(res, *batch);
    }
} // namespace

core::result_wrapper_t<data_chunk_t> ndjson_to_chunk(std::pmr::memory_resource* res, const std::string& file_path) {
    OTX_ZONE_N("tsl::ndjson_to_chunk(file)");
    auto file_result = arrow::io::ReadableFile::Open(file_path);
    if (!file_result.ok()) {
        return arrow_error(res, core::error_code_t::io_error,
                           "ndjson_to_chunk: cannot open file", file_result.status());
    }
    return read_json(res, *file_result);
}

core::result_wrapper_t<data_chunk_t> ndjson_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size) {
    OTX_ZONE_N("tsl::ndjson_to_chunk(buffer)");
    auto unwrapped = maybe_unwrap_array(res, data, size);
    auto buffer = std::make_shared<arrow::Buffer>(unwrapped.data(),
                                                  static_cast<int64_t>(unwrapped.size()));
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    return read_json(res, input);
}

} // namespace tsl
