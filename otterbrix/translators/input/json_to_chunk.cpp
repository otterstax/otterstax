// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "json_to_chunk.hpp"

#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/json/api.h>

#include <fstream>
#include <sstream>

namespace tsl {

    namespace impl {

        // Detect if buffer starts with '[' (JSON array) and convert to NDJSON
        std::string maybe_convert_array_to_ndjson(const uint8_t* data, size_t size) {
            // Skip leading whitespace
            size_t i = 0;
            while (i < size && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r'))
                ++i;
            if (i < size && data[i] == '[') {
                // It's a JSON array — parse and re-emit as NDJSON
                // Simple approach: remove outer [] and split by },{ pattern
                std::string content(reinterpret_cast<const char*>(data), size);
                // Remove leading [ and trailing ]
                auto first_bracket = content.find('[');
                auto last_bracket = content.rfind(']');
                if (first_bracket != std::string::npos && last_bracket != std::string::npos) {
                    content = content.substr(first_bracket + 1, last_bracket - first_bracket - 1);
                }
                // Replace },{ with }\n{ to make NDJSON
                std::string ndjson;
                ndjson.reserve(content.size());
                int depth = 0;
                for (size_t j = 0; j < content.size(); ++j) {
                    char c = content[j];
                    if (c == '{')
                        ++depth;
                    if (c == '}')
                        --depth;
                    ndjson += c;
                    if (c == '}' && depth == 0) {
                        // Skip comma and whitespace between objects
                        size_t k = j + 1;
                        while (k < content.size() &&
                               (content[k] == ',' || content[k] == ' ' || content[k] == '\n' ||
                                content[k] == '\r' || content[k] == '\t'))
                            ++k;
                        if (k < content.size()) {
                            ndjson += '\n';
                        }
                        j = k - 1;
                    }
                }
                return ndjson;
            }
            return {};
        }

        std::shared_ptr<arrow::Table> read_json_from_input(std::shared_ptr<arrow::io::InputStream> input) {
            auto reader_result = arrow::json::TableReader::Make(
                arrow::default_memory_pool(),
                input,
                arrow::json::ReadOptions::Defaults(),
                arrow::json::ParseOptions::Defaults());
            if (!reader_result.ok()) {
                throw std::runtime_error("json_to_chunk: TableReader::Make failed: " +
                                         reader_result.status().ToString());
            }
            auto table_result = (*reader_result)->Read();
            if (!table_result.ok()) {
                throw std::runtime_error("json_to_chunk: Read failed: " + table_result.status().ToString());
            }
            return *table_result;
        }

    } // namespace impl

    data_chunk_t json_to_chunk(std::pmr::memory_resource* res, const std::string& file_path) {
        // Read file to check if it's array format
        std::ifstream ifs(file_path, std::ios::binary);
        if (!ifs.is_open()) {
            throw std::runtime_error("json_to_chunk: cannot open file: " + file_path);
        }
        std::ostringstream oss;
        oss << ifs.rdbuf();
        std::string content = oss.str();

        auto ndjson = impl::maybe_convert_array_to_ndjson(
            reinterpret_cast<const uint8_t*>(content.data()), content.size());

        if (!ndjson.empty()) {
            auto buffer = arrow::Buffer::FromString(std::move(ndjson));
            auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);
            auto table = impl::read_json_from_input(buf_reader);
            return arrow_to_chunk(res, table);
        }

        // It's already NDJSON — read directly
        auto buffer = arrow::Buffer::FromString(std::move(content));
        auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);
        auto table = impl::read_json_from_input(buf_reader);
        return arrow_to_chunk(res, table);
    }

    data_chunk_t json_to_chunk(std::pmr::memory_resource* res, const uint8_t* data, size_t size) {
        auto ndjson = impl::maybe_convert_array_to_ndjson(data, size);

        if (!ndjson.empty()) {
            auto buffer = arrow::Buffer::FromString(std::move(ndjson));
            auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);
            auto table = impl::read_json_from_input(buf_reader);
            return arrow_to_chunk(res, table);
        }

        auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(size));
        auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);
        auto table = impl::read_json_from_input(buf_reader);
        return arrow_to_chunk(res, table);
    }

} // namespace tsl
