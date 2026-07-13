// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_ndjson.hpp"

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

#include <fstream>
#include <memory_resource>
#include <string>
#include <vector>

namespace tsl {

namespace {

std::string escape_json(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += static_cast<char>(c); break;
        }
    }
    return out;
}

std::string value_to_json(const components::types::logical_value_t& val) {
    using components::types::physical_type;
    if (val.is_null()) return "null";
    switch (val.type().to_physical_type()) {
        case physical_type::BOOL:   return val.value<bool>() ? "true" : "false";
        case physical_type::INT8:   return std::to_string(val.value<int8_t>());
        case physical_type::INT16:  return std::to_string(val.value<int16_t>());
        case physical_type::INT32:  return std::to_string(val.value<int32_t>());
        case physical_type::INT64:  return std::to_string(val.value<int64_t>());
        case physical_type::UINT8:  return std::to_string(val.value<uint8_t>());
        case physical_type::UINT16: return std::to_string(val.value<uint16_t>());
        case physical_type::UINT32: return std::to_string(val.value<uint32_t>());
        case physical_type::UINT64: return std::to_string(val.value<uint64_t>());
        case physical_type::FLOAT:  return std::to_string(val.value<float>());
        case physical_type::DOUBLE: return std::to_string(val.value<double>());
        case physical_type::STRING:
            return "\"" + escape_json(val.value<const std::string&>()) + "\"";
        default: return "null";
    }
}

} // namespace

void chunk_to_ndjson(const components::vector::data_chunk_t& chunk, const std::string& path) {
    std::ofstream out(path);
    if (!out)
        throw std::runtime_error("chunk_to_json: cannot open output: " + path);

    const auto types = chunk.types();
    const size_t ncols = static_cast<size_t>(chunk.column_count());
    const size_t nrows = static_cast<size_t>(chunk.size());

    for (size_t r = 0; r < nrows; r++) {
        out << '{';
        for (size_t c = 0; c < ncols; c++) {
            if (c > 0) out << ',';
            out << '"' << types[c].alias() << '"' << ':';
            auto val = chunk.value(static_cast<uint64_t>(c), static_cast<uint64_t>(r));
            out << value_to_json(val);
        }
        out << "}\n";
    }

    if (!out.good())
        throw std::runtime_error("chunk_to_json: write error for: " + path);
}

void chunk_to_ndjson(const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                     const std::string& path) {
    std::ofstream out(path);
    if (!out)
        throw std::runtime_error("chunk_to_json: cannot open output: " + path);

    for (const auto& chunk : chunks) {
        const auto types = chunk.types();
        const size_t ncols = static_cast<size_t>(chunk.column_count());
        const size_t nrows = static_cast<size_t>(chunk.size());
        for (size_t r = 0; r < nrows; r++) {
            out << '{';
            for (size_t c = 0; c < ncols; c++) {
                if (c > 0) out << ',';
                out << '"' << types[c].alias() << '"' << ':';
                auto val = chunk.value(static_cast<uint64_t>(c), static_cast<uint64_t>(r));
                out << value_to_json(val);
            }
            out << "}\n";
        }
    }

    if (!out.good())
        throw std::runtime_error("chunk_to_json: write error for: " + path);
}

} // namespace tsl
