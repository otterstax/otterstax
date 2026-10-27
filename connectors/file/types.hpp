// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>
#include <string_view>

namespace conn::file {

enum class FileFormat : uint8_t { NDJSON, CSV, Parquet, Unknown };

// Resolve the file format from an explicit format string, falling back to the
// file extension when the format is empty or unrecognised.
inline FileFormat resolve_format(std::string_view format, std::string_view path) {
    if      (format == "parquet")                     return FileFormat::Parquet;
    else if (format == "csv")                         return FileFormat::CSV;
    else if (format == "ndjson" || format == "jsonl") return FileFormat::NDJSON;

    // The extension as std::filesystem::path::extension() reads it — from the last '.' of the
    // file name, none for a dot file — without building a path.
    const auto slash = path.find_last_of('/');
    const auto name = slash == std::string_view::npos ? path : path.substr(slash + 1);
    const auto dot = name.find_last_of('.');
    const auto ext = (dot == std::string_view::npos || dot == 0 || name == "..") ? std::string_view{} : name.substr(dot);
    if      (ext == ".parquet")                   return FileFormat::Parquet;
    else if (ext == ".csv" || ext == ".tsv")      return FileFormat::CSV;
    else if (ext == ".ndjson" || ext == ".jsonl") return FileFormat::NDJSON;

    return FileFormat::Unknown;
}

} // namespace conn::file
